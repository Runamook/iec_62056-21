# from curses.ascii import ACK
import serial
import time
import functools
import operator
from serial.serialutil import SerialException
import sys
import datetime
import pytz


# TODO: Each function shall return {data: data, error_code: 1, error_text: asd}, the `library user shall decide wheather to terminate the flow. In other words - lib shall return object



def normalize_log(logstring: bytes):
    """
    Substitutes special symbols with their ASCII representation
    :param logstring: bytes
    :return: b'\r' => b'<CR>', b'\n' => '<LF>' etc.
    """


    special_chars_list = [b'\x01', b'\x02', b'\x03', b'\x06', b'\x15', b'\x04', b'\n', b'\r']
    special_chars = {
        b'\x01': b'<SOH>',
        b'\x02': b'<STX>',
        b'\x03': b'<ETX>',
        b'\x06': b'<ACK>',
        b'\x15': b'<NAK>',
        b'\x04': b'<EOT>',
        b'\n': b'<LF>',
        b'\r': b'<CR>'
    }
    # Make the string pretty
    for sp_char in special_chars_list:
        if sp_char in logstring:
            logstring = logstring.replace(sp_char, special_chars[sp_char])
    return logstring


def normalize_log_str(logstring: str):
    """
    Substitutes special symbols with their ASCII representation
    :param logstring: str
    :return: b'\r' => b'<CR>', b'\n' => '<LF>' etc.
    """

    special_chars_list = ['\\x01', '\\x02', '\\x03', '\\x06', '\\x15', '\\x04', '\\n', '\\r']
    special_chars = {
        '\\x01': '<SOH>',
        '\\x02': '<STX>',
        '\\x03': '<ETX>',
        '\\x06': '<ACK>',
        '\\x15': '<NAK>',
        '\\x04': '<EOT>',
        '\\n': '<LF>',
        '\\r': '<CR>'
    }
    # Make the string pretty
    logstring = logstring.replace("b'","").replace("'","")
    for sp_char in special_chars_list:
        if sp_char in logstring:
            logstring = logstring.replace(sp_char, special_chars[sp_char])
    return logstring

class Meter:
    """
    Mode C
    """
    BAUD_RATES = {'0': '300',
                  '1': '600',
                  '2': '1200',
                  '3': '2400',
                  '4': '4800',
                  '5': '9600',
                  '6': '19200',
                  '7': 'reserved',
                  '8': 'reserved',
                  '9': 'reserved'
                  }

    SOH = b'\x01'
    STX = b'\x02'
    ETX = b'\x03'
    ACK = b'\x06'
    NAK = b'\x15'
    EOT = b'\x04'
    LF = b'\n'
    CRLF = b'\r\n'

    CTLBYTES = SOH + STX + ETX

    # Tr = 2.2      # Tr should be 2.2 seconds, but some meters respond longer
    Tr = 4
    Inactivity_timeout = 60  # 60 - 120s 62056-21 Annex A, note 1

    def __init__(self, timeout: int = 300,  **meter):
        self.result_obj = {'data': None, 'error_code': None, 'error_text': None }
        self.url = f'socket://{meter["ip_address"]}:{meter["port"]}'
        self.timeout = timeout
        self.data = None
        self.ser = None
        self.manufacturer = meter['manufacturer'].lower()
        self.meter_id = meter["meter_id"]
        self.use_meter_id = meter['use_id']
        self.password = meter.get('password') or None
        self.password_type = meter.get('password_type') or 'utility'
        tz = meter.get('timezone') or 'CET'
        self.timezone = pytz.timezone(tz)
        self.p01_from = meter.get('p01_from') or None
        self.p98_from = meter.get('p98_from') or None
        self._connect()

    def log(self, severity, logstring):
        print(f'{severity}, {logstring}')

    def _mod_result_obj(self, code: int, text: str, data=None):
        """
        Tries to modify result_obj
        """
        self.result_obj['error_text'] = text
        self.result_obj['error_code'] = code
        if data:
            if self.result_obj.get('data'):
                self.log('WARN', f'"result_obj.data" = {self.result_obj["data"]}, modifying to "{data}"')
                self.result_obj['data'] = data


    def _connect(self):
        try:
            self.ser = serial.serial_for_url(self.url,
                                             baudrate=300,
                                             bytesize=serial.SEVENBITS,
                                             parity=serial.PARITY_EVEN,
                                             timeout=self.timeout)
            self.log('DEBUG', f'Connected to {self.url}, timeout = {self.timeout}')
        except SerialException:
            self.log('WARN', 'Unable to establish TCP connection')
            self._mod_result_obj(1, 'Unable to establish TCP connection')
            sys.exit(0)

    def _request(self):
        """
        Send request message (6.3.1), return identification message (6.3.2) received from meter
        HHU: /?12345678!<CR><LF>
        Meter: /MCS5\@V0050710000051<CR><LF>
        """
        if self.use_meter_id:
            cmd = f'/?{self.meter_id}!\r\n'.encode()
        else:
            cmd = b'/?!\r\n'
        try:
            if self.manufacturer == 'metcom':
                id_message = self._sendcmd(cmd, etx=self.LF).decode()
            elif self.manufacturer == 'emh':
                id_message = self._sendcmd_and_decode_response(cmd)
            else:
                self.log('WARN', f'Unknown manufacturer {self.manufacturer}, working as EMH')
                id_message = self._sendcmd_and_decode_response(cmd)

        except Exception as e:
            self.log('ERROR', e)
            self._mod_result_obj(1, e)
            sys.exit(1)
        if 'B0' in id_message:
            self.log('WARN', 'Meter ended communication with B0')
            self._mod_result_obj(1, 'Meter ended communication with B0')
            sys.exit(1)
        self._parse_id_message(id_message)

    def _parse_id_message(self, id_message):
        if len(id_message) < 14:
            self.log('ERROR', f'Incorrect id_message "{str(id_message.encode())}"')
            self._mod_result_obj(1, f'Incorrect id_message "{str(id_message.encode())}"')
            sys.exit(1)
        try:
            quick_tr = False
            manufacturer = id_message[1:4]
            if manufacturer[-1].islower():
                # Device supports Tr timer 20ms
                quick_tr = True
            if self.BAUD_RATES.get(id_message[4]):
                baud_rate = self.BAUD_RATES[id_message[4]]
            else:
                baud_rate = 'Unknown'
            communication_id = id_message[7:-2]
            self.log('DEBUG', f'Manufacturer: {manufacturer}, 20ms Tr support: {quick_tr}, baud rate: {baud_rate}, communication ID: {communication_id}')
        except Exception as e:
            self.log('ERROR', f'ID {id_message} parsing error: "{e}"')
            return

    def _ackOptionSelect(self, data_readout_mode: bool = True):
        """
        Send ACK/optionSelect message (6.3.3)
        HHU: <ACK>051<CR><LF>
        """
        baud_rate = '5'
        if data_readout_mode:
            y = '0'
        else:
            # Programming mode
            y = '1'
        cmd = self.ACK + f'0{baud_rate}{y}\r\n'.encode()
        return self._sendcmd_and_decode_response(cmd)

    def _command(self, password, password_type: str = 'utility'):
        if password_type == 'utility':
            # Utility password
            cmd = b'P1'
        else:
            # P2 manufacturer password
            cmd = b'P2'
        data = f'({password})'.encode()
        for _ in range(3):
            # If NAK repeat twice
            response = self._sendcmd_and_decode_response(cmd,data)
            if response == self.ACK:
                return response
            time.sleep(0.5)
        return

    def _sendcmd(self, cmd, data=None, etx=ETX, check_bcc=True):
        """
        param: cmd = 'R5'
        param: data = '1.8.1()'
        """

        # Remember IEC 62056-21 timers:
        # (20 ms) 200 ms <= tr <= 1500 ms The time between the reception of a message and the transmission of an answer
        # 1500 ms < tt <= 2200 ms
        # ta <= 1500 ms       The time between two characters in a character sequence
        result = b""

        if data:
            # Commands with data:
            #       HHU: <SOH>R5<STX>1.8.1()<ETX><BCC>
            #           R5 - cmd, 1.8.1() - data
            # Add <SOH>, <STX>, <ETX>, <BCC> symbols

            cmdwithdata = cmd + Meter.STX + data + Meter.ETX
            cmdwithdata = Meter.SOH + cmdwithdata + Meter.bcc(cmdwithdata)
        else:
            # Commands with no data
            #       HHU: /?!<CR><LF>
            #       HHU: <ACK>051<CR><LF>
            # Send as-is
            cmdwithdata = cmd

        self.log('DEBUG', f'HHU -> Meter: {cmdwithdata}')
        self.ser.write(cmdwithdata)
        tic = time.time()

        try:
            while True:
                if self.ser.in_waiting > 0:
                    # If there is data to read - read it and reset the Tr timer
                    result += self.ser.read(self.ser.in_waiting)
                    tic = time.time()
                    continue
                elif self.ser.in_waiting == 0:

                    # If no more data to read:
                    if len(result) > 0 and (result[-2:-1] == etx or result[-1:] == etx):
                        # Check if the second-last read byte is End-of-Text (or similar)
                        if etx == Meter.ETX:
                            if check_bcc:
                                result = result[:-1]  # Remove BCC from result
                        # self.log('DEBUG', f'ETX found, received {result}')
                        self.log('DEBUG', f'Meter -> HHU: {result}')
                        return result
                    
                    if len(result) == 1 and result[-1:] == self.NAK:
                        self.log('DEBUG', f'Meter -> HHU: {result}')
                        return result

                    # If the last is read byte not ETX - wait for more data
                    if time.time() - tic > Meter.Tr:
                        # No more data for the Tr timer, assuming end of transmission
                        # self.log('DEBUG', f'Tr timeout reached ({self.Tr} seconds), received: "{result}"')
                        # self.log('DEBUG', f'result[-2:-1] = {result[-2:-1]}; result[-1:] = {result[-1:]}, etx = {etx}')
                        self.log('DEBUG', f'Meter -> HHU (Tr = {self.Tr}): "{result}"')
                        if len(result) < 1:
                            self.ser.close()
                            self.log('ERROR', 'No data received')
                            self._mod_result_obj(1, f'No data received')
                            sys.exit(1)
                        return result
        except Exception as e:
            self.log('ERROR', e)
            self._mod_result_obj(1, e)
            sys.exit(1)

    def _sendcmd_and_decode_response(self, cmd, data=None, etx=ETX, check_bcc=True):
        """
            Send data to the meter, decode the response and return the result
        """
        response = self._sendcmd(cmd, data, etx=etx, check_bcc=check_bcc)
        if response == Meter.NAK:
            # Try one retransmit
            self.log('WARN', f'{response} received, retransmitting')
            response = self._sendcmd(cmd, data, etx=etx, check_bcc=check_bcc)
            
        self.data = Meter.drop_ctl_bytes(Meter.remove_parity_bits(response)).decode("ascii")
        self.log('DEBUG', self.data)
        return self.data

    @staticmethod
    def drop_ctl_bytes(data):
        """Removes the standard delimiter bytes from the (response) data"""
        return bytes(filter(lambda b: b not in Meter.CTLBYTES, data))

    @staticmethod
    def remove_parity_bits(data):
        """Removes the parity bits from the (response) data"""
        return bytes(b & 0x7f for b in data)

    @staticmethod
    def bcc(data):
        """Computes the BCC (block check character) value"""
        return bytes([functools.reduce(operator.xor, data, 0)])

    def readList(self, list_number: str = '?'):
        """
        Reads a list/table # from the meter

        HHU: /?12345678!<CR><LF>
        Meter: /MCS5\@V0050710000051<CR><LF>
        HHU: <ACK>051<CR><LF>
        Meter: <STX>F.F(00000000)<CR><LF>
                1-0:0.0.0(10000051)<CR><LF>
                1-0:0.9.1(14:45:59)<CR><LF>
                1-0:0.2.2(12345678)<CR><LF>
                1-0:1.8.1(123.34kWh)<CR><LF>
                1-0:1.8.2(37.57kWh)<CR><LF>
                ....
        In reality EMH and sometimes MetCom works differently:
        HHU: /?12345678!<CR><LF>
        Meter: <STX>F.F(00000000)<CR><LF>
                1-0:0.0.0(10000051)<CR><LF>
                1-0:0.9.1(14:45:59)<CR><LF>
                1-0:0.2.2(12345678)<CR><LF>
                1-0:1.8.1(123.34kWh)<CR><LF>

        :param list_number: one of ['1', '2', '3', '4', 'list1', 'list2', 'list3', 'list4']
        :return: meter response


        """
        if list_number in ['1', 'list1']:
            list_number = '?'
        elif list_number in ['2', 'list2']:
            list_number = '2'
        elif list_number in ['3', 'list3']:
            list_number = '3'
        elif list_number in ['4', 'list4']:
            list_number = '4'
        else:
            self.log('ERROR', f'List {list_number} not implemented')
            self._mod_result_obj(1, f'List {list_number} not implemented')
            sys.exit(1)

        if self.use_meter_id:
            cmd = f'/{list_number}{self.meter_id}!\r\n'.encode()
        else:
            cmd = f'/{list_number}!\r\n'.encode()
        try:
            if self.manufacturer == 'metcom_new':
                id_message = self._sendcmd(cmd, etx=self.LF).decode()
                if 'B0' in id_message:
                    self._mod_result_obj(1, 'Meter ended communication')
                    self.log('WARN', 'Meter ended communication')
                    sys.exit(1)
                self._parse_id_message(id_message)
                result = self._ackOptionSelect()
                return result

            elif self.manufacturer == 'emh':
                return self._sendcmd_and_decode_response(cmd)
            elif self.manufacturer == 'metcom':
                return self._sendcmd_and_decode_response(cmd)
        except Exception as e:
            self.log('ERROR', e)
            self._mod_result_obj(1, e)
            sys.exit(1)

    def _readLog(self, log_type: str='P.98'):
        """
        Read logbook

        HHU:        /?!<CR><LF>
        Meter:      /MCS5\@V0050710000051<CR><LF>
        HHU:        <ACK>051<CR><LF>
        Meter:      <SOH>P0<STX>(00000001)<ETX><BCC>
        HHU:        <SOH>P1<STX>(00000000)<ETX><BCC>
        Meter:      <ACK>
        HHU:        <SOH>R5<STX>P.98(01808130001;01808191600)<ETX><BCC>
        Meter:      <STX>P.98(0180813152310)(00)()(2)(91.11.0)()(91.11.10)()(1)(0) <CR><LF>
                    P.98(0180813161205)(00)()(2)(91.11.0)()(91.11.10)()(2)(0) <CR><LF>
                    P.98(0180814110004)(00)()(2)(91.11.0)()(91.11.10)()(4)(0) <CR><LF>
                    P.98(0180814110500)(00)()(2)(91.11.0)()(91.11.10)()(5)(0) <CR><LF>
                    ... 
                    P.98(0180817170320)(00)()(2)(91.11.0)()(91.11.10)()(1)(0) <CR><LF>
                    P.98(0180817171407)(00)()(2)(91.11.0)()(91.11.10)()(2)(0) <CR><LF>
                    <ETX>BCC>
        HHU:        <SOH>B0<ETX><BCC>
        Meter:      <SOH>B0<ETX><BCC>
        """
        log_types = ['P.98', 'P.99', 'P.200', 'P.210', 'P.211']
        if log_type not in log_types:
            self.log('ERROR', f'Log {log_type} not implemented. Available types are {log_types}')
            self._mod_result_obj(1, f'Log {log_type} not implemented.')
            sys.exit(1)
        
        now = datetime.datetime.now(self.timezone)

        # Query log based on the DB value or the last 90 minutes
        if self.p98_from:
            from_ts = datetime.datetime.strptime(self.p98_from, '%Y-%m-%dT%H:%M:%S%z')
            self.log('DEBUG', f'Log {log_type} p98_from: "{self.p98_from}": {from_ts}')
            ninty_min_ago = now - datetime.timedelta(minutes=90)
            if from_ts > ninty_min_ago:
                before = ninty_min_ago
            else:
                before = from_ts
        else:
            before = now - datetime.timedelta(minutes=90)

        t_from = f"0{before.strftime('%y%m%d%H%M')}"

        # data = f'P.0{profile_number}({t_from};{t_to})'.encode()
        data = f'{log_type}({t_from};)'.encode()
        cmd = b'R5'

        return self.send_to_meter(in_cmd=cmd, in_data=data)

    def readP98(self):
        return self._readLog('P.98')

    def readP99(self):
        return self._readLog('P.99')

    def readP200(self):
        return self._readLog('P.200')

    def readP210(self):
        return self._readLog('P.210')

    def readP211(self):
        return self._readLog('P.211')

    def send_password(self):
        # -> Meter: <SOH>P1<STX>({password})<ETX><BCC>  
        # Meter ->: <ACK>
        
        if self.password_type == 'utility':
            # P1 - utility-password
            cmd = b'P1'
        elif self.password_type == 'manufacturer':
            # P2 - manufacturer-password
            cmd = b'P2'
        else:
            self.log('WARN', f'Password usage requested but the type "{self.password_type}" is unknown. Use "utility"/"manufacturer"')

        data = f'({self.password})'.encode()
        # <SOH>P1<STX>(00000000)<ETX><BCC>
        result = self._sendcmd_and_decode_response(cmd, data, etx=Meter.ACK, check_bcc=False)
        if result.encode() == self.NAK:
            # TODO: retransmit on <NAK>
            self.log('WARN', f'<NAK> received: {result}, terminating')
            sys.exit(1)

        if result == 'B0':
            self._mod_result_obj(1, f'{result} received, check password. Terminating')
            self.log('WARN', f'{result} received, check password. Terminating')
            sys.exit(1)

        if result.encode() != self.ACK:
            self._mod_result_obj(1, f'<ACK> expected, returned {result}, terminating')
            self.log('WARN', f'<ACK> expected, returned {result}, terminating')
            sys.exit(1)

        return

    def rw_register(self, cmd, data=None):
        """
        Unfinished, unused.
        Should be updated to arbitrary read (write) registers
        # TODO: Check and finish
        """
        if self.password:
            # -> Meter: <SOH>P1<STX>({password})<ETX><BCC>  
            # Meter ->: <ACK>
            self.send_password()
        self._sendcmd_and_decode_response(cmd, data)
        return

    def readLoadProfile(self, profile_number):
        """
        P.01 or others

        param: profile_number - str: 1, 2
        Creates cmd and data structures, then invokes send_to_meter(cmd, data)
        """

        # Grab the last 30 minutes

        # time from;to ([syymmddhhmm];[syymmddhhmm])
        # yy = year (00..99)
        # mm = month (1..12)
        # dd = day (1..31)
        # hh = hour (0..23)
        # mm = minute (0..59)
        # ss = second (0..59)
        # s = season flag (The season flag “s” will be ignored by the meter)

        ### How the Metcom meter works when the query is received

        # - Meter will roundup the request to the 15 min boarder (0, 15, 30, 45)
        # - Meter will add up 1 hour (but sometimes two) - probably a bug
        # - Meter will respond from that time till the end requested
        
        now = datetime.datetime.now(self.timezone)

        if self.p01_from:
            # P01 was queried before. Field format = "2022-04-12T21:59:15+03:00"
            from_ts = datetime.datetime.strptime(self.p01_from, '%Y-%m-%dT%H:%M:%S%z')
            self.log('DEBUG', f'P01_from: "{self.p01_from}": {from_ts}')
            ninty_min_ago = now - datetime.timedelta(minutes=90)
            if from_ts > ninty_min_ago:
                before = ninty_min_ago
            else:
                before = from_ts
        else:
            # P01 was not queried before
            before = now - datetime.timedelta(minutes=90)

        t_from = f"0{before.strftime('%y%m%d%H%M')}"

        # data = f'P.0{profile_number}({t_from};{t_to})'.encode()
        data = f'P.0{profile_number}({t_from};)'.encode()
        cmd = b'R5'

        return self.send_to_meter(in_cmd=cmd, in_data=data)

    def send_to_meter(self, in_cmd: bytes, in_data: bytes):
        """
        Send sequence of messages to the meter, depending on the instance attributes.
        Example final command: <SOH>W5<STX>0.9.2(0171021)(00000000)<ETX><BCC>
        cmd = 'W5'
        data = '0.9.2(0171021)(00000000)'

        """

        # HHU -> Meter: /?{meter_id}!<CR><LF>
        # Meter -> HHU: /MCS5\@V0050710000051<CR><LF>

        self._request()

        # HHU -> Meter: <ACK>051<CR><LF>
        # Meter -> HHU: <SOH>P0<STX>(00000001)<ETX><BCC>
        self._ackOptionSelect(data_readout_mode=False)

        if self.password:
            self.send_password()

        # HHU -> Meter: <SOH>R5<STX>P.01(01808130001;01808191600)<ETX><BCC>
        # Meter -> HHU: Data
        result = self._sendcmd_and_decode_response(cmd=in_cmd, data=in_data)
        if '(ERROR' in result:
            self._mod_result_obj(1, f'Meter responded with error: {result}')
            self.log('WARN', f'Meter responded with error: {result}')
            sys.exit(1)
        else:
            return result


"""<SOH>R5<STX>P.98(01808130001;01808191600)<ETX><BCC>
Example: readout log file
    Identifier = P.98 (Log File)
    Time window = from 13.08.2018, 00:01.-19.08.2018, 16:00

o Power outage at: 13.08.2018, 15:23:10
o Power outage up: 13.08.2018, 16:12:05
o Time change at 14.08.2018, 11:00:04 to 14.08.2018, 11:05:00
o Power outage at: 17.08.2018, 17:03:20
o Power outage up: 17.08.2018, 17:14:07

HHU:        /?!<CR><LF>                                                                 Request 
Meter:      /MCS5\@V0050710000051<CR><LF>                                               Identification
HHU:        <ACK>051<CR><LF>                                                            Acknowledgement/Option select (051 - programming mode)

===== Programming mode commands =====
Meter:      <SOH>P0<STX>(00000001)<ETX><BCC>                                            00000001 - serial number       
HHU:        <SOH>P1<STX>(00000000)<ETX><BCC>                                            00000000 - password, but can send command directly (skip this line)
Meter:      <ACK>                                                                       Acknowledgement
HHU:        <SOH>R5<STX>P.98(01808130001;01808191600)<ETX><BCC>                         Command (readout logs)
Meter:      <STX>P.98(0180813152310)(00)()(2)(91.11.0)()(91.11.10)()(1)(0) <CR><LF>
            P.98(0180813161205)(00)()(2)(91.11.0)()(91.11.10)()(2)(0) <CR><LF>
            P.98(0180814110004)(00)()(2)(91.11.0)()(91.11.10)()(4)(0) <CR><LF>
            P.98(0180814110500)(00)()(2)(91.11.0)()(91.11.10)()(5)(0) <CR><LF>
            ... 
            P.98(0180817170320)(00)()(2)(91.11.0)()(91.11.10)()(1)(0) <CR><LF>
            P.98(0180817171407)(00)()(2)(91.11.0)()(91.11.10)()(2)(0) <CR><LF>
            <ETX>BCC>
HHU:        <SOH>B0<ETX><BCC>

Meter → HHU:
    <SOH>B0<ETX><BCC>






Programming Mode: Command Message
After the “Option Select Message”, which switches the meter to the “Programming Mode”, the meter responds with the “Programming Command Message”

    Meter → HHU:
    <SOH>P0<STX>(nnnnnnnn)<ETX><BCC>

with nn... = Serial number of the meter.
After the meter has sent his serial number, the HHU is asked for sending the password of the meter.

    HHU → Meter:
    <SOH>P1<STX>(pppppppp)<ETX><BCC>

with pp... = utility password of the meter (8 BCD characters) or

    HHU → Meter:
    <SOH>P2<STX>(pppppppp)<ETX><BCC>

with pp... = Manufacturer-Password of the meter (8 BCD characters)

An other possibility is, that the HHU sends directly the R5-, W5-, R6-commands to the meter after the meter has received a correct command, it responds with a “Acknowledgement Message”:

    Meter → HHU:
    <ACK>

In the next step the HHU is sending the next “Programming Command Message”, or (after a “Repeat-request Message”, <NAK>) the previous message will be repeated.
The communication ends with the ”Programming Command Message”

    Meter → HHU:
    <SOH>B0<ETX><BCC>

"""
