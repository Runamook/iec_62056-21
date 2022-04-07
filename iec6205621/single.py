#!/usr/bin/env python3

import serial
import argparse
import time
import functools
import operator
import os


def bcc(data):
    """Computes the BCC (block  check character) value"""
    return bytes([functools.reduce(operator.xor, data, 0)])

def remove_parity_bits(data):
    """Removes the parity bits from the (response) data"""
    return bytes(b & 0x7f for b in data)


debug = False


def debuglog(*args):
    if debug:
        print("DEBUG:", *args)


SOH = b'\x01'
STX = b'\x02'
ETX = b'\x03'
ACK = b'\x06'
LF = b'\n'

CTLBYTES = SOH + STX + ETX


def drop_ctl_bytes(data):
    """Removes the standard delimiter bytes from the (response) data"""
    return bytes(filter(lambda b: b not in CTLBYTES, data))


"""

HHU:    /?!<CR><LF>                         # Request message
Meter:  /MCS5\@V0050710000051<CR><LF>       # Identification message
HHU:    <ACK>051<CR><LF>                    # Acknowledgement/Option Select message (programing mode)
Meter:  <SOH>P0<STX>(00000001)<ETX><BCC>    # Programing command message (00000001 - meter serial #)
HHU:    <SOH>P1<STX>(00000000)<ETX><BCC>    # Programing command message (00000000 - meter BCD password)
Meter:  <ACK>                               # Acknowledge message
HHU:    <SOH>W1<STX>S0G(01FF)<ETX><BCC>     # Set output
Meter:  <ACK>                               # Acknowledge message
HHU:    <SOH>B0<ETX><BCC>                   # Programing command message

"""

class Meter:
    # def __init__(self, port, timeout, meter_name):
    def __init__(self, port, timeout, readout_list=False, manufacturer='EMH'):
        self.port = port
        self.timeout = timeout
        self.manufacturer = manufacturer
        if readout_list == '?':
            readout_list = '1'
        self.readout_list = readout_list
        # self.meter_name = meter_name.encode()

    def __enter__(self):
        debuglog("Opening connection")
        self.ser = serial.serial_for_url(self.port,
                                         baudrate=300,
                                         bytesize=serial.SEVENBITS,
                                         parity=serial.PARITY_EVEN,
                                         timeout=self.timeout)
        time.sleep(3)
        # self.id = self.sendcmd(b'/?' + self.meter_name + b'!\r\n', etx=LF)
        if not self.readout_list:
            self.id = self.sendcmd(b'/?!\r\n', etx=LF)
        else:
            self.id = self.sendcmd(b'/' + self.readout_list.encode() + b'!\r\n', etx=LF)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        debuglog("Closing connection")
        self.ser.close()

    def sendcmd(self, cmd, data=None, etx=ETX):
        if data:
            cmdwithdata = cmd + STX + data + ETX
            cmdwithdata = SOH + cmdwithdata + bcc(cmdwithdata)
        else:
            cmdwithdata = cmd
        while True:
            debuglog("Sending {}".format(cmdwithdata))
            self.ser.write(cmdwithdata)
            r = self.ser.read_until(etx)
            debuglog("Received {} bytes: {}".format(len(r), r))
            if len(etx) > 0 and r[-1:] == etx:
                if etx == ETX:
                    bcbyte = self.ser.read(1)
                    debuglog("Read BCC: {}".format(bcbyte))
                return r
            debuglog("Retrying...")
            time.sleep(2)

    def sendcmd_and_decode_response(self, cmd, data=None):
        response = self.sendcmd(cmd, data)
        debuglog('-' * 40)
        debuglog('Cmd:', cmd)
        if data:
            debuglog('Data:', data)
        debuglog('Response:', response)
        decoded_response = drop_ctl_bytes(remove_parity_bits(response)).decode()
        debuglog('Decoded response:', decoded_response)
        debuglog('-' * 40)
        return decoded_response


myname = os.path.basename(__file__)

description = """
Sends a single command to the EMH LZQJ-XC meter and prints the decoded response
"""

examples = """
Examples:

    # Read the date from the meter
    python3 {0} socket://10.124.2.120:8000 R5 "0.9.2()"

    # Read the clock from the meter
    python3 {0} socket://10.124.2.120:8000 R5 "0.9.1()"

    # Read the clock from the meter and see how it works
    python3 {0} --debug socket://10.124.2.120:8000 R5 "0.9.1()"

    # Read the load profile since 2019-04-30 00:00
    python3 {0} socket://10.124.2.34:8000 R5 "P.01(11904300000;)"
    
    # Set output
    python3 {0} --password 00000000 socket://10.124.2.34:8000 W1 "S0G(0300)"

""".format(myname)

parser = argparse.ArgumentParser(
    description=description,
    epilog=examples,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument('--debug', action='store_true',
                    help='Enable debugging output')
parser.add_argument('--timeout', default=None, type=float,
                    help='Data readout timeout value in seconds (default: disabled)')
parser.add_argument('--password', default=None, type=str,
                    help='Password for the meter (default: Not used)')
parser.add_argument('--list', default=False, type=str,
                    help='List number for data readout mode, can be \'1\', \'2\' or \'3\'')
parser.add_argument('--command', default=None, type=str,
                    help='Command to send to the meter')
parser.add_argument('--data', default=None, type=str,
                    help='Command data to send to the meter')
parser.add_argument('device', help='Meter address in socket://host:port format')
# parser.add_argument('meterid', help='Meter id')
args = parser.parse_args()

debug = args.debug
if args.command:
    cmd = args.command.encode()
if args.data:
    data = args.data.encode()
# meter_name = args.meterid


# with Meter(args.device, args.timeout, meter_name) as meter:
if args.list:
    with Meter(args.device, args.timeout, args.list) as meter:
        print(meter.sendcmd_and_decode_response(ACK + b'051\r\n'))
else:
    with Meter(args.device, args.timeout) as meter:
        # meter.sendcmd_and_decode_response(ACK + b'041\r\n')
        meter.sendcmd_and_decode_response(ACK + b'051\r\n')
        meter.ser.baudrate = 4800
        if args.password:
            password = args.password.encode()
            meter.sendcmd_and_decode_response('P1'.encode(), password)
        print(meter.sendcmd_and_decode_response(cmd, data))


# python3 single.py --password 12345678 socket://10.224.70.69:8000 --command R5 --data "0.0.0()" --debug
# pw - 00000000/12345678        