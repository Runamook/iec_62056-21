#!/usr/bin/env python3

import argparse
import os
import sys
import iec6205621.client as client
import datetime



class MyMeter(client.Meter):
    def __init__(self, timeout: int = 300, **meter):
        super().__init__(timeout=timeout, **meter)

    def log(self, severity, log_string):
        """
        Reassign log method to write into file
        Try normalizing byte strings
        """
        t = datetime.datetime.now().strftime('%d-%m-%y %H:%M:%S')
        try:
            if isinstance(log_string, bytes):
                logstring = client.normalize_log(log_string)
            else:
                logstring = client.normalize_log_str(log_string)

        except Exception as e:
            print(f"{t} ERROR Normalization problem: {e}, str = [{log_string}], type = {type(log_string)}")
            logstring = log_string

        # logstring = log_string
        #self.logger.debug(f'Received for normalization: [{logstring}] type = {type(logstring)}')

        if severity == 'ERROR':
            print(f'{t} ERROR {self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'WARN':
            print(f'{t} WARN {self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'INFO':
            print(f'{t} INFO {self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'DEBUG':
            print(f'{t} DEBUG {self.meter_id} {self.url[9:]} {logstring}')


def main(meter):
    """
    Query the meter
    """
    t = datetime.datetime.now().strftime('%d-%m-%y %H:%M:%S')
    try:
        m = MyMeter(timeout=10, **meter)
    except Exception as e:
        print(f"{t} '{e}'")
        sys.exit(1)

    data_id = meter['data_id']
    
    print(f'{t} DEBUG data_id = {data_id} {meter}')
    if data_id == 'list4':
        m.readList(list_number=data_id, use_meter_id=True)
    elif data_id == 'p01':
        m.readLoadProfile(profile_number='1', use_meter_id=True)
    else:
        print(f'{t} WARN Unknown data_id = {data_id}')
        sys.exit(1)

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


"""{
    'id': 1, 
    'melo': 'some_uuid', 
    'description': 'Some description', 
    'manufacturer': 'Metcom', 
    'installation_date': None, 
    'is_active': True, 
    'meter_id': 'some id', 
    'ip_address': '192.168.135.5', 
    'port': 8000, 
    'voltagefactor': 1100, 
    'currentfactor': 200, 
    'org': 'Baasem', 
    'guid': None, 
    'source': None, 
    'password': None, 
    'use_password': False, 
    'timezone': 'CET', 
    'p01': 900, 'p02': 0, 'list1': 0, 'list2': 0, 'list3': 0, 'list4': 0, 'p98': 0, 
    'last_run': 0
    }
    """