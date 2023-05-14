#!/usr/bin/env python3

import argparse
import os
import sys
import client
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

    if meter.get('data_id'):
        # Data_id is defined - user requested a particular data set 
        data_id = meter['data_id']
        
        print(f'{t} DEBUG data_id = {data_id} {meter}')

        if data_id == 'list4':
            m.readList(list_number=data_id)
        elif data_id == 'list2':
            m.readList(list_number=data_id)      
        elif data_id == 'list3':
            m.readList(list_number=data_id)                      
        elif data_id == 'list4':
            m.readList(list_number=data_id)            
        elif data_id == 'p01':
            m.readLoadProfile_new(profile_number='1')
        else:
            print(f'{t} WARN Unknown data_id = {data_id}')
            sys.exit(1)
    else:
        # Unknown data_id - go one by one
        if meter.get('cmd'):
            cmd = meter.get('cmd').encode()
        if meter.get('data'):
            data = meter.get('data').encode()
        m.send_to_meter(cmd, data)

myname = os.path.basename(__file__)

description = """
Sends a single command to the EMH LZQJ-XC meter and prints the decoded response
"""

examples = """
Examples:

    # Read the date from the meter
    python3 {0} socket://10.224.70.21:8000 --structure P01 --password 00000000
    python3 {0} socket://10.224.70.21:8000 --cmd R5 --data "0.9.2(0171021)(00000000)" --password 00000000

""".format(myname)
#    python3 {0} 10.224.70.21 --structure P01 --password 00000000
#    python3 {0} 10.224.70.21 --structure P01 --password 00000000

parser = argparse.ArgumentParser(
    description=description,
    epilog=examples,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument('--password', default=None, type=str,
                    help='Password for the meter (default: Not used)')
parser.add_argument('--id', default=None, type=str,
                    help='Meter id (default: None)')
#parser.add_argument('--port', default=8000, type=int, help='Meter port')
#parser.add_argument('host', help='Meter IP address')
parser.add_argument('--structure', default=None, type=str,
                    help='Read the structure - P01, list1, list2 etc.')
parser.add_argument('--data', default=None, type=str,
                    help='W5')
parser.add_argument('--cmd', default=None, type=str,
                    help='0.9.2(0171021)(00000000)')
parser.add_argument('socket', default='socket://10.224.70.21:8000', type=str, help='Meter socket')
# parser.add_argument('meterid', help='Meter id')
args = parser.parse_args()

'''
debug = args.debug
if args.command:
    cmd = args.command.encode()
if args.data:
    data = args.data.encode()
'''
# meter_name = args.meterid

m = {
    'id': 23, 
    'melo': None, 
    'description': 'Test meter with password', 
    'manufacturer': 'Metcom', 
    'installation_date': None, 
    'is_active': True, 
    'meter_id': None, 
    'use_id': False, 
    'ip_address': '10.224.70.21', 
    'port': 8000, 
    'org': 'Acteno', 
    'guid': None, 
    'source': None, 
    'password': '00000000', 
    'timezone': 'CET', 
    'p01_from': None, 
    'p01': 45, 
    'p02': 0, 
    'list1': 0, 
    'list2': 0, 
    'list3': 0, 
    'list4': 0, 
    'p98': 0,
    'data':None,
    'cmd':None,
    'data_id': None
    }
 
try:
    sock = args.socket.split(':')
    m['port'] = sock[2]
    m['ip_address'] = sock[1].strip('/')
except Exception as e:
    print(f'\nERROR: Unable to parse provided socket: "{args.socket}"\nPlease define it as "socket://IP:port"\n\n')
    sys.exit(1)

if args.password:
    m['password'] = args.password
else:
    m['password'] = None
if args.id:
    m['meter_id'] = args.id
    m['use_id'] = True
else:
    m['meter_id'] = None
    m['use_id'] = False
if args.structure:
    m['data_id'] = args.structure.lower()
if args.data:
    m['data'] = args.data
if args.cmd:
    m['cmd'] = args.cmd

if m['data_id'] == None and m['cmd'] == None:
    print(f'\nERROR, no query defined\n\nPlease use --structure or --data\n')
    sys.exit(1)

if __name__ == '__main__':
    main(m)
# python3 single.py --password 12345678 socket://10.224.70.69:8000 --command R5 --data "0.0.0()" --debug
# pw - 00000000/12345678

