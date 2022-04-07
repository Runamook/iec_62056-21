import iec6205621.client as client
import logging
import sys

logger = logging.getLogger('Test')
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

fh = logging.FileHandler(filename='test_log')
fh.setFormatter(fmt)
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
sh.setLevel(logging.DEBUG)
logger.addHandler(sh)
# logger.addHandler(fh)


class MyMeter(client.Meter):
    def log(self, severity, logstring):
        # logstring = client.normalize_log(logstring)
        logger.debug(logstring)

# Not really main, 
# Rather use list*.py or p*.py to query corresponding datastructures


if __name__ == '__main__':
    meter = {'id': 28, 'melo': None, 'description': 'test', 'manufacturer': 'EMH', 'installation_date': None, 'is_active': None, 'meter_id': '10201787', 'ip_address': '100.80.141.124', 'port': 8000, 'voltagefactor': 1, 'currentfactor': 600, 'org': None, 'p01': 900, 'p02': 0, 'list1': 10, 'list2': 0, 'list3': 0, 'list4': 0, 'p98': 0, 'last_run': 0}
    # m = client.Meter(address='100.80.141.128')
    m = MyMeter(**meter)
    # m = MyMeter(address='100.80.141.154')
    m.readList(list_number='1', use_meter_id=True)
    # m.readLoadProfile(profile_number='1', device_address='10201787')


