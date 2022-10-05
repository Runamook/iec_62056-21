import json
import os
import unittest
from time import sleep
from iec6205621 import client


class IECTest(unittest.TestCase):

    meter_in = {
            'id': 9, 
            'melo': 'DE0073323316500000000000000609942', 
            'description': 'ACT-00001-0001-04 II H08', 
            'manufacturer': 'EMH', 
            'installation_date': None, 
            'is_active': True, 
            'meter_id': '08354050', 
            #'ip_address': '192.168.107.52', 
            'ip_address': '127.0.0.1', 
            'port': 8000,
            'use_id': True, 
            'voltagefactor': 300, 
            'currentfactor': 100, 
            'org': 'Acteno', 
            'guid': None, 
            'source': None, 
            'password': None, 
            'use_password': False, 
            'p01': 900, 
            'p02': 0, 
            'list1': 0, 
            'list2': 0, 
            'list3': 0, 
            'list4': 30, 
            'p98': 0, 
            'last_run': 0
            }
    def test_meter(self):
        m = client.Meter(**IECTest.meter_in)
        self.assertIsNotNone(m, 'Unable to initilize Meter object')

    def test__request(self):
        m = client.Meter(**IECTest.meter_in)
        m._request()
        print(m.data)
        self.assertIn('/MCS5\\@0050010067967\r\n', m.data, 'No data received')
    def test_readLoadProfile(self):
        m = client.Meter(**IECTest.meter_in)
        m.readLoadProfile(1)
        print(m.data)
        self.assertIn('P.01', m.data, 'No data received')
        
        #self.assertIsInstance(test_response, dict, '\nAPI response is not a dict')
        #self.assertIn('dt', test_response, '\nNo "dt" section in API response')

if __name__ == '__main__':
    unittest.main()