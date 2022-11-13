import sys
import unittest
from time import sleep
from iec6205621 import parser
import logging

Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(stream=sys.stdout,
                    format=Log_Format,
                    level=logging.DEBUG)
logger = logging.getLogger()

class IECTest(unittest.TestCase):

    meter_emh = {
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


    meter_metcom = {
            'id': 9, 
            'melo': 'DE0073323316500000000000000609942', 
            'description': 'ACT-00001-0001-04 II H08', 
            'manufacturer': 'metcom', 
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
    
    # EMH
    P01_data_1 = 'P.01(1220823161500)(00000000)(15)(6)(1.5)(kW)(2.5)(kW)(5.5)(kvar)(6.5)(kvar)(7.5)(kvar)(8.5)(kvar)\r\n(0.18374)(0.00078)(0.00000)(0.00006)(0.00087)(0.02431)\r\n(0.16832)(0.00000)(0.00000)(0.00000)(0.00000)(0.02686)'
    # Metcom
    P01_data_2 = 'P.01(1220823150000)(08)(15)(6)(1-0:1.5.0)(kW)(1-0:2.5.0)(kW)(1-0:5.5.0)(kvar)(1-0:6.5.0)(kvar)(1-0:7.5.0)(kvar)(1-0:8.5.0)(kvar)\r\n(0.00000)(0.12683)(0.00000)(0.11411)(0.00000)(0.00000)\r\n(0.00028)(0.06188)(0.00059)(0.07409)(0.00000)(0.00000)\r\n(0.00000)(0.11250)(0.00000)(0.11677)(0.00000)(0.00000)\r\n(0.00006)(0.04656)(0.00082)(0.09270)(0.00000)(0.00000)\r\n'
    # EMH 2
    P01_data_3 = 'P.01(1221005001500)(00000000)(15)(8)(1-1:1.29)(kWh)(1-1:2.29)(kWh)(1-1:5.29)(kvarh)(1-1:6.29)(kvarh)(1-1:7.29)(kvarh)(1-1:8.29)(kvarh)(1-2:1.29)(kWh)(1-3:2.29)(kWh)\r\n(0.00000)(0.04088)(0.00000)(0.00358)(0.00000)(0.00000)(0.00000)(0.00000)\r\n(0.00000)(0.03964)(0.00000)(0.00350)(0.00000)(0.00000)(0.00000)(0.00000)\r\n' 

    # EMH? P.98
    P98_data_1 = '/EMH5\@\201LZQJL0013F\r\n0.0.0(23456321)\r\nP.98(1041007095703)(00002000)()(0)\r\nP.98(1041007095703)(00004000)()(0)\r\nP.98(1041007095807)(00000100)()(0)\r\nP.98(1041007095914)(00000080)()(0)\r\n'

    # Metcom P.98
    P98_data_2 = 'P.98(1220906234904)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(4)(0)\r\nP.98(1220906234849)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(5)(0)\r\nP.98(1220906234850)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(4)(0)\r\nP.98(1220906234907)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(5)(0)\r\nP.98(1220919161837)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(17)(1)\r\nP.98(1220919162143)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(18)(1)\r\n'

    # EMG 2 P.98
    P98_data_3 = 'P.98(1220826235646)(00008020)()(2)(0.9.1)()(0.9.2)()(1235703)(1220826)\r\nP.98(1220828235723)(00008020)()(2)(0.9.1)()(0.9.2)()(1235710)(1220828)\r\nP.98(1220829235716)(00008020)()(2)(0.9.1)()(0.9.2)()(1235706)(1220829)\r\nP.98(1220901000000)(00000010)()(0)\r\nP.98(1220906115553)(00000080)()(0)\r\nP.98(1220906120814)(00000040)()(0)'

    # EMH F.F
    FF_data = 'F.F(00000000)\r\n'

    # Metcom table1
    Table1_data = 'F.F(00000000)\r\n0.0.0(10067967)\r\n0.0.1(10067967)\r\n0.9.1(202405)\r\n0.9.2(221113)\r\n0.1.0(12)\r\n0.1.2(2211010000)\r\n0.1.2*12(2211010000)\r\n0.1.2*11(2210010000)\r\n0.1.2*10(2209010000)\r\n1.6.1(0.50262*kW)(2211120730)\r\n1.6.1*12(0.39912*kW)(2210130900)\r\n1.6.1*11(0.74906*kW)(2209281400)\r\n1.6.1*10(0.49578*kW)(2208111330)\r\n2.6.1(0.00000*kW)(2211010000)\r\n2.6.1*12(0.00000*kW)(2210010000)\r\n2.6.1*11(0.00000*kW)(2209010000)\r\n2.6.1*10(0.00000*kW)(2208010000)\r\n1.8.0(01281.6601*kWh)\r\n'

    def test_parseP01_1(self):
        p = parser.Parser(raw_data=IECTest.P01_data_1, data_type='P.01', logger=logger, **IECTest.meter_emh)
        p._parseP01()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 12, 'Parse EMH P01 failed')

    def test_parseP01_2(self):
        p = parser.Parser(raw_data=IECTest.P01_data_2, data_type='P.01', logger=logger, **IECTest.meter_emh)
        p._parseP01()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 24, 'Parse Metcom P01 failed')

    def test_parseP01_3(self):
        p = parser.Parser(raw_data=IECTest.P01_data_3, data_type='P.01', logger=logger, **IECTest.meter_emh)
        p._parseP01()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 16, 'Parse EMH P01 set 2 failed')

    def test_parseP98_1(self):
        p = parser.Parser(raw_data=IECTest.P98_data_1, data_type='P.98', logger=logger, **IECTest.meter_emh)
        p._parseP98()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 4, 'Parse EMH P98 set 1 failed')

    def test_parseP98_2(self):
        p = parser.Parser(raw_data=IECTest.P98_data_2, data_type='P.98', logger=logger, **IECTest.meter_metcom)
        p._parseP98()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 12, 'Parse Metcom P98 set 2 failed')

    def test_parseP98_3(self):
        p = parser.Parser(raw_data=IECTest.P98_data_3, data_type='P.98', logger=logger, **IECTest.meter_emh)
        #p.log('DEBUG', p.unparsed_data)
        p._parseP98()
        #p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 6, 'Parse EMH P98 set 2 failed')

    def test_parseEmhError(self):
        p = parser.Parser(raw_data=IECTest.FF_data, data_type='error', logger=logger, **IECTest.meter_emh)
        #p.log('DEBUG', p.unparsed_data)
        p._parseErrorLog()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 1, 'Parse EMH error log dailed')

    def test_parseMcsTable1(self):
        p = parser.Parser(raw_data=IECTest.Table1_data, data_type='list1', logger=logger, **IECTest.meter_metcom)
        #p.log('DEBUG', p.unparsed_data)
        p._parse_list1()
        p.log('DEBUG', f'\n\n{p.parsed_data}')
        self.assertEqual(len(p.parsed_data), 10, 'Parse MCS Table1 failed')



if __name__ == '__main__':
    unittest.main()