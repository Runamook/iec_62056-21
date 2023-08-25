import json
from time import sleep
import datetime
import configparser
import pathlib
import logging
import os
import sys
import requests


def create_logger(filename, severity_code: str = 'ERROR', log_stdout: bool = True):
    if severity_code == 'DEBUG':
        severity = logging.DEBUG
    elif severity_code == 'INFO':
        severity = logging.INFO
    elif severity_code == 'WARN':
        severity = logging.WARN
    else:
        severity = logging.ERROR

    if not os.path.exists(filename):
        # Create file
        pathlib.Path(filename).touch(exist_ok=True)

    logger = logging.getLogger(__name__)
    logger.setLevel(severity)
    fmt = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    fh = logging.FileHandler(filename=filename)
    fh.setFormatter(fmt)
    fh.setLevel(severity)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    if log_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        sh.setLevel(severity)
        logger.addHandler(sh)
    return logger


def read_cfg(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


class CRMtoPG:

    file_cache = "/tmp/_crm_to_redis_cache_1wed.txt"

    def __init__(self, config):
        log_stdout = config['DEFAULT'].get('log_stdout') or False

        self.logger = create_logger(filename=config['DEFAULT']['logfile'], severity_code=config['DEFAULT']['severity'], log_stdout=log_stdout)
        self.url = config['DEFAULT'].get('CRM_URL')

    def validate_meter(self, meter):
        """
        Transforms data to the format expected by emhmeter.py
        More or less

        API:
       {
          "installedCommunicationModule":"20000000",
          "customer":"Plan GmbH",
          "dgo":"Q.AAA AG",
          "ip":"10.124.2.34",
          "meteringPointGuid":"111111-2222-e5555-810f-12314464",
          "meteringPointLabel":"DE001123456789",
          "operator":"Plan GmbH",
          "installedRouter":"NONE",
          "shortName":"PAR-1900000-01 R",
          "installedSim":"123456789000",
          "installedMeter":{
             "name":"10001802",
             "type":"MCS301-CW31B-2EMIS-024100",
             "manufacturer":"Metcom"
          },
          "transformerFactors":{
             "current":150,
             "voltage":5
          },
          "schedule":{
             "p01":"24 Hours",
             "p200":"24 Hours",
             "p211":"24 Hours",
             "table1":"24 Hours",
             "table2":"24 Hours",
             "table3":"24 Hours",
             "table4":"24 Hours",
             "time":"24 Hours"
          }
       }

        """
        try:
            # Check if anything is missing
            operator = meter["operator"]
            ip = meter["ip"]
            name = meter["installedMeter"]["name"]
            manufacturer = meter["installedMeter"]["manufacturer"]
            transform_curent = meter["transformerFactors"]["current"]
            transform_voltage = meter["transformerFactors"]["voltage"]
            schedule = meter["schedule"]
        except KeyError:
            self.logger.error(f"Some mandatory key is missing in {meter}")
            return None

        return meter

    @staticmethod
    def get_cache():
        with open(CRMtoPG.file_cache, 'r') as f:
            return f.read()

    @staticmethod
    def update_cahce(value):
        with open(CRMtoPG.file_cache, 'w') as f:
            f.write(value)
        return

    def get_crm_data(self):
        """
        Parses data returned by API
        """
        try:
            results = requests.get(self.url, timeout=10).json()
            self.update_cahce(json.dumps(results))
        except Exception as e:
            self.logger.error(f"{e} error when getting data from {self.url}")
            self.logger.warning(f"Reading cached data")
            results = json.loads(self.get_cache())

        meter_list = []
        for meter in results:
            self.logger.debug(f"Found meter {meter}")
            new_meter = self.validate_meter(meter)
            if new_meter:
                self.logger.debug(f"Transform to {new_meter}")
                meter_list.append(new_meter)

        return json.dumps(meter_list)

    def run(self):
        self.logger.debug(self.get_crm_data())


# INSERT INTO edge.meters (description,melo,manufacturer,meter_id,ip_address,currentfactor,voltagefactor,org) VALUES
# ('CEC Z12 W17 Ladepark Ladepunkt L1.1', 'INTCECZ1200000000000000000NSLAD01', 'Metcom_edge', '10087411', '192.168.114.112',1,1,'edge');

# + enabled = True


# INSERT INTO edge.queries
# (id, p01, p02, list1, list2, list3, list4, p98, error)
# VALUES
# (22, 900, 0, 0, 60, 0, 0, 0, 0);





if __name__ == "__main__":
    start = datetime.datetime.now()

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'crm2pg_settings.ini'

    config = read_cfg(config_file)

    a = CRMtoPG(config)
    a.run()