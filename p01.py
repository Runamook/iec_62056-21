#! THIS IS AN OLD VERSION

# NOT REALLY WORKING


from logging import config
import iec6205621.client as client
import time
import sqlalchemy
import concurrent.futures
import logging
import sys
import configparser


# (Re)Reads meters from Postgres
# Starts a loop :
# - Check if a meter should be queried
# - Starts a job

def create_logger(filename, severity_code: str = 'ERROR', log_stdout: bool = True):
    if severity_code == 'DEBUG':
        severity = logging.DEBUG
    elif severity_code == 'INFO':
        severity = logging.INFO
    elif severity_code == 'WARN':
        severity = logging.WARN
    else:
        severity = logging.ERROR

    logger = logging.getLogger('P01')
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


class MyMeter(client.Meter):
    def __init__(self, logger, address, meter_name, meter_type, port: int = 8000, timeout: int = 300):
        self.logger = logger
        super().__init__(address=address, port=port, timeout=timeout, meter_name=meter_name, meter_type=meter_type)

    def log(self, severity, logstring):
        # logstring = client.normalize_log(logstring)
        self.logger.debug(f'{self.meter_name} {self.url[9:]} {logstring}')


def get_meters_from_pg(request,conf):
    """
    Get only meters, which shall be queried for load profile or list/table
    Return list of meters
    :argument request: str like 'p01', 'p98', 'list1' etc
    """
    db_name = f"postgresql://{conf['DB']['pg_user']}:{conf['DB']['pg_pass']}@{conf['DB']['pg_host']}/{conf['DB']['pg_db']}"
    engine = sqlalchemy.create_engine(db_name)
    conn = engine.connect()
    '''
    query = f'select queries.{request}, meters.manufacturer, meters.meter_id, meters.ip_address , meters.port\
     , meters.voltagefactor , meters.currentfactor, meters.password from meters.meters inner join meters.queries\
      on meters.id = queries.id where queries.{request} > 0 and meters.is_active = true;'
    '''
    query = f'select queries.{request}, meters.manufacturer, meters.meter_id, meters.ip_address , meters.port\
     , meters.voltagefactor , meters.currentfactor, meters.password from meters.meters inner join meters.queries\
      on meters.id = queries.id where queries.{request} > 0 and meters.is_active = true;'

    query_result = conn.execute(query).fetchall()
    result = []
    for meter in query_result:
        result.append({'interval': meter[0],
                       'manufacturer': meter[1],
                       'meter_name': meter[2],
                       'ip': meter[3],
                       'port': meter[4],
                       'voltageFactor': meter[5],
                       'currentFactor': meter[6],
                       'currentFactor': meter[7],
                       'last_run': 0})
    return result


def process_data(meter, logger):
    """
    Query the meter, parse the data, push to pg
    """
    # device_address = meter['meter_name']
    logger.debug(f'Process_data for meter {meter["meter_name"]}')
    m = MyMeter(address=meter['ip'], port=meter['port'], meter_name=meter['meter_name'], logger=logger, meter_type=meter['manufacturer'], timeout=10)
    raw_data = m.readLoadProfile(profile_number='1', use_meter_name=True)
    print(raw_data)
    # Parse data
    # parsed_data = Parser.parse(raw_data)

    # Push data to pg
    # result = Pusher.push(parsed_data)

    return


def main(meters_in_db, config):
    """
    Loop over the list of meters from DB
    If the meters should be queried now - execute query
    :param config: ConfigParser object
    :param meters_in_db: list of dicts [{
                        'interval': 900, 'manufacturer': 'MetCom',
                        'meter_name': '1MCS0010045438', 'ip': '192.168.121.101',
                        'port': 8000, 'voltageFactor': 1100, 'currentFactor': 400,
                        'last_run': 0
                        }, {} ...]
    """
    logger = create_logger(filename=config['DEFAULT']['logfile'], severity_code=config['DEFAULT']['severity'])
    logger.debug(f'{len(meters_in_db)} meters found in DB:\n{meters_in_db}')
    while True:
        meters_to_process = []
        for meter in meters_in_db:
            check = time.time() // meter['interval']
            if check > meter['last_run']:
                logger.debug(meter)
                meter['last_run'] = check
                meters_to_process.append(meter)
            else:
                if time.time()//10 == 0:
                    logger.debug('Waiting')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for meter in meters_to_process:
                executor.submit(process_data, meter=meter, logger=logger)


if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = '/home/eg/Code/iec_62056-21/settings.ini'
       
    configuration = configparser.ConfigParser()
    configuration.read(config_file)

    list_of_meters = get_meters_from_pg('p01', configuration)
    main(list_of_meters, configuration)
