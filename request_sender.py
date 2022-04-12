import iec6205621.client as client
import iec6205621.parser as p
import iec6205621.inserter as i
import time
import sqlalchemy
import concurrent.futures
import logging
import configparser
import sys
import os
import pathlib
import datetime


# (Re)Reads meters from Postgres
# Starts a loop :
# - Check if a meter should be queried
# - Starts a job

# TODO: meter locking
# TODO: statistics counters


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


class MyMeter(client.Meter):
    def __init__(self, logger, timeout: int = 300, **meter):
        self.logger = logger
        super().__init__(timeout=timeout, **meter)

    def log(self, severity, log_string):
        """
        Reassign log method to write into file
        Try normalizing byte strings
        """

        try:
            if isinstance(log_string, bytes):
                logstring = client.normalize_log(log_string)
            else:
                logstring = client.normalize_log_str(log_string)

        except Exception as e:
            self.logger.error(f"Normalization problem: {e}, str = [{log_string}], type = {type(log_string)}")
            logstring = log_string

        # logstring = log_string
        #self.logger.debug(f'Received for normalization: [{logstring}] type = {type(logstring)}')

        if severity == 'ERROR':
            self.logger.error(f'{self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'WARN':
            self.logger.warning(f'{self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'INFO':
            self.logger.info(f'{self.meter_id} {self.url[9:]} {logstring}')
        elif severity == 'DEBUG':
            self.logger.debug(f'{self.meter_id} {self.url[9:]} {logstring}')

class MeterDB:

    def __init__(self, system_logger, **config):
       
        pg_user = config.get('pg_user') or 'postgres'
        pg_password = config.get('pg_pass') or 'postgres'
        pg_host = config.get('pg_host') or 'localhost'
        pg_db = config.get('pg_db') or 'postgres'
        pg_schema = config.get('pg_schema') or 'meters'

        db_name = f'postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
        log_db_name = f'postgresql://{pg_user}:********@{pg_host}/{pg_db}'

        self.logger = system_logger
        self.pg_schema = pg_schema

        try:
            engine = sqlalchemy.create_engine(db_name)
            self.conn = engine.connect()
            self.logger.info(f'Connected to the DB {log_db_name}')
        except Exception as e:
            self.logger.error(f'Error "{e}" while connecting to the DB {log_db_name}')
            

    def update_p01(self, meter, action: str='set'):
        """
        Updates meter SQL profile with P01 request ts
        If request was successfull - reset the ts
        """
        self.logger.debug(f'{meter} P01 action = "{action}"')

        now = f"to_timestamp(\'{datetime.datetime.now().strftime('%s')}\')"

        if action == 'set':
            query = f"UPDATE {self.pg_schema}.meters SET p01_from={now} WHERE meters.meter_id = '{meter}';"
        elif action == 'delete':
            query = f"UPDATE {self.pg_schema}.meters SET p01_from=NULL WHERE meters.meter_id = '{meter}';"
        else:
            self.logger.warn(f'Unknown method "{action}" provided')

        try:
            self.conn.execute(query)
            self.logger.debug(f'Executed {query}')
        except Exception as e:
            self.logger.error(f'Error "{e}" during meter from_p01 update, last query = "{query}"')

        return


    def get_meters_from_pg(self, request):
        """
        Get only meters, which shall be queried for load profile or list/table
        Return list of meters
        :param pg_password: pass
        :param pg_user: user
        :argument request: str like 'p01', 'p98', 'list1' etc
        :returns list of dicts [{
                            'interval': 900, 'manufacturer': 'MetCom',
                            'meter_id': '1MCS0010045438', 'ip': '192.168.121.101',
                            'port': 8000, 'voltageFactor': 1100, 'currentFactor': 400,
                            'last_run': 0...
                            }, {} ...]
        """
        query = f'SELECT row_to_json(m) FROM (\
        SELECT * FROM {self.pg_schema}.meters INNER JOIN {self.pg_schema}.queries ON meters.id = queries.id WHERE queries.{request} > 0 and meters.is_active = True) m;'

        try:
            self.logger.info(f'Query: {query}')
            query_result = self.conn.execute(query).fetchall()
            result = []
            for meter in query_result:
                meter = meter[0]
                # meter['last_run'] = 0
                result.append(meter)
            self.logger.info(f'{len(result)} meters found in DB:')
            for i in result:
                self.logger.info(i)
            return result
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)


def process_data(meter, logger, data_id, db=None):
    """
    Query the meter, parse the data, push to pg
    """
    meter_id = meter['meter_id']

    if data_id == 'p01':
        if not meter['p01_from']:
            # Set p01_from field in SQL meter profile if not set
            db.update_p01(meter_id, action='set')

    try:
        m = MyMeter(logger=logger, timeout=10, **meter)
    except Exception as e:
        logger.error(e)
        sys.exit(1)

    logger.debug(f'data_id = {data_id} {meter}')
    if data_id == 'list4':
        raw_data = m.readList(list_number=data_id)
    elif data_id == 'p01':
        raw_data = m.readLoadProfile(profile_number='1')
    else:
        logger.warn(f'Unknown data_id = {data_id}')
        sys.exit(1)

    # Parse data
    try:
        parser = p.Parser(raw_data, data_type=data_id, logger=logger, **meter)
        parsed_data = parser.parse()
        logger.debug(f'{meter_id} Parsed data: {parsed_data}')
    except Exception as e:
        logger.error(f'{meter_id} Error during parsing: "{e}"')
        sys.exit(1)
    # Push data somewhere
    if len(parsed_data) > 0:

        # org 10067967 1649100604
        meter_ts = [meter["org"].lower(), meter_id, int(time.time())]
        inserter = i.Inserter(logger=logger, meter_ts=meter_ts)
        if inserter.insert(parsed_data):
            # All good - unset p01_from field in SQL meter profile
            db.update_p01(meter_id, action='delete')
    else:
        logger.debug(f'{meter_id} nothing to insert')
    return


def read_cfg(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def select_meters(meters_from_db, meters_to_process):
    """
    Merge existing meters with the new DB response
    take 'last_run' field from meters_to_process and add it to meters_from_db
    """
    meters_from_db_updated = []

    if len(meters_to_process) == 0:
        return meters_from_db
    else:
        last_runs = dict()
        # Take 'last_run' field from the meters already processed
        for processed_meter in meters_to_process:
            last_runs[processed_meter['meter_id']] = processed_meter['last_run']

        for meter_in_db in meters_from_db:
            meter_id = meter_in_db['meter_id']

            if meter_id in last_runs:
                # Meter was already processed - take it's 'last_run' field
                meter_in_db['last_run'] = last_runs[meter_id]
            
            meters_from_db_updated.append(meter_in_db)

        return meters_from_db_updated

def main(config_file):
    """
    Loop over the list of meters from DB
    If the meters should be queried now - execute query
    :param logger: logging object to write logs to
    :param config: ConfigParser object
    """

    config_timer = 0
    config = read_cfg(config_file)
    logger = create_logger(filename=config['DEFAULT']['logfile'], severity_code=config['DEFAULT']['severity'])
    data_id = config['DEFAULT']['data_id'].lower()

    last_runs = dict()
    db = MeterDB(logger, config=config['DB'])

    while True:

        if time.time() // 60 > config_timer:
            # Re-read config every minute
            config = read_cfg(config_file)
            try:
                logger.setLevel(config['DEFAULT']['severity'])
            except Exception as e:
                print(f'Error {e}')
                sys.exit(1)
                
            # Re-read meters from DB every minute
            meters_in_db = db.get_meters_from_pg(data_id)
            config_timer = time.time() // 60

        try:
            if len(meters_in_db) < 1:
                logger.info('No meters found')
                # sys.exit(1)
                time.sleep(10)
                continue

        except Exception as e:
            print(f'Something went wrong: "{e}"')
            sys.exit(1)

        meters_to_process = []                  # These meters will be subject to processing 
        # Run the main loop
        for meter in meters_in_db:
            meter_id = meter['meter_id']
            check = time.time() // meter[data_id]

            if last_runs.get(meter_id):
                # Meter in last_runs - it was processed already
                if check > last_runs[meter_id]:
                    logger.debug(f'{meter_id} Interval: {meter[data_id]}, Last_run: {last_runs[meter_id]}, Check {check}')
                    last_runs[meter_id] = check
                    meters_to_process.append(meter)
            else:
                # Meter not in last_runs - it was not processed before
                logger.debug(f'{meter_id} Interval: {meter[data_id]}, Last_run: 0, Check {check}')
                meters_to_process.append(meter)
                last_runs[meter_id] = check

        if len(meters_to_process) > 0:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for meter in meters_to_process:
                    executor.submit(process_data, meter=meter, logger=logger, data_id=data_id, db=db)
        else:
            # To decrease CPU usage
            time.sleep(0.05)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = '/home/eg/Code/iec_62056-21/settings.ini'
        
    try:
        main(config_file)
    except KeyboardInterrupt:
        sys.exit(0)
