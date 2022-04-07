import iec6205621.client as client
import iec6205621.parser as p
import iec6205621.inserter as i
import time
import sqlalchemy
import concurrent.futures
import logging
import configparser
import sys


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


def get_meters_from_pg(request,
                       system_logger,
                       pg_user: str = 'postgres',
                       pg_password: str = 'postgres',
                       pg_host: str = 'localhost',
                       pg_db: str = 'postgres',
                       pg_schema: str = 'meters'
                       ):
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
    SELECT * FROM {pg_schema}.meters INNER JOIN {pg_schema}.queries ON meters.id = queries.id WHERE queries.{request} > 0 and meters.is_active = True) m;'

    db_name = f'postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
    system_logger.info(f'Connecting to the DB {db_name}')
    try:
        engine = sqlalchemy.create_engine(db_name)
        conn = engine.connect()
        system_logger.info(f'Query: {query}')
        query_result = conn.execute(query).fetchall()
        result = []
        for meter in query_result:
            meter = meter[0]
            meter['last_run'] = 0
            result.append(meter)
        system_logger.info(f'{len(result)} meters found in DB:')
        for i in result:
            system_logger.info(i)
        return result
    except Exception as e:
        system_logger.error(e)
        sys.exit(1)


def process_data(meter, logger, data_id):
    """
    Query the meter, parse the data, push to pg
    """

    try:
        m = MyMeter(logger=logger, timeout=10, **meter)
    except Exception as e:
        logger.error(e)
        sys.exit(1)

    logger.debug(f'data_id = {data_id} {meter}')
    if data_id == 'list4':
        raw_data = m.readList(list_number=data_id, use_meter_id=True)
    elif data_id == 'p01':
        raw_data = m.readLoadProfile(profile_number='1', use_meter_id=True)
    else:
        logger.warn(f'Unknown data_id = {data_id}')
        sys.exit(1)

    # Parse data
    try:
        parser = p.Parser(raw_data, data_type=data_id, logger=logger, **meter)
        parsed_data = parser.parse()
        logger.debug(f'{meter["meter_id"] } Parsed data: {parsed_data}')
    except Exception as e:
        logger.error(f'Error during parsing: "{e}"')
        sys.exit(1)
    # Push data somewhere
    if len(parsed_data) > 0:

        # org_10067967_1649100604
        meter_ts = [meter["org"], meter["meter_id"], int(time.time())]
        inserter = i.Inserter(logger=logger, meter_ts=meter_ts)
        inserter.insert(parsed_data)
    else:
        logger.debug(f'{meter["meter_id"]} nothing to insert')
    return


def read_cfg(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


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
    data_id = config['DEFAULT']['data_id']

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
            meters_in_db = get_meters_from_pg(
                data_id,
                logger, 
                pg_user=config['DB']['pg_user'], 
                pg_password=config['DB']['pg_pass'], 
                pg_host=config['DB']['pg_host'], 
                pg_db=config['DB']['pg_db'],
                pg_schema=config['DB']['pg_schema']
                )

            config_timer = time.time() // 60

        try:
            if len(meters_in_db) < 1:
                logger.info('No meters found')
                # sys.exit(1)
                time.sleep(10)
                continue

        except Exception as e:
            print(f'Something went wrong: {e}')
            sys.exit(1)

        meters_to_process = []

        # Run the main loops
        for meter in meters_in_db:
            check = time.time() // meter[data_id]
            if check > meter['last_run']:
                logger.debug(f'{meter["meter_id"]} Interval: {meter[data_id]}, Last_run: {meter["last_run"]}, Check {check}')
                meter['last_run'] = check
                meters_to_process.append(meter)
            else:
                # To decrease CPU usage
                time.sleep(0.05)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for meter in meters_to_process:
                executor.submit(process_data, meter=meter, logger=logger, data_id=data_id)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = '/home/eg/Code/iec_62056-21/settings.ini'
        
    try:
        main(config_file)
    except KeyboardInterrupt:
        sys.exit(0)
