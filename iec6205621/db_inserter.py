from redis import Redis
import sys
import json
import time
import psycopg2
import configparser
import logging
import sys

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


class R2PG:
    # Insert to PG every {insert_interval} seconds
    insert_interval = 10

    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.obis_cache = dict()
        self.meter_cache = dict()
        self.r_keys = None
        self.prepared = False

        # Controls
        self.delete = kwargs.get('delete') or 1          # 1 - True, 0 - False. If True - delete Redis record after
        self.get_all = kwargs.get('get_all') or 0        # 1 - True, 0 - False. If True - do not use org, get all keys from Redis
        
        self.org = kwargs.get('org') or 'test'
        r_host = kwargs.get('r_host') or 'localhost'
        r_port = kwargs.get('r_port') or 6379
        r_db = kwargs.get('r_db')
        pg_host = kwargs.get('pg_host') or 'localhost'
        pg_port = kwargs.get('pg_port') or 5432
        pg_user = kwargs.get('pg_user') or 'postgres'
        pg_pass = kwargs.get('pg_pass') or 'postgres'
        pg_db = kwargs.get('pg_db') or 'postgres'
        self.pg_schema = kwargs.get('pg_schema') or 'meters'
        pg_db_string = {'user': pg_user, 'password': pg_pass, 'host': pg_host, 'port': pg_port, 'dbname': pg_db}
        try:
            self.pg_engine = psycopg2.connect(**pg_db_string)
        except Exception as e:
            self.logger.error(f'Database issues: {e}')
            sys.exit(1)
        try:
            self.pg_conn = self.pg_engine.cursor()
            self.logger.info(f'Connected to Postgres {pg_host}:{pg_port}/{pg_db} as "{pg_user}"')
        except Exception as e:
            self.logger.error(f'Database issues: {e}')

        try:
            self.r = Redis(host=r_host, port=r_port, db=r_db)
            self.logger.info(f'Connected to Redis {r_host}:{r_port}/{r_db}')
        except Exception as e:
            self.logger.error(f'Unable to connect to Redis: {e}')

        self._cache_obis_codes()
        self._cache_meters()

    def _execute_query(self, query, commit: bool = False):
        try:
            self.pg_conn.execute(query)
            if commit:
                self.pg_engine.commit()
                result = True
            else:
                result = self.pg_conn.fetchall()
        except Exception as e:
            self.logger.error(f'DB operation error: {e}, query "{query}" failed')
            sys.exit(1)
        return result

    def _cache_obis_codes(self):
        """
        Connect to DB, load OBIS table, convert it to dict
        Function updates property self.obis_cache
        :return: None
        """
        self.obis_cache = dict()
        try:
            query = f'SELECT row_to_json(m) FROM (SELECT obis, id FROM {self.pg_schema}.obis ORDER BY id) m;'
            db_response = self._execute_query(query)

            for i in db_response:
                self.obis_cache[i[0]['obis']] = i[0]['id']
            self.logger.info(f'{len(self.obis_cache)} OBIS codes loaded from DB')
            self.logger.debug(f'OBIS codes: {self.obis_cache}')
        except Exception as e:
            self.logger.error(f'Unable to continue - no OBIS codes. {e}')
            sys.exit(1)

    def _cache_meters(self):
        """
        Connect to DB, load meters table, convert it to dict
        Function updates property self.meter_cache
        :return: None
        """
        self.meter_cache = dict()
        try:
            query = f'SELECT row_to_json(m) FROM (SELECT meter_id, id FROM {self.pg_schema}.meters ORDER BY id) m;'
            db_response = self._execute_query(query)
            for i in db_response:
                self.meter_cache[i[0]['meter_id']] = i[0]['id']
            self.logger.info(f'{len(self.meter_cache)} Meters loaded from DB')
            self.logger.debug(f'Meters: {self.meter_cache}')
        except Exception as e:
            self.logger.error(f'Unable to continue - no meters. {e}')
            sys.exit(1)

    def _add_obis(self, code):
        """
        Insert newly discovered OBIS code into DB and update cache
        :param code: str, OBIS code
        :return: boolean, True if successfully inserted new OBIS and updated cache
        """
        query = f"INSERT INTO {self.pg_schema}.obis (obis) VALUES ('{code}');"
        try:
            self._execute_query(query, commit=True)
            self.logger.info(f'Inserted new OBIS code {code}')
            self._cache_obis_codes()
            return True
        except Exception as e:
            self.logger.error(f'Unable to insert new OBIS code "{query}" - {e}')
            return False

    def _get_data(self):
        """
        Get data from redis and return list of objects
        Redis inserter inserts as: r.set(name=self.meter_ts, value=data)
        :return: data
        """
        data = []
        try:

            if self.get_all:
                # Get all keys from Redis
                self.r_keys = self.r.keys()
            else:
                # Get keys from Redis by org
                self.r_keys = self.r.keys(f'{self.org}*')
            for r_key in self.r_keys:
                # Load key from Redis and transform it to JSON
                data.append(
                    (r_key, json.loads(self.r.get(r_key)))
                )
        except Exception as e:
            self.logger.error(f'Unable to get data: "{e}"')
        return data

    def _insert_many(self, queries):
        #query_header = f'PREPARE m (int, timestamptz, int, varchar(40)) \
        #AS INSERT INTO {self.pg_schema}.data (meter_id, ts, obis_id, value) VALUES($1, $2, $3, $4);'
        query_header = f'PREPARE m (int, timestamptz, int, varchar(40)) \
            AS INSERT INTO {self.pg_schema}.data (meter_id, ts, obis_id, value) VALUES($1, $2, $3, $4) ON CONFLICT ON CONSTRAINT data_un DO UPDATE SET value=$4;'

        q = None
        try:
            start = time.time()
            if not self.prepared:
                self.pg_conn.execute(query_header)
                self.prepared = True
            for q in queries:
                self.logger.debug(q)
                self.pg_conn.execute(q)
            self.pg_engine.commit()
            self.logger.info(f'Inserted {len(queries)} records in {time.time() - start} seconds')
            return True
        except Exception as e:
            self.logger.error(f'Error inserting data to DB "{e}", query "{q}"')
            return False

    def _clean_keys(self):
        """
        Remove inserted keys from Redis
        :return:
        """
        try:
            keys_deleted = self.r.delete(*self.r_keys)
            self.r_keys = None
            self.logger.info(f'Removed {keys_deleted} keys from redis')

            # Insert statistics counter
            self.r.incrby(f'stats_{self.org}_db', keys_deleted)
            
            return True
        except Exception as e:
            self.logger.error(f'Error, while removing keys from redis {e}')
            return False

    def _push_data(self, data):
        """
        Push data to PG
        :param data: [(b'org:10179636_1611222547:wind', [{'id': '0.0.0', 'value': '1', 'unit': None}, ... {}]) ... ()]
        :param  data: OR [(b'org:10179636_1611222547:p01', [{'id': '0.0.0', 'value': '1', 'unit': None, 'line_time': 'epoch'}, ... {}]) ... ()]
        :return:

        PREPARE m (int, timestamptz, int, varchar(40)) AS
        INSERT INTO {self.pg_schema}.data (meter_id, ts, obis_id, value) VALUES($1, $2, $3, $4);
        EXECUTE m('1', to_timestamp('1611263296'), '1', '123');
        EXECUTE m('1', to_timestamp('1611263297'), '1', '123');
        """
        if len(data) > 0:
            self.logger.debug(f'{len(data)} objects to insert')
        if len(data) == 0:
            return
        queries = []
        for meter_query_data in data:
            # meter_query_data[0] = b'org:10179636_1611222547:p01'
            meter_id, ts = meter_query_data[0].decode().split(':')[1].split('_')
            id = self.meter_cache[meter_id]

            for query_result in meter_query_data[1]:
                # {'id': '0.0.0', 'value': '1', 'unit': None}
                # {'id': '0.0.0', 'value': '1', 'unit': None, 'line_time': 'epoch'} - line_time exists if P01 was processed
                value = query_result['value']
                received_obis = query_result['id']

                if 'line_time' in query_result:
                    # P01 line
                    ts = query_result['line_time']
                    
                # Check for OBIS in cache
                if received_obis not in self.obis_cache:
                    # Try updating DB/Cache
                    self.logger.error(f'OBIS {received_obis} not found in OBIS cache, skipping')
                    if received_obis.endswith('P.01'):
                        # TODO: в _add_obis не работает continue
                        continue
                    if not self._add_obis(received_obis):
                        continue

                obis_id = self.obis_cache[query_result['id']]
                queries.append(f"EXECUTE m('{id}', to_timestamp('{ts}'), '{obis_id}', '{value}');")
        # self.logger.debug(query_header)
        # self.logger.debug(queries)
        if self._insert_many(queries):
            if self.delete:
                self._clean_keys()
        return

    def run(self):
        """
        Main execution loop. Executed every R2PG.insert_interval
        Receives data from Redis, Pushes it to PG
        :return:
        """
        last_insert = 0
        while True:

            if time.time() // R2PG.insert_interval > last_insert:
                try:
                    self._push_data(self._get_data())
                    last_insert = time.time() // R2PG.insert_interval
                except Exception as e:
                    self.logger.error(f'Unable to push data: {e}')
                    continue
            else:
                time.sleep(1)


if __name__ == '__main__':
    """
    Load data from Refis and push it into PG
    """

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = '/home/eg/Code/iec_62056-21/inserter-settings.ini'
        
    config = configparser.ConfigParser()
    config.read(config_file)
    log_file = config['DEFAULT']['logfile']
    log_severity = config['DEFAULT']['severity']
    process_logger = create_logger(filename=log_file, severity_code=log_severity)

    process_kwargs = config['KWARGS']

    try:
        a = R2PG(process_logger, **process_kwargs)
        a.run()
    except KeyboardInterrupt:
        sys.exit(0)
