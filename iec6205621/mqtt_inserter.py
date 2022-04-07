from redis import Redis
import sys
import json
import time
from paho.mqtt import client as mqtt_client
import configparser
import logging


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


class R2MQTT:
    # Insert to MQTT every {insert_interval} seconds
    insert_interval = 10

    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.obis_cache = dict()
        self.meter_cache = dict()
        self.r_keys = None
        self.prepared = False
        self.delete = kwargs.get('delete') or 0          # 1 - True, 0 - False. If True - delete Redis record after
        r_host = kwargs.get('r_host') or 'localhost'
        r_port = kwargs.get('r_port') or 6379
        r_db = kwargs.get('r_db')
        mqtt_broker = kwargs.get('mqtt_broker') or 'hairdresser.cloudmqtt.com'
        mqtt_port = kwargs.get('mqtt_port') or 15525
        mqtt_user = kwargs.get('mqtt_user') or 'user'
        mqtt_pass = kwargs.get('mqtt_pass') or 'password'
        mqtt_client_id = kwargs.get('client_id') or 'Acteno_meters'
        self.topic = kwargs.get('topic') or 'METER'

        self.client = self._connect_mqtt(mqtt_user, mqtt_pass, mqtt_client_id, mqtt_broker, mqtt_port)

        try:
            self.r = Redis(host=r_host, port=r_port, db=r_db)
            self.logger.info(f'Connected to Redis {r_host}:{r_port}/{r_db}')
        except Exception as e:
            self.logger.error(f'Unable to connect to Redis: {e}')

        self._cache_obis_codes()
        self._cache_meters()

    def _connect_mqtt(self, username, password, client_id, broker, port):
        def on_connect(client, userdata, flags, rc):
            rc_data = {
                0: 'Connection successful',
                1: 'Connection refused - incorrect protocol version',
                2: 'Connection refused - invalid client identifier',
                3: 'Connection refused - server unavailable',
                4: 'Connection refused - bad username or password',
                5: 'Connection refused - not authorised'
                } # 6 - 255 unused

            if rc == 0:
                self.logger.info(f"Connected to MQTT Broker {broker}:{port} {username}, client_id = {client_id}")
            else:
                self.logger.error(f"Failed to connect, return code {rc}\n{rc_data.get(rc)}")
        # Set Connecting Client ID
        client = mqtt_client.Client(client_id)
        client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(broker, port)
        return client

    def _publish(self, payload):
        """
        Publish data to MQTT broker
        return: True on success, False otherwise
        """
        
        try:
            result = self.client.publish(self.topic, payload, qos=0)
            # result: [0, 1]
            status = result[0]
            if status == 0:
                self.logger.debug(f"Send `{payload}` to topic `{self.topic}`")
                return True
            else:
                self.logger.error(f"Failed to send `{self.payload}` to topic `{self.topic}`\n{result}")
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def _cache_obis_codes(self):
        """
        Connect to DB, load OBIS table, convert it to dict
        Function updates property self.obis_cache
        :return: None
        """
        self.obis_cache = dict()
        try:
            query = 'SELECT row_to_json(m) FROM (SELECT obis, id FROM meters.obis ORDER BY id) m;'
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
            query = 'SELECT row_to_json(m) FROM (SELECT meter_id, id FROM meters.meters ORDER BY id) m;'
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
        query = f"INSERT INTO meters.obis (obis) VALUES '{code}'"
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
        :return:
        """
        data = []
        try:
            # Get all keys from Redis
            self.r_keys = self.r.keys()
            for r_key in self.r_keys:
                # Load key from Redis and transform it to JSON
                data.append(
                    (r_key, json.loads(self.r.get(r_key)))
                )
        except Exception as e:
            self.logger.error(f'Unable to get data {e}')
        return data

    def _insert_many(self, queries):
        q = None
        try:
            start = time.time()
            for q in queries:
                self.logger.debug(q)
                self._publish(q)
            self.logger.info(f'Inserted {len(queries)} records in {time.time() - start} seconds')
            return True
        except Exception as e:
            self.logger.error(f'Error publishing data into MQTT {e}, query "{q}"')
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
            return True
        except Exception as e:
            self.logger.error(f'Error, while removing keys from redis {e}')
            return False

    def _push_data(self, data):
        """
        Push data to MQTT
        :param data: [(b'meter:10179636_1611222547', [{'id': '0.0.0', 'value': '1', 'unit': None}, ... {}]) ... ()]
        :return:

        """
        self.logger.debug(f'{len(data)} objects to insert')
        if len(data) == 0:
            return
        queries = []
        for meter_query_data in data:
            # meter_query_data[0] = b'meter:10179636_1611222547'
            meter_id, ts = meter_query_data[0].decode().split(':')[1].split('_')
            id = self.meter_cache[meter_id]

            for query_result in meter_query_data[1]:
                # {'id': '0.0.0', 'value': '1', 'unit': None}
                value = query_result['value']
                received_obis = query_result['id']

                # Check for OBIS in cache
                if received_obis not in self.obis_cache:
                    self.logger.warn(f'Unknown OBIS {received_obis} in {query_result}')
                    """
                    # Try updating DB/Cache
                    if not self._add_obis(received_obis):
                        self.logger.error(f'OBIS {received_obis} not found in OBIS cache, skipping, data: {query_result}')
                        continue
                    else:
                        self.logger.warn(f'Inserted OBIS {received_obis} for {query_result}')
                    """

                obis_id = self.obis_cache[query_result['id']]

                # Values to push to MQTT
                queries.append({
                    'meter_id': id, 
                    'ts': ts, 
                    'obis': obis_id, 
                    'value': value
                    })

        if self._insert_many(queries):
            if self.delete:
                self._clean_keys()

        return

    def run(self):
        """
        Main execution loop. Executed every R2MQTT.insert_interval
        Receives data from Redis, Pushes it to PG
        :return:
        """
        last_insert = 0
        while True:

            if time.time() // R2MQTT.insert_interval > last_insert:
                try:
                    self._push_data(self._get_data())
                    last_insert = time.time() // R2MQTT.insert_interval
                except Exception as e:
                    self.logger.error(f'Unable to push data: {e}')
                    continue
            else:
                time.sleep(1)


if __name__ == '__main__':
    """
    Load data from Refis and push it into PG
    """

    config_file = '/home/egk/PycharmProjects/Meters_2/inserter-settings.ini'
    config = configparser.ConfigParser()
    config.read(config_file)
    log_file = config['DEFAULT']['logfile']
    log_severity = config['DEFAULT']['severity']
    process_logger = create_logger(filename=log_file, severity_code=log_severity)

    process_kwargs = config['KWARGS']

    try:
        a = R2MQTT(process_logger, **process_kwargs)
        a.run()
    except KeyboardInterrupt:
        sys.exit(0)
