from redis import Redis
import json

class Inserter:

    def __init__(self, logger, meter_ts, host: str = 'localhost', port: int = 6379):
        self.logger = logger
        self.host = host
        self.port = port
        self.meter_id = meter_ts[1]
        self.meter_ts = f'{meter_ts[0]}:{self.meter_id}_{meter_ts[2]}' # org:meterId_ts

    def insert(self, data):
        self.redis_insert(data)

    def redis_insert(self, data):
        """
        :data: data to be sent to redis
        :return: None, watch logs
        """
        try:
            data = json.dumps(data)
            r = Redis(host=self.host, port=self.port)
            r.set(name=self.meter_ts, value=data)
            self.logger.debug(f'{self.meter_id} Redis insert successful')
        except Exception as e:
            self.logger.error(f'{self.meter_id} Redis instert failed "{e}"')
