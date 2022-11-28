import logging
from redis import Redis
import json

class Inserter:

    def __init__(self, logger, meter_ts, host: str = 'localhost', port: int = 6379, expiry=None):
        self.logger = logger
        self.host = host
        self.port = port
        self.meter_id = meter_ts[1]
        self.meter_ts = f'{meter_ts[0]}:{self.meter_id}_{meter_ts[2]}:{meter_ts[3]}' # org:meterId_ts:data_id
        # Keys will be stored in redis for this time unless deleted explicitly
        #       86400 - 24 hours
        #       2592000 - 30 days
        self.expiry = expiry

    def insert(self, data):
        return self.redis_insert(data)

    def redis_insert(self, data):
        """
        :data: data to be sent to redis
        :return: None, watch logs
        """
        try:
            data = json.dumps(data)
            r = Redis(host=self.host, port=self.port)
            
            if self.expiry:
                # Redis with automatic key expiry
                r.set(name=self.meter_ts, value=data, ex=self.expiry)
            else:
                r.set(name=self.meter_ts, value=data)
    
            self.logger.debug(f'{self.meter_id} {self.meter_ts} Redis insert successful')
            return True
        except Exception as e:
            self.logger.error(f'{self.meter_id} {self.meter_ts} Redis instert failed "{e}"')
            return False
