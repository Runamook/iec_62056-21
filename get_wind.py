import time
import datetime
import sqlalchemy
import concurrent.futures
import logging
import configparser
import sys
import os
import pathlib
import requests
import pandas as pd
from windpowerlib import ModelChain, WindTurbine
import iec6205621.inserter as i
from requests.auth import HTTPBasicAuth


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

class VirtualMeter:

    turbine_types = [
        'AD116/5000','E-101/3050','E-101/3500','E-115/3000','E-115/3200','E-126/4200','E-126/7500','E-126/7580','E-141/4200',
        'E-53/800','E-70/2000','E-70/2300','E-82/2000','E-82/2300','E-82/2350','E-82/3000','E-92/2350','E48/800','ENO100/2200',
        'ENO114/3500','ENO126/3500','GE100/2500','GE103/2750','GE120/2500','GE120/2750','GE130/3200','N100/2500','N117/2400',
        'N131/3000','N131/3300','N131/3600','N90/2500','MM100/2000','MM92/2050','S104/3400','S114/3200','S114/3400','S122/3000',
        'S122/3200','S126/6150','S152/6330','SWT113/2300','SWT113/3200','SWT120/3600','SWT130/3300','SWT130/3600','SWT142/3150',
        'VS112/2500','V100/1800','V100/1800/GS','V112/3000','V112/3075','V112/3300','V112/3450','V117/3300','V117/3450','V117/3600',
        'V126/3000','V126/3300','V126/3450','V164/8000','V164/9500','V80/2000','V90/2000','V90/2000/GS','V90/3000','SCD168/8000'
        ]

    def __init__(self, meter: dict, logger: logging.Logger, api_key=None, api_user=None, api_password=None, api_url=None, api_provider=None):
        self.logger = logger

        self.turbine_type = meter.get('turbine_type')
        self.turbine_hub_height = int(meter.get('turbine_hub_height'))
        self.meter_id = meter.get('meter_id')
        self.roughness_length = meter.get('roughness_length') or 0.15
        self.lat = meter.get('latitude') or None
        self.lon = meter.get('longitude') or None
        self.api_provider = api_provider
        self.api_user = api_user
        self.api_password = api_password
        if self.lat is None or self.lon is None:
            self.logger.error(f'Missing coordinates. lat: "{self.lat}", lon: "{self.lon}"')
            sys.exit(1)

        if self.api_provider == 'meteomatics':

            # dt.now().strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
            utcnow = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            self.api = f'https://api.meteomatics.com/{utcnow}/wind_speed_100m:ms,wind_dir_10m:d,msl_pressure:hPa,t_100m:C,wind_gusts_100m_1h:ms/{self.lat},{self.lon}/json'
            self.api_user = api_user
            self.api_password = api_password

        else:
            # Openweathermap by default
            self.api = f'https://api.openweathermap.org/data/2.5/weather?units=metric&lat={self.lat}&lon={self.lon}&appid={api_key}'

        self.weather = None
        self.turbine = self._initialize_turbine()
        self.result = {'weather_data': None, 'power': None, 'error_code': 0, 'error_text': None}


    def _get_wind_openwheathermap(self):
        """
        Get wheather data from Openwheathermap
        openweathermap returns:
        {
            'coord': {'lon': 7.6964, 'lat': 53.0717}, 
            'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}], 
            'base': 'stations', 
            'main': {'temp': 13.33, 'feels_like': 12.34, 'temp_min': 11.66, 'temp_max': 14.45, 'pressure': 1028, 'humidity': 62, 'sea_level': 1028, 'grnd_level': 1027}, 
            'visibility': 10000, 
            'wind': {'speed': 4.05, 'deg': 6, 'gust': 4.17}, 
            'clouds': {'all': 10}, 
            'dt': 1652030898, 
            'sys': {'type': 2, 'id': 2010114, 'country': 'DE', 'sunrise': 1651981373, 'sunset': 1652036912}, 
            'timezone': 7200, 'id': 2834060, 'name': 'Sedelsberg', 'cod': 200
        }
        """

        self.result['weather_data'] = requests.get(self.api).json()
        self.logger.debug(self.result)


    def _get_wind_meteomatics(self):
        """
        Gathers the data from Meteomatics API and restructures it to openwheathermap format
        TZ is hardcoded to UTC

        $ curl -su user:pass https://api.meteomatics.com/2022-11-28T00:37:18.000+01:00/wind_speed_2m:ms,wind_dir_10m:d,msl_pressure:hPa,t_2m:C/47.412164,9.340652/json    
        {
            "version":"3.0","user":"ac_a","dateGenerated":"2022-11-27T23:39:20Z","status":"OK",
            "data":[
                {"parameter":"wind_speed_2m:ms","coordinates":[{"lat":47.412164,"lon":9.340652,"dates":[{"date":"2022-11-27T23:37:18Z","value":0.3}]}]},
                {"parameter":"wind_dir_10m:d","coordinates":[{"lat":47.412164,"lon":9.340652,"dates":[{"date":"2022-11-27T23:37:18Z","value":316.4}]}]},
                {"parameter":"msl_pressure:hPa","coordinates":[{"lat":47.412164,"lon":9.340652,"dates":[{"date":"2022-11-27T23:37:18Z","value":1019}]}]},
                {"parameter":"t_2m:C","coordinates":[{"lat":47.412164,"lon":9.340652,"dates":[{"date":"2022-11-27T23:37:18Z","value":2.8}]}]}]}
        """
        
        auth = HTTPBasicAuth(self.api_user,self.api_password)

        self.logger.debug(f'URL = "{self.api}"')
        result = requests.get(self.api, auth=auth).json()

        self.result['weather_data'] = {
            'coord': {'lon': self.lon, 'lat': self.lat}, 
            'base': 'meteomatics', 
            'main': {}, 
            'wind': {}, 
        }

        # ['data'][0]['coordinates'][0]['dates'][0]['value'] - wind speed. Array index corresponds to URL parameter sequence

        self.result['weather_data']['wind']['speed'] = result['data'][0]['coordinates'][0]['dates'][0]['value']
        self.result['weather_data']['wind']['deg'] = result['data'][1]['coordinates'][0]['dates'][0]['value']
        self.result['weather_data']['main']['pressure'] = result['data'][2]['coordinates'][0]['dates'][0]['value']
        self.result['weather_data']['main']['temp'] = result['data'][3]['coordinates'][0]['dates'][0]['value']
        self.result['weather_data']['wind']['gust'] = result['data'][4]['coordinates'][0]['dates'][0]['value']



    def get_wind(self):
        """
        get_wind expects to find similar structure in self.results

        {
            'coord': {'lon': 7.6964, 'lat': 53.0717}, 
            'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}], 
            'base': 'stations', 
            'main': {'temp': 13.33, 'feels_like': 12.34, 'temp_min': 11.66, 'temp_max': 14.45, 'pressure': 1028, 'humidity': 62, 'sea_level': 1028, 'grnd_level': 1027}, 
            'visibility': 10000, 
            'wind': {'speed': 4.05, 'deg': 6, 'gust': 4.17}, 
            'clouds': {'all': 10}, 
            'dt': 1652030898, 
            'sys': {'type': 2, 'id': 2010114, 'country': 'DE', 'sunrise': 1651981373, 'sunset': 1652036912}, 
            'timezone': 7200, 'id': 2834060, 'name': 'Sedelsberg', 'cod': 200
        }

        Not all fields are in use
        """

        try:

            if self.api_provider == 'meteomatics':
                # curl -su user:pass https://api.meteomatics.com/2022-11-26T00:00:00Z--2022-11-26T01:00:00Z:PT15M/wind_power_turbine_vestas_v90_2000_hub_height_110m:MW/53.645859,5.449016/json | json_pp
                # meteomatics can return power
                self._get_wind_meteomatics()

            else:
                self._get_wind_openwheathermap()

            #variable_name,pressure,temperature,wind_speed,roughness_length,temperature,wind_speed
            #height,0,2,10,0,10,80
            #2010-01-01 00:00:00+01:00,98405.7,267.6,5.32697,0.15,267.57,7.80697
            #2010-01-01 01:00:00+01:00,98382.7,267.6,5.46199,0.15,267.55,7.86199   
            # 
            # DataFrame with time series for wind speed `wind_speed` in m/s,
            # temperature `temperature` in K, roughness length `roughness_length`
            # in m, and pressure `pressure` in Pa.
            # The columns of the DataFrame are a MultiIndex where the first level
            # contains the variable name as string (e.g. 'wind_speed') and the
            # second level contains the height as integer at which it applies
            # (e.g. 10, if it was measured at a height of 10 m). The index is a DateTimeIndex.      

            # variable_name,pressure,temperature,wind_speed,roughness_length
            # height,0,2,10,0
            # 2010-01-01 00:00:00+01:00,98405.7,267.6,5.32697,0.15

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
            pressure = self.result['weather_data']['main']['pressure']
            temp = self.result['weather_data']['main']['temp']
            wind_speed = self.result['weather_data']['wind']['speed']

            f = f'/tmp/meter_{self.meter_id}'
            with open(f, 'w') as fout:
                lines = []
                lines.append(f'variable_name,pressure,temperature,wind_speed,roughness_length\r\n')
                lines.append(f'height,0,2,10,0\r\n')
                lines.append(f'{now},{pressure},{temp},{wind_speed},{self.roughness_length}\r\n')

                for line in lines:
                    fout.write(line)

            self.weather = pd.read_csv(
                f,
                index_col=0,
                header=[0, 1],
                date_parser=lambda idx: pd.to_datetime(idx, utc=True))
            # weather_df.index = weather_df.index.tz_convert('Europe/Berlin')

        except Exception as e:
            self.result['error_code'] = 1
            self.result['error_text'] = e
            return
            
    def _initialize_turbine(self):
        """
        There are three ways to initialize a WindTurbine object in the windpowerlib.
            - use turbine data from the OpenEnergy Database (oedb) turbine library provided
            - specify your own turbine by directly providing a power (coefficient) curve
            - provide your own turbine data in csv files

        returns: WindTurbine
        """     

        if self.turbine_type not in VirtualMeter.turbine_types:
            self.logger.warn(f'Turbine "{self.turbine_type}" not in VirtualMeter.turbine_types')
            self.result['error_code'] = 1
            self.result['error_text'] = f'Turbine "{self.turbine_type}" not in VirtualMeter.turbine_types'
            return

        turbine_data = {
            'turbine_type': self.turbine_type,
            'hub_height': self.turbine_hub_height
        }
        return WindTurbine(**turbine_data)

    def get_power(self):
        """
        Calculates the power based on weather data
        Need to verify the wind speed is at the tube level
        """
        # initialize ModelChain with default parameters and use run_model method to calculate power output
        mc = ModelChain(self.turbine).run_model(self.weather)
        # write power output time series to WindTurbine object
        self.turbine.power_output = mc.power_output
        self.result['power'] = int(self.turbine.power_output.values[0]) / 1000

        """
        if self.turbine.power_output.value_counts() != 1:
            # Expecting single value
            self.result['error_code'] = 1
            self.result['error_text'] = f'Expecting single value, received {self.turbine.power_output.values}'
            return
        else:
            self.result['power'] = self.turbine.power_output.values[0]
        """

def check_error(vmeter,logger, meter):
    if vmeter.result['error_code'] != 0:
        logger.error(f'{meter["meter_id"]} code: {vmeter.result["error_code"]}, text: {vmeter.result["error_text"]}')
        sys.exit(1)
    return


def process_wind(meter, logger: logging.Logger, db, api_key=None, api_user=None, api_password=None, api_url=None, api_provider=None):
    meter_id = meter['meter_id']

    vmeter = VirtualMeter(meter, logger, api_key, api_user, api_password, api_url, api_provider)
    #logger.debug('1')
    check_error(vmeter, logger, meter)

    vmeter.get_wind()
    #logger.debug('2')
    check_error(vmeter, logger, meter)
    
    vmeter.get_power()
    #logger.debug('3')
    check_error(vmeter, logger, meter)

    logger.debug(f"Wind: {vmeter.result['weather_data']['wind']['speed']}, power: {vmeter.result['power']}")

    # [('org:10179636_1611222547', [{'id': '0.0.0', 'value': '1', 'unit': None, 'line_time': 'epoch'}, ... {}]) ... ()]
    parsed_data = [
        {"id": '99.99.96', "value": vmeter.result['weather_data']['main']['pressure'], "unit": "hPa"},
        {"id": '99.99.97', "value": vmeter.result['weather_data']['main']['temp'], "unit": "C"},
        {"id": '99.99.98', "value": vmeter.result['weather_data']['wind']['speed'], "unit": "m/s"},
        {"id": '99.99.99', "value": vmeter.result['power'], "unit": "kW"},
        {"id": '99.99.100', "value": vmeter.result['weather_data']['wind']['deg'], "unit": "degree"},
        {"id": '99.99.101', "value": vmeter.result['weather_data']['wind']['gust'], "unit": "m/s"}
        ]
    # Push data somewhere
    # ['org', '10067967', 1649100604, 'wind']
    meter_ts = [meter["org"].lower(), meter_id, int(time.time()), 'wind']
    inserter = i.Inserter(logger=logger, meter_ts=meter_ts)
    inserter.insert(parsed_data)

class MeterDB:

    def __init__(self, system_logger, **config):
        
        self.logger = system_logger
        pg_user = config.get('pg_user') or 'postgres'
        pg_password = config.get('pg_pass') or 'postgres'
        pg_host = config.get('pg_host') or 'localhost'
        pg_db = config.get('pg_db') or 'postgres'
        self.pg_schema = config.get('pg_schema') or 'meters'

        db_name = f'postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
        log_db_name = f'postgresql://{pg_user}:********@{pg_host}/{pg_db}'


        try:
            engine = sqlalchemy.create_engine(db_name)
            self.conn = engine.connect()
            self.logger.info(f'Connected to the DB {log_db_name}')
        except Exception as e:
            self.logger.error(f'Error "{e}" while connecting to the DB {log_db_name}')

    def get_meters_from_pg(self):
        """
        Get only meters, which shall be queried for load profile or list/table
        Return list of meters
        :param pg_password: pass
        :param pg_user: user
        :returns list of dicts [
            {"id":23,"meter_id":"10067967","latitude":53.0717,"longitude":7.6964, ...}, {} ...
            ]
        """
        query = f'SELECT row_to_json(t) FROM (select * from {self.pg_schema}.meters where latitude IS NOT null AND longitude IS NOT null AND turbine_type IS NOT NULL AND is_active = True) t;'

        try:
            self.logger.info(f'Query: {query}')
            query_result = self.conn.execute(query).fetchall()
            result = []
            for meter in query_result:
                meter = meter[0]
                result.append(meter)
            self.logger.info(f'{len(result)} meters found in DB:')
            for i in result:
                self.logger.info(i)
            return result
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)


def read_cfg(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config



def main(config_file):
    """
    Loop over the list of meters from DB
    """

    config_timer = 0
    config = read_cfg(config_file)
    logger = create_logger(filename=config['DEFAULT']['logfile'], severity_code=config['DEFAULT']['severity'])

    api_provider = config['API']['api_provider']
    api_url = config['API']['api_url']
    
    api_key, api_user, api_password = None, None, None

    if api_provider == 'meteomatics':
        api_user = config['API']['api_user']
        api_password = config['API']['api_password']
    else:
        api_key = config['API']['key']

    db = MeterDB(logger, **config['DB'])
    meters_in_db = []
    last_runs = dict()
    query_interval = int(config['API']['query_interval'])

    while True:

        if time.time() // 60 > config_timer:
            # Re-read config every minute
            config_timer = time.time() // 60

            config = read_cfg(config_file)
            try:
                logger.setLevel(config['DEFAULT']['severity'])
            except Exception as e:
                print(f'Error {e}')
                sys.exit(1)
                
            query_interval = int(config['API']['query_interval'])
            # Re-read meters from DB every minute
            meters_in_db = db.get_meters_from_pg()

        if len(meters_in_db) < 1:
            logger.info('No meters found')
            time.sleep(60)
            continue

        # meters_to_process = meters_in_db
        meters_to_process = []
        for meter in meters_in_db:
            meter_id = meter['meter_id']
            check = time.time() // query_interval
            if last_runs.get(meter_id):
                # Meter in last_runs - it was processed already
                if check > last_runs[meter_id]:
                    logger.debug(f'{meter_id} Interval: {query_interval}, Last_run: {last_runs[meter_id]}, Check {check}')
                    last_runs[meter_id] = check
                    meters_to_process.append(meter)
            else:
                # Meter not in last_runs - it was not processed before
                logger.debug(f'{meter_id} Interval: {query_interval}, Last_run: 0, Check {check}')
                meters_to_process.append(meter)
                last_runs[meter_id] = check

        if len(meters_to_process) > 0:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for meter in meters_to_process:
                    executor.submit(process_wind, 
                    meter=meter, 
                    logger=logger, 
                    db=db, 
                    api_key=api_key, 
                    api_user=api_user, 
                    api_password=api_password, 
                    api_url=api_url, 
                    api_provider=api_provider
                    )
        else:
            # To decrease CPU usage
            time.sleep(0.05)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = '/home/eg/Code/iec_62056-21/wind_settings.ini'
        
    try:
        main(config_file)
    except KeyboardInterrupt:
        sys.exit(0)
