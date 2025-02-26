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
from windpowerlib import ModelChain, WindTurbine, create_power_curve
import iec6205621.inserter as iec_inserter
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

    own_turbine_types = ['act_v126_3300', 'act_e-66/18.70', 'act_V162-5.6/6.0 MW STE', 'act_V136-4.0/4.2 MW STE','act_V150-5.6/6.0 MW STE']
    own_turbine_data = {
        'act_v126_3300': pd.DataFrame(data={
            'value': [0.0, 0.0, 49022.0, 49022.0, 101881.0, 185361.0, 279269.0, 401162.0, 548734.0, 711842.0, 913150.0, 1154923.0, 1428947.0, 1734931.0, 2057662.0, 2433037.0, 2785466.0, 3065281.0, 3222313.0, 3283706.0, 3297786.0, 3299872.0, 3300000.0, 3300000.0, 3300000.0],  # in W
            'wind_speed': [0.0,1.0,2.0,3.2,3.7,4.2,4.5,4.7,5.2,5.7,6.2,6.7,7.2,7.7,8.2,8.7,9.2,9.7,10.2,10.7,11.2,11.7,12.2,18.2,100.0]
            }),
        'act_V162-5.6/6.0 MW STE': pd.DataFrame(data={
            'value': [0, 0, 0, 0, 0, 0, 27000, 144000, 289000, 464000, 669000, 919000, 1220000, 1574000, 1990000, 2467000, 3010000, 3617000, 4257000, 4834000, 5256000, 5482000, 5578000, 5598000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5568000, 5418000, 5179000, 4894000, 4609000, 4329000, 4043000, 3764000, 3488000, 3203000, 2914000, 2616000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'wind_speed': [0,0.5,1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,6,6.5,7,7.5,8,8.5,9,9.5,10,10.5,11,11.5,12,12.5,13,13.5,14,14.5,15,15.5,16,16.5,17,17.5,18,18.5,19,19.5,20,20.5,21,21.5,22,22.5,23,23.5,24,24.5,25,25.5,26,26.5,27,27.5,28,28.5,29,29.5,30,30.5,31,31.5,32,32.5,33,33.5,34,34.5,35]
            }),
        'act_V136-4.0/4.2 MW STE': pd.DataFrame(data={
            'value': [0,0,0,0,0,0,57000,133000,225000,338000,479000,650000,856000,1100000,1386000,1710000,2077000,2472000,2858000,3212000,3548000,3834000,4029000,4140000,4185000,4197000,4199000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,4200000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            'wind_speed': [0,0.5,1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,6,6.5,7,7.5,8,8.5,9,9.5,10,10.5,11,11.5,12,12.5,13,13.5,14,14.5,15,15.5,16,16.5,17,17.5,18,18.5,19,19.5,20,20.5,21,21.5,22,22.5,23,23.5,24,24.5,25,25.5,26,26.5,27,27.5,28,28.5,29,29.5,30,30.5,31,31.5,32,32.5,33,33.5,34,34.5,35]
            }),
        'act_V150-5.6/6.0 MW STE': pd.DataFrame(data={
            'value': [0, 0, 0, 0, 0, 0, 27000, 144000, 289000, 464000, 669000, 919000, 1220000, 1574000, 1990000, 2467000, 3010000, 3617000, 4257000, 4834000, 5256000, 5482000, 5578000, 5598000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5600000, 5568000, 5418000, 5179000, 4894000, 4609000, 4329000, 4043000, 3764000, 3488000, 3203000, 2914000, 2616000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'wind_speed': [0,0.5,1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,6,6.5,7,7.5,8,8.5,9,9.5,10,10.5,11,11.5,12,12.5,13,13.5,14,14.5,15,15.5,16,16.5,17,17.5,18,18.5,19,19.5,20,20.5,21,21.5,22,22.5,23,23.5,24,24.5,25,25.5,26,26.5,27,27.5,28,28.5,29,29.5,30,30.5,31,31.5,32,32.5,33,33.5,34,34.5,35]
            }),
        'act_e-66/18.70': pd.DataFrame(data={
            'value':      [0.0, 0.0, 0.0, 0.0, 3820.0, 15340.0, 30970.0, 51820.0, 81120.0, 116760.0, 161680.0, 208760.0, 272960.0, 338740.0, 424350.0, 510110.0, 628180.0, 725600.0, 859770.0, 991000.0, 1135170.0, 1274960.0, 1430380.0, 1549430.0, 1648140.0, 1729380.0, 1770200.0, 1817160.0, 1847380.0, 1864440.0, 1865730.0, 1866640.0, 1867270.0, 1865640.0, 1867290.0, 1865790.0, 1866320.0, 1800000.0, 1800000.0, 1800000.0, 1800000.0, 1800000.0, 1800000.0],
            'wind_speed': [0.0,1,1.5,2,2.5,3.01,3.49,4,4.51,5,5.5,5.99,6.51,6.98,7.5,7.96,8.49,8.98,9.49,10.01,10.48,10.98,11.52,12,12.48,12.98,13.49,14.01,14.96,15.49,15.96,16.5,16.91,17.52,18.07,18.48,18.91,20,21,22,23,24,25]
        })
        }

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

        self.api_mass = meter.get('API_mass') or False
        self.meter = meter
        if self.api_provider == 'meteomatics':

            # dt.now().strftime('%Y-%m-%dT%H:%M:%S.000+01:00')
            utcnow = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            self.api = f'https://api.meteomatics.com/{utcnow}/wind_speed_149m:ms,wind_dir_10m:d,msl_pressure:hPa,t_2m:K,wind_gusts_100m_1h:ms/{self.lat},{self.lon}/json'
            self.api_user = api_user
            self.api_password = api_password

        else:
            # Openweathermap by default
            self.api = f'https://api.openweathermap.org/data/2.5/weather?units=metric&lat={self.lat}&lon={self.lon}&appid={api_key}'

        self.weather = None
        self.turbine = self._initialize_turbine()
        self.result = {'weather_data': None, 'power': None, 'error_code': 0, 'error_text': None}

    @staticmethod
    def get_wind_many(api_user, api_password, meters, logger, hours=24, forecast=0):
        """
        Queries wheather data for the last X hours for all meters in the list.
        Returns meter data enriched with the wheather, per location 
        meters = [
            {
                'id': 23, 
                'melo': None, 
                'description': 'Test meter with password', 
                'manufacturer': 'Metcom', 
                'installation_date': None, 
                'is_active': True, 
                'meter_id': '10067967', 
                'ip_address': '10.0.0.1', 
                'port': 8000, 
                'voltagefactor': 1, 
                'currentfactor': 1, 
                'org': 'Test', 
                'guid': None, 
                'source': None, 
                'password': '12345678', 
                'use_password': True, 
                'timezone': 'CET', 
                'use_id': True, 
                'p01_from': None,
                'latitude': 53.0, 
                'longitude': 4.0, 
                'turbine_manufacturer': 'Enercon', 
                'turbine_type': 'E-101/3050', 
                'turbine_hub_height': 120, 
                'roughness_length': 0.2, 
                'mastr_id': None, 
                'p01_to': None, 
                'inverted': 1, 
                'p98_from': '2022-10-23T00:55:00+03:00'},
                 {}, {}
                ]
        """
        
        try:
            # Create coord_set from input metes
            coord_set = ''
            result_data = {}

            for meter in meters:
                lat = meter['latitude']
                lon = meter['longitude']
                coord_set += f'{lat},{lon}+'

                # Template for result storage
                result_data[f'{lat}:{lon}'] = { 'meter_id': meter['meter_id'] }

            coord_set = coord_set.rstrip('+')
            
            # Request data
            utcnow = datetime.datetime.utcnow()
            rounded_utcnow = utcnow.replace(minute=0, second=0, microsecond=0)

            hours = int(hours) - 1
            utcbefore = (rounded_utcnow-datetime.timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S.000+00')

            forecast = int(forecast) + 1
            utcafter = (rounded_utcnow+datetime.timedelta(hours=forecast)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

            time_string = f'{utcbefore}--{utcafter}'
            
            # coord_set = "51.5073219,-0.1276474+51.5073219,-0.1276474"

            # Every 15 minutes for the last {hours} hours
            # time = '2022-12-22T01:55:00.000+00:00--2022-12-22T02:05:00.000+00:00:PT15M'

            api = f'https://api.meteomatics.com/{time_string}:PT15M/wind_speed_149m:ms,wind_dir_10m:d,msl_pressure:hPa,t_2m:K,wind_gusts_100m_1h:ms/{coord_set}/json?model=mix'

            auth = HTTPBasicAuth(api_user,api_password)

            logger.debug(f'URL for all = "{api}"')
            result = requests.get(api, auth=auth).json()
            logger.debug(f'Result = "{result}"')

            for data_set in result['data']:
                measurement_name = data_set['parameter']
                # wind_speed_2m:ms
                # wind_dir_10m:d
                # msl_pressure:hPa
                # t_2m:C
                # wind_gusts_100m_1h:ms

                for measurement in data_set['coordinates']:
                    # {'lat': 53.507322, 'lon': -3.127647, 'dates': [{'date': '2022-12-25T01:55:00Z', 'value': 4.7}, {'date': '2022-12-25T02:10:00Z', 'value': 4.7}, {'date': '2022-12-25T02:25:00Z', 'value': 4.7}]}
                    lat = measurement['lat']
                    lon = measurement['lon']

                    result_data[f'{lat}:{lon}'][measurement_name] = []

                    for value_pair in measurement['dates']:

                        # {'date': '2022-12-25T02:25:00Z', 'value': 4.7}
                        # {'53.507322:-3.127647': { 'meter_id': "12345678", 'wind_speed_2m:ms':  [{'date': '2022-12-25T02:25:00Z', 'value': 4.7},{},{}]}
                        result_data[f'{lat}:{lon}'][measurement_name].append(value_pair)

            
            # Enrich meters object with data
            meters_enriched = []
            for meter in meters:
                meter_enriched = dict()
                lat = meter['latitude']
                lon = meter['longitude']

                # [{'date': '2022-12-25T02:25:00Z', 'value': 4.7},{},{}]
                wind_speed = result_data[f'{lat}:{lon}']['wind_speed_149m:ms']
                wind_dir_10m = result_data[f'{lat}:{lon}']['wind_dir_10m:d']
                msl_pressure = result_data[f'{lat}:{lon}']['msl_pressure:hPa']
                t_2m = result_data[f'{lat}:{lon}']['t_2m:K']
                wind_gusts_100m_1h = result_data[f'{lat}:{lon}']['wind_gusts_100m_1h:ms']

                meter_enriched = meter
                meter_enriched['wind_speed'] = wind_speed
                meter_enriched['wind_dir_10m'] = wind_dir_10m
                meter_enriched['msl_pressure'] = msl_pressure
                meter_enriched['t_2m'] = t_2m
                meter_enriched['wind_gusts_100m_1h'] = wind_gusts_100m_1h
                meter_enriched['API_mass'] = True

                meters_enriched.append(meter_enriched)
                logger.debug(f'Meter_enriched: "{meter_enriched}"')
            return meters_enriched

        except Exception as e:
            logger.error(f"'{e}' when quering API")
            return

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
        # """
        # TODO: make heights dynamic!!

        # Currently collected
        # wind_speed_149m:ms,
        # wind_dir_10m:d,
        # msl_pressure:hPa,
        # t_2m:C,
        # wind_gusts_100m_1h:ms
        
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

            if self.api_mass:
                # The data was queried already
                pass
            elif self.api_provider == 'meteomatics':
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

            f = f'/tmp/meter_{self.meter_id}'
            lines = []
            lines.append(f'variable_name,pressure,temperature,wind_speed,roughness_length\r\n')

            # TODO: MAKE IT ADJUSTABLE TO DIFFERENT HEIGHTS!!!!
            lines.append(f'height,0,2,149,0\r\n')

            if not self.api_mass:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
                pressure = self.result['weather_data']['main']['pressure']
                temp = self.result['weather_data']['main']['temp']
                wind_speed = self.result['weather_data']['wind']['speed']
                lines.append(f'{now},{pressure},{temp},{wind_speed},{self.roughness_length}\r\n')

            else:
                # meter[measurement].[value_pair, value_pair]
                # {'date': '2022-12-25T02:25:00Z', 'value': 4.7}
                # wind_speed
                # wind_dir_10m
                # msl_pressure
                # t_2m
                # wind_gusts_100m_1h

                if len(self.meter['wind_speed']) != len(self.meter['msl_pressure']) or len(self.meter['wind_speed']) != len(self.meter['t_2m']):
                    self.logger.warn(f'Different length of wheather data, please check\n {self.meter}')
                for i in range(len(self.meter['wind_speed'])):
                    now = self.meter['wind_speed'][i]['date'].replace('T',' ').rstrip('Z')
                    wind_speed = self.meter['wind_speed'][i]['value']
                    pressure = self.meter['msl_pressure'][i]['value']
                    temp = self.meter['t_2m'][i]['value']
                    lines.append(f'{now},{pressure},{temp},{wind_speed},{self.roughness_length}\r\n')

            self.logger.debug(f'Data for power calculation: {lines}')
            with open(f, 'w') as fout:
                for line in lines:
                    fout.write(line)                   

            self.weather = pd.read_csv(
                f,
                index_col=0,
                header=[0, 1],
                date_parser=lambda idx: pd.to_datetime(idx, utc=True))
            #self.logger.debug(self.weather)
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

        if self.turbine_type not in VirtualMeter.turbine_types and self.turbine_type not in VirtualMeter.own_turbine_types:
            self.logger.warn(f'Turbine "{self.turbine_type}" not in VirtualMeter.turbine_types')
            self.result['error_code'] = 1
            self.result['error_text'] = f'Turbine "{self.turbine_type}" not in VirtualMeter.turbine_types'
            return


        if self.turbine_type in VirtualMeter.turbine_types:
            # Self define a power curve cause default shuts off at 10 m/s
            turbine_data = {
                'turbine_type': self.turbine_type,
                'hub_height': self.turbine_hub_height
            }

            return WindTurbine(**turbine_data)

        elif self.turbine_type in VirtualMeter.own_turbine_types:
            self.logger.debug(f'{self.meter_id} own turbine {self.turbine_type}')

            own_turbine_data = {
                "nominal_power": 3300000,  # in W
                "hub_height": self.turbine_hub_height,  # in m
                "power_curve": VirtualMeter.own_turbine_data[self.turbine_type],
                 }

            return WindTurbine(**own_turbine_data)

    def get_power(self):
        """
        Calculates the power based on weather data
        Need to verify the wind speed is at the tube level
        """
        # initialize ModelChain with default parameters and use run_model method to calculate power output
        mc = ModelChain(self.turbine).run_model(self.weather)
        # write power output time series to WindTurbine object

        self.turbine.power_output = mc.power_output
        if self.api_mass:
            # Mass calculation returns a list of values
            # One can iterate over it and get the power
            # for i in t.power_output.values:
            #    print(i/1000) 
            self.result['power'] = self.turbine.power_output.values

        else:
            self.result['power'] = int(self.turbine.power_output.values[0]) / 1000

        self.logger.debug(f'{self.meter_id} Power: {self.result["power"]}')

        """
        if self.turbine.power_output.value_counts() != 1:
            # Expecting single value
            self.result['error_code'] = 1
            self.result['error_text'] = f'Expecting single value, received {self.turbine.power_output.values}'
            return
        else:
            self.result['power'] = self.turbine.power_output.values[0]
        """
        return


def check_error(vmeter,logger, meter):
    if vmeter.result['error_code'] != 0:
        logger.error(f'{meter["meter_id"]} code: {vmeter.result["error_code"]}, text: {vmeter.result["error_text"]}')
        sys.exit(1)
    return


def process_wind(meter, logger: logging.Logger, db, api_key=None, api_user=None, api_password=None, api_url=None, api_provider=None):
    '''
    Meta function
    Gathers wind data (one or many) and inserts it to redis
    '''
    
    
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

    if vmeter.api_mass:
        # Wind data is stored in vmeter.meter['key see below'] as a list of dicts 
        # wind_speed_2m
        # wind_dir_10m
        # msl_pressure
        # t_2m
        # wind_gusts_100m_1h
        # 't_2m' : [{'date': '2022-12-27T21:44:59Z', 'value': 15.0}, {}, {}]

        # Power data is stored in vmeter.result['power'] as a list of values [1.001, 2.002, ...]
        measurement_names = {
            'wind_speed': ('99.99.98','m/s'),
            'wind_dir_10m': ('99.99.100','degree'),
            'msl_pressure': ('99.99.96','hPa'),
            't_2m': ('99.99.97','C'),
            'wind_gusts_100m_1h': ('99.99.101','m/s')
            }

        time_stamps = []
        parsed_data = []

        try:
            for measurement in measurement_names.keys():
                logger.debug(f"Result {measurement}: {vmeter.meter[measurement]}")

                for value_pair in vmeter.meter[measurement]:
                    logger.debug(f"{vmeter.meter_id} Measurement: {measurement}, VP: {value_pair}")
                    # [{'id': '0.0.0', 'value': '1', 'unit': None, 'line_time': 'epoch'}, ... {}]
                    line_time = int(datetime.datetime.strptime(value_pair['date'], '%Y-%m-%dT%H:%M:%S%z').timestamp())

                    parsed_data.append(
                        {
                            "id": measurement_names[measurement][0], 
                            "value": value_pair['value'], 
                            "unit": measurement_names[measurement][1], 
                            "line_time": line_time
                            })

                    if measurement == 'wind_speed':
                        # Construct ts list for power measurement
                        time_stamps.append(line_time)


            for i in  range(len(vmeter.result['power'])):
                
                # TODO: How is the power is returned LE or BE?
                power = str(int(vmeter.result['power'][i])/1000)
                line_time = time_stamps[i]
                logger.debug(f"Power: {power} {line_time}")

                parsed_data.append(
                    {
                        "id": "99.99.99",
                        "value": power, 
                        "unit": "kW", 
                        "line_time": line_time
                        })

            # Push data
            meter_ts = [meter["org"].lower(), meter_id, int(time.time()), 'wind']
            inserter = iec_inserter.Inserter(logger=logger, meter_ts=meter_ts)
            inserter.insert(parsed_data)       
        except Exception as e:
            logger.error(f"Something went wrong {e}")


    else:
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
        inserter = iec_inserter.Inserter(logger=logger, meter_ts=meter_ts)
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

    # Mass processing
    if config['API']['Mass'] == 'True':
        mass = True
    else:
        mass = False

    if mass:
        hours = config['API']['hours_to_read']
        forecast = config['API']['hours_to_forecast']

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

            if mass:

                # Query in batches
                # curl -su user:pass https://api.meteomatics.com/2022-12-18T01Z,2022-12-18T01Z,2022-12-18T01Z/wind_speed_2m:ms,wind_dir_10m:d,msl_pressure:hPa,t_2m:C,wind_gusts_100m_1h:ms/47.412164,9.340652+47.512164,9.44065+47.912164,9.94065/json?route=true

                # 1. Make a request for all meters
                # 2. Enrich the meter object and set a flag not to query the api
                # 3. Process each data individually in regular thread                

                result = VirtualMeter.get_wind_many(api_user, api_password, meters_to_process, logger, hours, forecast)

                if result:

                    # Enrich the object
                    meters_to_process = result
                else:
                    logger.warning(f'Unable to process get_wind_many')
                    continue

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
