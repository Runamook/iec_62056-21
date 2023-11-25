import re
import sys
import datetime
import pytz

class Parser:

    # Everything from the beginning to the first parenthesis
    # re_id = re.compile('^(.+?)[(]')
    #  Old expression do not capture values like 1-0:31.7.0(2.414*A)
    re_id = re.compile('^(.*?:)?(.+?)[(]')
    # Integer from opening parenthesis until the closing or until the asterisk. Maybe there is a minus in the beginning
    re_value1 = re.compile('[(](-?[0-9]+\\.?[0-9]*)\\*?.*[)]')
    re_value1 = re.compile('[(](-?[0-9]+\\.?[0-9]*)\\*?.*[)]')
    # Alphanumeric from the opening parenthesis to the closing
    re_value2 = re.compile('^.+?[(](\w+)[)]$')
    # After asterisk inside parenthesis until closing one
    re_unit = re.compile('.+?[(].+\\*(.*)[)]')

    """
        MCS301 load profiles

        P.01 Load Profile 1 (e.g. 1h or 15min load profile)
        P.02 Load Profile 2 (e.g. daily load profile)
        P.03 Average Values Profile
        P.04 Max. Values Profile
        P.05 Min. Values Profile
        P.06 Harmonics Profile
        P.07 M-Bus Load Profile Channel 1 (M-Bus meter 1)
        P.08 M-Bus Load Profile Channel 2 (M-Bus meter 2)
        P.09 M-Bus Load Profile Channel 3 (M-Bus meter 3)
        P.10 M-Bus Load Profile Channel 4 (M-Bus meter 4)

        P.50 Manipulation Log file
        P.51 Communication Log file
        P.52 Power outage Log file
        P.53 Power Quality log file
        P.54 M-Bus Log file
        P.55 Load relay (disconnect) Log file
        P.56 Log file incl. registration of active energy
        P.98 Standard Log file
    """
    
    log_obis = {
        'emh_p98': '100.0.98',
        'emh_p99': '100.0.99',
        'emh_p200': '100.0.200',
        'emh_p210': '100.0.210',
        'emh_p211': '100.0.211',
        'metcom_p98_1': '101.1.98',
        'metcom_p98_2': '101.2.98',
        'metcom_p99': '101.1.99',
        'metcom_p200': '101.1.200',
        'metcom_p210': '101.1.210',
        'metcom_p211': '101.1.211'
    }

    tz_offset = dict()
    tz_offset['CET'] = '+0200'

    load_profiles = ['P.01','P.02','P.03','P.04','P.05','P.06','P.07','P.08','P.09','P.10']

    def __init__(self, raw_data: str, data_type: str, logger, **meter):
        self.logger = logger
        self.unparsed_data = raw_data
        self.parsed_data = []
        self.data_type = data_type
        self.meter_id = meter['meter_id']
        self.manufacturer = meter['manufacturer'].lower() or 'emh'

        self.timezone = meter.get('timezone') or 'CET'
        self.offset = Parser.tz_offset.get(self.timezone)
        if self.offset is None:
            self.log('ERROR', f'Unknown timezone "{self.timezone}". Please define in "{Parser.tz_offset}"')
            sys.exit(1)

        self.time_format = '%y%m%d%H%M%S %z'


    def parse(self):
        if self.data_type == 'list3':
            self._parse_list3_new()
        elif self.data_type == 'list1':
            self._parse_list1()
        elif self.data_type == 'list2':
            self._parse_list2()
        elif self.data_type == 'list4':
            self._parse_list2()
        elif self.data_type == 'p01':
            self._parseP01()            
        elif self.data_type == 'p02':
            self._parseP02()            
        elif self.data_type in ['P.98', 'p98']:
            self._parseP98()
        elif self.data_type in ['P.99', 'p99']:
            self._parseP99()
        elif self.data_type in ['P.200', 'p200']:
            self._parseP200()
        elif self.data_type in ['P.210', 'p210']:
            self._parseP210()
        elif self.data_type in ['P.211', 'p211']:
            self._parseP211()
        elif self.data_type in ['error', 'Error']:
            self._parseErrorLog()        
        else:
            self.log('ERROR', f'{self.data_type} parser not implemented')
            sys.exit(1)
        return self.parsed_data

    def log(self, severity, logstring):
        if severity == 'ERROR':
            self.logger.error(f'{self.meter_id} {logstring}')
        elif severity == 'WARN':
            self.logger.warning(f'{self.meter_id} {logstring}')
        elif severity == 'INFO':
            self.logger.info(f'{self.meter_id} {logstring}')
        elif severity == 'DEBUG':
            self.logger.debug(f'{self.meter_id} {logstring}')

    def _parse_list3_new(self):
        """
        Metcom_new
        1.0.0(07E50113020E0A2E00003C00)

        EMH
        [{id: '8.8.0', value: '00008.423', unit: 'kvarh'}, {id: '0.2.0', value: '26500000', unit: None}, ...]
        8.8.0(00008.423*kvarh)
        0.2.0(26500000)
        0.2.1*50(20092541)
        0.3.0(5000.0000*Imp/kWh)
        F.F(00000000)
        0.2.1*01(20100843)
        0.2.1*02(26510100)
        0.2.1*50(20100843)
        2.35.0.01(7.0000*kW)
        Data set = id/address(value*unit)

        Id/address -  16 printable characters maximum with the exception of # (, ), /, and !.
        is taken from the identification code in the glossary system of the equipment concerned.

        Value: 32 printable characters maximum with the exception of (, ), *, / and !. For decimal values,
        only points (not commas) shall be used and shall be counted as characters.

        The separator character "*" between value and unit is not needed if there are no units.

        Unit: 16 printable characters maximum except for (, ), / and !.
        """

        # Data line = data set * N
        re_id = re.compile('^(.+?)[(]')
        re_value1 = re.compile('[(]([0-9]+\\.?[0-9]*)\\*?.*[)]')
        re_value2 = re.compile('^.+?[(](\w+)[)]$')
        re_unit = re.compile('.+?[(].+\\*(.*)[)]')

        pre_parsed = self._find_data_blocks()

        if pre_parsed.get('P.99'):
            self._parseP99(pre_parsed['P.99'])
        if pre_parsed.get('P.01'):
            self._parseP01(pre_parsed['P.01'])
        if pre_parsed.get('list'):
            for line in pre_parsed['list']:
                parsed_line = {
                    'id': None,
                    'value': None,
                    'unit': None
                }
                try:
                    parsed_line['id'] = re_id.search(line).groups()[0]
                    if re_value1.search(line):
                        # TODO: WARNING re matches 'C.90.2(70D4EF6C)' value as '70'
                        value = re_value1.search(line).groups()[0]
                    else:
                        value = re_value2.search(line).groups()[0]
                    parsed_line['value'] = value
                    if re_unit.search(line):
                        unit = re_unit.search(line).groups()[0]
                        parsed_line['unit'] = unit
                    self.parsed_data.append(parsed_line)
                except Exception as e:
                    self.log('ERROR', f'{e} while processing {line}')
                    continue


    def _parse_list1(self):
        """
        Metcom list1

            F.F(00000000)
            0.0.0(10067967)
            0.0.1(10067967)
            0.9.1(121752)
            0.9.2(221113)
            0.1.0(12)
            0.1.2(2211010000)
            0.1.2*12(2211010000)
            0.1.2*11(2210010000)
            0.1.2*10(2209010000)
            1.6.1(0.50262*kW)(2211120730)
            1.6.1*12(0.39912*kW)(2210130900)
            1.6.1*11(0.74906*kW)(2209281400)
            1.6.1*10(0.49578*kW)(2208111330)
            2.6.1(0.00000*kW)(2211010000)
            2.6.1*12(0.00000*kW)(2210010000)
            2.6.1*11(0.00000*kW)(2209010000)
            2.6.1*10(0.00000*kW)(2208010000)
            1.8.0(01280.7125*kWh)
            1.8.0*12(01236.1958*kWh)
            1.8.0*11(01158.9747*kWh)
            1.8.0*10(01097.4085*kWh)
            2.8.0(00000.0000*kWh)
            2.8.0*12(00000.0000*kWh)
            2.8.0*11(00000.0000*kWh)
            2.8.0*10(00000.0000*kWh)
            5.8.0(00049.1783*kvarh)
            5.8.0*12(00048.8006*kvarh)
            5.8.0*11(00045.9754*kvarh)
            5.8.0*10(00041.7958*kvarh)
            6.8.0(00000.0000*kvarh)
            6.8.0*12(00000.0000*kvarh)
            6.8.0*11(00000.0000*kvarh)
            6.8.0*10(00000.0000*kvarh)
            7.8.0(00000.0000*kvarh)
            7.8.0*12(00000.0000*kvarh)
            7.8.0*11(00000.0000*kvarh)
            7.8.0*10(00000.0000*kvarh)
            8.8.0(00079.8389*kvarh)
            8.8.0*12(00075.0837*kvarh)
            8.8.0*11(00062.7016*kvarh)
            8.8.0*10(00050.2358*kvarh)
            0.3.3(3000)
            0.2.2(00000001)
            0.2.0(01.01.28)
            0.2.0(02.02.13)
            0.2.0(2.2.8)
            !
        :return:
        """
        pre_parsed = self._find_data_blocks()

        if pre_parsed.get('list'):
            for line in pre_parsed['list']:
                parsed_line = {
                    'id': None,
                    'value': None,
                    'unit': None
                }
                try:
                    # re_id may return one or two groups - actual OBIS would be in the last
                    parsed_line['id'] = Parser.re_id.search(line).groups()[-1]

                    if '*' in parsed_line['id']:
                        # Skip history data like
                        # 0.1.2*12(2211010000)\r\n
                        # 0.1.2*11(2210010000)\r\n
                        # 0.1.2*10(2209010000)\r\n
                        # 1.6.1(0.50262*kW)(2211120730)\r\n
                        # 1.6.1*12(0.39912*kW)(2210130900)\r\n
                        # 1.6.1*11(0.74906*kW)(2209281400)\r\n
                        # 1.6.1*10(0.49578*kW)(2208111330)\r\n
                        continue

                    if Parser.re_value1.search(line):
                        # TODO: WARNING re matches 'C.90.2(70D4EF6C)' value as '70'
                        # TODO: WARNING re matches '0.0.0(1EMH0010134075)' value as '1'
                        value = Parser.re_value1.search(line).groups()[0]
                    else:
                        value = Parser.re_value2.search(line).groups()[0]
                    parsed_line['value'] = value
                    if Parser.re_unit.search(line):
                        unit = Parser.re_unit.search(line).groups()[0]
                        parsed_line['unit'] = unit
                    self.parsed_data.append(parsed_line)
                except Exception as e:
                    self.log('ERROR', f'{e} while processing {line}')
                    continue


    def _parse_list2(self):
        """
        Metcom_new list2
        1.0.0(07E50113020B031D00003C00)
        32.7.0(58.50*V)
        52.7.0(58.95*V)
        72.7.0(59.09*V)
        31.7.0(0.604*A)
        51.7.0(0.610*A)
        71.7.0(0.606*A)
        81.7.0(0.0*deg)
        81.7.10(119.5*deg)
        81.7.20(-120.0*deg)
        81.7.4(-178.4*deg)
        81.7.15(-178.3*deg)
        81.7.26(-178.0*deg)
        !

        Add
        1-0:72.7.0(58.28*V)^M
        1-0:71.7.0(2.408*A)^M
        1-0:81.7.0(0.0*deg)^M
        1-0:81.7.2(-120.1*deg)^M
        1-0:14.7.0(0.04995*kHz)^M

        TODO: 1-0:1.8.0(00000391.3*Wh)^M\r\n


        
        :return:
        """
        pre_parsed = self._find_data_blocks()

        if pre_parsed.get('list'):
            for line in pre_parsed['list']:
                parsed_line = {
                    'id': None,
                    'value': None,
                    'unit': None
                }
                try:
                    # re_id may return one or two groups - actual OBIS would be in the last
                    # Filter out results with '..', '/', count('.') > 2 [not used]
                    
                    id_line = Parser.re_id.search(line).groups()[-1]

                    if '..' in id_line or '/' in id_line:
                        # The obis code is incorect - raise and continue
                        raise BaseException
                    else:
                        parsed_line['id'] = id_line
    
                    if Parser.re_value1.search(line):
                        # TODO: WARNING re matches 'C.90.2(70D4EF6C)' value as '70'
                        # TODO: WARNING re matches '0.0.0(1EMH0010134075)' value as '1'
                        value = Parser.re_value1.search(line).groups()[0]
                    else:
                        value = Parser.re_value2.search(line).groups()[0]
                    parsed_line['value'] = value
                    if Parser.re_unit.search(line):
                        unit = Parser.re_unit.search(line).groups()[0]
                        parsed_line['unit'] = unit
                    self.parsed_data.append(parsed_line)
                except Exception as e:
                    self.log('ERROR', f'{e} while processing {line}')
                    continue

    def _parseErrorLog(self):
        """
        Parses the error log
        
        EMH returns string 'F.F(00000000)'
        
        TODO: Metcom retuns something else
        """


        if self.manufacturer == 'emh':

            line = self.unparsed_data.strip()
            if '\r\n' in line:
                self.log('ERROR', f'Meter returned something unexpected in F.F log: "{line}"')
                sys.exit(1)
            elif 'F.F' not in line:
                self.log('ERROR', f'Meter returned something unexpected in F.F log: "{line}"')
                sys.exit(1)
            else:
                try:
                    # F.F(00000000)
                    log_record = line.split('(')[1].strip(')')

                    self.parsed_data.append({'id': 'F.F', 'value': log_record, 'unit': 'log'})
                except Exception as e:
                    self.log('ERROR', f'Something went wrong when parsing ErrorLog {line}')
                    sys.exit(1)
        return

    def _parseP98(self):
        """
        EMH

            P.98([z]YYMMDDhhmmss)(SSSSSSSS)()(k)[(<Kenn1>)()..[(identk>)()]][(<value1 >)..[(<valuek>)]]<CR><LF>
            P.98([z]YYMMDDhhmmss)(SSSSSSSS)()(k)[(<Kenn1>)()..[( identk>)()]][(<value1 >)..[(<valuek>)]]<CR><LF>

            2022-10-23 02:07:36,738 __main__     DEBUG    08032332 192.168.104.12:8000 P.98(1220826235646)(00008020)()(2)(0.9.1)()(0.9.2)()(1235703)(1220826)
            P.98(1220828235723)(00008020)()(2)(0.9.1)()(0.9.2)()(1235710)(1220828)
            P.98(1220829235716)(00008020)()(2)(0.9.1)()(0.9.2)()(1235706)(1220829)
            P.98(1220901000000)(00000010)()(0)
            P.98(1220906115553)(00000080)()(0)
            P.98(1220906120814)(00000040)()(0)
            P.98(1220910235706)(00008020)()(2)(0.9.1)()(0.9.2)()(1235654)(1220910)
            P.98(1220915214214)(00008020)()(2)(0.9.1)()(0.9.2)()(1214203)(1220915)
            P.98(1220927000028)(00008020)()(2)(0.9.1)()(0.9.2)()(1000013)(1220927)
            P.98(1221001000000)(00000010)()(0)
            P.98(1221003235705)(00008020)()(2)(0.9.1)()(0.9.2)()(1235650)(1221003)
            P.98(1221016235720)(00008020)()(2)(0.9.1)()(0.9.2)()(1235709)(1221016)
            P.98(1221020125609)(00008020)()(2)(0.9.1)()(0.9.2)()(1125620)(1221020)
            z:  Season-codes: 0 = normal time, 1 = summer time, 2 = UTC
                Note: Depending on the setting of the meter the output of this value can be controlled.
                It is then accepted as z=2 or depending on the season z=0 or 1.
            YYMMDDhhmmss: Time stamp of the log book entry. 
                In the case of a time adjustment this time stamp gives the time before the adjustment.
            SSSSSSSS: Status in form of a 32-Bit length ASCII-HEX-number.
                The high quality nibble (Bits 31..28) is on the left, 
                the low quality nibble (Bits 3..0) on the right. 
                The meaning of the bits is clear from the following table.
            k: Number of values which will follow. 
                If it is k=0, then the information about ident n and valuen is left out.
            identn: Recognition of the value (OBIS code). 
                In case of a time adjustment, these values correspond to the values after the adjustment.
            valuen: value

        Metcom
            KZ(ZSTs13)(S)()(z)(KZ1)(E1)(KZ2)(E2)Data 1 Data 2
            P.98(1220906234907)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(5)(0)
            P.98(1220919161837)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(17)(1)
            
            KZ OBIS-Identifier "P.98"
            ZSTs13 Time stamp format of the entry
            S Status Byte -> always: 0000
            z Number of additional log file data
            KZ1 Identifier of the additional log file data -> 96.11.0 (Standard log)
                Remark: from FW 02.12 onwards the identifier 96.11.0 can be defined as C.11.0 too (configuration)
            KZ2 Identifier of the additional log file data -> 96.11.10 (Standard log)
                Remark: from FW 02.12 onwards the identifier 96.11.10 can be defined as C.11.10 too (configuration)
            E1 Units of additional log file data -> always: ()
            E2 Units of additional log file data -> always: ()
            D1 additional log file data -> (Status information, see below)
            D2 (0) (Standard log)

        """

        if self.manufacturer == 'emh':

            # P.98(1041007095703)(00002000)()(0) find 1041007095703
            re_log_ts = re.compile('^P.98[(](\d+?)[)]')
            # P.98(1041007095703)(00002000)()(0) find 00002000
            # EMH
            re_log_record = re.compile('^.+[(]\d+?[)][(](\d+?)[)]')

            for log_line in self.unparsed_data.split('\r\n'):
                try:
                    log_line = log_line.strip()

                    # Parse only lines starting from "^P.98"
                    if not log_line.startswith('P.98'):
                        continue
                    else:

                        # This part didn't work well on some new EMH logs like:
                        # P.98(1220829235716)(00008020)()(2)(0.9.1)()(0.9.2)()(1235706)(1220829)
                        # Patch using split instead of re

                        # P.98(1041007095703)(00002000)()(0)
                        # log_ts = 1041007095703
                        # log_record = 00002000
                        # log_ts = re_log_ts.search(log_line).groups()[0][1:]     # strip left-most digit (usually '1')
                        # log_record = re_log_record.search(log_line).groups()[0]

                        log_ts = log_line.split('(')[1].strip(')')[1:]
                        log_record = log_line.split('(')[2].strip(')')

                        # Take meter TZ and make timestamp UTC
                        log_ts_parsed = datetime.datetime.strptime(f"{log_ts} {self.offset}", self.time_format)

                        parsed_line = {
                            'id': Parser.log_obis['emh_p98'],
                            'value': log_record,
                            'unit': None,
                            'line_time': (log_ts_parsed).strftime('%s')
                        }
                        self.parsed_data.append(parsed_line)
                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P98 line parsing "{log_line}"')
                    sys.exit(1)

            # [
            #   {'id': '100.0.98', 'value': '00002000', 'unit': None, 'line_time': '1097135823'}, 
            #   {'id': '100.0.98', 'value': '00004000', 'unit': None, 'line_time': '1097135823'}, 
            #   {'id': '100.0.98', 'value': '00000100', 'unit': None, 'line_time': '1097135887'}, 
            #   {'id': '100.0.98', 'value': '00000080', 'unit': None, 'line_time': '1097135954'}
            # ]
            # There may be a situation where two log events would happen in the same time.
            # I will add one second to one of them

            result = []
            for parsed_line in self.parsed_data:
                ts = parsed_line['line_time']
                if ts in result:
                    # This time already exists in the result
                    # Remove the line and increase by one, fingers crossed
                    self.parsed_data.remove(parsed_line)
                    parsed_line['line_time'] = f"{int(parsed_line['line_time'])+1}"
                    self.parsed_data.append(parsed_line)
                else:
                    result.append(ts)

            return

        elif self.manufacturer == 'metcom':
            # Metcom
            # P.98(1220906234907)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(5)(0)
            # P.98(1220919161837)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(17)(1)
            re_log_ts = re.compile('^P.98[(](\d+?)[)]')

            for log_line in self.unparsed_data.split('\r\n'):
                try:
                    log_line = log_line.strip()

                    # Parse only lines starting from "^P.98"
                    if not log_line.startswith('P.98'):
                        continue
                    else:

                        # P.98(1220906234907)(00)()(2)(0-0:C.11.0)()(0-0:C.11.10)()(5)(0)
                        # log_ts = 1220906234907
                        # log_data_1 = 5
                        # log_data_2 = 0
                        log_ts = re_log_ts.search(log_line).groups()[0][1:]     # strip left-most digit (usually '1')
                        log_record_1 = log_line.split('(')[-2].strip(')')
                        log_record_2 = log_line.split('(')[-1].strip(')')

                        # Take meter TZ and make timestamp UTC
                        log_ts_parsed = datetime.datetime.strptime(f"{log_ts} {self.offset}", self.time_format)

                        parsed_line_1 = {
                            'id': Parser.log_obis['metcom_p98_1'],
                            'value': log_record_1,
                            'unit': None,
                            'line_time': (log_ts_parsed).strftime('%s')
                        }
                        parsed_line_2 = {
                            'id': Parser.log_obis['metcom_p98_2'],
                            'value': log_record_2,
                            'unit': None,
                            'line_time': (log_ts_parsed).strftime('%s')
                        }
                        self.parsed_data.extend([parsed_line_1, parsed_line_2])
                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P98 line parsing "{log_line}"')
                    sys.exit(1)

        else:
            self.log('ERROR', f'Unknown manufacturer {self.manufacturer}, expecting one of ["emh", "metcom"]')
            sys.exit(1)            

    def _parseP99(self, raw_line):
        """
        :param raw_line:
        :return: [{id: '8.8.0', value: '00008.423', unit: 'kvarh'}...]

        P.99(1201021132243)(00002000)()(0)
        1. Entry without value
        P.99([z]YYMMDDhhmmss)(SSSSSSSS)()(0)(< identifier>)<CR><LF>
        2. Entry with values
        P.99([z]YYMMDDhhmmss)(SSSSSSSS)()(1)(< identifier >)(unit)(<old value>;<new value>)<CR><LF>
        The values mean:
        z:  Season-identification: 0 = normal time 1 = summer time, 2 = UTC
        Note: Depending on the setting of the meter the output of this value can be controlled.
        It is then accepted as z=2 or, depending on the season, z=0 or 1.
        YYMMDDhhmmss:   Timestamp of the log book entry. In case of a clock adjustment, this time stamp gives the
        time before the adjustment.
        SSSSSSSS:   Status in form of a 32-Bit length ASCII-HEX-number.
        The high quality nibble (Bits 31..28) is on the left, the low quality nibble (Bits 3..0) on the
        right. The meaning of the bits is clear from the following table.
        1:  Number of the changed parameters
        Recognition:    Recognition of the changed values (OBIS code)
        Old value:  Value before change
        New value:  Value after change
        """

        re_log_ts = re.compile('^.+[(](\d+?)[)]')
        re_log_record = re.compile('^.+[(]\d+?[)][(](\d+?)[)]')
        result = []

        try:
            log_ts = re_log_ts.search(raw_line).groups()[0]
            log_record = re_log_record.search(raw_line).groups()[0]

            # print('{0:b}'.format(int('2000', base=16)))
            # print(f'{int(log_record[:4], base=16):b}')
            # print(f'{int(log_record[4:], base=16):b}')
            pre_bin_log = bin(int(log_record, base=16))[2:]
            bin_log = '0' * (32 - len(pre_bin_log)) + pre_bin_log

            for i in range(32):
                log_bit = {'id': f'p99_bit{i}', 'value': bin_log[::-1][i], 'unit': None}
                self.parsed_data.append(log_bit)
            return result

        except Exception as e:
            self.log('ERROR', f'{e} while processing "{raw_line}"')
            return None

    def _parseP200(self, raw_line):
        
        return

    def _parseP210(self, raw_line):
        
        return

    def _parseP211(self, raw_line):
        
        return                

    def _parseP01(self):
        """
        P.01(0210114223000)(00000000)(15)(6)(1.5)(kW)(2.5)(kW)(5.5)(kvar)(6.5)(kvar)(7.5)(kvar)(8.5)(kvar)
        (0.00063)(0.00000)(0.00023)(0.00000)(0.00000)(0.00000)
        (0.02093)(0.00000)(0.00184)(0.00000)(0.00000)(0.00033)
        (0.03719)(0.00000)(0.00068)(0.00000)(0.00000)(0.00103)
        (0.05106)(0.00001)(0.00086)(0.00001)(0.00000)(0.00098)
        (0.01038)(0.00000)(0.00154)(0.00000)(0.00000)(0.00011)
        (0.02948)(0.00000)(0.00085)(0.00000)(0.00000)(0.00061)
        (0.03728)(0.00000)(0.00145)(0.00000)(0.00000)(0.00063)
        P.01(0210115001500)(00000000)(15)(6)(1.5)(kW)(2.5)(kW)(5.5)(kvar)(6.5)(kvar)(7.5)(kvar)(8.5)(kvar)
        (0.00788)(0.00000)(0.00103)(0.00000)(0.00000)(0.00010)
        (0.00121)(0.00000)(0.00050)(0.00000)(0.00000)(0.00000)
        (0.11853)(0.00000)(0.00083)(0.00000)(0.00000)(0.00338)
        :return: None
        Function updates self.parsed_data

        KZ(ZSTs13)(S)(RP)(z)(KZ1)(E1 ).. (KZz)(Ez)
        (Mw1)...(Mwz)

        KZ          OBIS-Identifier "P.01" or “P.02” or “P.03” .....
        ZSTs13      Time stamp format of the oldest measured value
        S           Profile status word
        RP          Registration period in minutes
        z           Number of different measured values in one registration period
        KZn         Identifier of the measured values (without tariff particulars or preceding-value Identifier)
        En          Units of measured values
        Mwn         Measured values
        """
        
        # TODO: Parse P01 with error
        #
        #  Ends with <CR><LF>(0.03746)(0.00000)(0.00000)(0.00000)(0.00000)(0.00926)<CR><LF>(0.04063)(0.00000)(0.00000)(0.00000)(0.00000)(0.00918)<CR><LF>(0.03770)(0.
        #    00000)(0.00000)(0.00000)(0.00000)(0.00923)<CR><LF><ETX>~<SOH>B0<ETX>

        # TODO: Parse P01 with another error
        # 2023-09-22 12:06:42,266 __main__     DEBUG    10132380 10.179.31.196:5000 Meter -> HHU (Tr = 4): 
        # "<STX>P.01(1230920203000)(08)(15)(6)(1-0:1.5.0)(kW)(1-0:2.5.0)(kW)(1-0:5.5.0)(kvar)(1-0:6.5.0)(kvar)(1-0:7.5.0)(kvar)(1-0:8.5.0)(kvar)<CR><LF>
        # (0.26)(0.00)(0.00)(0.00)(0.00)(0.05)<CR><LF>
        # (0.17)(0.00)(0.03)(0.00)(0.00)(0.00)<CR><LF>
        # (0.17)(0.00"
        # Terminated by timeout prematurely
        # (0.10)(0.00)(0.01)(0.00)(0.00)(0.05)^M
        # (0.22)(0.00)(0.01)(0.00)(0.00)(0.03)^M
        # (0.17)(0.00
        # 2023-09-22 12:06:42,268 __main__     ERROR    10132380 Expected z=6 values, found 2 in line "['0.17)', '0.00']"
        # Stop parsing and return last date

        data = self.unparsed_data.split('\n')
        for line in data:

            if line[0:4] in self.load_profiles:
                # Parse header line
                # Metcom
                # [
                #   'P.01', 
                #   '1220403160000)', 
                #   '08)', 
                #   '15)', 
                #   '6)', 
                #   '1-0:1.5.0)', 'kW)', 
                #   '1-0:2.5.0)', 'kW)', 
                #   '1-0:5.5.0)', 'kvar)', 
                #   '1-0:6.5.0)', 'kvar)', 
                #   '1-0:7.5.0)', 'kvar)', 
                #   '1-0:8.5.0)', 'kvar)'
                # ]
                # 
                # EMH meter
                # P.01(1220823161500)(00000000)(15)(6)(1.5)(kW)(2.5)(kW)(5.5)(kvar)(6.5)(kvar)(7.5)(kvar)(8.5)(kvar)
                # P.01([z]YYMMDDhhmmss)(SSSSSSSS)(r)(k)(K1)(E1)..[(Kk)(Ek)](x...x)...[(y...y)]
                # Or
                # P.01(1221005001500)(00000000)(15)(8)(1-1:1.29)(kWh)(1-1:2.29)(kWh)(1-1:5.29)(kvarh)(1-1:6.29)(kvarh)(1-1:7.29)(kvarh)(1-1:8.29)(kvarh)(1-2:1.29)(kWh)(1-3:2.29)(kWh)^M
                
                line_number = 0
                try:
                    line = line.split('(')
                    kz = line[0]

                    # Take meter TZ and make timestamp UTC
                    zsts13 = datetime.datetime.strptime(f"{line[1].strip(')')[1:]} {self.offset}", self.time_format)


                    if self.manufacturer == 'metcom':
                        # '08)' => '00001000'
                        # Bit
                        # 7 PDN Power down
                        # 6 RSV Reserved
                        # 5 CAD Clock adjusted
                        # 4 RSV Reserved
                        # 3 DST Daylight saving
                        # 2 DNV Data not valid
                        # 1 CIV Clock invalid
                        # 0 ERR Critical error
                        s = format((int(line[2].strip(')'))), '08b')
                    else:
                        # 'emh'
                        # '00000000)'
                        s =  line[2].strip(')')

                    # '15)'
                    rp = datetime.timedelta(minutes=int(line[3].strip(')')))

                    # '6)' - amount of values in a line
                    z = int(line[4].strip(')'))

                    if z != 6 and z != 8:
                        self.log('WARN', f'Not expecting z other than 6 or 8, z={z} received. Update the parser code. {line}')
                        sys.exit(1)
                    
                    ids = list()
                    units = list()
                    

                    # Values in line with index 0-4 are a constant prefix like
                    # ['P.01', '1221002001500)', '00000000)', '15)', '8)',
                    # Values with index 5+ are a variable key list like
                    # '1-1:1.29)', 'kWh)', '1-1:2.29)', 'kWh)', '1-1:5.29)', 'kvarh)', '1-1:6.29)', 'kvarh)', '1-1:7.29)', 'kvarh)', '1-1:8.29)', 'kvarh)', '1-2:1.29)', 'kWh)', '1-3:2.29)', 'kWh)'
                    if self.manufacturer == 'metcom':
                        # z = 6 => range(5,16,2)
                        # z = 8 => range(5,20,2)
                        for i in range(5,4+z*2,2):
                            # '1.5)'
                            # 'kW)'
                            ids.append(line[i].strip().strip(')').split(':')[1])
                            units.append(line[i+1].strip().strip(')'))
                    else:
                        # 'emh'
                        # z = 6 => range(5,16,2)
                        # z = 8 => range(5,20,2)
                        for i in range(5,4+z*2,2):
                        
                            # '1-0:1.5.0)'
                            # 'kW)'
                            ids.append(line[i].strip().strip(')'))
                            units.append(line[i+1].strip().strip(')'))                        

                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 header parsing "{line}"')
                    sys.exit(1)
            else:
                try:
                    # Parse data line
                    # (0.00063)(0.00000)(0.00023)(0.00000)(0.00000)(0.00000)
                    # or 
                    # # (0.00000)(0.04088)(0.00000)(0.00358)(0.00000)(0.00000)(0.00000)(0.00000)

                    if len(line) < 2:
                        self.log('DEBUG', f'Line "{line}" to short, skipping')
                        # Probably, end of message
                        return

                    line = line.split('(')
                    line.pop(0)

                    if len(line) != z:
                        self.log('ERROR', f'Expected z={z} values, found {len(line)} in line "{line}"')
                        sys.exit(1)
                    
                    for i in range(z):
                        parsed_line = {
                            'id': ids[i],
                            'value': line[i].strip().strip(')'),
                            'unit': units[i],
                            'line_time': (zsts13 + rp * line_number).strftime('%s')
                        }

                        # Match parsed_line['value'] with regex \d+\.\d+\. and skip incorrect value
                        if not re.match(r'(\d+\.\d+$)', parsed_line['value']):
                            self.log('ERROR', f'Expected float value, found "{parsed_line["value"]}" in line "{line}"')
                            # sys.exit(1)
                            continue
                        
                        self.parsed_data.append(parsed_line)
                    line_number += 1
                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 line parsing "{line}"')
                    sys.exit(1)

    def _parseP02(self):
        """
        P.02(0231122000000)(00)(1440)(6)(1-0:1.8.0)(kWh)(1-0:2.8.0)(kWh)(1-0:5.8.0)(kvarh)(1-0:6.8.0)(kvarh)(1-0:7.8.0)(kvarh)(1-0:8.8.0)(kvarh)
        (02704.2331)(00000.0000)(00095.7249)(00000.0000)(00000.0000)(00203.1329)
        P.02(0231123000000)(00)(1440)(6)(1-0:1.8.0)(kWh)(1-0:2.8.0)(kWh)(1-0:5.8.0)(kvarh)(1-0:6.8.0)(kvarh)(1-0:7.8.0)(kvarh)(1-0:8.8.0)(kvarh)
        (02709.9431)(00000.0000)(00095.7401)(00000.0000)(00000.0000)(00203.2860)
        P.02(0231124000000)(00)(1440)(6)(1-0:1.8.0)(kWh)(1-0:2.8.0)(kWh)(1-0:5.8.0)(kvarh)(1-0:6.8.0)(kvarh)(1-0:7.8.0)(kvarh)(1-0:8.8.0)(kvarh)
        (02717.5189)(00000.0000)(00096.3410)(00000.0000)(00000.0000)(00203.3189)
        P.02(0231125000000)(00)(1440)(6)(1-0:1.8.0)(kWh)(1-0:2.8.0)(kWh)(1-0:5.8.0)(kvarh)(1-0:6.8.0)(kvarh)(1-0:7.8.0)(kvarh)(1-0:8.8.0)(kvarh)
        (02725.7063)(00000.0000)(00096.8274)(00000.0000)(00000.0000)(00203.3904)
        :return: None
        Function updates self.parsed_data

        KZ(ZSTs13)(S)(RP)(z)(KZ1)(E1 ).. (KZz)(Ez)
        (Mw1)...(Mwz)

        KZ          OBIS-Identifier "P.01" or “P.02” or “P.03” .....
        ZSTs13      Time stamp format of the oldest measured value
        S           Profile status word
        RP          Registration period in minutes
        z           Number of different measured values in one registration period
        KZn         Identifier of the measured values (without tariff particulars or preceding-value Identifier)
        En          Units of measured values
        Mwn         Measured values
        """

        data = self.unparsed_data.split('\n')
        for line in data:

            if line[0:4] in self.load_profiles:
                # Parse header line
                # Metcom
                # [
                #   'P.02', 
                #   '0231122000000)', 
                #   '00)', 
                #   '1440)', 
                #   '6)', 
                #   '1-0:1.8.0)', 'kW)', 
                #   '1-0:2.8.0)', 'kW)', 
                #   '1-0:5.8.0)', 'kvar)', 
                #   '1-0:6.8.0)', 'kvar)', 
                #   '1-0:7.8.0)', 'kvar)', 
                #   '1-0:8.8.0)', 'kvar)'
                # ]

                line_number = 0
                try:
                    line = line.split('(')
                    kz = line[0]

                    # Take meter TZ and make timestamp UTC
                    
                    time_line = line[1].strip(')')[1:-6] # 231122
                    tz = pytz.timezone(self.timezone)

                    time_format = '%y%m%d'
                    zsts13 = datetime.datetime.strptime(time_line, time_format)
                    zsts13 = tz.localize(zsts13)

                    if self.manufacturer == 'metcom':
                        # '08)' => '00001000'
                        # Bit
                        # 7 PDN Power down
                        # 6 RSV Reserved
                        # 5 CAD Clock adjusted
                        # 4 RSV Reserved
                        # 3 DST Daylight saving
                        # 2 DNV Data not valid
                        # 1 CIV Clock invalid
                        # 0 ERR Critical error
                        s = format((int(line[2].strip(')'))), '08b')
                    else:
                        # 'emh'
                        # '00000000)'
                        s =  line[2].strip(')')

                    # '1440)'
                    rp = datetime.timedelta(minutes=int(line[3].strip(')')))

                    # '6)' - amount of values in a line
                    z = int(line[4].strip(')'))

                    if z != 6 and z != 8:
                        self.log('WARN', f'Not expecting z other than 6 or 8, z={z} received. Update the parser code. {line}')
                        sys.exit(1)
                    
                    ids = list()
                    units = list()
                    

                    # Values in line with index 0-4 are a constant prefix like
                    # ['P.01', '1221002001500)', '00000000)', '15)', '8)',
                    # Values with index 5+ are a variable key list like
                    # '1-1:1.29)', 'kWh)', '1-1:2.29)', 'kWh)', '1-1:5.29)', 'kvarh)', '1-1:6.29)', 'kvarh)', '1-1:7.29)', 'kvarh)', '1-1:8.29)', 'kvarh)', '1-2:1.29)', 'kWh)', '1-3:2.29)', 'kWh)'
                    if self.manufacturer == 'metcom':
                        # z = 6 => range(5,16,2)
                        # z = 8 => range(5,20,2)
                        for i in range(5,4+z*2,2):
                            # '1.5)'
                            # 'kW)'
                            ids.append(line[i].strip().strip(')').split(':')[1])
                            units.append(line[i+1].strip().strip(')'))
                    else:
                        # 'emh'
                        # z = 6 => range(5,16,2)
                        # z = 8 => range(5,20,2)
                        for i in range(5,4+z*2,2):
                        
                            # '1-0:1.5.0)'
                            # 'kW)'
                            ids.append(line[i].strip().strip(')'))
                            units.append(line[i+1].strip().strip(')'))                        

                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 header parsing "{line}"')
                    sys.exit(1)
            else:
                try:
                    # Parse data line
                    # (0.00063)(0.00000)(0.00023)(0.00000)(0.00000)(0.00000)
                    # or 
                    # # (0.00000)(0.04088)(0.00000)(0.00358)(0.00000)(0.00000)(0.00000)(0.00000)

                    if len(line) < 2:
                        self.log('DEBUG', f'Line "{line}" to short, skipping')
                        # Probably, end of message
                        return

                    line = line.split('(')
                    line.pop(0)

                    if len(line) != z:
                        self.log('ERROR', f'Expected z={z} values, found {len(line)} in line "{line}"')
                        sys.exit(1)
                    
                    for i in range(z):
                        parsed_line = {
                            'id': ids[i],
                            'value': line[i].strip().strip(')'),
                            'unit': units[i],
                            'line_time': (zsts13 + rp * line_number).strftime('%s')
                        }

                        # Match parsed_line['value'] with regex \d+\.\d+\. and skip incorrect value
                        if not re.match(r'(\d+\.\d+$)', parsed_line['value']):
                            self.log('ERROR', f'Expected float value, found "{parsed_line["value"]}" in line "{line}"')
                            # sys.exit(1)
                            continue
                        
                        self.parsed_data.append(parsed_line)
                    line_number += 1
                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 line parsing "{line}"')
                    sys.exit(1)


    def _find_data_blocks(self):

        '''
        Fix 
        1-0:72.7.0(58.28*V)
        1-0:71.7.0(2.408*A)
        1-0:81.7.0(0.0*deg)
        1-0:81.7.2(-120.1*deg)
        1-0:14.7.0(0.04995*kHz)
        
        '''
        splitted_data = self.unparsed_data.split('\r\n')
        
        # Table 1 provides F.F error register in the first line,
        # Other tables may provide meter name
        if not 'F.F' in splitted_data[0]:
            splitted_data = splitted_data[1:]

        pre_parsed = dict()
        
        # Old pattern
        # re_list_pattern1 = re.compile('^\w+\\.\w.*?[(].*?[)]')

        re_list_pattern1 = re.compile('^(.+?-.+?:)?\w+\\.\w.*?[(].*?[)]')
        p01_started = False

        try:
            for line in splitted_data:
                if line.startswith('/'):
                    # Skip header
                    self.log('DEBUG', f'Skipping header {line}')
                    continue
                elif len(line) < 5 and '!' in line:
                    # Probably the end of the message
                    self.log('DEBUG', f'Blocks found: {list(pre_parsed.keys())}')
                    self.log('DEBUG', pre_parsed)
                    self.log('DEBUG', f'End of the message found in {line}')
                    return pre_parsed
                elif line.startswith('P.99'):
                    pre_parsed['P.99'] = line
                elif p01_started:
                    pre_parsed['P.01'].append(line)
                elif line.startswith('P.01'):
                    p01_started = True
                    if pre_parsed.get('P.01'):
                        pre_parsed['P.01'].append(line)
                    else:
                        pre_parsed['P.01'] = [line]
                elif re_list_pattern1.search(line):
                    if pre_parsed.get('list'):
                        pre_parsed['list'].append(line)
                    else:
                        pre_parsed['list'] = [line]
        except Exception as e:
            self.log('ERROR', e)
            sys.exit(1)
        self.log('DEBUG', f'Did not find ! in the end of the message')
        return pre_parsed