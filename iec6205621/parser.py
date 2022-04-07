import re
import sys
import datetime

class Parser:

    # Everything from the beginning to the first parenthesis
    re_id = re.compile('^(.+?)[(]')
    # Integer from opening parenthesis until the closing or until the asterisk. Maybe there is a minus in the beginning
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
    """
    
    tz_offset = dict()
    tz_offset['CET'] = '+0200'

    load_profiles = ['P.01','P.02','P.03','P.04','P.05','P.06','P.07','P.08','P.09','P.10']

    def __init__(self, raw_data: str, data_type: str, logger, **meter):
        self.logger = logger
        self.unparsed_data = raw_data
        self.parsed_data = []
        self.data_type = data_type
        self.meter_id = meter['meter_id']

        self.timezone = meter.get('timezone') or 'CET'
        # self.timezone = pytz.timezone(tz)

        # self.log('DEBUG', f'Data type: {data_type}:\n{raw_data}')


    def parse(self):
        if self.data_type == 'list3':
            self._parse_list3_new()
        elif self.data_type == 'list2':
            self._parse_list2()
        elif self.data_type == 'list4':
            self._parse_list2()
        elif self.data_type == 'p01':
            self._parseP01()
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
                    parsed_line['id'] = Parser.re_id.search(line).groups()[0]
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

        time_format = '%y%m%d%H%M%S %z'
        # tz_utc = pytz.timezone('UTC')

        offset = Parser.tz_offset.get(self.timezone)
        if offset is None:
            self.log('ERROR', f'Unknown timezone "{self.timezone}". Please define in "{Parser.tz_offset}"')
            sys.exit(1)

        data = self.unparsed_data.split('\n')
        for line in data:

            if line[0:4] in self.load_profiles:
                # Parse header line
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

                line_number = 0
                try:
                    line = line.split('(')
                    kz = line[0]

                    # Take meter TZ and make timestamp UTC
                    zsts13 = datetime.datetime.strptime(f"{line[1].strip(')')[1:]} {offset}", time_format)

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

                    # '15)'
                    rp = datetime.timedelta(minutes=int(line[3].strip(')')))
                    z = int(line[4].strip(')'))

                    if z != 6:
                        self.log('WARN', f'6 values expected, {z} received. Update the parser code')
                        sys.exit(1)
                    
                    ids = list()
                    units = list()
                    
                    for i in range(5,16,2):
                       
                        # '1-0:1.5.0)'
                        # 'kW)'
                        ids.append(line[i].strip().strip(')').split(':')[1])
                        units.append(line[i+1].strip().strip(')'))

                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 header parsing "{line}"')
                    sys.exit(1)
            else:
                try:
                    # Parse data line
                    # (0.00063)(0.00000)(0.00023)(0.00000)(0.00000)(0.00000)
                    if len(line) < 2:
                        # self.log('DEBUG', f'Line "{line}" to short, skipping')
                        # Probably, end of message
                        return

                    line = line.split('(')
                    line.pop(0)
                    if len(line) != 6:
                        self.log('ERROR', f'Expected 6 values, found {len(line)} in line "{line}"')
                        sys.exit(1)

                    for i in range(6):
                        parsed_line = {
                            'id': ids[i],
                            'value': line[i].strip().strip(')'),
                            'unit': units[i],
                            'line_time': (zsts13 + rp * line_number).strftime('%s')
                        }
                        self.parsed_data.append(parsed_line)
                    line_number += 1
                except Exception as e:
                    self.log('ERROR', f'Exception "{e}" during P01 line parsing "{line}"')
                    sys.exit(1)


    def _find_data_blocks(self):
        splitted_data = self.unparsed_data.split('\r\n')[1:]
        pre_parsed = dict()
        # TODO: Parse MetCom line 1.6.1(0.00061*kW)(2101021645)
        re_list_pattern1 = re.compile('^\w+\\.\w.*?[(].*?[)]')
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