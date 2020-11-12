from abc import ABC, abstractmethod
from pathlib import Path
import datetime
import re
import os


class LogParser(ABC):

    @abstractmethod
    def parse(self, log_entry):
        pass

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.__class__.__name__


class CheckPointLogParser(LogParser):

    def __init__(self):
        self.entry_mask = re.compile(r'^(\w+ [ ]?\d+ \d\d:\d\d:\d\d) ([\d\.]+) ([\+\-]\d\d:\d\d) ([\d\.]+) (.*)$')
        self.params_mask = re.compile(r'((?<=\" ).*?)[:]?=\"(.*?)\"')

    def parse(self, log_entry: str):
        entry_parts = self._parse_entry(log_entry)
        params_str = entry_parts.pop('params')
        params_dict = self._parse_params(params_str)

        entry_parts.update(params_dict)
        return entry_parts

    def _parse_entry(self, text: str, year: str = '2020'):
        match = self.entry_mask.match(text)
        date_base, ip1, date_tz, ip2, params = match.groups()

        date_str_repr = ' '.join([year, date_base])
        dt = self._parse_date(date_str_repr)

        res = {
            '_fw_timestamp': dt.strftime('%Y%m%d%H%M%S'),
            '_fw_datetime': str(dt),
            '_fw_interface_1': ip1,
            '_fw_interface_2': ip2,
            'params': params
        }
        return res

    @staticmethod
    def _parse_date(date_str: str):
        return datetime.datetime.strptime(date_str, '%Y %b %d %H:%M:%S')

    def _parse_params(self, params_str: str):
        return dict(self.params_mask.findall('" ' + params_str.strip()))


class HuaweiLogParser(LogParser):

    def __init__(self):
        self.entry_mask = re.compile(r'^(?P<timestamp1>\w+ [ ]?\d+ \d\d:\d\d:\d\d) (?:(?P<ip>[\d\.]+) )'
                                     r'?(?:(?P<timestamp2>\d\d\d\d\-\d\d\-\d\d \d\d:\d\d:\d\d) )'
                                     r'?(?P<interface>\S+) %%(?P<policy>\S+):(?P<params>.*)$')
        self.params_mask = re.compile(r'((?:(?<=, )|(?<=,)|(?<=\())[^\s\(\)&,]+?)=\"?(.*?)\"?(?:,|\.$|\)$)')

        # entry mask fixed = ^(?P<timestamp1>\w+ [ ]?\d+ \d\d:\d\d:\d\d) (?:(?P<ip>[\d\.]+) )?(?:(?P<timestamp2>\d\d\d\d\-\d\d\-\d\d \d\d:\d\d:\d\d) )?(?P<interface>\S+) %%(?P<policy>\S+?):(?P<params>.*)$
        # additional params mask = (?:(?:(?<=; )|(?<=;)|(?<=\())(?:\[.*\])?([^\s\(\)&,]+?)):\"?(.*?)\"?(?:;|\.$|\)$)
        # add separate parsing with second mask and update dictionary obtained with the first

    def parse(self, log_entry: str):
        entry_parts = self._parse_entry(log_entry)
        params_str = entry_parts.pop('params')
        params_dict = self._parse_params(params_str)

        entry_parts.update(params_dict)
        return entry_parts

    def _parse_entry(self, log_entry: str, year: str = '2020'):
        match = self.entry_mask.match(log_entry)
        group_dict = match.groupdict()

        date = group_dict.get('timestamp1')
        ip = group_dict.get('ip')
        interface = group_dict.get('interface')
        policy = group_dict.get('policy')
        params = group_dict.get('params')

        date_str_repr = ' '.join([year, date])
        dt = self._parse_date(date_str_repr)

        res = {
            '_fw_timestamp': int(dt.strftime('%Y%m%d%H%M%S')),
            '_fw_datetime': str(dt),
            '_fw_interface_1': ip,
            '_fw_interface_2': interface,
            '_policy': policy,
            'params': params}
        return res

    @staticmethod
    def _parse_date(date_str: str):
        return datetime.datetime.strptime(date_str, '%Y %b %d %H:%M:%S')

    def _parse_params(self, params_str: str):
        return dict(self.params_mask.findall(',' + params_str.strip()))


if __name__ == '__main__':
    PROJ_DIR = Path(__file__).parent.parent
    SRC_PATH = os.path.join(PROJ_DIR, 'data', 'central_log_file.log')
    OUT_DIR = os.path.join(PROJ_DIR, 'data', '1_central_log_parsed_chunks')

    # set parameters
    experiment_name = datetime.datetime.now().strftime('%H%M%S')
    chunk_size = 1000000

    # make outdir
    out_dir = OUT_DIR # os.path.join(OUT_DIR, experiment_name)
    os.makedirs(out_dir, exist_ok=True)

    # initialize parsers
    parsers = {'hw': HuaweiLogParser(), 'cp': CheckPointLogParser()}

    # parse
    print(f'start: {datetime.datetime.now()}')
    with open(SRC_PATH) as file:

        chunk = 0
        res_dict = {k: [] for k in list(parsers.keys()) + ['rest']}
        keys_dict = {k: set() for k in list(parsers.keys())}
        for idx, log_entry in enumerate(file):
            log_dict = None
            for parser_name, parser in parsers.items():
                try:
                    log_dict = parser.parse(log_entry)
                    res_dict[parser_name].append(log_dict)
                    keys_dict[parser_name].update(log_dict)
                    break
                except Exception as e:
                    pass

            if not log_dict:
                res_dict['rest'].append(log_entry.strip())

            if idx % chunk_size == 0 and idx != 0:
                chunk += 1
                print(f'chunk_{chunk}: {datetime.datetime.now()}')
                for name, values in res_dict.items():
                    file_name = f'chunk_{chunk}.{name}'
                    with open(os.path.join(out_dir, file_name), mode='w+') as sfile:
                        for v in values:
                            sfile.write(str(v)+'\n')

                for name, values in keys_dict.items():
                    file_name = f'chunk_{chunk}.{name}.keys'
                    with open(os.path.join(out_dir, file_name), mode='w+') as sfile:
                        for v in values:
                            sfile.write(str(v)+'\n')

                res_dict = {k: [] for k in list(parsers.keys()) + ['rest']}
                keys_dict = {k: set() for k in list(parsers.keys())}

        if any([len(v) > 0 for v in res_dict.values()]):
            chunk += 1
            print(f'chunk_{chunk}: {datetime.datetime.now()}')
            for name, values in res_dict.items():
                file_name = f'chunk_{chunk}.{name}'
                with open(os.path.join(out_dir, file_name), mode='w+') as sfile:
                    for v in values:
                        sfile.write(str(v) + '\n')

            for name, values in keys_dict.items():
                file_name = f'chunk_{chunk}.{name}.keys'
                with open(os.path.join(out_dir, file_name), mode='w+') as sfile:
                    for v in values:
                        sfile.write(str(v) + '\n')

            res_dict = {k: [] for k in list(parsers.keys()) + ['rest']}
            keys_dict = {k: set() for k in list(parsers.keys())}

# incorrectly parsed but what can you do
#Sep 9 11:56:41 10.123.169.1 2020-09-09 10:05:11 FW-OUT-01 %%01SECLOG/6/SESSION_TEARDOWN(l):IPVer=4,Protocol=udp,SourceIP=10.123.169.167,DestinationIP=8.8.8.8,SourcePort=52451,DestinationPort=53,SourceNatIP=84.10.61.54,SourceNatPort=3711,BeginTime=1599645876,EndTime=1599645911,SendPkts=1,SendBytes=74,RcvPkts=1,RcvBytes=120,SourceVpnID=0,DestinationVpnID=0,SourceZone=untrust,DestinationZone=untrust,PolicyName=Permit_all,UserName=jakub.gawinkowski,CloseReason=aged-out.
#Jun 19 17:42:29 10.123.169.1 2020-06-19 15:43:30 FW-OUT-01 %%01SECLOG/6/SESSION_URL(l):IPVer=4,Protocol=tcp,SourceIP=10.123.169.152,DestinationIP=34.196.179.197,SourcePort=52858,DestinationPort=80,SourceNatIP=84.10.61.54,SourceNatPort=2465,BeginTime=1592581410,EndTime=1592581410,SourceVpnID=0,DestinationVpnID=0,Page=/pulse?authon&user=5E2F338FF5DDACBC577C29D2515B0CF9&url_ecommerce=20,0,2382,2579,0&catalog_lookup_counts_304=150,0,0,0,0&db_conn=1,0,0,0,0,Host=heartbeat.dm.origin.com.
