import datetime
import re
from abc import ABC, abstractmethod
from typing import Dict, Any


class UnparsableLogError(Exception):
    pass


class LogParser(ABC):
    """Abstract class for log parsers."""

    @property
    @abstractmethod
    def short_name(self) -> str:
        pass

    @abstractmethod
    def parse(self, log_entry) -> Dict[str, str]:
        pass

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.__class__.__name__


class CheckPointLogParser(LogParser):
    """Checkpoint firewall log parser."""
    short_name = 'cp'

    def __init__(self):
        self.entry_mask = re.compile(r'^(\w+ [ ]?\d+ \d\d:\d\d:\d\d) '
                                     r'([\d\.]+) '
                                     r'([\+\-]\d\d:\d\d) '
                                     r'([\d\.]+) '
                                     r'(.*)$')
        self.params_mask = re.compile(r'((?<=\" ).*?)'
                                      r'[:]?='
                                      r'\"(.*?)\"')

    def parse(self, log_entry: str) -> Dict[str, str]:
        entry_parts = self._parse_entry(log_entry)
        params_str = entry_parts.pop('params')
        params_dict = self._parse_params(params_str)
        record = entry_parts | params_dict

        # strip record keys of trailing/leading white characters (precaution)
        record = {k.strip(): v for k, v in record.items()}

        return record

    def _parse_entry(self, log_entry: str, year: str = '2020') -> Dict[str, Any]:
        if not (match := self.entry_mask.match(log_entry)):
            raise UnparsableLogError('Input does not match entry mask.')
        date_base, interface_1, date_timezone, interface_2, params = match.groups()

        date_str_repr = ' '.join([year, date_base])
        dt = self._parse_date(date_str_repr)

        res = {
            '_a_timestamp': dt.strftime('%Y%m%d%H%M%S'),
            '_b_datetime': str(dt),
            '_c_interface_1': interface_1,
            '_d_interface_2': interface_2,
            'params': params
        }
        return res

    @staticmethod
    def _parse_date(date_str: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_str, '%Y %b %d %H:%M:%S')

    def _parse_params(self, params_str: str) -> dict:
        return dict(self.params_mask.findall('" ' + params_str.strip()))


class HuaweiLogParser(LogParser):
    """Huawei firewall log parser."""
    short_name = 'hw'

    def __init__(self):
        self.entry_mask = re.compile(r'^(?P<timestamp_1>\w+ [ ]?\d+ \d\d:\d\d:\d\d) '
                                     r'(?:(?P<interface_1>[\d\.]+) )?'
                                     r'(?:(?P<timestamp_2>\d\d\d\d\-\d\d\-\d\d \d\d:\d\d:\d\d) )?'
                                     r'(?P<interface_2>\S+) '
                                     r'%%'
                                     r'(?P<event_name>[^\s\(\)\[\]]+?)'
                                     r'(?:[\(](?P<event_brace_round>\S+)[\)])?'
                                     r'(?:[\[](?P<event_brace_square>\S+)[\]])?'
                                     r':'
                                     r'(?P<params>.*)$')
        self.params_mask_1 = re.compile(r'((?:(?<=, )|(?<=,)|(?<=\())[^\s\(\)&,]+?)'
                                        r'='
                                        r'\"?(.*?)\"?'
                                        r'(?:,|\.$|\)$)')
        self.params_mask_2 = re.compile(r'(?:(?:(?<=; )|(?<=;)|(?<=\())(?:\[.*\])?([^(\)&,]+?))'
                                        r':'
                                        r'\"?(.*?)\"?'
                                        r'(?:;)')  # orig: (?:;|\)$)     older orig: (?:;|\.$|\)$)

    def parse(self, log_entry: str) -> Dict[str, str]:
        entry_parts = self._parse_entry(log_entry)
        params_str = entry_parts.pop('params')
        params_dict = self._parse_params_1(params_str)
        params_dict.update(self._parse_params_2(params_str))
        record = entry_parts | params_dict

        # strip record keys of trailing/leading white characters (precaution)
        record = {k.strip(): v for k, v in record.items()}

        return record

    def _parse_entry(self, log_entry: str, year: str = '2020') -> Dict[str, Any]:
        if not (match := self.entry_mask.match(log_entry)):
            raise UnparsableLogError('Input does not match entry mask.')
        group_dict = match.groupdict()

        date = group_dict.get('timestamp_1')
        interface_1 = group_dict.get('interface_1')
        interface_2 = group_dict.get('interface_2')
        event_name = group_dict.get('event_name')
        event_brace_round = group_dict.get('event_brace_round')
        event_brace_square = group_dict.get('event_brace_square')
        params = group_dict.get('params')

        date_str_repr = ' '.join([year, date])
        dt = self._parse_date(date_str_repr)

        res = {
            '_a_timestamp': int(dt.strftime('%Y%m%d%H%M%S')),
            '_b_datetime': str(dt),
            '_c_interface_1': interface_1,
            '_d_interface_2': interface_2,
            '_e_event_name': event_name,
            '_f_event_brace_round': event_brace_round,
            '_g_event_brace_square': event_brace_square,
            'params': params}
        return res

    @staticmethod
    def _parse_date(date_str: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_str, '%Y %b %d %H:%M:%S')

    def _parse_params_1(self, params_str: str) -> dict:
        return dict(self.params_mask_1.findall(',' + params_str.strip()))

    def _parse_params_2(self, params_str: str) -> dict:
        return dict(self.params_mask_2.findall(';' + params_str.strip()))
