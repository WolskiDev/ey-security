import csv
import multiprocessing
import os
import pandas as pd
import re
from fsplit.filesplit import Filesplit
from typing import Tuple, List, Dict

from src.utils import Timer
from src.log_parsers import HuaweiLogParser, CheckPointLogParser
from src.parallel_script import ParallelScript, params

# TODO: Remove before final
# def temp_get_paths():
#     dir_name = r'C:\Users\Mateusz.Wolski\PycharmProjects\ey-security\data\central_log_file\.parsed'
#     records = {'cp': [], 'hw': []}
#     keys = {'cp': [], 'hw': []}
#     na = []
#     for filename in os.listdir(dir_name):
#         filepath = os.path.join(dir_name, filename)
#         for parser_name in ['hw', 'cp']:
#             if f'.{parser_name}.' in filename:
#                 if filename.endswith('.keys'):
#                     keys[parser_name].append(filepath)
#                 else:
#                     records[parser_name].append(filepath)
#         if filename.endswith('.log'):
#             na.append(filepath)
#
#     return records, keys, na


class FileParser(ParallelScript):
    """Main log file parser class."""

    def __init__(self, log_parsers: list, max_processes: int = None, max_threads: int = None):
        super().__init__(max_processes, max_threads)
        self.log_parsers = log_parsers

    def parse_file(self, src_file_path) -> None:
        self.log.info(f'Parsing {src_file_path}')
        try:
            with Timer() as timer:
                self._parse_file_main(src_file_path)
        except Exception as e:
            self.log.critical(f'Parsing failed with exception: {str(e)}', exc_info=True)
        else:
            self.log.info(f'Parsing completed (wall time: {timer.time_string})')

    def _parse_file_main(self, src_file_path: str) -> None:
        # parse source file path
        src_file_dir, src_file_basename = os.path.split(src_file_path)
        src_file_name, src_file_ext = os.path.splitext(src_file_basename)

        # define output directories
        output_dir_path = os.path.join(src_file_dir, src_file_name)
        split_dir_path = os.path.join(src_file_dir, src_file_name, '.split')
        parsed_dir_path = os.path.join(src_file_dir, src_file_name, '.parsed')
        tabularized_dir_path = os.path.join(src_file_dir, src_file_name, '.tabularized')

        # initialize output directories
        self.log.info('Initializing output directories')
        os.makedirs(output_dir_path)
        os.makedirs(parsed_dir_path)
        os.makedirs(split_dir_path)
        os.makedirs(tabularized_dir_path)

        # split source file into evenly sized chunks of logs
        self.log.info('Splitting source file into chunks...')
        chunk_file_paths = self._split_file_into_chunks(src_file_path=src_file_path,
                                                        dst_dir_path=split_dir_path)
        self.log.info(f'Source file split into {len(chunk_file_paths)} chunks')

        # extract features from chunks of logs and save them as records
        self.log.info('Parsing source file chunks...')
        parsing_results = self._parse_file_chunks(chunk_file_paths=chunk_file_paths,
                                                  dst_dir_path=parsed_dir_path)
        records_file_paths_dict, keys_file_paths_dict, unparsed_file_paths = parsing_results
        self.log.info('Done parsing source file chunks')

        # TODO: Remove before final
        # records_file_paths_dict, keys_file_paths_dict, unparsed_file_paths = temp_get_paths()

        # get all unique feature names extracted from chunks by each parser and order them to form column names
        self.log.info('Gathering unique feature names...')
        records_table_headers_dict = self._get_final_table_headers(keys_file_paths_dict=keys_file_paths_dict)
        self.log.info('Done gathering unique feature names')

        # convert files with records into tabularic tsv files with matching headers
        self.log.info('Tabularizing parsed file chunks...')
        table_file_paths_dict = self._tabularize_parsed_chunks(records_file_paths_dict=records_file_paths_dict,
                                                               records_table_headers_dict=records_table_headers_dict,
                                                               dst_dir_path=tabularized_dir_path)
        self.log.info('Done tabularizing parsed file chunks')

        # merge parsed table chunks
        self.log.info('Concatenating parsed file chunks...')
        self._concatenate_tabularized_chunks(original_file_name=src_file_name,
                                             tabularized_file_paths_dict=table_file_paths_dict,
                                             dst_dir_path=output_dir_path)
        self.log.info('Done concatenating parsed file chunks')

        # merge files with leftover logs that were not parsed by any of the parsers
        self.log.info('Concatenating unparsed file chunks...')
        self._concatenate_unparsed_chunks(original_file_name=src_file_name,
                                          unparsed_file_paths=unparsed_file_paths,
                                          dst_dir_path=output_dir_path)
        self.log.info('Done concatenating unparsed file chunks')

    @staticmethod
    def _split_file_into_chunks(src_file_path: str,
                                dst_dir_path: str,
                                chunk_byte_size: int = 1_000_000_000
                                ) -> List[str]:
        # split file
        fs = Filesplit()
        chunk_file_paths = []
        fs.split(file=src_file_path,
                 split_size=chunk_byte_size,
                 output_dir=dst_dir_path,
                 callback=lambda path, bytes_: chunk_file_paths.append(path),
                 newline=True)

        # delete manifest file
        manifest_file_path = os.path.join(dst_dir_path, 'fs_manifest.csv')
        if os.path.exists(manifest_file_path):
            os.remove(manifest_file_path)

        return chunk_file_paths

    def _parse_file_chunks(self,
                           chunk_file_paths: List[str],
                           dst_dir_path: str
                           ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]], List[str]]:
        # parse files in parallel
        params_list = [params(src_file_path, dst_dir_path) for src_file_path in chunk_file_paths]
        task_results = self.execute_parallel_task(task=self._parse_file_chunk,
                                                  params_list=params_list)

        # merge task results
        records_file_paths = dict()
        keys_file_paths = dict()
        unparsed_file_paths = list()
        for records_file_path_dict, keys_file_path_dict, unparsed_file_path in task_results:

            # aggregate record file paths
            for parser_name, records_file_path in records_file_path_dict.items():
                if parser_name in records_file_paths:
                    records_file_paths[parser_name].append(records_file_path)
                else:
                    records_file_paths[parser_name] = [records_file_path]

            # aggregate keys file paths
            for parser_name, keys_file_path in records_file_path_dict.items():
                if parser_name in keys_file_paths:
                    keys_file_paths[parser_name].append(keys_file_path)
                else:
                    keys_file_paths[parser_name] = [keys_file_path]

            # aggregate unparsed logs file paths
            unparsed_file_paths.append(unparsed_file_path)

        return records_file_paths, keys_file_paths, unparsed_file_paths

    def _parse_file_chunk(self,
                          src_file_path: str,
                          dst_dir_path: str
                          ) -> Tuple[Dict[str, str], Dict[str, str], str]:
        src_file_dir, src_file_basename = os.path.split(src_file_path)
        src_file_name, src_file_ext = os.path.splitext(src_file_basename)
        self.log.info(f'Parsing {src_file_basename}')
        # initialize parsers
        parsers = {p.short_name: p() for p in self.log_parsers}

        # initialize parsing results
        records_dict = {k: [] for k in parsers.keys()}
        keys_dict = {k: set() for k in parsers.keys()}
        unparsed_logs = []

        # parse file chunk
        with open(src_file_path) as file:
            for idx, log_entry in enumerate(file):
                for parser_name, parser in parsers.items():
                    try:
                        record = parser.parse(log_entry)
                        records_dict[parser_name].append(record)
                        keys_dict[parser_name].update(record)
                        break
                    except Exception:
                        pass
                else:
                    unparsed_logs.append(log_entry.strip())

        # save successfully parsed results as records and keys (unique features from all records)
        records_file_path_dict = dict()
        for parser_name, records in records_dict.items():
            rows_file_path = os.path.join(dst_dir_path, f'{src_file_name}.{parser_name}.records')
            records_file_path_dict[parser_name] = rows_file_path
            with open(rows_file_path, mode='w+') as file:
                for record in records:
                    file.write(str(record) + '\n')

        keys_file_path_dict = dict()
        for parser_name, keys in keys_dict.items():
            keys_file_path = os.path.join(dst_dir_path, f'{src_file_name}.{parser_name}.keys')
            keys_file_path_dict[parser_name] = keys_file_path
            with open(keys_file_path, mode='w+') as file:
                for key in keys:
                    file.write(str(key) + '\n')

        # save leftover (unparsed) logs
        unparsed_file_path = os.path.join(dst_dir_path, f'{src_file_name}.na{src_file_ext}')
        with open(unparsed_file_path, mode='w+') as file:
            for log in unparsed_logs:
                file.write(str(log) + '\n')

        return records_file_path_dict, keys_file_path_dict, unparsed_file_path

    @staticmethod
    def _get_final_table_headers(keys_file_paths_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        headers_dict = dict()
        for parser_name, keys_file_paths in keys_file_paths_dict.items():

            # get unique keys sets for each parser
            unique_keys = set()
            for file_path in keys_file_paths:
                with open(file_path) as file:
                    lines = file.read()
                keys = filter(None, lines.split('\n'))
                unique_keys.update(keys)

            # sort keys so that user made fields (starting with '_') are a the beginning
            sorted_keys = sorted(unique_keys, key=lambda x: '0' + str(x) if str(x).startswith('_') else '1' + str(x))
            headers_dict[parser_name] = sorted_keys

        return headers_dict

    def _tabularize_parsed_chunks(self,
                                  records_file_paths_dict: Dict[str, List[str]],
                                  records_table_headers_dict: Dict[str, List[str]],
                                  dst_dir_path: str
                                  ) -> Dict[str, List[str]]:
        # get params for tabularizer function
        params_list = []
        for parser_name, records_file_paths in records_file_paths_dict.items():
            table_headers = records_table_headers_dict[parser_name]
            for records_file_path in records_file_paths:
                params_list.append(
                    params(
                        parser_name=parser_name,
                        table_headers=table_headers,
                        src_file_path=records_file_path,
                        dst_dir_path=dst_dir_path
                    )
                )

        # convert record files into tables (tsv) with specified headers
        task_results = self.execute_parallel_task(task=self._tabularize_parsed_chunk,
                                                  params_list=params_list)

        # merge all task results
        table_file_paths_dict = dict()
        for parser_name, table_file_path in task_results:
            if parser_name in table_file_paths_dict:
                table_file_paths_dict[parser_name].append(table_file_path)
            else:
                table_file_paths_dict[parser_name] = [table_file_path]

        return table_file_paths_dict

    def _tabularize_parsed_chunk(self,
                                 parser_name: str,
                                 table_headers: List[str],
                                 src_file_path: str,
                                 dst_dir_path: str
                                 ) -> Tuple[str, str]:
        # parse source file path
        src_file_dir, src_file_basename = os.path.split(src_file_path)
        src_file_name, src_file_ext = os.path.splitext(src_file_basename)
        self.log.info(f'Tabularizing {src_file_basename}')

        # get result file path
        dst_file_name = f'{src_file_name}.tsv'
        dst_file_path = os.path.join(dst_dir_path, dst_file_name)

        # load records from file
        with open(src_file_path) as file:
            lines = file.readlines()
        records = list(map(eval, lines))

        # format records as table
        result_df = pd.DataFrame(columns=table_headers)
        result_df = result_df.append(records, ignore_index=True)

        # export table to tsv
        result_df.to_csv(path_or_buf=dst_file_path,
                         sep='\t',
                         encoding='utf8',
                         header=True,
                         index=False,
                         quoting=csv.QUOTE_ALL,
                         quotechar='"',
                         mode='w')

        return parser_name, dst_file_path

    def _concatenate_tabularized_chunks(self,
                                        original_file_name: str,
                                        tabularized_file_paths_dict: Dict[str, List[str]],
                                        dst_dir_path: str
                                        ) -> Dict[str, str]:
        concatenated_paths_dict = dict()
        for parser_name, tabularized_file_paths in tabularized_file_paths_dict.items():
            # parse first of the source paths
            src_file_dir, src_file_basename = os.path.split(tabularized_file_paths[0])
            src_file_name, src_file_ext = os.path.splitext(src_file_basename)

            # sort tabularized file paths
            tabularized_file_paths_sorted = sorted(tabularized_file_paths,
                                                   key=lambda x: self._get_chunk_file_idx(original_file_name, x))

            # concatenate files
            dst_file_path = os.path.join(dst_dir_path, f'{original_file_name}.{parser_name}{src_file_ext}')
            concatenated_paths_dict[parser_name] = dst_file_path
            for table_idx, table_file_path in enumerate(tabularized_file_paths_sorted):
                print(table_file_path)
                with open(dst_file_path, mode='a+') as dst_file:
                    with open(table_file_path) as table_file:
                        if table_idx > 0:
                            next(table_file)
                        lines = table_file.readlines()
                    dst_file.writelines(lines)

        return concatenated_paths_dict

    def _concatenate_unparsed_chunks(self,
                                     original_file_name: str,
                                     unparsed_file_paths: List[str],
                                     dst_dir_path: str
                                     ) -> str:
        # parse first of the source paths
        src_file_dir, src_file_basename = os.path.split(unparsed_file_paths[0])
        src_file_name, src_file_ext = os.path.splitext(src_file_basename)

        # sort unparsed file paths
        unparsed_file_paths_sorted = sorted(unparsed_file_paths,
                                            key=lambda x: self._get_chunk_file_idx(original_file_name, x))

        # concatenate unparsed logs
        dst_file_path = os.path.join(dst_dir_path, f'{original_file_name}.na{src_file_ext}')
        for unparsed_file_path in unparsed_file_paths_sorted:
            print(unparsed_file_path)
            with open(dst_file_path, mode='a+') as dst_file:
                with open(unparsed_file_path) as unparsed_file:
                    lines = unparsed_file.readlines()
                dst_file.writelines(lines)

        return dst_file_path

    @staticmethod
    def _get_chunk_file_idx(original_file_name: str, chunk_file_name: str):
        groups = re.findall(rf".*{original_file_name}_(\d+)[.].*", chunk_file_name)
        idx_str = groups.pop()
        idx = int(idx_str)

        return idx


if __name__ == '__main__':

    fp = FileParser(log_parsers=[HuaweiLogParser, CheckPointLogParser],
                    max_processes=multiprocessing.cpu_count() - 3,
                    max_threads=1)
    fp.parse_file(r'C:\Users\Mateusz.Wolski\PycharmProjects\ey-security\data\central_log_file.log')
