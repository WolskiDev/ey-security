import re
import os
import pandas as pd
import csv
from pathlib import Path


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


if __name__ == '__main__':
    PROJ_DIR = Path(__file__).parent.parent
    SRC_DIR = os.path.join(PROJ_DIR, 'data', '1_central_log_parsed_chunks')
    DST_DIR = os.path.join(PROJ_DIR, 'data', '2.0_central_log_tables')
    os.makedirs(DST_DIR, exist_ok=True)

    cp_mask = re.compile(r'^[^\.]+[.]cp$')
    cp_keys_mask = re.compile(r'.+[.]cp[.]keys$')
    hw_mask = re.compile(r'.+[.]hw$')
    hw_keys_mask = re.compile(r'.+[.]hw[.]keys$')
    rest_mask = re.compile(r'.+[.]rest$')

    sort_key = lambda x: int(re.findall(r'_(\d+)\.', x).pop())
    cp_filenames = sorted([f for f in os.listdir(SRC_DIR) if cp_mask.match(f)], key=sort_key)
    cp_keys_filenames = sorted([f for f in os.listdir(SRC_DIR) if cp_keys_mask.match(f)], key=sort_key)
    hw_filenames = sorted([f for f in os.listdir(SRC_DIR) if hw_mask.match(f)], key=sort_key)
    hw_keys_filenames = sorted([f for f in os.listdir(SRC_DIR) if hw_keys_mask.match(f)], key=sort_key)
    rest_filenames = sorted([f for f in os.listdir(SRC_DIR) if rest_mask.match(f)], key=sort_key)

    # CHECKPOINT: column names
    keys_set = set()
    for filename in cp_keys_filenames:
        with open(os.path.join(SRC_DIR, filename)) as file:
            lines = file.read()
        keys_set.update(filter(None, lines.split('\n')))

    base_columns = ['_fw_timestamp', '_fw_datetime', '_fw_interface_1', '_fw_interface_2']
    rest_columns = sorted(filter(lambda x: x not in base_columns, keys_set))
    columns = base_columns + rest_columns

    # CHECKPOINT: merge
    header_df = pd.DataFrame(columns=columns)
    header_df.to_csv(os.path.join(DST_DIR, 'cp.header'),
                     sep='\t',
                     encoding='utf8',
                     header=True,
                     index=False,
                     quoting=csv.QUOTE_ALL,
                     quotechar='"',
                     mode='w')

    for filename in cp_filenames:
        print(filename)
        with open(os.path.join(SRC_DIR, filename)) as file:
            lines = file.readlines()
        rows = list(map(eval, lines))

        data_df = pd.DataFrame(columns=columns)
        data_df = data_df.append(rows, ignore_index=True)
        data_df.to_csv(os.path.join(DST_DIR, 'cp.data'),
                       sep='\t',
                       encoding='utf8',
                       header=False,
                       index=False,
                       quoting=csv.QUOTE_ALL,
                       quotechar='"',
                       mode='a')

    # HUAWEI: column names
    keys_set = set()
    for filename in hw_keys_filenames:
        with open(os.path.join(SRC_DIR, filename)) as file:
            lines = file.read()
        keys_set.update(filter(None, lines.split('\n')))

    base_columns = ['_fw_timestamp', '_fw_datetime', '_fw_interface_1', '_fw_interface_2', '_policy']
    rest_columns = sorted(filter(lambda x: x not in base_columns, keys_set))
    columns = base_columns + rest_columns

    # HUAWEI: merge
    header_df = pd.DataFrame(columns=columns)
    header_df.to_csv(os.path.join(DST_DIR, 'hw.header'),
                     sep='\t',
                     encoding='utf8',
                     header=True,
                     index=False,
                     quoting=csv.QUOTE_ALL,
                     quotechar='"',
                     mode='w')

    for filename in hw_filenames:
        print(filename)
        with open(os.path.join(SRC_DIR, filename)) as file:
            lines = file.readlines()
        rows = list(map(eval, lines))

        data_df = pd.DataFrame(columns=columns)
        data_df = data_df.append(rows, ignore_index=True)
        data_df.to_csv(os.path.join(DST_DIR, 'hw.data'),
                       sep='\t',
                       encoding='utf8',
                       header=False,
                       index=False,
                       quoting=csv.QUOTE_ALL,
                       quotechar='"',
                       mode='a')

    # REST: merge
    for filename in rest_filenames:
        print(filename)
        with open(os.path.join(SRC_DIR, filename)) as file:
            lines = file.readlines()
        with open(os.path.join(DST_DIR, 'rest.log'), mode='a+') as file:
            file.writelines(lines)
