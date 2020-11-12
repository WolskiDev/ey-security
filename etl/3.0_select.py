import vaex
import pandas as pd
import os
import csv
import shutil
from pathlib import Path

allowed_cols_cp = [
    '_fw_datetime',
    '_fw_interface_1',
    '_fw_interface_2',
    '_fw_timestamp',
    'Action',
    'dst',
    'during_sec',
    'encryption fail reason',
    'encryption failure',
    'fragments_dropped',
    'fw_subproduct',
    'ICMP',
    'ICMP Code',
    'ICMP Type',
    'inzone',
    'ip_id',
    'ip_len',
    'ip_offset',
    'Log delay',
    'Log ID',
    'message',
    'message_info',
    'NAT_addtnl_rulenum',
    'NAT_rulenum',
    'outzone',
    'peer gateway',
    'product',
    'proto',
    'rule',
    's_port',
    'scheme',
    'service',
    'service_id',
    'src',
    'UUid',
    'vpn_feature_name',
    'xlatesrc'
]


def select_cp(chunk: pd.DataFrame):
    res = chunk[chunk['product'].str.startswith('VPN')]
    return res


allowed_cols_hw = [
    '_policy',
    '_fw_datetime',
    '_fw_interface_2',
    '_fw_timestamp',
    'destination-ip',
    'destination-port',
    'destination-zone',
    'protocol',
    'rule-name',
    'source-ip',
    'source-port',
    'source-zone',
    'time',
    'vsys',
]


def select_hw(chunk: pd.DataFrame):
    chunk = chunk.rename(columns={'_policy': '_action'})
    res = chunk[chunk['rule-name'].notna()]
    return res


COLUMNS_DICT = {
    'cp': allowed_cols_cp,
    'hw': allowed_cols_hw,
}

METHOD_DICT = {
    'cp': select_cp,
    'hw': select_hw
}

if __name__ == '__main__':
    PROJ_DIR = Path(__file__).parent.parent
    SRC_DIR = os.path.join(PROJ_DIR, 'data', '2.1_central_log_tables')
    DST_DIR = os.path.join(PROJ_DIR, 'data', '3.0_selected_data')
    os.makedirs(DST_DIR, exist_ok=True)

    for name in ('cp', 'hw'):
        read_path = os.path.join(SRC_DIR, name + '.tsv')
        save_path = os.path.join(DST_DIR, name + '.tsv')

        header = True
        for idx, chunk in enumerate(pd.read_csv(read_path,
                                                sep='\t',
                                                quotechar='"',
                                                quoting=csv.QUOTE_ALL,
                                                chunksize=1_000_000,
                                                low_memory=False,
                                                usecols=COLUMNS_DICT[name])):
            print(name, idx)
            chunk = METHOD_DICT[name](chunk)
            chunk.to_csv(save_path,
                         sep='\t',
                         encoding='utf8',
                         header=header,
                         index=False,
                         quoting=csv.QUOTE_ALL,
                         quotechar='"',
                         mode='a')
            header = False
