import vaex as vx
import csv
import os
import pandas as pd
import shutil
from pathlib import Path

if __name__ == '__main__':
    PROJ_DIR = Path(__file__).parent.parent
    SRC_DIR = os.path.join(PROJ_DIR, 'data', '2.0_central_log_tables')
    DST_DIR = os.path.join(PROJ_DIR, 'data', '2.1_central_log_tables')
    DST_TEMP_DIR = os.path.join(DST_DIR, 'temp')
    os.makedirs(DST_DIR, exist_ok=True)
    os.makedirs(DST_TEMP_DIR, exist_ok=True)

    # for name in ('cp', 'hw'):
    #     save_path = os.path.join(DST_DIR, name+'.tsv')
    #     save_paths[name] = save_path
    #     with open(save_path, 'wb') as wfd:
    #         for f in [os.path.join(SRC_DIR, name+i) for i in ('.header', '.data')]:
    #
    #             with open(f, 'rb') as fd:
    #                 shutil.copyfileobj(fd, wfd)

    save_paths = dict()
    save_paths['cp'] = r'/Users/mateusz/PycharmProjects/ey-security/data/2.1_central_log_tables/cp.tsv'

    for name, path in save_paths.items():
        tsv_reader = pd.read_csv(path, sep='\t', quotechar='"', quoting=csv.QUOTE_ALL, chunksize=1_000_000,
                                 low_memory=False , dtype=pd.StringDtype())
        for idx, chunk in enumerate(tsv_reader):
            print('chunk', idx)
            # chunk.to_parquet(os.path.join(DST_TEMP_DIR, f'{name}_chunk_{idx}.parquet'))
            # chunk.to_hdf(os.path.join(DST_TEMP_DIR, f'{name}_chunk_{idx}.hdf5'), key='data', append=True)
            vaex_df = vx.from_pandas(chunk, copy_index=False)
            vaex_df.export_hdf5(os.path.join(DST_TEMP_DIR, f'{name}_chunk_{idx}.hdf5'))
            vx.from_dict()