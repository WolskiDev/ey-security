import vaex
import os
import shutil
from pathlib import Path

if __name__ == '__main__':
    PROJ_DIR = Path(__file__).parent.parent
    SRC_DIR = os.path.join(PROJ_DIR, 'data', '2.0_central_log_tables')
    DST_DIR = os.path.join(PROJ_DIR, 'data', '2.1_central_log_tables')
    DST_TEMP_DIR = os.path.join(DST_DIR, 'temp')
    os.makedirs(DST_DIR, exist_ok=True)
    os.makedirs(DST_TEMP_DIR, exist_ok=True)

    for name in ('cp', 'hw'):
        save_path = os.path.join(DST_DIR, name+'.tsv')
        with open(save_path, 'wb') as wfd:
            for f in [os.path.join(SRC_DIR, name+i) for i in ('.header', '.data')]:

                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
