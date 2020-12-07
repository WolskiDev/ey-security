import csv
import logging
import pandas as pd
import sys
import time


class Timer:
    """Time code execution."""

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.time = self.end - self.start

    @property
    def time_string(self) -> str:
        """Return formatted time string."""
        m, s = divmod(self.time, 60)
        h, m = divmod(m, 60)

        if h > 0:
            return f"{int(h)}h {int(m)}m {int(s)}s"
        elif m > 0:
            return f"{int(m)}m {int(s)}s"
        elif s >= 1:
            return f"{int(s)}s"
        else:
            ms = self.time * 1000
            if ms >= 1:
                return f"{int(round(ms))}ms"
            else:
                return f"<1ms"


def initialize_logger(name: str) -> logging.Logger:
    """Initialize logger logging message, timestamp, process name and thread name."""
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    message_format = logging.Formatter(
        fmt='[%(asctime)s] [%(processName)s %(threadName)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt=message_format)
    logger.addHandler(console_handler)
    return logger


def df2tsv(df: pd.DataFrame, dst_file_path: str, **kwargs) -> None:
    df.to_csv(
        path_or_buf=dst_file_path,
        sep='\t',
        header=True,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        encoding='utf8',
        mode='w',
        **kwargs
    )


def tsv2df(src_file_path: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(
        filepath_or_buffer=src_file_path,
        sep='\t',
        header=0,
        index_col=None,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        encoding='utf8',
        low_memory=False,
        **kwargs
    )
