# Firewall log parser

Simple tool for parsing firewall log files using choice of programmed log parsers. Parses log file under specified path 
and saves the results in a tabular format in a directory named after the source file (appended with timestamp). 
Predefined set of log parsers currently includes basic Huawei (hw) and Checkpoint (cp) parsers but can be extended.

## Installation

```bash
$ python --version # Python 3.9.0
$ git clone https://github.com/WolskiDev/ey-security.git
$ cd ey-security
$ pip install -f requirements.txt
```

## Usage

```
Usage:
  parse.py [options] <path>

Options:
  -h, --help                            Show help message and exit
  -o, --out-dir-path    <dir>           Overwrite the default output directory (specified 
                                        directory must not already exist)
  -c, --chunk-size      <bytes>         Parse source file in chunks of a given size (defaults to 
                                        1`000`000`000 B or ~1 GB)
  -p, --max-processes   <num>           Limit the maximum number of spawned processes (defaults 
                                        to the number of CPUs in the system minus one)
  -t, --max-threads     <num>           Limit the maximum number of threads spawned per each 
                                        process (defaults to one)
  -i, --preserve-intermediate-results   Do not delete temporary directories with intermediate 
                                        results created during parsing process
```

## Example

```bash
$ python parse.py central_log_file.log
[2020-11-28 11:39:51] [MainProcess MainThread] Parsing: central_log_file.log
[2020-11-28 11:39:51] [MainProcess MainThread] Initializing output directory: central_log_file_20201128_113951
[2020-11-28 11:39:51] [MainProcess MainThread] STAGE_1: Splitting source file into chunks...
...
```

```bash
$ ls central_log_file_20201128_113951
central_log_file.cp.tsv
central_log_file.hw.tsv
central_log_file.na.log
```