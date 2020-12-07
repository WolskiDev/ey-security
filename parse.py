import argparse
import multiprocessing
import sys
from src.file_parser import FileParser
from src.log_parsers import HuaweiLogParser, CheckPointLogParser


def main():
    args_parser = get_args_parser()
    args = args_parser.parse_args(sys.argv[1:])
    try:
        init(args)
    except KeyboardInterrupt:
        sys.exit(0)


def init(args):
    fp = FileParser(
        log_parsers=[HuaweiLogParser, CheckPointLogParser],
        max_processes=args.max_processes,
        max_threads=args.max_threads,
        parse_chunk_size=args.chunk_size,
        delete_intermediate_result_dirs=(not args.preserve_intermediate_results)
    )
    fp.parse_file(
        src_file_path=args.src_file_path,
        out_dir_path=args.out_dir_path
    )


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='Parses log file under specified path and saves the results in a tabular format in a directory '
                    'named after the source file (appended with timestamp).'
    )
    parser.add_argument(
        'src_file_path',
        metavar='<path>',
        action='store',
        type=str,
        help='the path to the source file with logs'
    )
    parser.add_argument(
        '-o', '--out-dir-path',
        help='overwrite the default output directory (specified directory must not already exist)',
        metavar='<dir>',
        action='store',
        default=None,
        type=str
    )
    parser.add_argument(
        '-c', '--chunk-size',
        help='parse source file in chunks of a given size (defaults to 1`000`000`000 B or ~1 GB)',
        metavar='<bytes>',
        action='store',
        default=1_000_000_000,
        type=int
    )
    parser.add_argument(
        '-p', '--max-processes',
        help='limit the maximum number of spawned processes (defaults to the number of CPUs in the system minus one)',
        metavar='<num>',
        action='store',
        default=multiprocessing.cpu_count() - 1,
        type=int
    )
    parser.add_argument(
        '-t', '--max-threads',
        help='limit the maximum number of threads spawned per each process (defaults to one)',
        metavar='<num>',
        action='store',
        default=1,
        type=int
    )
    parser.add_argument(
        '-i', '--preserve-intermediate-results',
        help='do not delete temporary directories with intermediate results created during parsing process',
        action='store_true'
    )
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.epilog = 'authors:\n  Mateusz Wolski (github.com/WolskiDev)'

    return parser


if __name__ == '__main__':
    main()
