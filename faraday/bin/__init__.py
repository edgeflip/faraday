from __future__ import print_function
import argparse
import importlib
import os
import re


SUBCOMMAND = re.compile(r'^([^_][\w]*)\.py$')


class CommandError(Exception):
    pass


def call_command(*args):
    run_command(args)


def main(argv=None):
    try:
        run_command(argv)
    except CommandError as exc:
        raise SystemExit(str(exc))


def putter(verbosity):
    def puts(*args, **kws):
        level = kws.pop('level', 1)
        if verbosity >= level:
            print(*args, **kws)
    return puts


def run_command(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-v', '--verbosity',
        type=int,
        choices=[0, 1, 2],
        default=1,
    )

    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands',
        dest='subparser_name',
        help='',
    )

    for node in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        match = SUBCOMMAND.search(node)
        if match:
            module_name = match.group(1)
            rel_path = ".{}".format(module_name)
            module = importlib.import_module(rel_path, __package__)
            subparser = module.add_parser(subparsers)
            subparser.set_defaults(func=module.main)

    args = parser.parse_args(argv)
    args.puts = putter(args.verbosity)
    args.puts("faraday: {}".format(args.subparser_name))
    args.func(args)
