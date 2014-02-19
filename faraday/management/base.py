from __future__ import print_function
import argparse
import importlib
import os
import re


SUBCOMMAND = re.compile(r'^([^_][\w]*)\.py$')
SUBCOMMANDS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'commands',
)


class CommandError(Exception):
    pass


class CallError(CommandError):
    pass


def call_command(*args):
    try:
        run_command(args)
    except SystemExit:
        # argparse itself may raise without message
        raise CallError


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

    for node in os.listdir(SUBCOMMANDS):
        match = SUBCOMMAND.search(node)
        if match:
            module_name = match.group(1)
            rel_path = ".commands.{}".format(module_name)
            module = importlib.import_module(rel_path, __package__)
            subparser = module.add_parser(subparsers)
            subparser.set_defaults(func=module.main)

    args = parser.parse_args(argv)
    args.puts = putter(args.verbosity)
    args.puts("faraday: {}".format(args.subparser_name))
    args.func(args)
