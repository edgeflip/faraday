from __future__ import print_function
import argparse
import importlib
import os.path
import pkgutil


class CommandError(Exception):
    pass


class CallError(CommandError):
    pass


def call_command(*args, **kws):
    final = list(args)
    final.extend(
        '--{}={}'.format(key, value) for (key, value) in kws.items()
    )
    try:
        run_command(final)
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

    commands_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'commands',
    )
    commands_package = __package__ + '.commands'
    for (_importer, module_name, _ispkg) in pkgutil.iter_modules([commands_path]):
        if not module_name.startswith('_'):
            module_path = commands_package + '.' + module_name
            module = importlib.import_module(module_path)
            subparser = module.add_parser(subparsers)
            subparser.set_defaults(func=module.main)

    args = parser.parse_args(argv)
    args.puts = putter(args.verbosity)
    args.puts("faraday: {}".format(args.subparser_name))
    args.func(args)
