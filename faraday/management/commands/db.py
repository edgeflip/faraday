"""Management of DynamoDB tables"""
import sys
from importlib import import_module
from itertools import chain
from StringIO import StringIO
from threading import Thread

from faraday import conf, db, utils
from faraday.management.base import CommandError, CommandRegistry


subcommand = SUBCOMMANDS = CommandRegistry()


def add_parser(subparsers):
    tagline = "Manage tables in DynamoDB"
    parser = subparsers.add_parser('db', description=tagline, help=tagline)

    subcommands = tuple(SUBCOMMANDS)
    parser.add_argument(
        'subcmds',
        metavar="SUBCOMMAND",
        nargs='+',
        choices=subcommands,
        help="database operation(s) to perform; one or more of {!r}"
             .format(subcommands),
    )
    parser.add_argument(
        '-m', '--models',
        default='models',
        help="import path at which to discover faraday item models [default: models]",
        metavar='PYTHON.PATH',
    )
    parser.add_argument(
        '-r', '--read',
        default=5,
        dest='read_throughput',
        help="read throughput for created tables [default: 5]",
        metavar='THROUGHPUT',
        type=int,
    )
    parser.add_argument(
        '-w', '--write',
        default=5,
        dest='write_throughput',
        help="write throughput for created tables [default: 5]",
        metavar='THROUGHPUT',
        type=int,
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="overrides warnings",
    )

    return parser


def main(context):
    try:
        subcommands = [SUBCOMMANDS[cmd] for cmd in context.subcmds]
    except KeyError as exc:
        # argparse should protect against this, but otherwise:
        raise CommandError("unknown command: {}".format(exc))

    # Ensure models loaded #
    try:
        import_module(context.models) # FIXME: will non-Django apps work like this?
    except ImportError:
        try:
            from django.core.management.validation import get_validation_errors
        except ImportError:
            raise CommandError("failed to import: {!r}".format(context.models))
        else:
            buf = StringIO()
            num_errors = get_validation_errors(buf)
            if num_errors:
                buf.seek(0)
                raise CommandError("one or more Django models did not validate:\n%s" % buf.read())

    for command in subcommands:
        command(context)


class StatusWorker(Thread):
    """Thread worker which attempts to advance the iterator and store its result."""
    def __init__(self, status_lines):
        super(StatusWorker, self).__init__()
        self.status_lines = status_lines
        self.head = None
        self.daemon = True

    def run(self):
        try:
            first = next(self.status_lines)
        except StopIteration:
            self.head = ()
        else:
            self.head = (first,)


@subcommand
def status(context):
    # Give first request 5s timeout (otherwise database must be unavailable)
    # StatusWorker lets us set timeout (without access to lower level)
    status_lines = db.iterstatus()
    worker = StatusWorker(status_lines)
    worker.start()
    worker.join(timeout=5)

    if worker.head is None:
        raise CommandError("No server response after 5 seconds")

    # Iterate over first result and remaining
    count = 0
    status_lines = chain(worker.head, status_lines)
    for (count, (table_name, status)) in enumerate(status_lines, 1):
        context.puts("{}: {}".format(table_name, status), level=0)

    if count == 0:
        context.puts("registry empty")


@subcommand
def build(context):
    count = db.build(
        timeout=(60 * 3), # 3 minutes per table
        stdout=(sys.stdout if context.verbosity > 0 else utils.dummyio),
        throughput={
            'read': context.read_throughput,
            'write': context.write_throughput,
        },
    )
    if count == 0:
        context.puts("registry empty")
    else:
        context.puts("tables created. this may require several minutes to take effect.")


def _confirm(message, default='y'):
    options = (option.upper() if option == default.lower() else option
               for option in ('y', 'n'))
    tail = " [{}]? ".format("|".join(options))

    response = None
    while response not in ('', 'y', 'yes', 'n', 'no'):
        response = raw_input(message + tail).strip().lower()

    return response[:1] == 'y' or (response == '' and default == 'y')


@subcommand
def destroy(context):
    if not context.force:
        prefix = (" with prefix '{}'".format(conf.settings.PREFIX)
                  if conf.settings.PREFIX else '')
        print("drop the following tables{} from DynamoDB?".format(prefix))
        print("     {}".format(', '.join(db._named_tables())))
        if not _confirm("", default='n'):
            return

    count = 0
    for (count, (table_name, status)) in enumerate(db.iterdestroy(), 1):
        context.puts("{}: {}".format(table_name, status))

    if count == 0:
        context.puts("registry empty")
    else:
        context.puts("tables destroyed. this may require several minutes to take effect.")


@subcommand
def truncate(context):
    if not context.force:
        prefix = (" with prefix '{}'".format(conf.settings.PREFIX)
                  if conf.settings.PREFIX else '')
        print("truncate the following tables{}?".format(prefix))
        print("     {}".format(', '.join(db._named_tables())))
        if not _confirm("", default='n'):
            return

    count = 0
    for (count, (table_name, status)) in enumerate(db.itertruncate(), 1):
        context.puts("{}: {}".format(table_name, status))

    if count == 0:
        context.puts("registry empty")
    else:
        context.puts("tables truncated")
