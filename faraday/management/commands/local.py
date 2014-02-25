"""Management of local DynamoDB server"""
import itertools
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib
from argparse import Namespace

import psutil

from faraday.conf import settings
from faraday.management.base import CommandError


SERVER_JAR = 'DynamoDBLocal.jar'
SERVER_ARGS = ('java', '-Djava.library.path=./DynamoDBLocal_lib', '-jar',
               os.path.join(os.curdir, SERVER_JAR))


def add_parser(subparsers):
    tagline = "Manage the local DDB server"
    description = (tagline + ". "
        "Note: 'start' requires Java Runtime Engine (JRE) version 6.x or newer.")
    parser = subparsers.add_parser('local', description=description, help=tagline)
    subcommands = tuple(SUBCOMMANDS)
    parser.add_argument(
        'subcmds',
        metavar="SUBCOMMAND",
        nargs='+',
        choices=subcommands,
        help="local server operation(s) to perform; one or more of {!r}"
             .format(subcommands),
    )
    parser.add_argument(
        '--install-path',
        help="path under which to install the local server and/or under which "
             "it should be found",
    )
    parser.add_argument(
        '--pid-path',
        help="path at which to store the process identifier of the local server",
    )
    parser.add_argument(
        '--db-path',
        help="path at which to store the database of the local server",
    )
    parser.add_argument(
        '--port',
        help="local port at which local server should respond",
    )
    parser.add_argument(
        '--memory',
        action='store_true',
        help="indicates that the database should be stored in memory only "
             "(and lost on server stop)",
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

    for command in subcommands:
        command(context)


# Helpers #

class CommandRegistry(dict):

    def __call__(self, func):
        self[func.__name__] = func
        return func

subcommand = SUBCOMMANDS = CommandRegistry()


def localdir():
    return os.path.join(os.getcwd(), '.dynamodb')


def installpath(context):
    return context.install_path or settings.LOCAL_PATH or localdir()


def jarpath(install_path=None):
    """Returns the path at which local server commands check for the install."""
    context = Namespace(install_path=install_path)
    path = installpath(context)
    return os.path.join(path, SERVER_JAR)


def pidpath(context):
    pid_path = context.pid_path or settings.LOCAL_PID
    if pid_path:
        return pid_path

    path = installpath(context)
    return os.path.join(path, 'pid')


class ServerNotFound(LookupError):
    pass


class StaleServerReference(ServerNotFound):
    pass


def server_pid(pid_path):
    """Return the process ID of the running local server given the path to its
    PID file.

    If no PID or process with that ID is found, raises exception ServerNotFound.
    (If the PID file exists and is non-empty, but no process with that ID is
    found, this is also a StaleServerReference.)

    """
    try:
        pid_fh = open(pid_path)
    except IOError:
        pass
    else:
        pidref = pid_fh.read()
        if pidref:
            pid = int(pidref)
            try:
                os.kill(pid, 0) # just checks that it's available
            except OSError:
                raise StaleServerReference(pid_path)
            else:
                return pid

    raise ServerNotFound(pid_path)


# Subcommands #

@subcommand
def install(context):
    path = installpath(context)
    if not path:
        raise CommandError("specify local server install path via console (--path) "
                           "or settings (LOCAL_PATH)")

    if os.path.exists(path):
        if context.force or not os.listdir(path):
            shutil.rmtree(path)
        else:
            raise CommandError("server destination directory non-empty "
                               "(specify --force to overwrite)\n"
                               "    (tried: `{}')".format(path))
    else:
        parentdir = os.path.dirname(path)
        if not os.path.exists(parentdir):
            os.makedirs(parentdir)

    context.puts("retrieving DDB local server package ...")
    (filename, _headers) = urllib.urlretrieve(settings.LOCAL_DOWNLOAD_URL)

    context.puts("extracting archive ...")
    tempdir = tempfile.mkdtemp()
    archive = tarfile.open(filename, 'r:gz')
    archive.extractall(tempdir)

    try:
        (archive_root,) = {member.name.split('/', 1)[0] for member in archive.members}
    except ValueError:
        srcdir = tempdir
    else:
        # Files are under a release directory, which we want to discard:
        srcdir = os.path.join(tempdir, archive_root)

    context.puts("moving files into place ...")
    shutil.move(srcdir, path)

    context.puts("done install")


@subcommand
def start(context):
    path = installpath(context)
    jar = os.path.join(path, SERVER_JAR)
    if not os.path.exists(jar):
        raise CommandError("no server installation found\n"
                           "    (tried: `{}')".format(jar))
    pid_path = pidpath(context)

    # Check for running server:
    try:
        pid = server_pid(pid_path)
    except StaleServerReference:
        context.puts("removing stale PID file\n"
                     "    (at: `{}') ...".format(pid_path),
                     level=2)
    except ServerNotFound:
        # No pid file or pid file empty
        pass
    else:
        message = "server already running at {}".format(pid)
        if not context.force:
            raise CommandError(message)

        context.puts(message)
        context.puts("removing PID file for {}\n"
                     "    (at `{}') ...".format(pid, pid_path))

    try:
        os.remove(pid_path)
    except OSError:
        pass

    pargs = list(SERVER_ARGS)

    port = context.port or settings.LOCAL_ENDPOINT
    if port:
        pargs.extend(['-port', port.split(':')[-1]])

    memory = settings.LOCAL_MEMORY if context.memory is None else context.memory
    if memory:
        pargs.append('-inMemory')
    else:
        db_path = context.db_path or settings.LOCAL_DB
        if db_path:
            pargs.extend(['-dbPath', db_path])

    final_args = ' '.join(pargs) # java doesn't like argument list
    context.puts("in `{}':".format(path))
    context.puts("    `{}'".format(final_args))
    process = subprocess.Popen(
        final_args,
        cwd=path,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        shell=True,
    )
    with open(pid_path, 'w') as fh:
        fh.write(str(process.pid))
    context.puts("local server started [{}]".format(process.pid))
    context.puts("done start")


@subcommand
def status(context):
    pid_path = pidpath(context)
    try:
        pid = server_pid(pid_path)
    except ServerNotFound:
        raise CommandError("no local server found active\n"
                           "    (tried: `{}')".format(pid_path))
    else:
        context.puts("local server found running [{}]".format(pid))


@subcommand
def stop(context):
    pid_path = pidpath(context)
    try:
        pid = open(pid_path).read()
        if pid:
            parent = psutil.Process(int(pid))
            children = parent.get_children()
            for proc in itertools.chain([parent], children):
                proc.terminate()
    except (IOError, OSError):
        pass
    else:
        os.remove(pid_path)
        context.puts("local server stopped [{}]".format(pid))
        context.puts("done stop")
        return

    raise CommandError("no local server found active\n"
                       "    (tried: `{}')".format(pid_path))
