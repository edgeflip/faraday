import os.path
import subprocess
import tarfile
import tempfile
import urllib

from faraday.conf import settings


SERVER_JAR = 'DynamoDBLocal.jar'
SERVER_ARGS = ('java', '-Djava.library.path=./DynamoDBLocal_lib', '-jar', SERVER_JAR)


def add_parser(subparsers):
    parser = subparsers.add_parser('mock', help="Manage the mock DDB server")
    parser.add_argument(
        'subcmds',
        metavar="SUBCOMMAND",
        nargs='+',
        choices=['install', 'start'],
    )
    parser.add_argument('--install-path')
    parser.add_argument('--pid-path')
    parser.add_argument('--db-path')
    parser.add_argument('--port')
    parser.add_argument('--memory', action='store_true')
    parser.add_argument('-f', '--force', action='store_true')
    return parser


def main(context):
    for cmd in context.subcmds:
        if cmd == 'install':
            install(context)
        elif cmd == 'start':
            start(context)
        else:
            raise SystemExit("Unknown command '{}'".format(cmd))


def localdir():
    return os.path.join(os.getcwd(), '.dynamodb')


def install(context):
    path = context.install_path or settings.MOCK_PATH or localdir()
    if not path:
        raise SystemExit("Specify mock server install path via console (--path) "
                         "or settings (MOCK_PATH)")
    if not context.force and os.path.exists(path) and os.listdir(path):
        raise SystemExit("Server destination directory non-empty (specify --force to overwrite)")

    if not os.path.exists(path):
        os.makedirs(path)

    context.puts("Retrieving DDB local server package ...")
    (filename, _headers) = urllib.urlretrieve(settings.MOCK_DOWNLOAD_URL)

    context.puts("Extracting archive ...")
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

    context.puts("Moving files into place ...")
    for node in os.listdir(srcdir):
        os.rename(os.path.join(srcdir, node), os.path.join(path, node))

    context.puts("Done")


def start(context):
    path = context.install_path or settings.MOCK_PATH or localdir()
    jar = os.path.join(path, SERVER_JAR)
    if not os.path.exists(jar):
        raise SystemExit("No server installation found (tried: `{}')".format(jar))

    pid_path = context.pid_path or settings.MOCK_PID
    if not pid_path:
        pid_path = os.path.join(path, 'pid')

    # Check for running server:
    try:
        pid_file = open(pid_path)
    except IOError:
        pass
    else:
        pid = pid_file.read()
        if pid:
            try:
                os.kill(int(pid), 0) # just checks that it's available
            except OSError:
                # Stale reference
                context.puts(
                    "Removing stale PID file for {} at `{}' ...".format(pid, pid_path),
                    level=2,
                )
            else:
                message = "Server already running at {}".format(pid)
                if not context.force:
                    raise SystemExit(message)

                context.puts(message)
                context.puts("Removing PID file for {} at `{}' ...".format(pid, pid_path))

        os.remove(pid_path)

    pargs = list(SERVER_ARGS)

    port = context.port or settings.MOCK_HOST
    if port:
        pargs.extend(['-port', port.split(':')[-1]])

    memory = settings.MOCK_MEMORY if context.memory is None else context.memory
    if memory:
        pargs.append('-inMemory')
    else:
        db_path = context.db_path or settings.MOCK_DB
        if db_path:
            pargs.extend(['-dbPath', db_path])

    os.chdir(path)
    context.puts("In `{}':".format(os.getcwd()))
    context.puts("    {}".format(' '.join(pargs)))
    process = subprocess.Popen(pargs, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    with open(pid_path) as fh:
        fh.write(process.pid)
    context.puts("DynamoDB Local server is started [{}]".format(process.pid))
