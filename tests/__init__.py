import os.path
import sys
from textwrap import dedent

import mock
from nose import tools

from faraday import db, loading
from faraday.conf import settings
from faraday.management import call_command, CommandError
from faraday.management.commands.local import jarpath


TEST_HOST = 'localhost'
TEST_PORT = 4444
TEST_ENDPOINT = '{}:{}'.format(TEST_HOST, TEST_PORT)

SETTINGS_PATCH = mock.patch.multiple(settings,
                                     ENGINE='local',
                                     LOCAL_ENDPOINT=TEST_ENDPOINT)

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(_TEST_DIR)
TEST_SERVER_PID = os.path.join(_PARENT_DIR, '.pid')

SERVER_PARAMS = {'pid-path': TEST_SERVER_PID, 'port': TEST_PORT}


def stop_server():
    """Stop any running test server"""
    try:
        call_command('local', 'stop', **SERVER_PARAMS)
    except CommandError:
        # server not running
        pass


def ensure_install():
    jar_path = jarpath()
    if os.path.exists(jar_path):
        return

    # Handle nose stdout capture:
    captured = not isinstance(sys.stdout, file)
    if captured:
        capture_patch = mock.patch.object(sys, 'stdout', sys.stderr)
        capture_patch.start()

    try:
        raw_input(dedent("""\
            no existing server installation found
            test will install server to default path:
                `{}'
            [^M to continue, ^C to abort]"""
        .format(os.path.dirname(jar_path))))
    finally:
        if captured:
            capture_patch.stop()

    call_command('local', 'install')


def setup_package():
    # Patch settings for test server:
    SETTINGS_PATCH.start()
    tools.eq_(db.connection.host, TEST_HOST)
    tools.eq_(db.connection.port, TEST_PORT)

    # Start new test server:
    ensure_install()
    stop_server()
    call_command('local', 'start', '--memory', **SERVER_PARAMS)


def teardown_package():
    stop_server()
    SETTINGS_PATCH.stop()


class FaradayTestCase(object):

    @classmethod
    def teardown_class(cls):
        # Concrete test classes may define their Item classes in setup_class.
        # Their tables will be created & destroyed per test run by setup &
        # teardown, and the Python class will be hidden within the test class's
        # scope; however, the Item definition cache on which build/destroy rely
        # will be polluted.
        # Therefore, clear all non-namespaced definitions from the cache;
        # (to avoid this, and e.g. to define your Item at the module level /
        # outside setup_class, set Meta.app_name to something novel):
        for model in loading.cache.values():
            if not model._meta.app_name:
                del loading.cache[model._meta.item_name]

    def setup(self):
        db.build()

    def teardown(self):
        db.destroy()
