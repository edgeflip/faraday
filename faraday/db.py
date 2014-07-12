import os
import time
import threading

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.exception import JSONResponseError

from . import conf, loading, utils


### connection ###

def _settings_or_environ(key):
    return getattr(conf.settings, key) or os.environ.get(key)


def _make_dynamo_aws():
    """Make a connection to dynamo, based on configuration.

    For internal use.

    :rtype: dynamo connection object

    """
    access_id = _settings_or_environ('AWS_ACCESS_KEY_ID')
    secret = _settings_or_environ('AWS_SECRET_ACCESS_KEY')
    return DynamoDBConnection(aws_access_key_id=access_id,
                              aws_secret_access_key=secret)


def _make_dynamo_local():
    """Make a connection to the local dynamo server, based on configuration.

    For internal use.

    :rtype: dynamo connection object

    """
    (host, port) = conf.settings.LOCAL_ENDPOINT.split(':')
    return DynamoDBConnection(aws_access_key_id='AXX',
                              aws_secret_access_key='SEKRIT',
                              is_secure=False,
                              host=host,
                              port=int(port))


def _make_dynamo():
    """Retrive a [aws|local] dynamo server connection, based on configuration.

    For internal use.

    """
    engine = conf.settings.ENGINE

    if engine == 'aws':
        return _make_dynamo_aws()

    if engine == 'local':
        return _make_dynamo_local()

    raise conf.ConfigurationValueError("Bad value {!r} for ENGINE".format(engine))


class DynamoDBConnectionProxy(object):
    """A lazily-connecting, thread-local proxy to a dynamo server connection."""
    _threadlocal = threading.local()

    @classmethod
    def get_connection(cls):
        try:
            return cls._threadlocal.dynamo
        except AttributeError:
            cls._threadlocal.dynamo = _make_dynamo()
            return cls._threadlocal.dynamo

    # Specify type(self) when calling get_connection to avoid reference to
    # any same-named method on proxied object:
    def __getattr__(self, name):
        return getattr(type(self).get_connection(), name)

    def __setattr__(self, name, value):
        return setattr(type(self).get_connection(), name, value)

connection = DynamoDBConnectionProxy()


### Management ###


def _tables():
    return (item.items.table for item in loading.cache.itervalues())


def _named_tables():
    return (table.short_name for table in _tables())


def iterstatus():
    for table in _tables():
        try:
            description = table.describe()
        except JSONResponseError as exc:
            if exc.error_code == 'ResourceNotFoundException':
                status = 'NOT FOUND'
            else:
                raise
        else:
            status = description['Table']['TableStatus']

        yield (table.table_name, status)


def status():
    return tuple(iterstatus())


def create_table(table, throughput=None):
    return table.create(
        table_name=table.table_name,
        item=table.item,
        schema=table.schema,
        throughput=(throughput or table.throughput),
        indexes=table.indexes,
        connection=table.connection,
    )


def build(timeout=0, wait=2, stdout=utils.dummyio, throughput=None):
    """Create all tables in Dynamo.

    Table creation commands cannot be issued for two tables with secondary keys at
    once, and so commands are issued in order, and job status polled, to finish as
    quickly as possible without error.

    You should only have to call this method once.

    """
    if timeout < 0:
        raise ValueError("Invalid creation timeout")

    # Sort tables so as to create those with secondary keys last:
    tables = sorted(_tables(), key=lambda table: len(table.schema))

    table_number = 0
    for (table_number, table_defn) in enumerate(tables, 1):
        # Issue creation directive to DDB:
        try:
            table = create_table(table_defn, throughput)
        except JSONResponseError:
            utils.LOG.exception('Failed to create table %s', table_defn.table_name)
            continue

        # Monitor job status:
        stdout.write("{}: ".format(table.table_name))
        count = 0
        while count <= timeout:
            # Retrieve status:
            description = table.describe()
            status = description['Table']['TableStatus']

            # Update console:
            if count > 0:
                stdout.write(".")

            if count == 0 or status != 'CREATING':
                stdout.write(status)
            elif count >= timeout:
                stdout.write("TIMEOUT")

            stdout.flush()

            if (
                status != 'CREATING' or        # Creation completed
                count >= timeout or            # We're out of time
                len(table.schema) == 1 or      # Still processing non-blocking tables
                table_number == len(tables)    # This is the last table anyway
            ):
                break # We're done, proceed

            if count + wait <= timeout:
                step = wait
            else:
                step = timeout - count
            time.sleep(step)
            count += step

        stdout.write('\n') # Break line

    return table_number


def iterdestroy():
    """Delete all tables in Dynamo"""
    for table in _tables():
        try:
            table.delete()
        except JSONResponseError as exc:
            if exc.error_code == 'ResourceNotFoundException':
                status = 'NOT FOUND'
            else:
                raise
        else:
            status = 'DELETED'
        yield (table.table_name, status)


def destroy():
    return tuple(iterdestroy())


def itertruncate():
    for table in _tables():
        try:
            with table.batch_write() as batch:
                for item in table.scan():
                    batch.delete_item(**item.get_keys())
        except JSONResponseError as exc:
            if exc.error_code == 'ResourceNotFoundException':
                status = 'NOT FOUND'
            else:
                raise
        else:
            status = 'TRUNCATED'
        yield (table.table_name, status)


def truncate():
    return tuple(itertruncate())
