"""faraday command interface for Django"""
from __future__ import absolute_import

import faraday.management


class Command(object):
    """Django management command framework proxy for faraday

        python manage.py faraday ...

    Facilitates invokation of faraday commands in a Django environment,
    no different from:

        DJANGO_SETTINGS_MODULE=settings PYTHONPATH=. faraday ...

    Install into your Django project via INSTALLED_APPS, as:

        'faraday.integration.django',

    """
    # No need to import Django and inherit BaseCommand;
    # we don't have much to do here
    def print_help(self, prog_name, subcommand):
        self.run_from_argv([prog_name, subcommand, '--help'])

    def run_from_argv(self, argv):
        faraday.management.main(argv[2:])
