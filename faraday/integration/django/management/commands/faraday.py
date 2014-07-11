from __future__ import absolute_import

import faraday.management


class Command(object):

    def run_from_argv(self, argv):
        faraday.management.main(argv[2:])
