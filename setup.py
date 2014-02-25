from __future__ import with_statement
import os.path
import re
import sys

import ez_setup
ez_setup.use_setuptools()

from setuptools import find_packages, setup

# For setup.py test in py2.7
# (See http://bugs.python.org/issue15881#msg170215)
try:
    import multiprocessing
except ImportError:
    pass


PACKAGE = 'faraday'
ROOTDIR = os.path.dirname(os.path.abspath(__file__))


def get_version():
    """Retrieve distribution version from `dispatch` package."""
    version_pattern = r'''__version__ *\= *['"]([\d.]+)['"]'''
    with open(os.path.join(ROOTDIR, PACKAGE, '__init__.py')) as init:
        match = re.search(version_pattern, init.read())
    return match.group(1)


def get_long_description():
    """Retrieve long distribution description from README.txt, if available."""
    try:
        return open('README.txt').read()
    except IOError:
        return None


def _is_install():
    return len(sys.argv) >= 2 and sys.argv[1] == 'install'


def get_packages():
    """Packages to list.

    Include `tests` package except on install.

    """
    exclude = ['tests'] if _is_install() else []
    return find_packages('.', exclude=exclude)


def get_py_modules():
    """Modules to list.

    Don't include `ez_setup` on install.

    """
    if _is_install():
        return []
    return ['ez_setup']


def get_requirements(name):
    with open(os.path.join(ROOTDIR, 'requirements', name + '.txt')) as fh:
        return fh.read().splitlines()


setup(
    name=PACKAGE.title(),
    description="A Pythonic modeling framework for DynamoDB",
    long_description=get_long_description(),
    version=get_version(),
    py_modules=get_py_modules(),
    packages=get_packages(),
    install_requires=get_requirements('base'),
    tests_require=get_requirements('dev'),
    test_suite='nose.collector',
    entry_points={
        'console_scripts': ['faraday = faraday.management:main'],
    },
    maintainer="Jesse London",
    maintainer_email="jesse@edgeflip.com",
    url="http://github.com/edgeflip/faraday",
    license='BSD',
    classifiers=(
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ),
)
