# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import io
from setuptools import setup, find_packages


def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding='utf-8').read().strip()
    return contents


PACKAGE = 'data_quality'
INSTALL_REQUIRES = ['click>=6.2', 'goodtables>=0.6.0', 'pytz']
TESTS_REQUIRE = ['tox']
README = read('README.md')
VERSION = read(PACKAGE, 'VERSION')
PACKAGES = find_packages(exclude=['examples', 'tests'])

setup(
    name=PACKAGE,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require = {'develop': TESTS_REQUIRE + ['pylint']},
    test_suite='tox',
    zip_safe=False,
    long_description=README,
    description='A CLI that builds a data quality assessment, for use in a Data Quality Dashboard.',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='https://github.com/okfn/data-quality-cli',
    license='MIT',
    keywords=['frictionless data', 'data quality'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    entry_points={
        'console_scripts': [
            'dq = dataquality.main:cli',
            'dataquality = dataquality.main:cli'
        ]
    },
)
