# -*- coding: utf-8 -*-
#pylint: skip-file
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import io
import csv
import os


_ver = sys.version_info
is_py2 = (_ver[0] == 2)
is_py3 = (_ver[0] == 3)
is_py33 = (is_py3 and _ver[1] == 3)
is_py34 = (is_py3 and _ver[1] == 4)
is_py27 = (is_py2 and _ver[1] == 7)


if is_py2:
    from urlparse import urljoin

    builtin_str = str
    bytes = str
    str = unicode
    basestring = basestring
    numeric_types = (int, long, float)

elif is_py3:
    from urllib.parse import urljoin

    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    numeric_types = (int, float)


def to_bytes(str):
    """Convert a text string to a byte string"""
    return str.encode('utf-8')


def to_builtin_str(str):
    """Convert a text string to the built-in `str` on the runtime."""
    if is_py2:
        return str.encode('utf-8')
    else:
        return str

class UnicodeWriter(object):
    """
        This class provides functionality for writing CSV files
        in a given encoding, python 2 and 3 compatible
        It is a slight adaptation of the code here:
        http://python3porting.com/problems.html#csv-api-changes
    """
    def __init__(self, filename,
                 encoding='utf-8', **kw):
        self.filename = filename
        self.encoding = encoding
        self.kw = kw

    def __enter__(self):
        if is_py3:
            self.f = open(self.filename, 'w+t',
                          encoding=self.encoding)
        else:
            self.f = open(self.filename, 'w+b')
        self.writer = csv.writer(self.f, lineterminator=os.linesep, **self.kw)
        return self

    def __exit__(self, type, value, traceback):
        self.f.close()

    def writerow(self, row):
        for index, val in enumerate(row):
            if type(val) not in [str, bytes, builtin_str]:
                if val is None:
                    val = ''
                val = str(val)
            if is_py2:
                val = val.encode(self.encoding)
            row[index] = val
        self.writer.writerow(row)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class UnicodeAppender(UnicodeWriter):
    """
       This class provides functionality for appending to CSV files
       in a given encoding, python 2 and 3 compatible
    """

    def __enter__(self):
        if is_py3:
            self.f = open(self.filename, 'at',
                          encoding=self.encoding)
        else:
            self.f = open(self.filename, 'ab')
        self.writer = csv.writer(self.f, lineterminator=os.linesep, **self.kw)
        return self


class UnicodeDictWriter(UnicodeWriter):
    """
        This class provides functionality for writing CSV file rows from dicts
        in a given encoding, python 2 and 3 compatible
    """
    def __init__(self, filename, fieldnames, encoding='utf-8', **kw):
        self.fieldnames = fieldnames
        super(UnicodeDictWriter, self).__init__(filename, encoding, **kw)

    def writerow(self, row):
        for key, val in row.items():
            if type(val) not in [str, bytes, builtin_str]:
                if val is None:
                    val = ''
                val = str(val)
            if is_py2:
                val = val.encode(self.encoding)
            row[key] = val
        self.writer.writerow([row.get(key, '') for key in self.fieldnames])

    def writeheader(self):
        self.writer.writerow(self.fieldnames)


class UnicodeReader(object):
    """
       This class provides functionality to read from CSV files
       in a given encoding, python 2 and 3 compatible
    """
    def __init__(self, filename, encoding='utf-8', **kw):
        self.filename = filename
        self.encoding = encoding
        self.kw = kw

    def __enter__(self):
        if is_py3:
            self.f = open(self.filename, 'rt', encoding=self.encoding)
        else:
            self.f = open(self.filename, 'rb')
        self.reader = csv.reader(self.f, **self.kw)
        return self

    def __exit__(self, type, value, traceback):
        self.f.close()

    def next(self):
        row = next(self.reader)
        if is_py3:
            return row
        return [s.decode('utf-8') for s in row]

    __next__ = next

    def __iter__(self):
        return self


class UnicodeDictReader(UnicodeReader):
    """
       This class provides functionality to read CSV file rows as dicts
       in a given encoding, python 2 and 3 compatible
    """
    def __init__(self, filename, encoding='utf-8', **kw):
        super(UnicodeDictReader, self).__init__(filename, encoding, **kw)

    def __enter__(self):
        if is_py3:
            self.f = open(self.filename, 'rt', encoding=self.encoding)
        else:
            self.f = open(self.filename, 'rb')
        self.reader = csv.reader(self.f, **self.kw)
        self.header = next(self.reader)
        return self

    def next(self):
        row = next(self.reader)
        if is_py2:
           row= [s.decode('utf-8') for s in row]
        return {self.header[x]: row[x] for x in range(len(self.header))}

    __next__ = next

    def __iter__(self):
        return
