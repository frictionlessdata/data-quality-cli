# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import unittest
import subprocess


class TestDataQualityCLI(unittest.TestCase):

    def test_start(self):
        config_path = os.path.join('fixtures', 'dp.json')
        c = ['python', os.path.join('data_quality', 'main.py'), 'run', config_path]
        subprocess.check_output(c)
