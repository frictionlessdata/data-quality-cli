# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import unittest
import subprocess
import data_quality

class TestDataQualityCLI(unittest.TestCase):

    def test_cli_run(self):
        config_path = os.path.join('tests', 'fixtures', 'dq.json')
        c = ['python', '-m', 'data_quality.main', 'run', config_path]
        subprocess.check_output(c)
