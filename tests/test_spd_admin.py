# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import unittest
import subprocess


#TEST_DIR = os.path.abspath(os.path.dirname(__file__))
#REPO_DIR = os.path.abspath(os.path.dirname(TEST_DIR))
#sys.path.insert(1, REPO_DIR)

#import pytest;pytest.set_trace()


class TestSPDAdmin(unittest.TestCase):

    def test_start(self):

        c = ['python', os.path.join('spd_admin', 'main.py'),'run', 'test-config.json']
        result = subprocess.check_output(c)
