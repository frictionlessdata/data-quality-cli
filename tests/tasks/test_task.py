# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from data_quality import utilities

class TestTask(unittest.TestCase):
    """Base class for task tests"""
    
    def setUp(self):
        """Load the fixture config"""

        config_filepath = os.path.join('tests', 'fixtures', 'dq.json')
        config = utilities.load_json_config(config_filepath)
        self.config = config
