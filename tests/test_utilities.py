# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from data_quality import utilities

class TestUtilities(unittest.TestCase):

    def test_that_config_is_correctly_leaded(self):
        config_filepath = os.path.join('tests', 'fixtures', 'dq.json')
        config = utilities.load_json_config(config_filepath)
        self.assertTrue(os.path.isabs(config['data_dir']))

    def test_that_datapackage_correctly_loaded(self):
        config_filepath = os.path.join('tests', 'fixtures', 'dq.json')
        config = utilities.load_json_config(config_filepath)
        generated_datapackage, path = utilities.load_json_datapackage(config)
        self.assertGreater(len(generated_datapackage.resources), 0)

    def test_that_datapackage_is_generated(self):
        path = 'tests/datapackage.json'
        config = {'datapackage_file': path, 'data_dir': 'tests/fixtures'}
        if os.path.exists(path):
            os.remove(path)
        datapackage, datapackage_path = utilities.load_json_datapackage(config)
        self.assertEqual(os.path.abspath(path), datapackage_path)
        os.remove(path)
