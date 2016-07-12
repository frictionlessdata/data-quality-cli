# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from data_quality import tasks, utilities, compat


class TestDataPackageInitializer(unittest.TestCase):
    """Test the DataPackageInitializer task"""

    def setUp(self):
        self.workspace_path = './tests/tmp_datapackage/'
        utilities.resolve_dir(self.workspace_path)

    def tearDown(self):
        utilities.set_up_cache_dir(self.workspace_path)
        os.rmdir(self.workspace_path)

    def test_config_initialized(self):
        """Test that DataPackageInitializer generates a config file if there isn\'t one"""

        initializer = tasks.DataPackageInitializer(self.workspace_path)
        initializer.initialize_config()
        self.assertTrue(os.path.exists(os.path.join(self.workspace_path, 'dq_config.json')))

    def test_run(self):
        """Test that DataPackageInitializer generates a 'datapackage.json' file"""

        initializer = tasks.DataPackageInitializer(self.workspace_path)
        initializer.run()
        self.assertTrue(os.path.exists(os.path.join(self.workspace_path, 'datapackage.json')))

