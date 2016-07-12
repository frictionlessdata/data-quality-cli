# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
import datapackage
from data_quality import tasks, utilities, compat
from .test_task import TestTask

class TestDataPackageChecker(TestTask):
    """Test the DataPackageChecker task"""

    def test_lacking_required_field(self):
        """Test that DataPackageChecker raises if required field is missing"""

        filename = 'datapackage_schema_missing_required.json'
        self.config['datapackage_file'] = os.path.join('tests', 'fixtures', filename)
        checker = tasks.check_datapackage.DataPackageChecker(self.config)
        default_datapkg = utilities.get_default_datapackage()
        self.assertRaisesRegexp(ValueError, 'miss', checker.check_resource_schema,
                                default_datapkg.resources[0], checker.datapackage.resources[0])

    def test_run(self):
        """Test that DataPackageChecker raises if required resource is missing"""

        filename = 'datapackage_schema_missing_required.json'
        self.config['datapackage_file'] = os.path.join('tests', 'fixtures', filename)
        checker = tasks.check_datapackage.DataPackageChecker(self.config)
        self.assertRaisesRegexp(ValueError, 'found', checker.run())

    def test_database_content(self):
        """Test that DataPackageChecker raises if a required file from the database 
           doesn't respect the schema described in datapackage
        """

        checker = tasks.check_datapackage.DataPackageChecker(self.config)
        self.assertRaisesRegexp(ValueError, 'schema', checker.check_database_content)