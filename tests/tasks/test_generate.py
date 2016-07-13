# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
import io
import json
from data_quality import tasks, utilities, compat, generators
from tests import mock_generator
from .test_task import TestTask

class TestGeneratorManagerTask(TestTask):
    """Test the GeneratorManager task"""

    def test_generate_built_in_generator(self):
        """Test that GeneratorManager task loads a built-in generator"""

        generator = tasks.GeneratorManager(self.config)
        generator_class = generator.run('ckan', 'endpoint', '',
                                        file_types=['csv','excel'], simulate=True)

        self.assertIsInstance(generator_class, generators.CkanGenerator)

    def test_generate_custom_generator(self):
        """Test that GeneratorManager task loads a custom generator"""

        generator = tasks.GeneratorManager(self.config)
        generator_path = 'tests.mock_generator.MockGenerator'
        generator_class = generator.run('mock', 'endpoint', generator_path,
                                        None, simulate=True)

        self.assertIsInstance(generator_class, mock_generator.MockGenerator)

    def test_generate_update_datapackage_sources(self):
        """Test that GeneratorManager task updates datapackage sources"""

        def empty_datapackage_sources(datapkg_path, datapkg):
            with io.open(datapkg_path, mode='w+', encoding='utf-8') as datapkg_file:
                datapkg.metadata['sources'] = []
                updated_json = json.dumps(datapkg.to_dict(), indent=4, sort_keys=True)
                datapkg_file.write(compat.str(updated_json))

        generator = tasks.GeneratorManager(self.config)
        datapkg_path = os.path.join(generator.datapackage.base_path,
                                    'datapackage.json')
        empty_datapackage_sources(datapkg_path, generator.datapackage)
        generator.update_datapackage_sources()
        second_generator = tasks.GeneratorManager(self.config)

        self.assertEquals(generator.datapackage.metadata['sources'],
                          second_generator.datapackage.metadata['sources'])
        self.assertGreater(len(generator.datapackage.metadata['sources']), 0)
        empty_datapackage_sources(datapkg_path, generator.datapackage)
