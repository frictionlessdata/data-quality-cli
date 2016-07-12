# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
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

