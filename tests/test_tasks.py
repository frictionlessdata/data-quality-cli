# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from data_quality import tasks, utilities, compat, generators
from . import mock_generator
from goodtables import pipeline

class TestTasks(unittest.TestCase):
    """Test `tasks.py` """

    def test_aggregator_run(self):
        """Test that Aggregator task runs as post task and updates results"""

        aggregator_task = tasks.Aggregator(self.get_config())
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        results_before_run = self.read_file_contents(aggregator_task.result_file)
        pipeline_instance.run()
        results_after_run = self.read_file_contents(aggregator_task.result_file)

        self.assertEqual(len(results_after_run), len(results_before_run) + 1)

    def test_agregator_batch_run(self):
        """Test that Aggregator task updates run file after each batch"""

        config = self.get_config()
        aggregator_task = tasks.Aggregator(config)

        def mokup_function(instance):
            aggregator_task.write_run()
        batch_options = config['goodtables']['arguments']['batch']
        batch_options['post_task'] = mokup_function
        batch_options['pipeline_options'] = config['goodtables']['arguments']['pipeline']
        batch = pipeline.Batch(aggregator_task.source_file, **batch_options)
        runs_before_run = self.read_file_contents(aggregator_task.run_file)
        batch.run()
        runs_after_run = self.read_file_contents(aggregator_task.run_file)

        self.assertGreater(len(runs_after_run), len(runs_before_run))

    def test_aggregator_fetch(self):
        """Test that Aggregator task fetches the source"""

        aggregator_task = tasks.Aggregator(self.get_config())
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        utilities.set_up_cache_dir(aggregator_task.cache_dir)

        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        pipeline_instance.run()
        file_names = []
        for file_name in os.listdir(aggregator_task.cache_dir):
            file_names.append(file_name)
        self.assertEquals(file_names,['valid.csv'])

    def test_generate_built_in_generator(self):
        """Test that Generate task loads a built-in generator"""

        generator = tasks.Generate(utilities.load_json_config(None))
        generator_class = generator.run('ckan', 'endpoint', '',
                                        file_types=['csv','excel'], simulate=True)

        self.assertIsInstance(generator_class, generators.CkanGenerator)

    def test_generate_custom_generator(self):
        """Test that Generate task loads a custom generator"""
        
        generator = tasks.Generate(self.get_config())
        generator_path = 'tests.mock_generator.MockGenerator'
        generator_class = generator.run('mock', 'endpoint', generator_path,
                                        None, simulate=True)

        self.assertIsInstance(generator_class, mock_generator.MockGenerator)

    def test_performance_created(self):
        """Test that AssessPerformance task creates the performance file"""

        config = self.get_config()
        assess_performance_task = tasks.AssessPerformance(config)
        assess_performance_task.run()
        self.assertTrue(os.path.exists(assess_performance_task.performance_file))

    def test_performance_calculation(self):
        """Test that AssessPerformance task calculates performance correctly"""

        config = self.get_config()
        assess_performance_task = tasks.AssessPerformance(config)
        assess_performance_task.run()
        test_dict = {'files_count_to_date': '1', 'valid_to_date': '100',
                     'score_to_date': '100', 'score': '100',
                     'period_id': '2015-01-01', 'publisher_id': 'xx_dept1',
                     'valid': '100', 'files_count': '1'}
        with compat.UnicodeDictReader(assess_performance_task.performance_file) as pf:
            self.assertGreater(self.find_in_sequence(pf, test_dict), -1)

    def read_file_contents(self, file_name):
        """Return file contents as list of dicts"""

        contents = []
        with compat.UnicodeDictReader(file_name) as src_file:
            for line in src_file:
                contents.append(line)
        return contents

    def get_config(self):
        """Load the fixture config"""

        config_filepath = os.path.join('tests', 'fixtures', 'dq.json')
        config = utilities.load_json_config(config_filepath)
        return config

    def find_in_sequence(self, sequence, target):
        """Find `target` in `sequence`"""

        found = False
        for position, value in enumerate(sequence):
            if value == target:
                found = True
                break
        if not found:
            return -1
        return position
