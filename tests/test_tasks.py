# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json
import os
from data_quality import tasks, utilities, compat
from goodtables import pipeline

class TestTasks(unittest.TestCase):
    """Test `tasks.py` """

    def test_aggregator_run(self):
        """Test that aggregator runs as post task and updates results"""

        aggregator = tasks.Aggregator(self.get_config())
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator.run)
        results_before_run = self.read_file_contents(aggregator.result_file)
        pipeline_instance.run()
        results_after_run = self.read_file_contents(aggregator.result_file)

        self.assertGreater(len(results_after_run), len(results_before_run))

    def test_agregator_batch_run(self):
        """Test that aggregator updates runs after each batch"""

        config = self.get_config()
        aggregator = tasks.Aggregator(config)

        def mokup_function(instance):
            aggregator.write_run()
        batch_options = config['goodtables']['arguments']['batch']
        batch_options['post_task'] = mokup_function
        batch_options['pipeline_options'] = config['goodtables']['arguments']['pipeline']
        batch = pipeline.Batch(aggregator.source_file, **batch_options)
        runs_before_run = self.read_file_contents(aggregator.run_file)
        batch.run()
        runs_after_run = self.read_file_contents(aggregator.run_file)

        self.assertGreater(len(runs_after_run), len(runs_before_run))

    def read_file_contents(self, file_name):
        """Return file contents as list of lists"""

        contents = []
        with compat.UnicodeReader(file_name) as src_file:
            for line in src_file:
                contents.append(line)
        return contents

    def get_config(self):
        """Load the fixture config"""

        config_filepath = os.path.join('tests', 'fixtures', 'dq.json')
        config = utilities.load_json_config(config_filepath)
        return config
