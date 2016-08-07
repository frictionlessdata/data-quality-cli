# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from .test_task import TestTask
from data_quality import tasks, utilities, compat
from goodtables import pipeline


class TestAggregatorTask(TestTask):
    """Test the Aggregator task"""

    def test_aggregator_run(self):
        """Test that Aggregator task runs as post task and updates results"""

        aggregator_task = tasks.Aggregator(self.config)
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        results_before_run = self.read_file_contents(aggregator_task.result_file)
        pipeline_instance.run()
        results_after_run = self.read_file_contents(aggregator_task.result_file)

        self.assertEqual(len(results_after_run), len(results_before_run) + 1)

    def test_agregator_batch_run(self):
        """Test that Aggregator task updates run file after each batch"""

        config = self.config
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

        aggregator_task = tasks.Aggregator(self.config)
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        utilities.set_up_cache_dir(aggregator_task.cache_dir)

        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        pipeline_instance.run()
        file_names = []
        for file_name in os.listdir(aggregator_task.cache_dir):
            file_names.append(file_name)
        self.assertEquals(file_names,['valid.csv'])

    def test_aggregator_assess_timeliness(self):
        """Test that Aggregator calls the RelevancePeriodExtractor"""

        self.config['source_file'] = 'sources_with_period_id.csv'
        self.config['datapackage_file'] = 'datapackage_sources_with_period.json'
        self.config['assess_timeliness'] = True
        self.config['timeliness']['timeliness_strategy'] = ['period_id']
        extractor = tasks.extract_relevance_period.RelevancePeriodExtractor(self.config)
        extractor.run()
        aggregator_task = tasks.Aggregator(self.config)
        url = 'https://raw.githubusercontent.com/okfn/tabular-validator/master/examples/valid.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        pipeline_instance.run()
        updated_sources = self.read_file_contents(aggregator_task.result_file)
        result = updated_sources[-1]
        score = int(result['score'])
        self.assertEqual(98, score)

    def test_aggregate_scoring_affected_rows(self):
        """Test Aggregator scoring based on affected_rows"""

        self.config['scoring_algorithm'] = 2
        aggregator_task = tasks.Aggregator(self.config)
        url = 'https://raw.githubusercontent.com/frictionlessdata/goodtables/master/examples/empty_rows_multiple.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        pipeline_instance.run()
        result = self.read_file_contents(aggregator_task.result_file)[-1]

        self.assertEqual(int(result['score']), 0)

    def tests_aggreate_scoring_occurrences(self):
        """Test Aggregator scoring based on error occurences"""

        aggregator_task = tasks.Aggregator(self.config)
        url = 'https://raw.githubusercontent.com/frictionlessdata/goodtables/master/examples/empty_rows_multiple.csv'
        pipeline_instance = pipeline.Pipeline(data=url, format='csv',
                                              post_task=aggregator_task.run)
        pipeline_instance.run()
        result = self.read_file_contents(aggregator_task.result_file)[-1]

        self.assertEqual(int(result['score']), 67)

    def read_file_contents(self, file_name):
        """Return file contents as list of dicts"""

        contents = []
        with compat.UnicodeDictReader(file_name) as src_file:
            for line in src_file:
                contents.append(line)
        return contents
