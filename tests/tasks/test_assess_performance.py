# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
from .test_task import TestTask
from data_quality import tasks, utilities, compat

class TestPerformanceAssessorTask(TestTask):
    """Test the PerformanceAssessor task"""

    def test_performance_created(self):
        """Test that PerformanceAssessor task creates the performance file"""

        config = self.config
        assess_performance_task = tasks.PerformanceAssessor(config)
        assess_performance_task.run()
        self.assertTrue(os.path.exists(assess_performance_task.performance_file))

    def test_performance_calculation(self):
        """Test that PerformanceAssessor task calculates performance correctly"""

        config = self.config
        assess_performance_task = tasks.PerformanceAssessor(config)
        assess_performance_task.run()
        test_dict = {'files_count_to_date': '1', 'valid_to_date': '100',
                     'score_to_date': '100', 'score': '100',
                     'period_id': '2015-01-01', 'publisher_id': 'xx_dept1',
                     'valid': '100', 'files_count': '1'}
        with compat.UnicodeDictReader(assess_performance_task.performance_file) as pf:
            self.assertGreater(self.find_in_sequence(pf, test_dict), -1)

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
