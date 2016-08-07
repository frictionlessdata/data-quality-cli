# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import datetime
from data_quality import exceptions
from data_quality.tasks.extract_relevance_period import RelevancePeriodExtractor
from .test_task import TestTask

class TestRelevancePeriodExtractor(TestTask):
    """Test the RelevancePeriodExtractor task"""

    def test_extract_dates(self):
        """Test the date extraction"""
        
        self.maxDiff = None
        examples = ['Transparency Data 1 to 30 April 2014',
                    'July 2011 return with descriptions',
                    'DH-May-2010-amnd4',
                    'April 2010 to December 2013',
                    '2010 October Return',
                    'MOD\'s spending over £25,000 for August2014',
                    'jncc-spend-over-25k-2012-01',
                    '12_03_15_data',
                    'Over_%C2%A325K_april_2014',
                    'Transparency_Sept2014_Final.csv',
                    'August - September 2015',
                    '20-12-2015/21-01-2016',
                    '17/07/2014 - 17/08/2014']
        expected =  [[datetime.datetime(2014,4,1), datetime.datetime(2014,4,30)],
                    [datetime.datetime(2011,7,31)],
                    [datetime.datetime(2010,5,31)],
                    [datetime.datetime(2010,4,30), datetime.datetime(2013,12,31)],
                    [datetime.datetime(2010,10,31)],
                    [datetime.datetime(2014,8,31)],
                    [datetime.datetime(2012,1,31)],
                    [datetime.datetime(2015,3,12)],
                    [datetime.datetime(2014,4,30)],
                    [datetime.datetime(2014,9,30)],
                    [datetime.datetime(2015,8,31), datetime.datetime(2015,9,30)],
                    [datetime.datetime(2015,12,20), datetime.datetime(2016,1,21)],
                    [datetime.datetime(2014,7,17), datetime.datetime(2014,8,17)]]

        self.config['timeliness']['timeliness_strategy'] = ['title', 'data']
        results = []
        extractor = RelevancePeriodExtractor(self.config)
        for line in examples:
            dates = extractor.extract_dates(line)
            results.append(dates)
        for index, result in enumerate(results):
            results[index] = sorted([extracted_date['date_obj']
                                     for extracted_date in result])

        self.assertSequenceEqual(results, expected)

    def test_resolve_period(self):
        """Test that a period is extracted and formated properly"""

        sources = [{
                        'title': 'MOD spending over £500 on a GPC and spending over £25,000, April 2010 to December 2013/December 2012 MOD GPC spend',
                        'data': 'https://www.gov.uk/government/uploads/GPC_transparency_data_travel_stationery_contracts_dec2012.csv'
                    },
                    {
                        'title': 'Spend over £25,000 in Natural England/July 2011 return',
                        'data': 'http://data.defra.gov.uk/ops/procurement/1107/ne-over-25k-1107.csv'
                    },
                    {
                        'title': 'Spending over £25,000, April 2010 to December 2013/1 to 29 February 2012 GPC spend',
                        'data': 'https://www.gov.uk/government/uploads/attachment_data/file/28883/GPCTRANSPARENCYDATA1FEBRUARYTO29FEBRUARY2012includingdescriptions.csv'
                    }]

        expected =  [(datetime.datetime(2012,12,1), datetime.datetime(2012,12,31)),
                     (datetime.datetime(2011,7,1), datetime.datetime(2011,7,31)),
                    # This will not be found because the title is uncertain and the file name doesn't have delimitators
                     None]

        self.config['timeliness']['timeliness_strategy'] = ['title', 'data']
        results = []
        extractor = RelevancePeriodExtractor(self.config)
        for source in sources:
            results.append(extractor.identify_period(source))

        self.assertSequenceEqual(results, expected)

    def test_run_raises_if_field_not_provided(self):
        """Test that RelevancePeriodExtractor raises if the field in timeliness_strategy
            doesn't exist in source_file
        """

        self.config['assess_timeliness'] = True
        self.config['timeliness']['timeliness_strategy'] = ['period_id']
        extractor = RelevancePeriodExtractor(self.config)
        self.assertRaisesRegexp(ValueError, 'timeliness_strategy', extractor.run)

    def test_run_raises_if_insufficient_period(self):
        """Tests that RelevancePeriodExtractor raises if sources without `period_id`
            make up over 10% of total sources
        """

        self.config['assess_timeliness'] = True
        self.config['timeliness']['timeliness_strategy'] = ['title', 'data']
        extractor = RelevancePeriodExtractor(self.config)
        self.assertRaises(exceptions.UnableToAssessTimeliness, extractor.run)
