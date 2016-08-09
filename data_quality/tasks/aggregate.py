# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import csv
import uuid
import pytz
import jsontableschema
from math import log
from datetime import datetime, timedelta
from data_quality import utilities, compat, exceptions
from .base_task import Task
from .check_datapackage import DataPackageChecker
from .extract_relevance_period import RelevancePeriodExtractor


class Aggregator(Task):

    """A Task runner to create results for data sources as they move
       through a processing pipeline.
    """

    def __init__(self, config, **kwargs):
        super(Aggregator, self).__init__(config, **kwargs)
        datapackage_check = DataPackageChecker(self.config)
        datapackage_check.run()
        run_resource = utilities.get_datapackage_resource(self.run_file,
                                                          self.datapackage)
        result_resource = utilities.get_datapackage_resource(self.result_file,
                                                             self.datapackage)
        self.run_schema = jsontableschema.model.SchemaModel(run_resource.descriptor['schema'])
        self.result_schema = jsontableschema.model.SchemaModel(result_resource.descriptor['schema'])
        self.initialize_file(self.result_file, self.result_schema.headers)
        self.initialize_file(self.run_file, self.run_schema.headers)
        self.run_id = compat.str(uuid.uuid4().hex)
        self.timestamp = datetime.now(pytz.utc)
        self.all_scores = []
        self.assess_timeliness = self.config['assess_timeliness']
        self.timeliness_period = self.config['timeliness'].get('timeliness_period', 1)
        self.max_score = 100
        required_resources = [self.result_file, self.source_file,
                              self.publisher_file, self.run_file]
        datapackage_check.check_database_completeness(required_resources)
        self.lookup = self.get_lookup()

    def run(self, pipeline):
        """Run on a Pipeline instance."""

        with compat.UnicodeAppender(self.result_file, quoting=csv.QUOTE_MINIMAL) as result_file:
            source = self.get_source(pipeline.data_source)
            result_id = compat.str(uuid.uuid4().hex)
            source['created_at'] = utilities.date_from_string(source['created_at'])
            if source['created_at'] is None:
                raise ValueError(('No date could be extracted from `created_at`'
                                 ' field in source: {0}.').format(source))
            score = self.get_pipeline_score(pipeline, source)
            data_source = pipeline.data_source
            schema = ''
            summary = '' # TODO: how/what should a summary be?
            report = self.get_pipeline_report_url(pipeline)

            result = [result_id, source['id'], source['publisher_id'],
                      source['created_at'], data_source, schema, score,
                      summary, self.run_id, self.timestamp, report]
            try:
                result_file.writerow(list(self.result_schema.convert_row(*result)))
            except jsontableschema.exceptions.MultipleInvalid as e:
                for error in e.errors:
                    raise error

            if pipeline.data:
                self.fetch_data(pipeline.data.stream, pipeline.data.encoding, source)

    def get_lookup(self):

        _keys = ['id', 'publisher_id', self.data_key, 'created_at', 'title',
                 'period_id']
        lookup = []

        with compat.UnicodeDictReader(self.source_file) as sources_file:
            for row in sources_file:
                lookup.append({k: v for k, v in row.items() if k in _keys})

        return lookup

    def initialize_file(self, filepath, headers):
        """"Make sure a file exists and has headers before appending to it.

        Args:
            filepath: path to the file to be created
            headers: a tuple to write as header

        """
        if not os.path.exists(filepath):
            with compat.UnicodeWriter(filepath, quoting=csv.QUOTE_MINIMAL) as a_file:
                a_file.writerow(headers)

    def write_run(self):
        """Write this run in the run file."""

        with compat.UnicodeAppender(self.run_file, quoting=csv.QUOTE_MINIMAL) as run_file:
            entry = [self.run_id, self.timestamp, int(round(sum(self.all_scores) / len(self.lookup)))]
            try:
                run_file.writerow(list(self.run_schema.convert_row(*entry)))
            except jsontableschema.exceptions.MultipleInvalid as e:
                for error in e.errors:
                    raise error

        return True

    def fetch_data(self, data_stream, encoding, source):
        """Cache the data source in the /fetched directory"""

        source_name = source.get('name', source[self.data_key].rsplit('/', 1)[-1])
        source_name = source_name or source['id']
        cached_file_name = os.path.join(self.cache_dir, source_name)
        data_stream.seek(0)

        with io.open(cached_file_name, mode='w+', encoding=encoding) as fetched_file:
            for line in data_stream:
                fetched_file.write(line)

    def get_source(self, data_src):
        """Find the entry correspoding to data_src from sources file"""

        matches = [match for match in self.lookup if match[self.data_key] == data_src]

        if len(matches) == 0:
            raise exceptions.SourceNotFoundError(source=data_src)
        elif len(matches) > 1:
            for pos in range(len(matches)-1):
                first_values = set(matches[pos].values())
                second_values = set(matches[pos+1].values())
                differences = first_values.symmetric_difference(second_values)
                if len(differences) != 0:
                    raise exceptions.DuplicateDataSourceError(source=data_src)

        return matches[0]

    def get_pipeline_report_url(self, pipeline):
        """Return a URL to a report on this data."""

        return self.config['goodtables']['goodtables_web']

    def get_pipeline_score(self, pipeline, source):
        """Return a score for this pipeline run."""

        score = self.max_score
        report = pipeline.report.generate()
        error_stats = self.get_error_stats(report)
        base_errors = {err: stats for err, stats in error_stats.items()
                       if stats['processor'] == 'base'}
        if base_errors:
            score = 0
        else:
            score = self.score_by_error_occurences(error_stats)
            if self.assess_timeliness:
                publication_delay = self.get_publication_delay(source)
                score -= publication_delay
        score = round(score)
        if score < 0:
            score = 0
        self.all_scores.append(score)
        return score

    def get_publication_delay(self, source):
        """Determine how long the data source publication was delayed"""

        dates = {}
        relevance_period = source['period_id'].split('/')
        relevance_period = relevance_period + [None]*(2 - len(relevance_period))
        dates['period_start'], dates['period_end'] = relevance_period
        dates = {k: utilities.date_from_string(v) for k, v in dates.items()}
        dates['period_end'] = dates['period_end'] or dates['period_start']
        timely_until = dates['period_end'] + \
                       timedelta(days=(self.timeliness_period * 30))
        if dates['period_start'] <= source['created_at'] <= timely_until:
            delay = 0
        else:
            delay = source['created_at'] - timely_until
            delay = delay.days
            if delay < 0:
                delay = 0
            delay = delay / 30.00
        return delay

    def get_error_stats(self, report):
        """Return dict with stats on errors"""

        results = report['results']
        dq_spec = utilities.get_data_quality_spec()
        error_stats = {}
        for result in results:
            if result['result_level'] == 'error':
                error = error_stats.get(result['result_id'], None)
                if not error:
                    if result['processor'] == 'base':
                        error_spec = {}
                    else:
                        error_number = result['result_id'].split('_')[-1]
                        error_number = str(int(error_number) - 1)
                        error_spec = dq_spec[result['processor']][error_number]
                    new_stats = {'occurrences': 1, 'rows': [result['row_index']],
                                 'weight': error_spec.get('weight', 1),
                                 'processor': result['processor']}
                    error_stats[result['result_id']] = new_stats
                else:
                    error['occurrences'] += 1
                    error['rows'].append(result['row_index'])
        return error_stats

    def score_by_error_occurences(self, error_stats):
        """Score data source based on based on number of occurrences of each error
           Algorithm: `total score - (error_weight * no_occurrences) /
                        (Σ 1/no_occurrences )`

           Args:
                error_stats: dict with stats on each error
        """

        score = self.max_score
        for error, stats in error_stats.items():
            no_occurrences = stats['occurrences']
            harmonic_mean_occ = no_occurrences / harmonic_number(no_occurrences)
            error_impact = stats['weight'] * harmonic_mean_occ
            score -= error_impact
        return score

def harmonic_number(n):
    """Return an approximate value of n-th harmonic number, based on the
        Euler-Mascheroni constant by the formula:  H(n)≈ln(n)+γ+1/2*n−1/12*n^2
    """

    gamma = 0.57721566490153286
    return gamma + log(n) + 0.5/n - 1./(12*n**2)
