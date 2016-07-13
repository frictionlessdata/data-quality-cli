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
import datetime
import jsontableschema
from data_quality import utilities, compat, exceptions
from .base_task import Task
from .check_datapackage import DataPackageChecker


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
        self.run_schema = jsontableschema.model.SchemaModel(run_resource.metadata['schema'])
        self.result_schema = jsontableschema.model.SchemaModel(result_resource.metadata['schema'])
        self.initialize_file(self.result_file, self.result_schema.headers)
        self.initialize_file(self.run_file, self.run_schema.headers)
        self.run_id = compat.str(uuid.uuid4().hex)
        self.timestamp = datetime.datetime.now(pytz.utc)
        self.max_score = 10
        required_resources = [self.result_file, self.source_file,
                              self.publisher_file, self.run_file]
        datapackage_check.check_database_completeness(required_resources)
        self.lookup = self.get_lookup()

    def run(self, pipeline):
        """Run on a Pipeline instance."""

        with compat.UnicodeAppender(self.result_file, quoting=csv.QUOTE_MINIMAL) as result_file:
            source = self.get_source(pipeline.data_source)
            result_id = compat.str(uuid.uuid4().hex)
            period_id = source['period_id']
            score = self.get_pipeline_score(pipeline)
            data_source = pipeline.data_source
            schema = ''
            summary = '' # TODO: how/what should a summary be?
            report = self.get_pipeline_report_url(pipeline)

            result = [result_id, source['id'], source['publisher_id'],
                      period_id, data_source, schema, score, summary,
                      self.run_id, self.timestamp, report]
            try:
                result_file.writerow(list(self.result_schema.convert_row(*result)))
            except jsontableschema.exceptions.MultipleInvalid as e:
                for error in e.errors:
                    raise error

            if pipeline.data:
                self.fetch_data(pipeline.data.stream, pipeline.data.encoding, source)

    def get_lookup(self):

        _keys = ['id', 'publisher_id', self.data_key, 'period_id', 'title']
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

    def get_pipeline_score(self, pipeline):
        """Return a score for this pipeline run."""

        score = self.max_score
        results = pipeline.report.generate()['results']

        if not results:
            pass
        else:
            result_count = len(results)

            if result_count > score:
                score = 0
            else:
                score = score - result_count

        self.all_scores.append(score)
        return score

    def get_pipeline_report_url(self, pipeline):
        """Return a URL to a report on this data."""

        return self.config['goodtables']['goodtables_web']

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
        cached_file_name = os.path.join(self.cache_dir, source_name)
        data_stream.seek(0)

        with io.open(cached_file_name, mode='w+', encoding=encoding) as fetched_file:
            for line in data_stream:
                fetched_file.write(line)
