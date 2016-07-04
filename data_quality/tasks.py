# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from time import gmtime, strftime
import os
import io
import csv
import uuid
import datetime
import subprocess
import contextlib
import pytz
import re
import json
import dateutil
import importlib
import jsontableschema
from runpy import run_path
from pydoc import locate
from . import compat, exceptions, generators, utilities

@contextlib.contextmanager
def cd(path):
    """Move into a dir while the context is active."""
    workpath = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(workpath)


class Task(object):

    """Base class for Data Quality CLI tasks."""

    def __init__(self, config, **kwargs):
        self.config = config
        self.remotes = self.config['remotes']
        self.branch = self.config['branch']
        self.data_dir = self.config['data_dir']
        self.result_file = os.path.join(self.data_dir, self.config['result_file'])
        self.run_file = os.path.join(self.data_dir, self.config['run_file'])
        self.source_file = os.path.join(self.data_dir, self.config['source_file'])
        self.performance_file = os.path.join(self.data_dir,
                                             self.config['performance_file'])
        self.publishers_file = os.path.join(self.data_dir,
                                            self.config['publisher_file'])
        self.cache_dir = self.config['cache_dir']
        self.datapackage = utilities.get_default_datapackage()
        self.all_scores = []


class Aggregator(Task):

    """A Task runner to create results for data sources as they move
    through a processing pipeline."""

    def __init__(self, *args, **kwargs):
        super(Aggregator, self).__init__(*args, **kwargs)
        self.max_score = 10
        self.lookup = self.get_lookup()
        run_resource = utilities.get_resource_by_name('run_file',
                                                      self.datapackage)
        result_resource = utilities.get_resource_by_name('result_file',
                                                         self.datapackage)
        self.run_schema = jsontableschema.model.SchemaModel(run_resource.metadata['schema'])
        self.result_schema = jsontableschema.model.SchemaModel(result_resource.metadata['schema'])
        self.run_id = compat.str(uuid.uuid4().hex)
        self.timestamp = datetime.datetime.now(pytz.utc)

        self.initialize_file(self.result_file, self.result_schema.headers)
        self.initialize_file(self.run_file, self.run_schema.headers)

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

        data_key = self.config['goodtables']['arguments']['batch']['data_key']
        _keys = ['id', 'publisher_id', data_key , 'period_id']
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

        data_key = self.config['goodtables']['arguments']['batch']['data_key']
        matches = [match for match in self.lookup if match[data_key] == data_src]

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

        data_key = self.config['goodtables']['arguments']['batch']['data_key']
        source_name = source.get('name', source[data_key].rsplit('/', 1)[-1])
        cached_file_name = os.path.join(self.cache_dir, source_name)
        data_stream.seek(0)

        with io.open(cached_file_name, mode='w+', encoding=encoding) as fetched_file:
            for line in data_stream:
                fetched_file.write(line)


class AssessPerformance(Task):

    """A Task runner to assess and write the performance of publishers for each
    period."""

    def __init__(self, *args, **kwargs):
        super(AssessPerformance, self).__init__(*args, **kwargs)

    def run(self):
        """Write the performance for all publishers."""

        publisher_ids = self.get_publishers()
        performance_resource = utilities.get_resource_by_name('performance_file',
                                                              self.datapackage)
        performance_schema = jsontableschema.model.SchemaModel(performance_resource.metadata['schema'])

        with compat.UnicodeWriter(self.performance_file) as performance_file:
            performance_file.writerow(performance_schema.headers)
            available_periods = []

            for publisher_id in publisher_ids:
                sources = self.get_sources(publisher_id)
                periods = self.get_unique_periods(sources)
                available_periods += periods
            all_periods = self.get_all_periods(available_periods)

            publishers_performances = []
            all_sources = []

            for publisher_id in publisher_ids:
                sources = self.get_sources(publisher_id)
                performances = self.get_periods_data(publisher_id, all_periods,
                                                     sources)
                publishers_performances += performances
                all_sources += sources
                for performance in performances:
                    try:
                        values = [performance[key] for key in performance_schema.headers]
                        row = list(performance_schema.convert_row(*values))
                        performance_file.writerow(row)
                    except jsontableschema.exceptions.MultipleInvalid as e:
                        for error in e.errors:
                            raise error

            all_performances = self.get_periods_data('all', all_periods, all_sources)

            for performance in all_performances:
                try:
                    values = [performance[key] for key in performance_schema.headers]
                    row = list(performance_schema.convert_row(*values))
                    performance_file.writerow(row)
                except jsontableschema.exceptions.MultipleInvalid as e:
                    for error in e.errors:
                        raise error

    def get_publishers(self):
        """Return list of publishers ids."""

        publisher_ids = []

        with compat.UnicodeDictReader(self.publishers_file) as publishers_file:
            for row in publishers_file:
                publisher_ids.append(row['id'])
        return publisher_ids

    def get_sources(self, publisher_id):
        """Return list of sources of a publisher with id, period and score. """

        sources = []

        with compat.UnicodeDictReader(self.source_file) as sources_file:
            for row in sources_file:
                source = {}
                if row['publisher_id'] == publisher_id:
                    source['id'] = row['id']
                    source['period_id'] = self.get_period(row['period_id'])
                    source['score'] = self.get_source_score(source['id'])
                    sources.append(source)
        return sources

    def get_source_score(self, source_id):
        """Return latest score of a source from results.

        Args:
            source_id: id of the source whose score is wanted
        """

        score = 0
        latest_timestamp = pytz.timezone('UTC').localize(datetime.datetime.min)

        with compat.UnicodeDictReader(self.result_file) as result_file:
            for row in result_file:
                if row['source_id'] == source_id:
                    timestamp = dateutil.parser.parse(row['timestamp'])
                    if timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        score = int(row['score']) * 10
        return score

    def get_period(self, period):
        """Return a valid period as date object

        Args:
            period: a string that might contain a date or range of dates

        """

        if not period:
            return ''
        try:
            date_object = dateutil.parser.parse(period).date()
            return date_object
        except ValueError:
            return ''

    def get_periods_data(self, publisher_id, periods, sources):
        """Return list of performances for a publisher, by period.

        Args:
            publisher_id: publisher in dicussion
            periods: list of all available_periods
            sources: list of publisher's sources

        """

        performances = []
        period_sources_to_date = []

        for period in periods:
            period_sources = self.get_period_sources(period, sources)
            period_sources_to_date += period_sources
            performance = {}
            performance['publisher_id'] = publisher_id
            performance['period_id'] = compat.str(period)
            performance['files_count'] = len(period_sources)
            performance['score'] = self.get_period_score(period_sources)
            performance['valid'] = self.get_period_valid(period_sources)
            performance['score_to_date'] = self.get_period_score(period_sources_to_date)
            performance['valid_to_date'] = self.get_period_valid(period_sources_to_date)
            performance['files_count_to_date'] = len(period_sources_to_date)
            performances.append(performance)
        return performances

    def get_period_sources(self, period, sources):
        """Return list of sources for a period.

        Args:
            period: a date object
            sources: list of sources

        """

        period_sources = []

        for source in sources:
            if period == source['period_id']:
                period_sources.append(source)
        return period_sources

    def get_period_score(self, period_sources):
        """Return average score from list of sources.

        Args:
            period_sources: sources correspoding to a certain period
        """

        score = 0

        if len(period_sources) > 0:
            total = 0
            for source in period_sources:
                total += int(source['score'])
            score = int(round(total / len(period_sources)))
        return score

    def get_period_valid(self, period_sources):
        """Return valid percentage from list of sources.

        Args:
            period_sources: sources correspoding to a certain period
        """

        valid = 0
        if len(period_sources) > 0:
            valids = []
            for source in period_sources:
                if int(source['score']) == 100:
                    valids.append(source)
            if valids:
                valid = int(round(len(valids) / len(period_sources) * 100))
        return valid

    def get_unique_periods(self, sources):
        """Return list of unique periods as date objects from sources.

        Args:
            sources: a list of sources

        """

        periods = []
        for source in sources:
            periods.append(source['period_id'])
        periods = list(set(periods))
        return periods

    def get_all_periods(self, periods):
        """Return all periods from oldest in periods to now.

        Args:
            periods: list of date objects

        """

        oldest_date = sorted(periods)[0]
        current_date = datetime.date.today()
        delta = dateutil.relativedelta.relativedelta(months=1)
        relative_date = oldest_date
        all_periods = []

        while relative_date <= current_date:
            all_periods.append(relative_date)
            relative_date += delta
        return all_periods


class Deploy(Task):

    """A Task runner to deploy a Data Quality repository to a remote."""

    commit_msg = 'New result and run data.'
    tag_msg = 'New result and run data.'
    tag_version = ''

    def run(self, *args):
        """Commit and deploy changes."""

        self._pull()
        self.update_last_modified()
        self._add()
        self._commit()
        # self._tag()
        self._push()

    def _pull(self):
        """Pull in any changes from remotes."""

        with cd(self.config['data_dir']):

            for remote in self.remotes:
                # fetch
                command = ['git', 'fetch', remote, self.branch]
                subprocess.call(command)
                # merge; prefer ours
                command = ['git', 'merge', '-s', 'recursive', '-X', 'ours',
                           '{0}/{1}'.format(remote, self.branch)]
                subprocess.call(command)

    def _add(self):
        """Add the changed files to the git index."""

        with cd(self.config['data_dir']):

            # add the changed files
            command = ['git', 'add', self.result_file]
            subprocess.call(command)
            command = ['git', 'add', self.run_file]
            subprocess.call(command)

    def _commit(self):

        with cd(self.config['data_dir']):
            command = ['git', 'commit', '-a', '-m', '{0}'.format(self.commit_msg)]
            subprocess.call(command)

    def _tag(self):
        with cd(self.config['data_dir']):
            command = ['git', 'tag', '-a', self.tag_version, '-m', '{0}'.format(self.tag_msg)]
            subprocess.call(command)

    def _push(self):

        with cd(self.config['data_dir']):
            command = ['git', 'push', '--follow-tags']
            subprocess.call(command)

    def update_last_modified(self):

        user_datapkg, user_datapkg_path = utilities.load_json_datapackage(self.config)
        datapkg_metadata = user_datapkg.metadata

        with io.open(user_datapkg_path, mode='w+', encoding='utf-8') as datapkg:
            current_time = strftime("%Y-%m-%d %H:%M:%S %Z", gmtime())
            datapkg_metadata['last_modified'] = current_time
            updated_json = json.dumps(datapkg_metadata, indent=4)
            datapkg.write(compat.str(updated_json))

class Generate(Task):

    """A Task runner to generate a dataset from a certain resource (ex: CKAN)."""

    def __init__(self, config):
        super(Generate, self).__init__(config)

    def run(self, generator_name, endpoint, generator_path, file_types, simulate=False):
        """Delegate the generation processes to the chosen generator

        Args:
            generator_name: Name of the generator (ex: ckan)
            endpoint: Url where the generator should get the data from
            generator_path: Path to the custom generator class, if used
            file_types: List of file types that should be included in sources
        """

        if  generators._built_in_generators.get(generator_name, None):
            generator_class = generators._built_in_generators[generator_name]
        else:
            try:
                _module, _class = generator_path.rsplit('.', 1)
                generator_class = getattr(importlib.import_module(_module), _class)
            except ValueError:
                raise ValueError(('The path you provided for the generator class is '
                                  'not valid. Should be of type `mymodule.MyGenerator`'))
        generator = generator_class(endpoint, self.datapackage)

        if simulate:
            return generator

        generator.generate_publishers(self.publishers_file)
        generator.generate_sources(self.source_file, file_types=file_types)
