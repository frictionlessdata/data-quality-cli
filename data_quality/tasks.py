# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import csv
import uuid
import datetime
import subprocess
import contextlib
import pytz
import re
import dateutil
from .utilities import compat
from .utilities import exceptions


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
        self.sources_file = os.path.join(self.data_dir, self.config['source_file'])
        self.performance_file = os.path.join(self.data_dir, 
                                             self.config['performance_file'])
        self.publishers_file = os.path.join(self.data_dir,
                                             self.config['publisher_file'])
        self.cache_dir = self.config['cache_dir']
        self.all_scores = []


class Aggregator(Task):

    """A Task runner to create results for data sources as they move
    through a processing pipeline."""

    def __init__(self, *args, **kwargs):
        super(Aggregator, self).__init__(*args, **kwargs)
        self.max_score = 10
        self.lookup = self.get_lookup()
        self.run_schema = ('id', 'timestamp', 'total_score')
        self.result_schema = ('id', 'source_id', 'publisher_id', 'period_id',
                              'score', 'data', 'schema', 'summary', 'run_id',
                              'timestamp', 'report')
        self.run_id = uuid.uuid4().hex
        self.timestamp = datetime.datetime.now(pytz.utc).isoformat()

        self.initialize_file(self.result_file, self.result_schema)
        self.initialize_file(self.run_file, self.run_schema)

    def run(self, pipeline):
        """Run on a Pipeline instance."""

        with io.open(self.result_file, mode='a+t', encoding='utf-8') as result_file:
            source = self.get_source(pipeline.data_source)
            result_id = uuid.uuid4().hex
            period_id = source['period_id']
            score = compat.str(self.get_pipeline_score(pipeline))
            data_source = pipeline.data_source
            schema = '' # pipeline.pipeline[1].schema_source
            summary = '' # TODO: how/what should a summary be?
            report = self.get_pipeline_report_url(pipeline)

            result_set = ','.join([result_id, source['id'], source['publisher_id'],
                                   period_id, score, data_source, schema,
                                   summary, self.run_id, self.timestamp, report])

            result_file.write('{0}\n'.format(result_set))
        
        if pipeline.data:
            self.fetch_data(pipeline.data.stream, source['id'])

    def get_lookup(self):
        
        data_key = self.config['goodtables']['arguments']['batch']['data_key']
        _keys = ['id', 'publisher_id', data_key , 'period_id']
        lookup = []

        with io.open(self.sources_file, mode='r+t', encoding='utf-8') as sources:
            reader = csv.DictReader(sources)
            for row in reader:
                lookup.append({k: v for k, v in row.items() if k in _keys})

        return lookup

    def initialize_file(self, filepath, headers):
        """"Make sure a file exists and has headers before appending to it.

        Args:
            filepath: path to the file to be created
            headers: a tuple to write as header

        """
        header_string = ','.join(headers)
        if not os.path.exists(filepath):
            with io.open(filepath, mode='w+', encoding='utf-8') as file:
                file.write(header_string + '\n')

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

        with io.open(self.run_file, mode='a+t', encoding='utf-8') as rf:
            entry = ','.join([self.run_id, self.timestamp, str(int(sum(self.all_scores) / len(self.lookup)))])
            rf.write('{0}\n'.format(entry))

        return True
    
    def fetch_data(self, data_stream, source_id):
        """Cache the data source in the /fetched directory"""
        
        cached_file_name = os.path.join(self.cache_dir, source_id)
        data_stream.seek(0)
        
        with io.open(cached_file_name, mode='w+', encoding='utf-8') as file:
            for line in data_stream:
                file.write(line)

        
class AssessPerformance(Task):

    """A Task runner to assess and write the performance of publishers for each
    period."""

    def __init__(self, *args, **kwargs):
        super(AssessPerformance, self).__init__(*args, **kwargs)

    def run(self):
        """Write the performance for all publishers."""
        
        def format_row(row_dict, header_list):
            ordered = list(row_dict.get(key) for key in header_list)
            string_values = [str(val) for val in ordered]
            return ','.join(string_values)

        publisher_ids = self.get_publishers()
        
        with io.open(self.performance_file, mode='w+', encoding='utf-8') as pfile:
            fieldnames = ['publisher_id', 'period_id', 'files_count', 'score', 'valid',
                          'files_count_to_date', 'score_to_date', 'valid_to_date']
            pfile.write(','.join(fieldnames) + '\n')
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
                    formated_row = format_row(performance, fieldnames)
                    pfile.write('{0}\n'.format(formated_row))

            all_performances = self.get_periods_data('all', all_periods, all_sources)

            for performance in all_performances:
                pfile.write('{0}\n'.format(format_row(performance, fieldnames)))

    def get_publishers(self):
        """Return list of publishers ids."""

        publisher_ids = []
        with io.open(self.publishers_file, mode='r', encoding='utf-8') as pub_file:
            reader = csv.DictReader(pub_file)
            for row in reader:
                publisher_ids.append(row['id'])
        return publisher_ids

    def get_sources(self, publisher_id):
        """Return list of sources of a publisher with id, period and score. """

        sources = []

        with io.open(self.sources_file, mode='r', encoding='utf-8') as sources_file:
            reader = csv.DictReader(sources_file)
            for row in reader:
                source = {}
                if row['publisher_id'] == publisher_id:
                    source['id'] = row['id']
                    if row['period_id']:
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

        with io.open(self.result_file, mode='r', encoding='utf-8') as results_file:
            reader = csv.DictReader(results_file)
            for row in reader:
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

        separators = re.findall(r"\W", period)
        occurrences = [{char, separators.count(char)} for char in separators]
        unique = [char for no_times, char in occurrences if no_times == 1]

        if len(unique) == 1:
            candidates = period.split(unique[0])
            periods = []
            for candidate in candidates:
                try:
                    date_object = dateutil.parser.parse(candidate, fuzzy=True).date()
                    periods.append(date_object)
                except ValueError:
                    break
            if len(periods) == len(candidates):
                return periods[-1]

        try:
            date_object = dateutil.parser.parse(period, fuzzy=True).date()
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
            performance['period_id'] = period
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
            score = round(total / len(period_sources))
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
                valid = round(len(valids) / len(period_sources) * 100)
        return valid

    def get_unique_periods(self, sources):
        """Return list of unique periods as date objects from sources.

        Args:
            sources: a list of sources

        """

        periods = []

        for source in sources:
            if source['period_id']:
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
