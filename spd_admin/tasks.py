# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import json
import csv
import uuid
import datetime
import subprocess
import contextlib
import pytz
from . import compat


@contextlib.contextmanager
def cd(path):
    """Move into a dir while the context is active."""
    workpath = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(workpath)


class Task(object):

    """Base class for Spend Publishing Dashboard tasks."""

    def __init__(self, config, **kwargs):
        self.config = config
        self.remotes = self.config['remotes']
        self.branch = self.config['branch']
        self.result_file = os.path.join(self.config['data_dir'], self.config['result_file'])
        self.run_file = os.path.join(self.config['data_dir'], self.config['run_file'])
        self.all_scores = []


class Aggregator(Task):

    """A Task runner to create results for data sources as they go through a processing pipeline."""

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

    def run(self, pipeline):
        """Run on a Pipeline instance."""

        with io.open(self.result_file, mode='a+t', encoding='utf-8') as f:

            source = self.get_source(pipeline.data.data_source)
            result_id = uuid.uuid4().hex
            period_id = source['period_id']
            score = compat.str(self.get_pipeline_score(pipeline))
            data = pipeline.data.data_source
            schema = '' # pipeline.pipeline[1].schema_source
            summary = '' # TODO: how/what should a summary be?
            report = self.get_pipeline_report_url(pipeline)

            result_set = ','.join([result_id, source['id'], source['publisher_id'],
                                   source['period_id'], score, data, schema,
                                   summary, self.run_id, self.timestamp, report])

            f.write('{0}\n'.format(result_set))

    def get_lookup(self):

        _keys = ['id', 'publisher_id', 'url', 'period_id']
        lookup = []
        source_filepath = os.path.join(self.config['data_dir'], self.config['source_file'])
        with io.open(source_filepath, mode='r+t', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                lookup.append({k: v for k, v in row.items() if k in _keys})

        return lookup

    def get_source(self, data):

        matches = [match for match in self.lookup if match['data'] == data]

        # TODO: if not matches
        # TODO: if multiple matches

        return matches[0]

    def get_pipeline_score(self, pipeline):
        """Return a score for this pipeline run."""

        score = self.max_score
        results = pipeline.generated_report['results']

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

        return self.config['goodtables_web']

    def write_run(self):
        """Write this run in the run file."""

        with io.open(self.run_file, mode='a+t', encoding='utf-8') as rf:
            entry = ','.join([self.run_id, self.timestamp, str(int(sum(self.all_scores) / len(self.lookup)))])
            rf.write('{0}\n'.format(entry))

        return True


class Deploy(Task):

    """A Task runner to deploy a Spend Publishing Dashboard data repository."""

    commit_msg = 'New result and run data.'
    tag_msg = 'New result and run data.'

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
