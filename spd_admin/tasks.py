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
import pytz


class Task(object):

    """Base class for Spend Publishing Dashboard tasks."""

    def __init__(self, config, **kwargs):
        self.config = config
        self.remotes = self.config['remotes']
        self.branch = self.config['branch']
        self.result_file = os.path.join(self.config['data_dir'], self.config['result_file'])
        self.run_file = os.path.join(self.config['data_dir'], self.config['run_file'])


class Aggregator(Task):

    """A Task runner to create results for data sources as they go through a processing pipeline."""

    def __init__(self, *args, **kwargs):
        super(Aggregator, self).__init__(*args, **kwargs)
        self.max_score = 10
        self.lookup = self.get_lookup()

        self.run_schema = ['id', 'timestamp', 'total_score']
        self.result_schema = ['id', 'source_id', 'publisher_id', 'period_id',
                              'run_id', 'timestamp', 'score', 'summary',
                              'data_url', 'schema_url','report_url']

        self.run_id = uuid.uuid4().hex
        self.timestamp = datetime.datetime.now(pytz.utc)

        self.log_run()

    def run(self, pipeline):
        """Run on a Pipeline instance."""

        with io.open(self.result_file, mode='a+t', encoding='utf-8') as f:
            source = self.get_source(pipeline.data.data_source)
            source_id = source['id']
            publisher_id = source['publisher_id']

            period_id = source['period_id']
            score = self.get_pipeline_score(pipeline)
            timestamp = self.timestamp
            result_set = ','.join([source['id'], source['publisher_id'],
                                   source['period_id'], self.run_id, self.timestamp,
                                   self.get_pipeline_score(pipeline), 'summary',
                                   pipeline.data_source, pipeline.schema_source,
                                   self.get_pipeline_report_url(pipeline)])
            f.write('{0}\n'.format(result_set))

    def get_lookup(self):

        _keys = ['id', 'publisher_id', 'source_url', 'period_id']
        lookup = []
        source_filepath = os.path.join(self.config['data_dir'], self.config['source_file'])
        reader = csv.DictReader(source_filepath)

        for row in reader:
            lookup.append({k: v for k, v in row.items() if k in _keys})

        return lookup

    def get_source(self, url):
        matches = [match for match in self.lookup if match['url'] == url]

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

        return score

    def get_pipeline_report_url(self, pipeline):
        """Return a URL to a report on this data."""

        return self.config['goodtables_web']

    def log_run(self):
        """Log this run in the run file."""

        with io.open(self.run_file, mode='a+t', encoding='utf-8') as rf:
            entry = ','.join([self.run_id, self.timestamp, self.total_score])
            rf.write('{0}\n'.format(entry))

        return True


class Deploy(Task):

    """A Task runner to deploy a Spend Publishing Dashboard data repository."""

    def run(self):
        """Commit and deploy changes."""

        self._pull()
        self._add()
        self._commit()
        self._push()

    def _pull(self):
        """Pull in any changes from remotes."""

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

        # add the changed files
        command = ['git', 'add', self.result_file]
        subprocess.call(command)

        command = ['git', 'add', self.run_file]
        subprocess.call(command)

    def _commit(self):
        command = ['git', 'commit', '-a', '-m', '"{0}"'.format(self.commit_msg)]
        subprocess.call(command)

    def _tag(self):
        command = ['git', 'tag', '-a', self.tag_version, '-m', '"{0}"'.format(self.tag_msg)]
        subprocess.call(command)

    def _push(self):
        command = ['git', 'push', '--follow-tags']
        subprocess.call(command)
