# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import subprocess
import contextlib
from time import strftime, gmtime
import json
from data_quality import compat
from .base_task import Task
from .check_datapackage import DataPackageChecker

@contextlib.contextmanager
def cd(path):
    """Move into a dir while the context is active."""
    workpath = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(workpath)


class Deployer(Task):

    """A Task runner to deploy a Data Quality repository to a remote."""

    commit_msg = 'New result and run data.'
    tag_msg = 'New result and run data.'
    tag_version = ''

    def run(self, simulate=False, *args):
        """Commit and deploy changes."""

        datapackage_check = DataPackageChecker(self.config)
        datapackage_check.run()
        self._pull()
        self.update_last_modified()
        datapackage_check.check_database_completeness()
        datapackage_check.check_database_content()
        self._add()
        self._commit()
        if simulate:
            return True
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
        """Update the 'last_modified' field in datapackage.json"""

        datapackage_path = os.path.join(self.datapackage.base_path,
                                        'datapackage.json')

        with io.open(datapackage_path, mode='w+', encoding='utf-8') as datapkg_file:
            current_time = strftime("%Y-%m-%d %H:%M:%S %Z", gmtime())
            self.datapackage.descriptor['last_modified'] = current_time
            updated_datapkg = json.dumps(self.datapackage.to_dict(), indent=4,
                                         sort_keys=True)
            datapkg_file.write(compat.str(updated_datapkg))
