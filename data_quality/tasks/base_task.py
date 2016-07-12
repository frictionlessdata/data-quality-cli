# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import datapackage


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
        self.publisher_file = os.path.join(self.data_dir,
                                           self.config['publisher_file'])
        self.cache_dir = self.config['cache_dir']

        datapkg_file_path = self.config.get('datapackage_file', 'datapackage.json')
        if not os.path.isabs(datapkg_file_path):
            datapkg_file_path = os.path.join(os.path.dirname(self.data_dir),
                                             datapkg_file_path)
        try:
            self.datapackage = datapackage.DataPackage(datapkg_file_path)
        except datapackage.exceptions.DataPackageException as e:
            raise ValueError(('A datapackage couldn\'t be created because of the '
                              'following error: "{0}". Consider using "dq init"').format(e))
        self.all_scores = []