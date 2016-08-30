# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import json
import datapackage
from data_quality import utilities, compat
from .check_datapackage import DataPackageChecker


class DataPackageInitializer(object):

    """A task runner that makes a data-quality style data package from a 
       given workspace folder
    """

    def __init__(self, workspace_path):
        self.workspace_path = workspace_path

    def run(self):
        """Initialize all necessary files and folders"""

        config = self.initialize_config()
        utilities.resolve_dir(config['data_dir'])
        utilities.resolve_dir(config['cache_dir'])
        self.initialize_datapackage(config)

    def initialize_config(self):
        """Create a config for this instance or use the existing one"""

        init_config_path = os.path.join(self.workspace_path, 'dq_config.json')

        if os.path.exists(init_config_path):
            config = utilities.load_json_config(init_config_path)
        else:
            config = utilities.load_json_config(None)

            with io.open(init_config_path, mode='w+', encoding='utf-8') as new_config:
                new_json_config = json.dumps(config, indent=4, sort_keys=True)
                new_config.write(compat.str(new_json_config))
                print(('A new config file has been created at {0}. '
                       'Please review and update it.'.format(init_config_path)))
        return config

    def initialize_datapackage(self, config):
        """Create a datapackage or return the existing one along with it's path"""

        datapkg_file_path = config.get('datapackage_file', '')
        if not datapkg_file_path or not os.path.isabs(datapkg_file_path):
            datapkg_file_path = os.path.join(self.workspace_path, 'datapackage.json')

        datapkg_file_path = os.path.abspath(datapkg_file_path)
        if not os.path.exists(datapkg_file_path):
            with io.open(datapkg_file_path, mode='w+', encoding='utf-8') as new_datapkg:
                default_datapkg = utilities.get_default_datapackage()
                for resource in default_datapkg.resources:
                    resource_path = config.get(resource.descriptor['name'],
                                               resource.descriptor['path'])
                    resource.descriptor['path'] = os.path.join(config['data_dir'],
                                                               resource_path)
                json_datapkg = json.dumps(default_datapkg.to_dict(), indent=4)
                new_datapkg.write(compat.str(json_datapkg))
                print(('A new "datapackage.json" file has been created at {0}. '
                      'Please review and update it.'.format(datapkg_file_path)))
                return default_datapkg
        else:
            datapackage_check = DataPackageChecker(config)
            datapackage_check.run()
            return  datapackage.DataPackage(datapkg_file_path)

