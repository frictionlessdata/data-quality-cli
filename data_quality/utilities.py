# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import json
import shutil
import collections
import datapackage
import pkg_resources

def set_up_cache_dir(cache_dir_path):
    """Reset /cache_dir before a new batch."""

    if os.path.lexists(cache_dir_path):
        for root, dirs, files in os.walk(cache_dir_path):
            for contained_file in files:
                os.unlink(os.path.join(root, contained_file))

            for directory in dirs:
                shutil.rmtree(os.path.join(root, directory))

def resolve_dir(dir_path):
    """ Make sure the dir_path given in the config exists

        Args:
            dir_path: path of directory from config that should be resolved
    """

    try:
        os.makedirs(dir_path)
    except OSError:
        if not os.path.isdir(dir_path):
            raise
    return dir_path

def resolve_dir_name(config_filepath, dir_path):
    """Create an absolute path from the file path and the path given in the config"""

    if not os.path.isabs(dir_path):
        config_path = os.path.abspath(os.path.dirname(config_filepath))
        return os.path.join(config_path, dir_path)
    else:
        return dir_path

def load_json_config(config_filepath):
    """Loads the json config into a dictionary, overwriting the defaults"""

    default_config = pkg_resources.resource_string('data_quality', 'dq.default.json')
    default_config = json.loads(default_config.decode('utf-8'))

    if not config_filepath:
        return default_config
    with io.open(config_filepath, mode='rt', encoding='utf-8') as config_file:
        user_config = json.loads(config_file.read())
        config = deep_update_dict(default_config, user_config)
        config['data_dir'] = resolve_dir_name(config_filepath, config['data_dir'])
        config['cache_dir'] = resolve_dir_name(config_filepath, config['cache_dir'])
    return config

def get_default_datapackage():
    """Return the default datapackage"""

    default_datapkg = pkg_resources.resource_string('data_quality',
                                                    'datapackage.default.json')
    datapkg = datapackage.DataPackage(json.loads(default_datapkg.decode('utf-8')))
    return datapkg

def get_datapackage_resource(resource_path, datapkg):
    """Return the resource correspondent to `resource_path` from datapackage or raise"""

    matching_resources = [res for res in datapkg.resources
                          if res.local_data_path == resource_path]
    if len(matching_resources) > 1:
        raise ValueError(('The resource with path "{0}" appears multiple times '
                          'in your datapackage.').format(resource_path))
    elif not matching_resources:
        raise ValueError(('The resource with path "{0}" can\'t be found in '
                          'your datapackage. Please include it or '
                          'use the "dq init" command.').format(resource_path))
    else:
        return matching_resources[0]

def deep_update_dict(source_dict, new_dict):
    """Update a nested dictionary (modified in place) with another dictionary.

    Args:
        source_dict: dict to be updated
        new_dict: dict to update with

    """

    for key, value in new_dict.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update_dict(source_dict.get(key, {}), value)
            source_dict[key] = returned
        else:
            source_dict[key] = new_dict[key]
    return source_dict
