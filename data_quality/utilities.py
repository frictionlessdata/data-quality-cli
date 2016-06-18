# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import json
import shutil
import collections
from . import compat

def set_up_cache_dir(cache_dir_path):
    """Reset /cache_dir before a new batch."""

    if os.path.lexists(cache_dir_path):
        for root, dirs, files in os.walk(cache_dir_path):
            for file in files:
                os.unlink(os.path.join(root, file))

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
       return os.path.join(os.path.dirname(config_filepath), dir_path)

def load_json_config(config_filepath):
    """Loads the json config into a dictionary, overwriting the defaults"""

    default_config = {
        'data_dir': 'data',
        'cache_dir': 'fetched',
        'result_file': 'results.csv',
        'run_file': 'runs.csv',
        'source_file': 'sources.csv',
        'publisher_file': 'publishers.csv',
        'performance_file': 'performance.csv',
        'remotes': ['origin'],
        'branch': 'master',
        'goodtables': {
            'goodtables_web': 'http://goodtables.okfnlabs.org',
            'arguments': {
                'pipeline': {},
                'batch': {
                    'data_key': 'data'
                }
            }
        }
    }

    if not config_filepath:
        return default_config
    with io.open(config_filepath, mode='rt', encoding='utf-8') as file:
        user_config = json.loads(file.read())
        config = deep_update_dict(default_config, user_config)
        config['data_dir'] = resolve_dir_name(config_filepath, config['data_dir'])
        config['cache_dir'] = resolve_dir_name(config_filepath, config['cache_dir'])
    return config

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
