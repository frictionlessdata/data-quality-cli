# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import json
import shutil
import pkg_resources
import collections
import datapackage
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
        config_path = os.path.abspath(os.path.dirname(config_filepath))
        return os.path.join(config_path, dir_path)

def get_resource_by_name(name, datapackage):
    """Retrieve resource from datapackage by name"""

    resources = [resource for resource in datapackage.resources
                 if resource.metadata['name'] == name]

    if len(resources) == 1:
        return resources[0]
    elif len(resources) == 0:
        return None
    else:
        raise ValueError('There is more than one resource with this name')

def load_json_config(config_filepath):
    """Loads the json config into a dictionary, overwriting the defaults"""

    default_config = pkg_resources.resource_string('data_quality', 'dq.default.json')
    default_config = json.loads(default_config.decode('utf-8'))

    if not config_filepath:
        return default_config
    with io.open(config_filepath, mode='rt', encoding='utf-8') as file:
        user_config = json.loads(file.read())
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

def load_json_datapackage(config):
    """Generate a datapackage or return the existing one along with it's path"""

    datapkg_filepath = config.get('datapackage_file', '')

    if not datapkg_filepath or not os.path.isabs(datapkg_filepath):
        data_dir_path = os.path.normpath(config['data_dir'])
        datapkg_dir_path = os.path.dirname(data_dir_path)
        datapkg_filepath = os.path.join(datapkg_dir_path, 'datapackage.json')

    datapkg_filepath = os.path.abspath(datapkg_filepath)

    if not os.path.exists(datapkg_filepath):
        with io.open(datapkg_filepath, mode='w+', encoding='utf-8') as new_datapkg:
            default_datapkg = get_default_datapackage()
            for resource in default_datapkg.resources:
                resource.metadata['path'] = os.path.join(config['data_dir'],
                                                         resource.metadata['path'])
            json_datapkg = json.dumps(default_datapkg.to_dict(), indent=4)
            new_datapkg.write(compat.str(json_datapkg))
            return (default_datapkg, datapkg_filepath)

    return  (datapackage.DataPackage(datapkg_filepath), datapkg_filepath)

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
