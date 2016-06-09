# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import shutil
import collections
from data_quality import compat

def set_up_cache_dir(cache_dir_path):
    """Reset /cache_dir before a new batch."""

    if os.path.lexists(cache_dir_path):
        for root, dirs, files in os.walk(cache_dir_path):
            for file in files:
                os.unlink(os.path.join(root, file))

            for directory in dirs:
                shutil.rmtree(os.path.join(root, directory))
    else:
        raise OSError("The folder chosen as \'cache_dir\' does not exist.")

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

def format_row(row_dict, header_list):
    """Format the values of a dict accoording to the headers list"""

    ordered = []
    for key in header_list:
        value = row_dict.get(key)
        if value is not str or bytes:
            ordered.append(str(value))
        else:
            ordered.append(value)

    return ordered
