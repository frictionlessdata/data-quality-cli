# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
from . import main
from . import tasks


def get_version():
    version_path = os.path.join(os.path.dirname(__file__), 'VERSION')
    return io.open(version_path, encoding='utf-8').readline().strip()


__version__ = get_version()

__all__ = ['main', 'tasks']
