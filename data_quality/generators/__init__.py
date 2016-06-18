# # -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .ckan import CkanGenerator
from .base import BaseGenerator

__all__ = ['CkanGenerator', 'BaseGenerator']

_built_in_generators = {'ckan': CkanGenerator}
