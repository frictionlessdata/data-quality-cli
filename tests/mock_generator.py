# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from data_quality import  generators

class MockGenerator(generators.BaseGenerator):
    """This class deletes the current database and regenerates it"""

    def __init__(self, url=None, datapackage=None):
        """Create an instance
        
        Args:
            url: something to please the Base Generator
        """

        super(MockGenerator, self).__init__(url)
