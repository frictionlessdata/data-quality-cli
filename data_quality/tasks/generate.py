# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import importlib
from data_quality import generators, utilities
from .base_task import Task
from .check_datapackage import DataPackageChecker


class GeneratorManager(Task):

    """A Task runner that manages dataset generators (ex: CkanGenerator)."""

    def __init__(self, config):
        super(GeneratorManager, self).__init__(config)
        datapackage_check = DataPackageChecker(self.config)
        datapackage_check.run()

    def run(self, generator_name, endpoint, generator_path, file_types, simulate=False):
        """Delegate the generation processes to the chosen generator

        Args:
            generator_name: Name of the generator (ex: ckan)
            endpoint: Url where the generator should get the data from
            generator_path: Path to the custom generator class, if used
            file_types: List of file types that should be included in sources
        """

        if  generators._built_in_generators.get(generator_name, None):
            default_datapkg = utilities.get_default_datapackage()
            default_resources = [res for res in default_datapkg.resources
                                 if res.metadata['name'] in ['source_file', 'publisher_file']]
            user_resources = [utilities.get_datapackage_resource(path, self.datapackage)
                              for path in [self.publisher_file, self.source_file]]
            for default_resource, resource in zip(default_resources, user_resources):
                if default_resource.metadata['schema'] != resource.metadata['schema']:
                    msg = ('Looks like you have a custom schema for "{0}". Generator '
                           '"{1}" only works with the default schema. Please use '
                           'a custom generator or match your schema to the default one.'
                          ).format(default_resource.metadata['name'], generator_name)
                    raise ValueError(msg)

            generator_class = generators._built_in_generators[generator_name]
        else:
            try:
                _module, _class = generator_path.rsplit('.', 1)
                generator_class = getattr(importlib.import_module(_module), _class)
            except ValueError:
                raise ValueError(('The path you provided for the generator class is '
                                  'not valid. Should be of type `mymodule.MyGenerator`'))
        generator = generator_class(endpoint, self.datapackage)

        if simulate:
            return generator

        generator.generate_publishers(self.publisher_file)
        generator.generate_sources(self.source_file, file_types=file_types)

