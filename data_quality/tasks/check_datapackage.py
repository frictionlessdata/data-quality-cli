# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
from jsontableschema.model import SchemaModel
from goodtables import pipeline
from data_quality import utilities
from . import Task


class DataPackageChecker(Task):

    """A task runner to check that the data package is correct"""

    def __init__(self, config):
        super(DataPackageChecker, self).__init__(config)

    def run(self):
        """Check user datapackage against default datapackage"""

        default_datapkg = utilities.get_default_datapackage()
        for default_resource in default_datapkg.resources:
            resource_path = os.path.join(self.config['data_dir'],
                                         self.config[default_resource.metadata['name']])
            resource = utilities.get_datapackage_resource(resource_path,
                                                          self.datapackage)
            self.check_resource_schema(default_resource, resource)

    def check_resource_schema(self, default_resource, resource):
        """Check that user resource schema contains all the mandatory fields"""

        inflexible_resources = ['run_file', 'result_file', 'performance_file']
        if default_resource.metadata['name'] in inflexible_resources:
            if default_resource.metadata['schema'] != resource.metadata['schema']:
                raise ValueError(('The schema for "{0}" is not subject to'
                                  'change').format(resource.local_data_path))

        resource_schema = SchemaModel(resource.metadata['schema'])
        default_schema = SchemaModel(default_resource.metadata['schema'])
        required_headers = set(default_schema.required_headers)
        resource_headers = set(resource_schema.headers)
        if not required_headers.issubset(resource_headers):
            missing_headers = required_headers.difference(resource_headers)
            msg = ('Fields {0} are needed for internal processing but are missing'
                   'from {1}.').format(missing_headers, resource.local_data_path)
            raise ValueError(msg)

    def check_database_content(self):
        """Check that the database content is compliant with the datapackage"""

        self.run()
        for resource in self.datapackage.resources:
            resource_path = resource.local_data_path
            if os.path.exists(resource_path):
                options = {'schema': {'schema': resource.metadata['schema']}}
                pipe = pipeline.Pipeline(resource_path, processors=['schema'],
                                                 options=options)
                result, report = pipe.run()
                if result is False:
                    issues = [res['result_message'] for res in report.generate()['results']]
                    msg = ('The file {0} is not compliant with the schema '
                           'you declared for it in "datapackage.json".'
                           'Errors: {1}').format(resource_path, ','.join(issues))
                    raise ValueError(msg)

    def check_database_completeness(self, required_resources=None):
        """Checks that 'required_resources', or all necessary ones exists in the database

            Args:
                required_resources: list of paths to required resources
        """

        all_resources = [res.local_data_path for res in self.datapackage.resources]
        resources = required_resources or all_resources
        for resource_file in resources:
            if not os.path.exists(resource_file):
                raise ValueError(('The file "{0}" is needed but it doesn\'t exist.'
                                  'Please create it or use "dq generate".').format(resource_file))
