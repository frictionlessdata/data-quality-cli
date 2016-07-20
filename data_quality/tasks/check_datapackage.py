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

    def __init__(self, config, inflexible_resources=[]):
        super(DataPackageChecker, self).__init__(config)
        self.inflexible_resources = ['run_file', 'result_file', 'performance_file']
        self.inflexible_resources.extend(inflexible_resources)
        self.inflexible_resources = set(inflexible_resources)

    def run(self):
        """Check user datapackage against default datapackage"""

        default_datapkg = utilities.get_default_datapackage()
        for default_resource in default_datapkg.resources:
            resource_path = os.path.join(self.config['data_dir'],
                                         self.config[default_resource.descriptor['name']])
            resource = utilities.get_datapackage_resource(resource_path,
                                                          self.datapackage)
            self.check_resource_schema(default_resource, resource)

    def check_resource_schema(self, default_resource, resource):
        """Check that user resource schema contains all the mandatory fields"""

        def get_uncustomizable_fields(schema):
            uncustomizable = ['constraints', 'format', 'name', 'type']
            field_filter = lambda field: {key: val for key, val in field.items()
                                          if key in uncustomizable}
            fields = [field_filter(field) for field in schema.fields]
            fields = sorted(fields, key=lambda k: k['name'])

        resource_schema = SchemaModel(resource.descriptor['schema'])
        default_schema_dict = default_resource.descriptor['schema']
        if default_resource.descriptor['name'] == 'source_file':
            for field in default_schema_dict['fields']:
                if field['name'] == 'data':
                    field['name'] = self.data_key
        default_schema = SchemaModel(default_schema_dict)

        if default_resource.descriptor['name'] in self.inflexible_resources:
            if get_uncustomizable_fields(default_schema) != \
               get_uncustomizable_fields(resource_schema):
                msg = ('The fields for "{0}" are not subject to'
                       'change').format(resource.local_data_path)
                raise ValueError(msg, resource.local_data_path)
        else:
            required_headers = set(default_schema.required_headers)
            resource_headers = set(resource_schema.headers)
            if not required_headers.issubset(resource_headers):
                missing_headers = required_headers.difference(resource_headers)
                msg = ('Fields [{0}] are needed for internal processing'
                       'but are missing from {1}.'
                       ).format(','.join(missing_headers), resource.local_data_path)
                raise ValueError(msg, resource.local_data_path)

    def check_database_content(self):
        """Check that the database content is compliant with the datapackage"""

        self.run()
        for resource in self.datapackage.resources:
            resource_path = resource.local_data_path
            if os.path.exists(resource_path):
                options = {'schema': {'schema': resource.descriptor['schema']}}
                pipe = pipeline.Pipeline(resource_path, processors=['schema'],
                                                 options=options)
                result, report = pipe.run()
                if result is False:
                    issues = [res['result_message'] for res in report.generate()['results']]
                    msg = ('The file {0} is not compliant with the schema '
                           'you declared for it in "datapackage.json".'
                           'Errors: {1}'
                          ).format(resource_path, ';'.join(issues))
                    raise ValueError(msg)

    def check_database_completeness(self, required_resources=None):
        """Checks that 'required_resources', or all necessary ones exist in the database

            Args:
                required_resources: list of paths to required resources
        """

        all_resources = [res.local_data_path for res in self.datapackage.resources]
        resources = required_resources or all_resources
        for resource_file in resources:
            if not os.path.exists(resource_file):
                msg = ('The file "{0}" is needed but it doesn\'t exist.'
                       'Please create it or use "dq generate".'
                      ).format(resource_file)
                raise ValueError(msg)
