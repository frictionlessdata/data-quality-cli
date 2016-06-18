# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import requests
from os import path
from data_quality import compat
from .base import BaseGenerator

class CkanGenerator(BaseGenerator):
    """This class generates a csv database from a CKAN instance located at the given url"""

    def __init__(self, url=None):
        """Create an instance if the source url is given.

        Args:
            url: the base url for the CKAN instance
        """

        super(CkanGenerator, self).__init__(url)

    def generate_sources(self, sources_filepath, file_types=['csv','excel']):
        """Generates sources_file from the url"""

        fieldnames = ['id', 'publisher_id', 'title', 'data', 'format', 'period_id']
        file_types = [ftype.lower() for ftype in file_types]
        results = self.get_sources()
        sources = []
        for result in results:
            sources += self.extract_sources(result, file_types)

        with compat.UnicodeDictWriter(sources_filepath, fieldnames,
                                      quoting=csv.QUOTE_MINIMAL) as sfile:
            sfile.writeheader()
            for source in sources:
                sfile.writerow(source)

    def get_sources(self):
        """Get all sources from CKAN API as a list"""

        extension = 'api/3/action/package_search'
        full_url = compat.urljoin(self.base_url, extension)
        response = requests.get(full_url)
        response.raise_for_status()
        data = response.json()
        count = data['result']['count']
        all_data = []
        for start in range(0, count, 500):
            payload = {'rows': 500, 'start': start}
            response = requests.get(full_url, params=payload)
            data = response.json()
            all_data += data['result']['results']
        return all_data

    def extract_sources(self, datum, file_types):
        """Extract all sources for one result"""

        resources = []
        for resource in datum.get('resources', {}):
            new_resource = {}
            new_resource['data'] = resource['url']
            ext = path.splitext(new_resource['data'])[1][1:].lower()
            new_resource['format'] = 'excel' if ext in ['xls', 'xlsx'] else ext
            file_types = ['excel' if ext in ['xls', 'xlsx'] else ext for ext in file_types]
            file_types.append('')
            if new_resource['format'] in file_types:
                publisher = datum.get('organization', {})
                new_resource['publisher_id'] = publisher.get('name')
                new_resource['id'] = resource['id']
                new_resource['period_id'] = resource['created']
                title = datum.get('title', '')
                name = resource.get('name', '')
                new_resource['title'] = ' / '.join(val for val in [title, name] if val)
                resources.append(new_resource)
        return resources

    def generate_publishers(self, publishers_filepath):
        """Generates publishers_file from the url"""

        results = self.get_publishers()
        fieldnames = ['id', 'title', 'type', 'contact', 'email']

        with compat.UnicodeDictWriter(publishers_filepath, fieldnames,
                                      quoting=csv.QUOTE_MINIMAL) as pfile:
            pfile.writeheader()
            for result in results:
                result = self.extract_publisher(result)
                pfile.writerow(result)

    def get_publishers(self):
        """Retrieves the publishers from CKAN API as a list"""

        extension = "api/3/action/organization_list"
        payload = {'all_fields':True,
                   'include_groups': True,
                   'include_extras':True
                  }
        full_url = compat.urljoin(self.base_url, extension)
        response = requests.get(full_url, params=payload)
        publishers = response.json()['result']
        return publishers

    def extract_publisher(self, result):
        """Converts `result` into dict with standard compliant field names"""

        publisher = {}
        publisher['id'] = result.get('name', '')
        publisher['title'] = result.get('display_name', '')
        for extra in result.get('extras', []):
            key = extra.get('key')
            if key == 'contact-email':
                publisher['email'] = extra.get('value')
            if key == 'contact-name':
                publisher['contact'] = extra.get('value')
            if key == 'category':
                publisher['type'] = extra.get('value')
        return publisher

