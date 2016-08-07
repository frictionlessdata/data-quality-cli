# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
import datetime
from dateparser.date import DateDataParser
from jsontableschema.model import SchemaModel
from data_quality import utilities, compat, exceptions
from .base_task import Task
from .check_datapackage import DataPackageChecker

class RelevancePeriodExtractor(Task):

    """A Task runner that extracts the period a sources's content reffers to
        (is relevant for).
    """

    def __init__(self, config):
        super(RelevancePeriodExtractor, self).__init__(config)
        timeliness_params = self.config['timeliness']
        self.extract_period = timeliness_params.get('extract_period', False)
        self.timeliness_strategy = timeliness_params.get('timeliness_strategy', [])
        self.date_order = timeliness_params.get('date_order', 'DMY')
        self.max_empty_relevance_period = timeliness_params.get('max_empty_relevance_period', 10)
        if not self.timeliness_strategy:
            raise ValueError('You need to provide values for "timeliness_strategy."')
        datapackage_check = DataPackageChecker(self.config)
        datapackage_check.check_database_completeness([self.source_file])
        settings = {'RETURN_AS_TIMEZONE_AWARE': False,
                    'PREFER_DAY_OF_MONTH': 'last',
                    'PREFER_DATES_FROM': 'past',
                    'SKIP_TOKENS': ['to'],
                    'DATE_ORDER': self.date_order}
        self.date_parser = DateDataParser(allow_redetect_language=True,
                                          settings=settings)

    def run(self):
        """Try to indentify the relevance period of sources"""

        sources = self.extract_period_from_sources()
        empty_period_sources = [source for source in sources
                                if source['period_id'] is None]
        empty_period_percent = (len(empty_period_sources) * 100) / len(sources)
        empty_period_percent = round(empty_period_percent)
        if empty_period_percent > int(self.max_empty_relevance_period):
            msg = ('The relevance period couldn\'t be identified for'
                   ' {0}% of sources therefore timeliness cannot be'
                   ' assessed. Please provide more fields for "timeliness_'
                   'strategy", set "assess_timeliness" to false or increase'
                   ' "max_empty_relevance_period".').format(empty_period_percent)
            raise exceptions.UnableToAssessTimeliness(msg)

        for source in sources:
            if source['period_id'] is None:
                creation_date = utilities.date_from_string(source['created_at'])
                dates = [creation_date, creation_date]
            else:
                period_start, period_end = source['period_id']
                dates = [period_start.date(), period_end.date()]
            dates = [date.strftime('%d-%m-%Y') if isinstance(date, datetime.date)
                     else '' for date in dates]
            source['period_id'] = '/'.join(dates)
        self.update_sources_period(sources)

    def extract_period_from_sources(self):
        """Try to extract relevance period for each source or return None"""

        sources = []
        with compat.UnicodeDictReader(self.source_file) as source_file:
            timeliness_set = set(self.timeliness_strategy)
            found_fields = timeliness_set.intersection(set(source_file.header))
            if not found_fields:
                raise ValueError(('At least one of the "timeliness_strategy" '
                                  'fields must be present in your "source_file".'))
            if not found_fields.issuperset(timeliness_set):
                missing_fields = timeliness_set.difference(found_fields)
                print(('Fields "{0}" from "timeliness_strategy" were not found '
                       'in your `source_file`').format(missing_fields))

            for source in source_file:
                timeliness_fields = {field: val for field, val in source.items()
                                     if field in self.timeliness_strategy}
                extracted_period = self.identify_period(timeliness_fields)
                source['period_id'] = extracted_period
                sources.append(source)
        return sources

    def identify_period(self, source={}):
        """Try to indentify the period of a source based on timeliess strategy

        Args:
            source: a dict corresponding to a source_file row
        """

        field_dates = {}
        for field in self.timeliness_strategy:
            value = source.get(field, '')
            if not value:
                continue
            field_dates[field] = self.extract_dates(value)

        for field in self.timeliness_strategy:
            dates = field_dates.get(field, [])
            if not dates:
                continue
            period = resolve_period(dates)
            if period:
                break
            else:
                # It means we have more than 2 dates
                other_fields = list(self.timeliness_strategy)
                other_fields.remove(field)
                other_values = [field_dates.get(other_field, [])
                                for other_field in other_fields]
                for values in other_values:
                    date_objects = set(date['date_obj'] for date in dates)
                    common_values = [date for date in values
                                     if date['date_obj'] in date_objects]
                    period = resolve_period(common_values)
            if period:
                break
        else:
            period = None
        return period

    def extract_dates(self, line=""):
        """Try to extract dates from a line

        Args:
            line: a string that could contain a date or time range
        """

        dates = []
        potential_dates = re.findall(r'[0-9]+[\W_][0-9]+[\W_][0-9]+', line)
        line_words = re.sub(r'[\W_]+', ' ', line).split()
        years = filter_years(line_words)
        for word in years:
            if re.search(r'[a-zA-Z]', word):
                potential_dates.append(word)
                break
            for index, entry in enumerate(line_words):
                if entry == word:
                    date = self.scan_for_date(line_words, index)
                    if date:
                        potential_dates.append(date)
                        # Try to find a range
                        if date['period'] != 'year' and date['date_obj']:
                            range_start = self.scan_for_range(line_words, index, date)
                            if not range_start:
                                continue
                            if range_start['date_obj'] < date['date_obj']:
                                potential_dates.append(range_start)

        for potential_date in potential_dates:
            try:
                dates.append(self.date_parser.get_date_data(potential_date))
            except TypeError:
                if isinstance(potential_date, dict):
                    dates.append(potential_date)
            except ValueError:
                potential_date = None
        dates = [date for date in dates if date['date_obj'] is not None]
        dates = list({date['date_obj']:date for date in dates}.values())
        return dates

    def scan_for_date(self, line_words, year_index):
        """Scan around the year for a date as complete as possible

        Args:
            line_words: a list of words (strings)
            year_index: index of a string from line_word that contains a year
        """

        date_parts = line_words[year_index-2:year_index+1] or \
                     line_words[:year_index+1]
        potential_date = self.create_date_from_parts(date_parts)
        if not potential_date or potential_date['period'] == 'year':
            new_parts = list(reversed(line_words[year_index:year_index+3]))
            new_potential_date = self.create_date_from_parts(new_parts)
            if new_potential_date:
                potential_date = new_potential_date
        return potential_date

    def scan_for_range(self, line_words, year_index, range_end):
        """Scan to the left of the year whose corresponding date has
            been extracted to see if there is a range.

          Args:
            line_words: a list of words (strings)
            year_index: index of a string from line_word that contains a year
            range_end: date that has already been extracted from the year at
                        year_index, potentially end of range
        """

        if range_end['period'] == 'month':
            scan_start = year_index-2
            scan_end = year_index-4
        else:
            scan_start = year_index-3
            scan_end = year_index-5
        range_start_parts = line_words[scan_end:scan_start+1] or \
                            line_words[:scan_start+1]
        range_start_parts = [part for part in range_start_parts
                             if self.create_date_from_parts([part]) is not None]
        years = filter_years(range_start_parts)
        if years:
            range_start_parts = []
        if range_start_parts:
            if len(range_start_parts) == 1 and range_end['period'] == 'day':
                range_start_parts.append(compat.str(range_end['date_obj'].month))
            range_start_parts.append(compat.str(range_end['date_obj'].year))
        range_start = self.create_date_from_parts(range_start_parts)
        if range_start and range_start['period'] != range_end['period']:
            range_start = None
        return range_start

    def create_date_from_parts(self, date_parts=None):
        """Try to create a date object with date_parser or return None."""

        if not date_parts:
            return None
        for index, part in enumerate(date_parts):
            if len(date_parts) == 2:
                if False not in [el.isdigit() for el in date_parts]:
                    date_parts.insert(index, '31')
            potential_date = ' '.join(date_parts[index:])
            try:
                date = self.date_parser.get_date_data(potential_date)
            except ValueError:
                date = None
            if date['date_obj'] is not None:
                break
        else:
            date = None
        return date

    def update_sources_period(self, new_sources):
        """Overwrite source_file with the identified period_id"""

        source_resource = utilities.get_datapackage_resource(self.source_file,
                                                             self.datapackage)
        source_idx = self.datapackage.resources.index(source_resource)
        source_schema_dict = self.datapackage.resources[source_idx].descriptor['schema']
        updates = {'fields':[{'name': 'period_id', 'type': 'string',
                   'title': 'The period source data is relevant for.'}]}
        utilities.deep_update_dict(source_schema_dict, updates)
        source_schema = SchemaModel(source_schema_dict)

        with compat.UnicodeWriter(self.source_file) as source_file:
            source_file.writerow(source_schema.headers)
            for row in utilities.dicts_to_schema_rows(new_sources,
                                                      source_schema):
                source_file.writerow(row)

def resolve_period(dates=None):
    """Given a list of dates, try to create a period tuple or return None"""

    if not dates:
        period = None
    elif len(dates) == 1:
        period = period_from_date(dates[0])
    elif len(dates) == 2:
        date_objects = sorted([date['date_obj'] for date in dates])
        if dates[0]['period'] == 'year':
            date_objects[0] = date_objects[0].replace(month=1, day=1)
        if dates[1]['period'] == 'year':
            date_objects[1] = date_objects[1].replace(month=12, day=31)
        if dates[0]['period'] == 'month':
            date_objects[0] = date_objects[0].replace(day=1)
        period = (date_objects[0], date_objects[1])
    else:
        period = None
    return period

def period_from_date(date={}):
    """Create a period from a `dateparser` date dict"""

    if date.get('date_obj', None) is None:
        return None
    if date['period'] == 'day':
        range_start = date['date_obj']
        range_end = date['date_obj'].replace(hour=23, minute=59)
    elif date['period'] == 'month':
        range_start = date['date_obj'].replace(day=1)
        range_end = date['date_obj']
    else:
        range_start = datetime.datetime(date['date_obj'].year, 1, 1)
        range_end = datetime.datetime(date['date_obj'].year, 12, 31)
    return (range_start, range_end)

def filter_years(words_list):
    """Filter strings that could contain a year from a list of words"""

    condition = lambda x: re.search(r'(?:19|20)[0-9]{2}', x)
    filtered_list = [word for word in filter(condition, words_list)]
    return filtered_list
