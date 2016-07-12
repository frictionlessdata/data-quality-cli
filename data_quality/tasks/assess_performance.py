# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pytz
import dateutil
import datetime
import jsontableschema
from data_quality import utilities, compat
from .base_task import Task
from .check_datapackage import DataPackageChecker


class PerformanceAssessor(Task):

    """A Task runner to assess and write the performance of publishers for each
       period.
    """

    def __init__(self, *args, **kwargs):
        super(PerformanceAssessor, self).__init__(*args, **kwargs)
        datapackage_check = DataPackageChecker(self.config)
        datapackage_check.run()
        required_resources = [self.result_file, self.source_file,
                              self.publisher_file, self.run_file]
        datapackage_check.check_database_completeness(required_resources)

    def run(self):
        """Write the performance for all publishers."""

        publisher_ids = self.get_publishers()
        performance_resource = utilities.get_datapackage_resource(self.performance_file,
                                                                  self.datapackage)
        performance_schema = jsontableschema.model.SchemaModel(performance_resource.metadata['schema'])

        with compat.UnicodeWriter(self.performance_file) as performance_file:
            performance_file.writerow(performance_schema.headers)
            available_periods = []

            for publisher_id in publisher_ids:
                sources = self.get_sources(publisher_id)
                periods = self.get_unique_periods(sources)
                available_periods += periods
            all_periods = self.get_all_periods(available_periods)

            publishers_performances = []
            all_sources = []

            for publisher_id in publisher_ids:
                sources = self.get_sources(publisher_id)
                performances = self.get_periods_data(publisher_id, all_periods,
                                                     sources)
                publishers_performances += performances
                all_sources += sources
                for performance in performances:
                    try:
                        values = [performance[key] for key in performance_schema.headers]
                        row = list(performance_schema.convert_row(*values))
                        performance_file.writerow(row)
                    except jsontableschema.exceptions.MultipleInvalid as e:
                        for error in e.errors:
                            raise error

            all_performances = self.get_periods_data('all', all_periods, all_sources)

            for performance in all_performances:
                try:
                    values = [performance[key] for key in performance_schema.headers]
                    row = list(performance_schema.convert_row(*values))
                    performance_file.writerow(row)
                except jsontableschema.exceptions.MultipleInvalid as e:
                    for error in e.errors:
                        raise error

    def get_publishers(self):
        """Return list of publishers ids."""

        publisher_ids = []

        with compat.UnicodeDictReader(self.publisher_file) as publishers_file:
            for row in publishers_file:
                publisher_ids.append(row['id'])
        return publisher_ids

    def get_sources(self, publisher_id):
        """Return list of sources of a publisher with id, period and score. """

        sources = []

        with compat.UnicodeDictReader(self.source_file) as sources_file:
            for row in sources_file:
                source = {}
                if row['publisher_id'] == publisher_id:
                    source['id'] = row['id']
                    source['period_id'] = self.get_period(row['period_id'])
                    source['score'] = self.get_source_score(source['id'])
                    sources.append(source)
        return sources

    def get_source_score(self, source_id):
        """Return latest score of a source from results.

        Args:
            source_id: id of the source whose score is wanted
        """

        score = 0
        latest_timestamp = pytz.timezone('UTC').localize(datetime.datetime.min)

        with compat.UnicodeDictReader(self.result_file) as result_file:
            for row in result_file:
                if row['source_id'] == source_id:
                    timestamp = dateutil.parser.parse(row['timestamp'])
                    if timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        score = int(row['score']) * 10
        return score

    def get_period(self, period):
        """Return a valid period as date object

        Args:
            period: a string that might contain a date or range of dates

        """

        if not period:
            return ''
        try:
            date_object = dateutil.parser.parse(period).date()
            return date_object
        except ValueError:
            return ''

    def get_periods_data(self, publisher_id, periods, sources):
        """Return list of performances for a publisher, by period.

        Args:
            publisher_id: publisher in dicussion
            periods: list of all available_periods
            sources: list of publisher's sources

        """

        performances = []
        period_sources_to_date = []

        for period in periods:
            period_sources = self.get_period_sources(period, sources)
            period_sources_to_date += period_sources
            performance = {}
            performance['publisher_id'] = publisher_id
            performance['period_id'] = compat.str(period)
            performance['files_count'] = len(period_sources)
            performance['score'] = self.get_period_score(period_sources)
            performance['valid'] = self.get_period_valid(period_sources)
            performance['score_to_date'] = self.get_period_score(period_sources_to_date)
            performance['valid_to_date'] = self.get_period_valid(period_sources_to_date)
            performance['files_count_to_date'] = len(period_sources_to_date)
            performances.append(performance)
        return performances

    def get_period_sources(self, period, sources):
        """Return list of sources for a period.

        Args:
            period: a date object
            sources: list of sources

        """

        period_sources = []

        for source in sources:
            if period == source['period_id']:
                period_sources.append(source)
        return period_sources

    def get_period_score(self, period_sources):
        """Return average score from list of sources.

        Args:
            period_sources: sources correspoding to a certain period
        """

        score = 0

        if len(period_sources) > 0:
            total = 0
            for source in period_sources:
                total += int(source['score'])
            score = int(round(total / len(period_sources)))
        return score

    def get_period_valid(self, period_sources):
        """Return valid percentage from list of sources.

        Args:
            period_sources: sources correspoding to a certain period
        """

        valid = 0
        if len(period_sources) > 0:
            valids = []
            for source in period_sources:
                if int(source['score']) == 100:
                    valids.append(source)
            if valids:
                valid = int(round(len(valids) / len(period_sources) * 100))
        return valid

    def get_unique_periods(self, sources):
        """Return list of unique periods as date objects from sources.

        Args:
            sources: a list of sources

        """

        periods = []
        for source in sources:
            periods.append(source['period_id'])
        periods = list(set(periods))
        return periods

    def get_all_periods(self, periods):
        """Return all periods from oldest in periods to now.

        Args:
            periods: list of date objects

        """

        oldest_date = sorted(periods)[0]
        current_date = datetime.date.today()
        delta = dateutil.relativedelta.relativedelta(months=1)
        relative_date = oldest_date
        all_periods = []

        while relative_date <= current_date:
            all_periods.append(relative_date)
            relative_date += delta
        return all_periods
