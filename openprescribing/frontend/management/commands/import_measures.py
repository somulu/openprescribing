"""Calculate and store measures based on definitions in
`measure_definitions/` folder.

We use BigQuery to compute measures. This is considerably cheaper than
the alternative, which is looping over thousands of practices
individually with a custom SQL query. However, the tradeoff is that
most of the logic now lives in SQL which is harder to read and test
clearly.
"""

from contextlib import contextmanager
import csv
import datetime
import glob
import json
import logging
import os
import psycopg2
import re
import sys
import tempfile

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db import transaction

from ebmdatalab import bigquery

from common import utils
from frontend.models import MeasureGlobal, MeasureValue, Measure, ImportLog

logger = logging.getLogger(__name__)

CENTILES = [10, 20, 30, 40, 50, 60, 70, 80, 90]


class Command(BaseCommand):
    '''Supply either --end_date to load data for all months
    up to that date, or --month to load data for just one
    month.

    You can also supply --start_date, or supply a file path that
    includes a timestamp with --month_from_prescribing_filename

    Specify a measure with a single string argument to `--measure`,
    and more than one with a comma-delimited list.

    '''

    def handle(self, *args, **options):
        options = self.parse_options(options)
        start = datetime.datetime.now()
        start_date = options['start_date']
        end_date = options['end_date']
        verbose = options['verbosity'] > 1
        with conditional_constraint_and_index_reconstructor(options):
            for measure_id in options['measure_ids']:
                measure_start = datetime.datetime.now()
                global_calculation = GlobalCalculation(
                    measure_id, verbose=verbose,
                    under_test=options['test_mode'])
                practice_calculation = PracticeCalculation(
                    measure_id, start_date=start_date, end_date=end_date,
                    verbose=verbose, under_test=options['test_mode']
                )
                ccg_calculation = CCGCalculation(
                    measure_id, start_date=start_date, end_date=end_date,
                    verbose=verbose, under_test=options['test_mode']
                )
                measure = global_calculation.create_or_update_measure()
                if options['definitions_only']:
                    continue

                # Delete any existing measures data relating to the
                # current month(s)
                MeasureValue.objects.filter(month__gte=start_date)\
                                    .filter(month__lte=end_date)\
                                    .filter(measure=measure).delete()
                MeasureGlobal.objects.filter(month__gte=start_date)\
                                     .filter(month__lte=end_date)\
                                     .filter(measure=measure).delete()

                # Compute the measures
                practice_rows_created = practice_calculation.calculate()
                ccg_rows_created = ccg_calculation.calculate()
                if practice_rows_created and ccg_rows_created:
                    if measure.is_cost_based:
                        global_calculation.calculate_global_cost_savings(
                            practice_calculation.full_table_name(),
                            ccg_calculation.full_table_name())
                    global_calculation.write_global_centiles_to_database()
                else:
                    raise CommandError(
                        "No rows generated by measure %s" % measure_id)
                elapsed = datetime.datetime.now() - measure_start
                logger.warning("Elapsed time for %s: %s seconds" % (
                    measure_id, elapsed.seconds))
        logger.warning("Total elapsed time: %s" % (
            datetime.datetime.now() - start))

    def add_arguments(self, parser):
        parser.add_argument('--month')
        parser.add_argument('--month_from_prescribing_filename')
        parser.add_argument('--start_date')
        parser.add_argument('--end_date')
        parser.add_argument('--measure')
        parser.add_argument('--test_mode', action='store_true')
        parser.add_argument('--definitions_only', action='store_true')

    def parse_options(self, options):
        """Parse command line options
        """
        if 'measure' in options and options['measure']:
            if "," in options['measure']:
                options['measure_ids'] = options['measure'].split(',')
            else:
                options['measure_ids'] = [options['measure']]
        else:
            options['measure_ids'] = [
                k for k, v in parse_measures().items() if 'skip' not in v]
        options['months'] = []
        if 'month' in options and options['month']:
            options['start_date'] = options['end_date'] = options['month']
        elif 'month_from_prescribing_filename' in options \
             and options['month_from_prescribing_filename']:
            filename = options['month_from_prescribing_filename']
            date_part = re.findall(r'/(\d{4}_\d{2})/', filename)[0]
            month = datetime.datetime.strptime(date_part + "_01", "%Y_%m_%d")

            options['start_date'] = options['end_date'] = \
                month.strftime('%Y-%m-01')
        else:
            l = ImportLog.objects.latest_in_category('prescribing')
            options['start_date'] = "%s-%s-%s" % (
                l.current_at.year - 5, l.current_at.month, l.current_at.day)
            options['end_date'] = l.current_at.strftime('%Y-%m-%d')
        # validate the date format
        datetime.datetime.strptime(options['start_date'], "%Y-%m-%d")
        datetime.datetime.strptime(options['end_date'], "%Y-%m-%d")
        return options


def parse_measures():
    """Deserialise JSON measures definition into dict
    """
    measures = {}
    fpath = os.path.dirname(__file__)
    files = glob.glob(os.path.join(fpath, "./measure_definitions/*.json"))
    for fname in files:
        measure_id = re.match(r'.*/([^/.]+)\.json', fname).groups()[0]
        if measure_id in measures:
            raise CommandError(
                "duplicate measure definition %s found!" % measure_id)
        fname = os.path.join(fpath, fname)
        json_data = open(fname).read()
        d = json.loads(json_data)
        measures[measure_id] = d
    return measures


# Utility methods

def float_or_null(v):
    """Return a value coerced to a float, unless it's a None.

    """
    if v is not None:
        v = float(v)
    return v


def float_or_zero(v):
    """Return a value coerced to a float; Nones become zero.
    """
    v = float_or_null(v)
    if v is None:
        v = 0.0
    return v


def convertSavingsToDict(datum, prefix=None):
    """Convert flat list of savings into a dict with centiles as
    keys

    """
    data = {}
    for centile in CENTILES:
        key = "cost_savings_%s" % centile
        if prefix:
            key = "%s_%s" % (prefix, key)
        data[str(centile)] = float_or_zero(datum.pop(key))
    return data


def convertDecilesToDict(datum, prefix=None):
    """Convert flat list of deciles into a dict with centiles as
    keys

    """
    assert prefix
    data = {}
    for centile in CENTILES:
        key = "%s_%sth" % (prefix, centile)
        data[str(centile)] = float_or_null(datum.pop(key))
    return data


def normalisePercentile(percentile):
    """Given a percentile between 0 and 1, or None, return a normalised
    version between 0 and 100, or None.

    """
    percentile = float_or_null(percentile)
    if percentile:
        percentile = percentile * 100
    return percentile


class MeasureCalculation(object):
    """An abstract base class containing bigquery connection helpers and
    logic common to both Practice- and CCG-level calculations.

    """
    def __init__(self, measure_id, start_date=None, end_date=None,
                 verbose=False, under_test=False):
        self.verbose = verbose
        self.fpath = os.path.dirname(__file__)
        self.measure_id = measure_id
        self.measure = parse_measures()[measure_id]
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if 'start_date' in self.measure:
            start_date = datetime.datetime.strptime(
                self.measure['start_date'], "%Y-%m-%d")
        if 'end_date' in self.measure:
            end_date = datetime.datetime.strptime(
                self.measure['end_date'], "%Y-%m-%d")
        self.start_date = start_date
        self.end_date = end_date
        self.under_test = under_test

        self.setup_db()

    def table_name(self):
        """Name of table to which we write ratios data.
        """
        raise NotImplementedError("Must be implemented in sublcass")

    def globals_table_name(self):
        """Name of table to which we write overall summary data

        """
        return "%s_%s" % (settings.BQ_GLOBALS_TABLE_PREFIX, self.measure_id)

    def full_table_name(self):
        """Fully qualified table name as used in bigquery SELECT
        (legacy SQL dialect)
        """
        return "[%s:%s.%s]" % (settings.BQ_PROJECT,
                               settings.BQ_MEASURES_DATASET,
                               self.table_name())

    def full_globals_table_name(self):
        """Fully qualified table name as used in bigquery SELECT
        (legacy SQL dialect)
        """
        return "[%s:%s.%s]" % (
            settings.BQ_PROJECT,
            settings.BQ_MEASURES_DATASET,
            self.globals_table_name())

    def full_practices_table_name(self):
        """Fully qualified name for practices table

        """
        return settings.BQ_FULL_PRACTICES_TABLE_NAME

    def setup_db(self):
        """Create a connection to postgres database
        """
        db_name = utils.get_env_setting('DB_NAME')
        db_user = utils.get_env_setting('DB_USER')
        db_pass = utils.get_env_setting('DB_PASS')
        db_host = utils.get_env_setting('DB_HOST', '127.0.0.1')
        self.conn = psycopg2.connect(database=db_name, user=db_user,
                                     password=db_pass, host=db_host)

    def get_columns_for_select(self, num_or_denom=None):
        """Parse measures definition for SELECT columns; add
        cost-savings-related columns when necessary.

        """
        assert num_or_denom in ['numerator', 'denominator']
        fieldname = "%s_columns" % num_or_denom
        cols = self.measure[fieldname][:]
        # Deal with possible inconsistencies in measure definition
        # trailing commas
        if cols[-1].strip()[-1] != ',':
            cols[-1] += ", "
        if self.measure['is_cost_based']:
            cols += ["SUM(items) AS items, ",
                     "SUM(actual_cost) AS cost, ",
                     "SUM(quantity) AS quantity "]
        # Deal with possible inconsistencies in measure definition
        # trailing commas
        if cols[-1].strip()[-1] == ',':
            cols[-1] = re.sub(r',\s*$', '', cols[-1])
        return cols

    def query_and_return(self, query, table_id, legacy=False):
        """Send query to BigQuery, wait, and return response object when the
        job has completed.

        Because the current specification format for a measure allows
        selecting the FROM table via free-text, and we want to support
        functional testing against real measure definitions, we have
        following hack to replace the table being queried with a test
        table name.

        A better thing to do would be to construct measure definitions
        specifically for testing.

        """
        if self.under_test:
            query = query.replace(
                "[ebmdatalab:hscic.normalised_prescribing_legacy]",
                "[ebmdatalab:measures.%s]" %
                settings.BQ_PRESCRIBING_TABLE_NAME)
        return bigquery.query_and_return(
            settings.BQ_PROJECT, 'measures', table_id, query, legacy)

    def get_rows(self, table_name):
        """Iterate over the specified bigquery table, returning a dict for
        each row of data.

        """
        return bigquery.get_rows(
            settings.BQ_PROJECT, settings.BQ_MEASURES_DATASET, table_name)

    def add_percent_rank(self):
        """Add a percentile rank to the ratios table
        """
        from_table = self.full_table_name()
        target_table = self.table_name()
        value_var = 'calc_value'
        sql_path = os.path.join(self.fpath, "./measure_sql/percent_rank.sql")
        with open(sql_path, "r") as sql_file:
            sql = sql_file.read()
            sql = sql.format(
                from_table=from_table,
                target_table=target_table,
                value_var=value_var)
            return self.query_and_return(sql, target_table, legacy=True)

    def log(self, message):
        if self.verbose:
            logger.warning(message)
        else:
            logger.info(message)

    def _query_and_write_global_centiles(self,
                                         sql_path,
                                         value_var,
                                         from_table,
                                         extra_select_sql):
        with open(sql_path) as sql_file:
            value_var = 'calc_value'
            sql = sql_file.read()
            sql = sql.format(
                from_table=from_table,
                extra_select_sql=extra_select_sql,
                value_var=value_var,
                global_centiles_table=self.full_globals_table_name())
            # We have to use legacy SQL because there' no
            # PERCENTILE_CONT equivalent in the standard SQL
            return self.query_and_return(
                sql, self.globals_table_name(), legacy=True)

    def _get_col_aliases(self, num_or_denom=None):
        """Return column names referred to in measure definitions for both
        numerator or denominator.

        When we use nested SELECTs, we need to know which column names
        have been aliased in the inner SELECT so we can select them
        explicitly by name in the outer SELECT.

        """
        assert num_or_denom in ['numerator', 'denominator']
        cols = []
        for col in self.get_columns_for_select(num_or_denom=num_or_denom):
            match = re.search(r"AS ([a-z0-9_]+)", col)
            if match:
                alias = match.group(1)
            else:
                raise CommandError("Could not find alias in %s" % col)
            if alias != num_or_denom:
                cols.append(alias)
        return cols


class GlobalCalculation(MeasureCalculation):
    def calculate_global_cost_savings(
            self, practice_table_name, ccg_table_name):
        """Execute a bigquery SQL statement to sum cost savings at practice
        and CCG levels.

        Reads from the existing global table and writes back to it again.
        """
        sql_path = os.path.join(
            self.fpath, "./measure_sql/global_cost_savings.sql")
        with open(sql_path, "r") as sql_file:
            sql = sql_file.read()
            sql = sql.format(
                practice_table=practice_table_name,
                ccg_table=ccg_table_name,
                global_table=self.full_globals_table_name()
            )
            target_table = self.globals_table_name()
            self.query_and_return(sql, target_table, legacy=True)

    def write_global_centiles_to_database(self):
        """Write the globals data from BigQuery to the local database
        """
        self.log("Writing global centiles from %s to database"
                 % self.globals_table_name())
        count = 0
        for d in self.get_rows(self.globals_table_name()):
            ccg_deciles = {}
            practice_deciles = {}
            ccg_cost_savings = {}
            practice_cost_savings = {}
            d['measure_id'] = self.measure_id
            # The cost-savings calculations prepend columns with
            # global_. There is probably a better way of contstructing
            # the query so this clean-up doesn't have to happen...
            new_d = {}
            for attr, value in d.iteritems():
                new_d[attr.replace('global_', '')] = value
            d = new_d

            mg, _ = MeasureGlobal.objects.get_or_create(
                measure_id=self.measure_id,
                month=d['month']
            )

            # Coerce decile-based values into JSON objects
            if self.measure['is_cost_based']:
                practice_cost_savings = convertSavingsToDict(
                    d, prefix='practice')
                ccg_cost_savings = convertSavingsToDict(
                    d, prefix='ccg')
                mg.cost_savings = {'ccg': ccg_cost_savings,
                                   'practice': practice_cost_savings}
            practice_deciles = convertDecilesToDict(d, prefix='practice')
            ccg_deciles = convertDecilesToDict(d, prefix='ccg')
            mg.percentiles = {'ccg': ccg_deciles, 'practice': practice_deciles}

            # Set the rest of the data returned from bigquery directly
            # on the model
            for attr, value in d.iteritems():
                setattr(mg, attr, value)
            mg.save()
            count += 1
        self.log("Created %s measureglobals" % count)

    def create_or_update_measure(self):
        """Create a measure object based on a measure definition

        """
        v = self.measure
        self.log('Updating measure: %s' % self.measure_id)
        v['title'] = ' '.join(v['title'])
        v['description'] = ' '.join(v['description'])
        v['why_it_matters'] = ' '.join(v['why_it_matters'])
        try:
            measure = Measure.objects.get(id=self.measure_id)
            measure.name = v['name']
            measure.title = v['title']
            measure.description = v['description']
            measure.why_it_matters = v['why_it_matters']
            measure.numerator_short = v['numerator_short']
            measure.denominator_short = v['denominator_short']
            measure.url = v['url']
            measure.is_cost_based = v['is_cost_based']
            measure.is_percentage = v['is_percentage']
            measure.low_is_good = v['low_is_good']
            measure.save()
        except ObjectDoesNotExist:
            measure = Measure.objects.create(
                id=self.measure_id,
                name=v['name'],
                title=v['title'],
                description=v['description'],
                why_it_matters=v['why_it_matters'],
                numerator_short=v['numerator_short'],
                denominator_short=v['denominator_short'],
                url=v['url'],
                is_cost_based=v['is_cost_based'],
                is_percentage=v['is_percentage'],
                low_is_good=v['low_is_good']
            )
        return measure


class PracticeCalculation(MeasureCalculation):
    def calculate(self):
        """Calculate ratios, centiles and (optionally) cost savings at a
        practice level, and write these to the database.

        """
        self.log("Calculating practice ratios")
        self.calculate_practice_ratios()
        self.log("Adding percent rank to practices")
        self.add_percent_rank()
        self.log("Calculating global centiles for practices")
        self.calculate_global_centiles_for_practices()
        if self.measure['is_cost_based']:
            self.log("Calculating cost savings for practices")
            self.calculate_cost_savings_for_practices()
        self.log("Writing practice ratios to postgres")
        number_rows_written = self.write_practice_ratios_to_database()
        return number_rows_written

    def table_name(self):
        """The name of the bigquery working table for practices

        """
        return "%s_%s" % (settings.BQ_PRACTICE_TABLE_PREFIX, self.measure_id)

    def calculate_practice_ratios(self):
        """Given a measure defition, construct a BigQuery query which computes
        numerator/denominator ratios for practices.

        See also comments in SQL.

        """
        numerator_where = " ".join(self.measure['numerator_where'])
        denominator_where = " ".join(self.measure['denominator_where'])
        numerator_aliases = ''
        denominator_aliases = ''
        aliased_numerators = ''
        aliased_denominators = ''
        for col in self._get_col_aliases('denominator'):
            denominator_aliases += ", denom.%s AS denom_%s" % (col, col)
            aliased_denominators += ", denom_%s" % col
        for col in self._get_col_aliases('numerator'):
            numerator_aliases += ", num.%s AS num_%s" % (col, col)
            aliased_numerators += ", num_%s" % col
        sql_path = os.path.join(
            self.fpath, "./measure_sql/practice_ratios.sql")
        with open(sql_path, "r") as sql_file:
            sql = sql_file.read()
            sql = sql.format(
                numerator_from=self.measure['numerator_from'],
                numerator_where=numerator_where,
                numerator_columns=" ".join(
                    self.get_columns_for_select('numerator')),
                denominator_columns=" ".join(
                    self.get_columns_for_select('denominator')),
                denominator_from=self.measure['denominator_from'],
                denominator_where=denominator_where,
                numerator_aliases=numerator_aliases,
                denominator_aliases=denominator_aliases,
                aliased_denominators=aliased_denominators,
                aliased_numerators=aliased_numerators,
                practices_from=self.full_practices_table_name(),
                start_date=self.start_date,
                end_date=self.end_date

            )
            return self.query_and_return(sql, self.table_name())

    def calculate_global_centiles_for_practices(self):
        """Compute overall sums and centiles for each practice.

        """
        sql_path = os.path.join(
            self.fpath, "./measure_sql/global_deciles_practices.sql")
        from_table = self.full_table_name()
        extra_fields = []
        # Add prefixes to the select columns so we can reference the
        # joined tables (bigquery legacy SQL flattens columns names
        # from subqueries using table alias + underscore)
        for col in self._get_col_aliases('numerator'):
            extra_fields.append("num_" + col)
        for col in self._get_col_aliases('denominator'):
            extra_fields.append("denom_" + col)
        extra_select_sql = ""
        for f in extra_fields:
            extra_select_sql += ", SUM(%s) as %s" % (f, f)
        if self.measure["is_cost_based"]:
            extra_select_sql += (
                ", "
                "(SUM(denom_cost) - SUM(num_cost)) / (SUM(denom_quantity)"
                "- SUM(num_quantity)) AS cost_per_denom,"
                "SUM(num_cost) / SUM(num_quantity) as cost_per_num")
        value_var = 'calc_value'
        return self._query_and_write_global_centiles(
            sql_path, value_var, from_table, extra_select_sql)

    def calculate_cost_savings_for_practices(self):
        """Append cost savings column to the Practice working table"""
        sql_path = os.path.join(self.fpath, "./measure_sql/cost_savings.sql")
        with open(sql_path, "r") as sql_file:
            sql = sql_file.read()
            ratios_table = self.full_table_name()
            global_table = self.full_globals_table_name()
            target_table = self.table_name()
            sql = sql.format(
                local_table=ratios_table,
                global_table=global_table,
                unit='practice'
            )
            self.query_and_return(sql, target_table)

    def write_practice_ratios_to_database(self):
        """Copy the bigquery ratios data to the local postgres database.

        Returns number of rows written.

        Uses COPY command via a CSV file for performance as this can
        be a very large number, especially when computing many months'
        data at once.  We drop and then recreate indexes to improve
        load time performance.

        """
        fieldnames = ['pct_id', 'measure_id', 'num_items', 'numerator',
                      'denominator', 'month',
                      'percentile', 'calc_value', 'denom_items',
                      'denom_quantity', 'denom_cost', 'num_cost',
                      'num_quantity', 'practice_id', 'cost_savings']
        f = tempfile.TemporaryFile(mode='r+')
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        c = 0
        # Write the data we want to load into a file
        for datum in self.get_rows(self.table_name()):
            datum['measure_id'] = self.measure_id
            if self.measure['is_cost_based']:
                datum['cost_savings'] = json.dumps(convertSavingsToDict(datum))
            datum['percentile'] = normalisePercentile(datum['percentile'])
            writer.writerow(datum)
            c += 1
        # load data
        copy_str = "COPY frontend_measurevalue(%s) FROM STDIN "
        copy_str += "WITH (FORMAT CSV)"
        self.log(copy_str % ", ".join(fieldnames))
        f.seek(0)
        with connection.cursor() as cursor:
            cursor.copy_expert(copy_str % ", ".join(fieldnames), f)
        f.close()
        self.log("Wrote %s values" % c)
        return c


class CCGCalculation(MeasureCalculation):
    def calculate(self):
        """Calculate ratios, centiles and (optionally) cost savings at a
        CCG level, and write these to the database.

        """
        self.log("Calculating CCG ratios")
        self.calculate_ccg_ratios()
        self.log("Adding rank to CCG ratios")
        self.add_percent_rank()
        self.log("Calculating global CCG centiles")
        self.calculate_global_centiles_for_ccgs()
        if self.measure['is_cost_based']:
            self.log("Calculating CCG cost savings")
            self.calculate_cost_savings_for_ccgs()
        self.log("Writing CCG data to postgres")
        number_rows_written = self.write_ccg_ratios_to_database()
        return number_rows_written

    def table_name(self):
        """The name of the bigquery working table for CCGs

        """
        return "%s_%s" % (settings.BQ_CCG_TABLE_PREFIX, self.measure_id)

    def calculate_ccg_ratios(self):
        """Sums all the fields in the per-practice table, grouped by
        CCG. Stores in a new table.

        """
        with open(os.path.join(
              self.fpath, "./measure_sql/ccg_ratios.sql")) as sql_file:
            sql = sql_file.read()
            numerator_aliases = denominator_aliases = ''
            for col in self._get_col_aliases('denominator'):
                denominator_aliases += ", SUM(denom_%s) AS denom_%s" % (
                    col, col)
            for col in self._get_col_aliases('numerator'):
                numerator_aliases += ", SUM(num_%s) AS num_%s" % (col, col)
            from_table = PracticeCalculation(
                self.measure_id, under_test=self.under_test).full_table_name()
            sql = sql.format(denominator_aliases=denominator_aliases,
                             numerator_aliases=numerator_aliases,
                             from_table=from_table)
            self.query_and_return(sql, self.table_name())

    def calculate_global_centiles_for_ccgs(self):
        """Adds CCG centiles to the already-existing CCG centiles table

        """
        extra_fields = []
        # Add prefixes to the select columns so we can reference the
        # joined tables (bigquery legacy SQL flattens columns names
        # from subqueries using table alias + underscore)
        for col in self._get_col_aliases('numerator'):
            extra_fields.append("num_" + col)
        for col in self._get_col_aliases('denominator'):
            extra_fields.append("denom_" + col)
        extra_select_sql = ""
        for f in extra_fields:
            extra_select_sql += ", practice_deciles.%s as %s" % (f, f)
        if self.measure["is_cost_based"]:
            extra_select_sql += (
                ", practice_deciles.cost_per_denom AS cost_per_denom"
                ", practice_deciles.cost_per_num AS cost_per_num")
        sql_path = os.path.join(
            self.fpath, "./measure_sql/global_deciles_ccgs.sql")
        from_table = self.full_table_name()
        value_var = 'calc_value'
        return self._query_and_write_global_centiles(
            sql_path, value_var, from_table, extra_select_sql)

    def calculate_cost_savings_for_ccgs(self):
        """Appends cost savings column to the CCG ratios table"""

        sql_path = os.path.join(self.fpath, "./measure_sql/cost_savings.sql")
        with open(sql_path, "r") as sql_file:
            sql = sql_file.read()
            ratios_table = self.full_table_name()
            global_table = self.full_globals_table_name()
            target_table = self.table_name()
            sql = sql.format(
                local_table=ratios_table,
                global_table=global_table,
                unit='ccg'
            )
            self.query_and_return(sql, target_table)

    def write_ccg_ratios_to_database(self):
        """Create measure values for CCG ratios (these are distinguished from
        practice ratios by having a NULL practice_id).

        Retuns number of rows written.

        """
        with transaction.atomic():
            c = 0
            for datum in self.get_rows(self.table_name()):
                datum['measure_id'] = self.measure_id
                if self.measure['is_cost_based']:
                    datum['cost_savings'] = convertSavingsToDict(datum)
                datum['percentile'] = normalisePercentile(datum['percentile'])
                MeasureValue.objects.create(**datum)
                c += 1
        self.log("Wrote %s CCG measures" % c)
        return c


@contextmanager
def conditional_constraint_and_index_reconstructor(options):
    if 'measure' in options and options['measure']:
        # This is an optimisation that only makes sense when we're
        # updating the entire table.
        yield
    else:
        yield utils.constraint_and_index_reconstructor('frontend_measurevalue')
