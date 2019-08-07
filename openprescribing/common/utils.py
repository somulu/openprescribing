from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
from os import environ
from titlecase import titlecase
import argparse
import html2text
import logging
import re

from django.core.exceptions import ImproperlyConfigured
from django import db


logger = logging.getLogger(__name__)


def nhs_abbreviations(word, **kwargs):
    if len(word) == 2 and word.lower() not in [
        "at",
        "of",
        "in",
        "on",
        "to",
        "is",
        "me",
        "by",
        "dr",
        "st",
    ]:
        return word.upper()
    elif word.lower() in ["dr", "st"]:
        return word.title()
    elif word.upper() in ("NHS", "CCG", "PMS", "SMA", "PWSI", "OOH", "HIV"):
        return word.upper()
    elif "&" in word:
        return word.upper()
    elif (word.lower() not in ["ptnrs", "by", "ccgs"]) and (
        not re.match(r".*[aeiou]{1}", word.lower())
    ):
        return word.upper()


def nhs_titlecase(words):
    if words:
        title_cased = titlecase(words, callback=nhs_abbreviations)
        words = re.sub(r"Dr ([a-z]{2})", "Dr \1", title_cased)
    return words


def email_as_text(html):
    text_maker = html2text.HTML2Text()
    text_maker.images_to_alt = True
    text_maker.asterisk_emphasis = True
    text_maker.wrap_links = False
    text_maker.pad_tables = True
    text_maker.ignore_images = True
    text = text_maker.handle(html)
    return text


def get_env_setting(setting, default=None):
    """ Get the environment setting.

    Return the default, or raise an exception if none supplied
    """
    try:
        return environ[setting]
    except KeyError:
        if default is not None:
            return default
        else:
            error_msg = "Set the %s env variable" % setting
            raise ImproperlyConfigured(error_msg)


def get_env_setting_bool(setting, default=None):
    """ Get the environment setting as a boolean

    Return the default, or raise an exception if none supplied
    """
    value = get_env_setting(setting, default=default)
    if value is default:
        return value
    normalised = value.lower().strip()
    if normalised == "true":
        return True
    elif normalised == "false":
        return False
    else:
        raise ImproperlyConfigured(
            "Value for env variable {} is not a valid boolean: {}".format(
                setting, value
            )
        )


def under_test():
    return db.connections.databases["default"]["NAME"].startswith("test_")


@contextmanager
def constraint_and_index_reconstructor(table_name):
    """A context manager that drops indexes and constraints on the
    specified table, yields, then recreates them.

    According to postgres documentation, when doing bulk loads, this
    should be faster than having the indexes update during the insert.

    See https://www.postgresql.org/docs/current/static/populate.html
    for more.

    """
    with db.connection.cursor() as cursor:

        # Record index and constraint definitions
        indexes = {}
        constraints = {}
        cluster = None

        # Build lists of current constraints and indexes, and any
        # existing cluster
        cursor.execute(
            "SELECT conname, pg_get_constraintdef(c.oid) "
            "FROM pg_constraint c "
            "JOIN pg_namespace n "
            "ON n.oid = c.connamespace "
            "WHERE contype IN ('f', 'p','c','u') "
            "AND conrelid = '%s'::regclass "
            "ORDER BY contype;" % table_name
        )
        for name, definition in cursor.fetchall():
            constraints[name] = definition
        cursor.execute(
            "SELECT indexname, indexdef "
            "FROM pg_indexes "
            "WHERE tablename = '%s';" % table_name
        )
        for name, definition in cursor.fetchall():
            if name not in constraints.keys():
                # UNIQUE constraints actuall create indexes, so
                # we mustn't attempt to handle them twice
                indexes[name] = definition
        cursor.execute(
            """
            SELECT
              i.relname AS index_for_cluster
            FROM
              pg_index AS idx
            JOIN
              pg_class AS i
            ON
              i.oid = idx.indexrelid
            WHERE
              idx.indisclustered
              AND idx.indrelid::regclass = '%s'::regclass;
        """
            % table_name
        )
        row = cursor.fetchone()
        if row:
            cluster = row[0]

        # drop foreign key constraints
        for name in constraints.keys():
            cursor.execute("ALTER TABLE %s DROP CONSTRAINT %s" % (table_name, name))

        # drop indexes
        logger.info("Dropping indexes")
        for name in indexes.keys():
            cursor.execute("DROP INDEX %s" % name)
            logger.info("Dropped index %s" % name)

        logger.info("Running wrapped command")
        try:
            yield
        finally:
            # we're updating everything. This takes 52 minutes.
            # restore indexes
            logger.info("Recreating indexes")
            for name, cmd in indexes.items():
                cursor.execute(cmd)
                logger.info("Recreated index %s" % name)

            logger.info("Recreating constraints")
            # restore foreign key constraints
            for name, cmd in constraints.items():
                cmd = "ALTER TABLE %s " "ADD CONSTRAINT %s %s" % (table_name, name, cmd)
                cursor.execute(cmd)
                logger.info("Recreated constraint %s" % name)
            if cluster:
                cursor.execute("CLUSTER %s USING %s" % (table_name, cluster))
                cursor.execute("ANALYZE %s" % table_name)
                logger.info("CLUSTERED %s" % table_name)


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def valid_date(s):
    """Validate ISO-formatted dates. For use in argparse arguments.
    """
    try:
        return parse_date(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def namedtuplefetchall(cursor):
    "Return all rows from a cursor as a namedtuple"
    desc = cursor.description
    nt_result = namedtuple("Result", [col[0] for col in desc])
    return [nt_result(*row) for row in cursor.fetchall()]


def ppu_sql(conditions=""):
    # Model imports here because util module is used in Django's
    # startup, before model registration is complete, leading to
    # errors
    from dmd2.models import VMP, VMPP
    from frontend.models import NCSOConcession
    from frontend.models import PPUSaving
    from frontend.models import Presentation
    from frontend.models import Practice
    from frontend.models import PCT

    # See https://github.com/ebmdatalab/price-per-dose/issues/1 for an
    # explanation of the extra BNF codes in vmp_bnf_codes below.

    sql = """
    WITH vmp_bnf_codes AS (
        SELECT DISTINCT bnf_code FROM {vmp_table}
        UNION ALL
        SELECT '0601060D0AAA0A0'  -- "Glucose Blood Testing Reagents"
        UNION ALL
        SELECT '0601060U0AAA0A0'  -- "Urine Testing Reagents"
    )

    SELECT
        {ppusavings_table}.id AS id,
        {ppusavings_table}.date AS date,
        {ppusavings_table}.lowest_decile AS lowest_decile,
        {ppusavings_table}.quantity AS quantity,
        {ppusavings_table}.price_per_unit AS price_per_unit,
        {ppusavings_table}.possible_savings AS possible_savings,
        {ppusavings_table}.formulation_swap AS formulation_swap,
        {ppusavings_table}.pct_id AS pct,
        {ppusavings_table}.practice_id AS practice,
        {ppusavings_table}.bnf_code AS presentation,
        {practice_table}.name AS practice_name,
        {pct_table}.name AS pct_name,
        subquery.price_concession IS NOT NULL as price_concession,
        COALESCE({presentation_table}.dmd_name, {presentation_table}.name) AS name
    FROM {ppusavings_table}
    LEFT OUTER JOIN {presentation_table}
        ON {ppusavings_table}.bnf_code = {presentation_table}.bnf_code
    LEFT OUTER JOIN {practice_table}
        ON {ppusavings_table}.practice_id = {practice_table}.code
    LEFT OUTER JOIN {pct_table}
        ON {ppusavings_table}.pct_id = {pct_table}.code
    LEFT OUTER JOIN (SELECT DISTINCT bnf_code, 1 AS price_concession
                     FROM {vmpp_table}
                     INNER JOIN {ncsoconcession_table}
                      ON {vmpp_table}.vppid = {ncsoconcession_table}.vmpp_id
                     WHERE {ncsoconcession_table}.date = %(date)s) AS subquery
        ON {ppusavings_table}.bnf_code = subquery.bnf_code
    WHERE
        {ppusavings_table}.date = %(date)s
        AND {ppusavings_table}.bnf_code IN (SELECT bnf_code FROM vmp_bnf_codes)
    """

    sql += conditions
    return sql.format(
        ppusavings_table=PPUSaving._meta.db_table,
        practice_table=Practice._meta.db_table,
        pct_table=PCT._meta.db_table,
        presentation_table=Presentation._meta.db_table,
        vmpp_table=VMPP._meta.db_table,
        vmp_table=VMP._meta.db_table,
        ncsoconcession_table=NCSOConcession._meta.db_table,
    )
