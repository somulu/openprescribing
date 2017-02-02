from sets import Set
import datetime
import json
import os
import time

import pandas as pd
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from django.core.management.base import BaseCommand
from django.db import transaction

from frontend.models import PPQSaving
from frontend.dmd_models import DMDProduct


def get_bq_service():
    """Returns a bigquery service endpoint
    """
    # We've started using the google-cloud library since first writing
    # this. When it settles down a bit, start using that rather than
    # this low-level API. See
    # https://googlecloudplatform.github.io/google-cloud-python/
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('bigquery', 'v2',
                           credentials=credentials)


def query_and_return(project_id, dataset_id, table_id, query, legacy=False):
    """Send query to BigQuery, wait, write it to table_id, and return
    response object when the job has completed.

    """
    payload = {
        "configuration": {
            "query": {
                "query": query,
                "flattenResuts": False,
                "allowLargeResults": True,
                "timeoutMs": 100000,
                "useQueryCache": True,
                "useLegacySql": legacy,
                "destinationTable": {
                    "projectId": project_id,
                    "tableId": table_id,
                    "datasetId": dataset_id
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    }
    # We've started using the google-cloud library since first
    # writing this. TODO: decide if we can use that throughout
    bq = get_bq_service()
    start = datetime.datetime.now()
    response = bq.jobs().insert(
        projectId=project_id,
        body=payload).execute()
    counter = 0
    job_id = response['jobReference']['jobId']
    while True:
        time.sleep(1)
        response = bq.jobs().get(
            projectId=project_id,
            jobId=job_id).execute()
        counter += 1
        if response['status']['state'] == 'DONE':
            if 'errors' in response['status']:
                query = str(response['configuration']['query']['query'])
                for i, l in enumerate(query.split("\n")):
                    # print SQL query with line numbers for debugging
                    print "{:>3}: {}".format(i + 1, l)
                raise StandardError(
                    json.dumps(response['status']['errors'], indent=2))
            else:
                break
    bytes_billed = float(
        response['statistics']['query']['totalBytesBilled'])
    gb_processed = round(bytes_billed / 1024 / 1024 / 1024, 2)
    est_cost = round(bytes_billed / 1e+12 * 5.0, 2)
    # Add our own metadata
    elapsed = (datetime.datetime.now() - start).total_seconds()
    response['openp'] = {'query': query,
                         'est_cost': est_cost,
                         'time': elapsed,
                         'gb_processed': gb_processed}
    return response


def get_substitutions():
    # https://docs.google.com/spreadsheets/d/1SvMGCKrmqsNkZYuGW18Sf0wTluXyV4bhyZQaVLcO41c/edit#gid=1799396915
    url = ("https://docs.google.com/spreadsheets/d/"
           "1SvMGCKrmqsNkZYuGW18Sf0wTluXyV4bhyZQaVLcO41c/"
           "pub?gid=1784930737&single=true&output=csv")
    df = None
    df = pd.read_csv(url)
    return df


def make_table_for_month(month='2016-09-01',
                         namespace='hscic',
                         prescribing_table='prescribing'):
    """Handle code substitutions.

    Because (for example) Tramadol tablets and capsules can almost
    always be substituted, we consider them the same chemical for the purposes of our analysis.

    Therefore, wherever Tramadol capsules appear in the source data,
    we treat them as Tramadol tablets (for example).

    The mapping of what we consider equivalent is stored in a Google
    Sheet.

    """
    cases = []
    seen = Set()
    df = get_substitutions()
    df = df[df['Really equivalent?'] == 'Y']
    for row in df.iterrows():
        data = row[1]
        source_code = data[1].strip()
        code_to_merge = data[10].strip()
        if source_code not in seen and code_to_merge not in seen:
            cases.append((source_code, code_to_merge))
        seen.add(source_code)
        seen.add(code_to_merge)

    query = """
      SELECT
        practice,
        pct,
      CASE bnf_code
        %s
        ELSE bnf_code
      END AS bnf_code,
        month,
        actual_cost,
        net_cost,
        quantity
      FROM
        ebmdatalab.%s.%s
      WHERE month = TIMESTAMP('%s')
    """ % (''.join(
        ["WHEN '%s' THEN '%s'" % (code_to_merge, source_code)
         for (source_code, code_to_merge) in cases]),
           namespace,
           prescribing_table,
           month)
    target_table_name = 'prescribing_with_merged_codes_%s' % month.replace(
        '-', '_')
    query_and_return('ebmdatalab', namespace,
                     target_table_name,
                     query, legacy=False)
    return target_table_name


def get_savings(for_entity='', group_by='', month='', cost_field='net_cost',
                sql_only=False, limit=1000, order_by_savings=True,
                min_saving=0, namespace='hscic',
                prescribing_table='prescribing'):
    assert month
    assert group_by or for_entity
    assert group_by in ['', 'pct', 'practice', 'product']
    prescribing_table = "ebmdatalab.%s.%s" % (
        namespace,
        make_table_for_month(
            month=month,
            namespace=namespace,
            prescribing_table=prescribing_table
        )
    )
    restricting_condition = (
        "AND LENGTH(RTRIM(p.bnf_code)) >= 15 "
        "AND p.bnf_code NOT LIKE '0302000C0____BE' "  # issue #10
        "AND p.bnf_code NOT LIKE '0302000C0____BF' "  # issue #10
        "AND p.bnf_code NOT LIKE '0302000C0____BH' "  # issue #10
        "AND p.bnf_code NOT LIKE '0302000C0____BG' "  # issue #10
        "AND p.bnf_code NOT LIKE '0904010H0%' "  # issue #9
        "AND p.bnf_code NOT LIKE '0904010H0%' "  # issue #9
        "AND p.bnf_code NOT LIKE '1311070S0A____AA' "  # issue #9
        "AND p.bnf_code NOT LIKE '1311020L0____BS' "  # issue #9
        "AND p.bnf_code NOT LIKE '0301020S0____AA' "  # issue #12
        "AND p.bnf_code NOT LIKE '190700000BBCJA0' "  # issue #12
        "AND p.bnf_code NOT LIKE '0604011L0BGAAAH' "  # issue #12
        "AND p.bnf_code NOT LIKE '1502010J0____BY' "  # issue #12
        "AND p.bnf_code NOT LIKE '1201010F0AAAAAA' "  # issue #12
        "AND p.bnf_code NOT LIKE '0107010S0AAAGAG' "  # issue #12
        "AND p.bnf_code NOT LIKE '060106000BBAAA0' "  # issue #14
        "AND p.bnf_code NOT LIKE '190201000AABJBJ' "  # issue #14
        "AND p.bnf_code NOT LIKE '190201000AABKBK' "  # issue #14
        "AND p.bnf_code NOT LIKE '190201000AABLBL' "  # issue #14
        "AND p.bnf_code NOT LIKE '190201000AABMBM' "  # issue #14
        "AND p.bnf_code NOT LIKE '190201000AABNBN' "  # issue #14
        "AND p.bnf_code NOT LIKE '190202000AAADAD' "  # issue #14
    )
    if len(for_entity) == 3:
        restricting_condition += 'AND pct = "%s"' % for_entity
        group_by = 'pct'
    elif len(for_entity) > 3:
        restricting_condition += 'AND practice = "%s"' % for_entity
        group_by = 'practice'
    if limit:
        limit = "LIMIT %s" % limit
    else:
        limit = ''
    if group_by == 'pct':
        select = 'savings.presentations.pct AS pct,'
        inner_select = 'presentations.pct, '
        group_by = 'presentations.pct, '
    elif group_by == 'practice':
        select = ('savings.presentations.practice AS practice,'
                  'savings.presentations.pct AS pct,')
        inner_select = ('presentations.pct, '
                        'presentations.practice,')
        group_by = ('presentations.practice, '
                    'presentations.pct,')
    elif group_by == 'product':
        select = ''
        inner_select = ''
        group_by = ''

    if order_by_savings:
        order_by = "ORDER BY possible_savings DESC"
    else:
        order_by = ''
    fpath = os.path.dirname(__file__)
    with open("%s/ppq_sql/savings_for_decile.sql" % fpath, "r") as f:
        sql = f.read()
        substitutions = (
            ('{{ restricting_condition }}', restricting_condition),
            ('{{ limit }}', limit),
            ('{{ month }}', month),
            ('{{ group_by }}', group_by),
            ('{{ order_by }}', order_by),
            ('{{ select }}', select),
            ('{{ prescribing_table }}', prescribing_table),
            ('{{ cost_field }}', cost_field),
            ('{{ inner_select }}', inner_select),
            ('{{ min_saving }}', min_saving)
        )
        for key, value in substitutions:
            sql = sql.replace(key, str(value))
        if sql_only:
            return sql
        else:
            df = run_gbq(sql)
            # Rename null values in category, so we can group by it
            df.loc[df['category'].isnull(), 'category'] = 'NP8'
            df = df.set_index(
                'generic_presentation')
            df.index.name = 'bnf_code'
            # Add in substitutions column
            subs = get_substitutions().set_index('Code')
            subs = subs[subs['Really equivalent?'] == 'Y']
            subs['formulation_swap'] = (
                subs['Formulation'] +
                ' / ' +
                subs['Alternative formulation'])
            df = df.join(
                subs[['formulation_swap']], how='left')
            # Convert nans to Nones
            df = df.where((pd.notnull(df)), None)
            return df


def run_gbq(sql):
    try:
        df = pd.io.gbq.read_gbq(
            sql,
            project_id="ebmdatalab",
            verbose=False,
            dialect='legacy',
            private_key=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        return df
    except:
        for n, line in enumerate(sql.split("\n")):
            print "%s: %s" % (n+1, line)
        raise


class Command(BaseCommand):
    args = ''
    help = 'Imports cost savings for a month'

    def add_arguments(self, parser):
        parser.add_argument('--source_directory')

    def handle(self, *args, **options):
        '''
        Import dm+d dataset.
        '''
        date = '2016-09-01'
        with transaction.atomic():
            # Create custom DMD Products for our overrides, if they don't exists
            DMDProduct.objects.get_or_create(
                dmdid=10000000000,
                bnf_code='0601060D0AAA0A0',
                display_name='Glucose Blood Testing Reagents',
                concept_class=1
            )
            DMDProduct.objects.get_or_create(
                dmdid=10000000001,
                bnf_code='0601060U0AAA0A0',
                display_name='Urine Testing Reagents',
                concept_class=1)

            PPQSaving.objects.filter(date=date).delete()
            for entity_type, min_saving in [('pct', 1000), ('practice', 50)]:
                result = get_savings(
                    group_by=entity_type,
                    month=date,
                    limit=0,
                    min_saving=min_saving)
                for row in result.itertuples():
                    asdict = row._asdict()
                    if asdict['price_per_dose']:
                        PPQSaving.objects.create(
                            date=date,
                            bnf_code=asdict['Index'],
                            lowest_decile=asdict['lowest_decile'],
                            quantity=asdict['quantity'],
                            price_per_dose=asdict['price_per_dose'],
                            possible_savings=asdict['possible_savings'],
                            formulation_swap=asdict['formulation_swap'] or None,
                            pct_id=asdict.get('pct', None),
                            practice_id=asdict.get('practice', None)
                        )
