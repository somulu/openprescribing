import tempfile

from django.core.management.base import BaseCommand
from django.db import connection

from gcutils.bigquery import Client
from gcutils.bigquery import build_schema


class Command(BaseCommand):
    help = ('Create metadata extract for team experimentation in bigquery')
    def handle(self, *args, **options):
        create_and_upload_csv()

def create_and_upload_csv():
    sql = """
      SELECT DISTINCT dmd_product.bnf_code,
        dmd_product.name,
        dmd_product.vpid,
        dmd_lookup_form."desc" AS form,
        dmd_lookup_ont_form_route."desc" AS form_route,
        indicator_lookup."desc" AS form_indicator,
        dmd_vmp.udfs AS form_size,
        form_size_lookup."desc" AS form_units,
        unit_dose_lookup."desc" AS unit_of_measure,
        dmd_vpi.strnt_nmrtr_val AS numerator,
        numerator_measure_lookup."desc" AS numerator_unit_of_measure,
        dmd_vpi.strnt_dnmtr_val AS denominator,
        denominator_measure_lookup."desc" AS denominator_unit_of_measure
       FROM dmd_product
         JOIN dmd_vmp ON dmd_vmp.vpid = dmd_product.vpid
         LEFT JOIN dmd_vpi ON dmd_vpi.vpid = dmd_product.vpid
         LEFT JOIN dmd_lookup_df_indicator indicator_lookup ON indicator_lookup.cd = dmd_vmp.df_indcd
         LEFT JOIN dmd_lookup_unit_of_measure form_size_lookup ON form_size_lookup.cd = dmd_vmp.udfs_uomcd
         LEFT JOIN dmd_lookup_unit_of_measure unit_dose_lookup ON unit_dose_lookup.cd = dmd_vmp.unit_dose_uomcd
         LEFT JOIN dmd_lookup_unit_of_measure numerator_measure_lookup ON numerator_measure_lookup.cd = dmd_vpi.strnt_nmrtr_uomcd
         LEFT JOIN dmd_lookup_unit_of_measure denominator_measure_lookup ON denominator_measure_lookup.cd = dmd_vpi.strnt_dnmtr_uomcd
         left join dmd_dform on dmd_dform.vpid = dmd_vmp.vpid inner join dmd_lookup_form on dmd_dform.formcd = dmd_lookup_form.cd
         LEFT JOIN dmd_ont ON dmd_product.vpid = dmd_ont.vpid
         LEFT JOIN dmd_lookup_ont_form_route ON dmd_lookup_ont_form_route.cd = dmd_ont.formcd
    WHERE bnf_code IS NOT NULL
    """

    sql = "COPY (%s) TO STDOUT (FORMAT CSV, NULL '')" % (sql)
    with tempfile.NamedTemporaryFile() as tmp_f:
        with connection.cursor() as c:
            c.copy_expert(sql, tmp_f)
        tmp_f.close()
        upload_csv(tmp_f.name)

def upload_csv(name):
    schema = [
        {'name': 'bnf_code', 'type': 'string'},
        {'name': 'name', 'type': 'string'},
        {'name': 'vpid', 'type': 'integer'},
        {'name': 'form', 'type': 'string'},
        {'name': 'form_route', 'type': 'string'},
        {'name': 'form_indicator', 'type': 'string'},
        {'name': 'form_size', 'type': 'float'},
        {'name': 'form_units', 'type': 'string'},
        {'name': 'unit_of_measure', 'type': 'string'},
        {'name': 'numerator', 'type': 'float'},
        {'name': 'numerator_unit_of_measure', 'type': 'string'},
        {'name': 'denominator', 'type': 'float'},
        {'name': 'denominator_unit_of_measure', 'type': 'string'},
    ]
    schema = build_schema(*[(x['name'], x['type']) for x in schema])
    client = Client('dmd')
    table = client.get_or_create_table('form_dose', schema)
    table.insert_rows_from_csv(name)
