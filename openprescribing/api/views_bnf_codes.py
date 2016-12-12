import simplejson
import datetime
import re
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.exceptions import APIException
from django.db.models import Q
from django.db import connection
import view_utils as utils
from frontend.models import Chemical, Section, Product, Presentation
from frontend.models import ImportLog
from frontend.models import Prescription


class NotValid(APIException):
    status_code = 400
    default_detail = 'The code you provided is not valid'


@api_view(['GET'])
def bnf_codes(request, format=None):
    codes = utils.param_to_list(request.query_params.get('q', []))
    codes = [c.upper() for c in codes]
    is_exact = request.GET.get('exact', None)
    is_exact = (is_exact == 'true')

    querysets = _get_matching_products(codes, is_exact)
    data = _convert_querysets(querysets)
    return Response(data)


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


@api_view(['GET'])
def data_for_equivalents(request, format=None):
    code = request.query_params.get('q', '')
    date = request.query_params.get('date', None)
    if date:
        try:
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise NotValid("%s is not a valid date" % date)
    else:
        date = ImportLog.objects.latest_in_category('prescribing').current_at
    if not re.match(r'[A-Z0-9]{15}', code):
        raise NotValid("%s is not a valid code" % code)
    pattern = "%s____%s" % (code[:9], code[13:15])
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT presentation_code, presentation_name, "
            "quantity, actual_cost "
            "FROM frontend_prescription "
            "WHERE processing_date = %s "
            "AND presentation_code LIKE %s "
            "ORDER BY presentation_code, quantity DESC", [date, pattern])
        data = dictfetchall(cursor)
    return Response(data)


def _convert_querysets(querysets):
    results = []
    for queryset in querysets:
        for qu_item in queryset:
            item = {
                'type': qu_item.type
            }
            if item['type'] == 0:
                item['id'] = qu_item.number_str
                item['name'] = qu_item.name
                levels = item['id'].split('.')
                if len(levels) > 2:
                    item['type'] = 'BNF paragraph'
                elif len(levels) == 2:
                    item['type'] = 'BNF section'
                else:
                    item['type'] = 'BNF chapter'
            elif item['type'] == 1:
                item['id'] = qu_item.bnf_code
                item['name'] = qu_item.chem_name
                item['type'] = 'chemical'
                item['section'] = qu_item.bnf_section()
            elif item['type'] == 2:
                item['id'] = qu_item.bnf_code
                item['name'] = qu_item.name
                item['type'] = 'product'
                item['is_generic'] = qu_item.is_generic
            else:
                item['id'] = qu_item.bnf_code
                item['name'] = qu_item.name
                item['type'] = 'product format'
                item['is_generic'] = qu_item.is_generic
            results.append(item)
    return results


def _get_matching_products(codes, is_exact):
    querysets = []
    for code in codes:
        if is_exact:
            sections = Section.objects \
                              .filter(Q(number_str=code) |
                                      Q(name=code)) \
                              .extra(select={'type': 0}) \
                              .order_by('number_str')
            chemicals = Chemical.objects \
                                .filter(Q(bnf_code=code) |
                                        Q(chem_name=code)) \
                                .extra(select={'type': 1}) \
                                .order_by('chem_name')
            products = Product.objects \
                              .filter(Q(bnf_code=code) |
                                      Q(name=code)) \
                              .extra(select={'type': 2}) \
                              .order_by('name')
            presentations = Presentation.objects \
                                        .filter(Q(bnf_code=code) |
                                                Q(name=code)) \
                                        .extra(select={'type': 3}) \
                                        .order_by('name')
        else:
            sections = Section.objects.filter(Q(number_str__startswith=code) |
                                              Q(name__icontains=code)) \
                              .extra(select={'type': 0}) \
                              .order_by('number_str')
            chemicals = Chemical.objects.filter(Q(bnf_code__startswith=code) |
                                                Q(chem_name__icontains=code)) \
                                .extra(select={'type': 1}) \
                                .order_by('chem_name')
            products = Product.objects.filter(Q(bnf_code__startswith=code) |
                                              Q(name__icontains=code)) \
                              .extra(select={'type': 2}) \
                              .order_by('name')
            presentations = Presentation.objects \
                                        .filter(Q(bnf_code__startswith=code) |
                                                Q(name__icontains=code)) \
                                        .extra(select={'type': 3}) \
                                        .order_by('name')
        querysets.append(sections)
        querysets.append(chemicals)
        querysets.append(products)
        querysets.append(presentations)
    return querysets
