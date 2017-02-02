from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.exceptions import APIException

from frontend.models import PPQSaving
from django.forms.models import model_to_dict

class NotValid(APIException):
    status_code = 400
    default_detail = 'The code you provided is not valid'


@api_view(['GET'])
def price_per_dose(request, format=None):
    entity_code = request.query_params.get('entity_code', None)
    date = request.query_params.get('date')
    bnf_code = request.query_params.get('bnf_code', None)
    if not (entity_code or bnf_code):
        raise NotValid("You must supply an entity code or a bnf_code")

    query = {'date': date}
    filename = date
    if entity_code:
        filename += "-%s" % entity_code
        if len(entity_code) == 3:
            query['pct'] = entity_code
            query['practice__isnull'] = True
        else:
            query['practice'] = entity_code
    if bnf_code:
        filename += "-%s" % bnf_code
        query['bnf_code'] = bnf_code
    savings = []
    for x in PPQSaving.objects.filter(**query):
        d = model_to_dict(x)
        d['name'] = x.product_name
        d['flag_bioequivalence'] = getattr(
            x.product, 'is_non_bioequivalent', None)
        savings.append(d)
    response = Response(savings)
    if request.accepted_renderer.format == 'csv':
        filename = "%s-ppd.csv" % (filename, date)
        response['content-disposition'] = "attachment; filename=%s" % filename
    return response
