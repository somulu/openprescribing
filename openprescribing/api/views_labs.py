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
    code = request.query_params.get('q')
    date = request.query_params.get('date')
    savings = []
    # We return the savings.
    # Somehow in this data we end up with codes like 0601060D0AAA0A0 which does not exist.
    for x in PPQSaving.objects.filter(pct=code, date=date):
        d = model_to_dict(x)
        d['name'] = x.product_name
        d['flag_bioequivalence'] = getattr(
            x.product, 'is_non_bioequivalent', None)
        savings.append(d)
    response = Response(savings)
    if request.accepted_renderer.format == 'csv':
        filename = "%s-%s-ppd.csv" % (code, date)
        response['content-disposition'] = "attachment; filename=%s" % filename
    return response
