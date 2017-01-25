import matplotlib
import numpy as np
import pandas as pd
import seaborn as sns

# Use thread-safe graphical backend for matplotlib. See
# http://matplotlib.org/faq/howto_faq.html#matplotlib-in-a-web-application-server
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from django.http import HttpResponse
from django.shortcuts import render

from frontend.dmd_models import DMDProduct
from frontend.models import Presentation


def data_for_equivalents(request, code, date):
    generic_name = ''
    product = DMDProduct.objects.filter(bnf_code=code).first()
    bnf_presentation = Presentation.objects.get(pk=code)
    context = {
        'generic_name': generic_name,
        'product': product,
        'bnf_presentation': bnf_presentation
    }
    return render(request, 'plot_brands.html', context)


def image_for_equivalents(request, code, date):
    hide_generic = request.GET.get('hide_generic', False)
    url = request.build_absolute_uri(
        "/api/1.0/bnf_code/data_for_equivalents"
        "?q=%s&date=%s&format=json" % (code, date))
    df = pd.read_json(url, orient='records')
    plt.figure(figsize=(12, 8))
    if len(df) > 0:
        df['ppq'] = df['actual_cost'] / df['quantity']
        data = []
        hist, bin_edges = np.histogram(df['ppq'])
        ordered = df.groupby('presentation_name')['ppq'].aggregate({'mean_ppq': 'mean'}).sort_values('mean_ppq').index
        for name in ordered:
            current = df[df.presentation_name == name]
            if hide_generic and current.iloc[0].presentation_code[9:11] == 'AA':
                continue
            data.append(
                plt.hist(
                    current['ppq'],
                    bins=len(bin_edges),
                    range=(min(bin_edges),max(bin_edges)),
                    label=name,
                    histtype='stepfilled',
                    normed=True,
                    edgecolor='none'))

        plt.legend(bbox_to_anchor=(1,1), loc=2)
        plt.ylabel('probability density')
        plt.xlabel('price per quantity')
        if hide_generic:
            plt.title("Price per quantity for brands")
        else:
            plt.title("Price per quantity for brands and generics")
    response = HttpResponse(content_type="image/png")
    plt.savefig(response, format="png", dpi=75, bbox_inches='tight')
    return response
