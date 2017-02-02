import matplotlib
import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from django.http import HttpResponse
from django.shortcuts import render
from django.shortcuts import get_object_or_404

from frontend.dmd_models import DMDProduct
from frontend.models import Presentation
from frontend.models import PCT
from frontend.models import PPQSaving


def price_per_dose(request, code):
    date = '2016-09-01'
    ccg = get_object_or_404(PCT, code=code)
    savings = PPQSaving.objects.filter(pct=code, date=date)
    context = {
        'ccg': ccg,
        'savings': savings,
        'date': date
    }
    return render(request, 'price_per_dose.html', context)


def data_for_equivalents(request, code, date):
    generic_name = bnf_presentation = ''
    product = DMDProduct.objects.filter(bnf_code=code).first()
    if not product:
        bnf_presentation = Presentation.objects.get(pk=code)

    context = {
        'generic_name': generic_name,
        'product': product,
        'bnf_presentation': bnf_presentation,
        'date': date
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
        max_labels = 22
        df['ppq'] = df['actual_cost'] / df['quantity']
        df = df[df.ppq <= df.ppq.quantile(0.99)]
        data = []
        logscale = request.GET.get('logscale', False)
        hist, bin_edges = np.histogram(df['ppq'])
        ordered = df.groupby(
            'presentation_name')['ppq'].aggregate(
                {'mean_ppq': 'mean'}).sort_values(
                    'mean_ppq').index
        sns.set_palette("Paired", min(max_labels, len(ordered)))
        for name in ordered:
            current = df[df.presentation_name == name]
            if hide_generic and current.iloc[0].presentation_code[9:11] == 'AA':
                continue
            data.append(current.ppq)

        plt.hist(
            data,
            alpha=0.7,
            stacked=True,
            bins=len(bin_edges) * 2,
            edgecolor='none',
            range=(min(bin_edges),max(bin_edges)))
        title =  "Brands"
        if len(ordered) > max_labels:
            title += " (%s not shown)" % (len(ordered) - max_labels)
            ordered = list(ordered[:max_labels])
        plt.legend(ordered, bbox_to_anchor=(1,1), loc=2, title=title)
        if logscale:
            plt.yscale('log', nonposy='clip')
        plt.ylabel('count')
        plt.xlabel('price per quantity')
        if hide_generic:
            plt.title("Price per quantity for brands")
        else:
            plt.title("Price per quantity for brands and generics")
    response = HttpResponse(content_type="image/png")
    plt.savefig(response, format="png", dpi=75, bbox_inches='tight')
    return response
