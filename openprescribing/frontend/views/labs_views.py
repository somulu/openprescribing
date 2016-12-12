import base64
import datetime
from django.shortcuts import render
import pandas as pd
import matplotlib
from StringIO import StringIO


def data_for_equivalents(request):
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    today = "2016-09-01"
    code = request.GET.get('q', '')
    url = 'http://staging.openprescribing.net/api/1.0/bnf_code/data_for_equivalents'

    df = pd.read_json(
        url + '?q=%s&date=%s&format=json' % (code, today),
        orient='records')
    df['ppq'] = df['actual_cost'] / df['quantity']
    fig, ax = plt.subplots()
    plt.figure(figsize=(14, 8))
    generic_name = ''
    if len(df) > 0:
        for name in reversed(df.groupby('presentation_name').groups.keys()):
            current = df[df.presentation_name == name]
            if current.iloc[0].presentation_code[9:11] == 'AA':
                generic_name = current.loc[0].presentation_name
                continue
            plt.hist(current['ppq'],
                     label=name,
                     histtype='stepfilled',
                     edgecolor='none')
            plt.legend(loc='upper right')
        plt.title("ppq for various brands of %s" % generic_name)
        canvas = plt.gca().figure.canvas
        canvas.draw()
        ob = StringIO()
        canvas.print_png(ob)
        ob.seek(0)
        imgdata = base64.b64encode(ob.read())
    else:
        imgdata = ""
        generic_name = "unknown"
    context = {
        'imgdata': imgdata,
        'generic_name': generic_name
    }
    return render(request, 'plot_brands.html', context)
