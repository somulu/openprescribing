# -*- coding: utf-8 -*-
# Generated by Django 1.9.1 on 2016-02-19 17:03
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('frontend', '0047_auto_20160215_1520'),
    ]

    operations = [
        migrations.AddField(
            model_name='practice',
            name='close_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='practice',
            name='open_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='practice',
            name='status_code',
            field=models.CharField(blank=True, choices=[(b'U', b'Unknown'), (b'A', b'Active'), (b'B', b'Retired'), (b'C', b'Closed'), (b'D', b'Dormant'), (b'P', b'Proposed')], max_length=1, null=True),
        ),
    ]