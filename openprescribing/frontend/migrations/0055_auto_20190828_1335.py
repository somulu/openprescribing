# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2019-08-28 12:35
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("frontend", "0054_add_pcn_to_measurevalue")]

    operations = [
        migrations.RunSQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx__vw__medians_for_tariff__vpid ON vw__medians_for_tariff (vpid);"
        )
    ]