# Generated by Django 2.0.13 on 2019-12-05 16:17

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('frontend', '0064_auto_20191127_1327'),
    ]

    operations = [
        migrations.AlterField(
            model_name='measure',
            name='analyse_url',
            field=models.CharField(max_length=5000, null=True),
        ),
    ]