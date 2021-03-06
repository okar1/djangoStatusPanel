# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2017-05-23 09:20
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Bridges',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название')),
                ('ip', models.GenericIPAddressField(protocol='IPv4', verbose_name='Ip')),
            ],
            options={
                'verbose_name_plural': 'видеобриджи',
                'verbose_name': 'видеобридж',
            },
        ),
        migrations.CreateModel(
            name='Channels',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название')),
                ('ip', models.GenericIPAddressField(protocol='IPv4', verbose_name='Ip')),
                ('port', models.IntegerField(verbose_name='Порт')),
                ('index', models.IntegerField(verbose_name='Индекс')),
                ('bridge', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='vbupdate.Bridges', verbose_name='Видеобридж')),
            ],
            options={
                'verbose_name_plural': 'каналы',
                'verbose_name': 'канал',
            },
        ),
        migrations.CreateModel(
            name='ControlBlocks',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название')),
                ('ip', models.GenericIPAddressField(protocol='IPv4', verbose_name='Ip')),
            ],
            options={
                'verbose_name_plural': 'блоки контроля',
                'verbose_name': 'блок контроля',
            },
        ),
        migrations.CreateModel(
            name='Options',
            fields=[
                ('name', models.CharField(max_length=255, primary_key=True, serialize=False, verbose_name='Имя')),
                ('value', models.TextField(max_length=255, verbose_name='Значение')),
            ],
            options={
                'verbose_name_plural': 'настройки',
                'verbose_name': 'настройка',
            },
        ),
        migrations.AddField(
            model_name='bridges',
            name='controlblock',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='vbupdate.ControlBlocks', verbose_name='блок контроля'),
        ),
    ]
