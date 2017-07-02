# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations

def myCustom(apps, schema_editor):
	# We can't import the Person model directly as it may be a newer
	# version than this migration expects. We use the historical version.
	Options = apps.get_model('vbupdate', 'Options')
	ControlBlocks = apps.get_model('vbupdate', 'ControlBlocks')
	Bridges = apps.get_model('vbupdate', 'Bridges')
	Options.objects.bulk_create([
		Options(name='channelTableOID', value='.1.3.6.1.4.1.24562.510'),
		Options(name='channelIndexOID', value='.1.3.6.1.4.1.24562.510.1.1'),
		Options(name='channelNameOID', value='.1.3.6.1.4.1.24562.510.1.2'),
		Options(name='channelIpOID', value='.1.3.6.1.4.1.24562.510.1.3'),
		Options(name='channelPortOID', value='.1.3.6.1.4.1.24562.510.1.4'),
		Options(name='snmpReadCommunity', value='teoco'),
		Options(name='taskFile', value="""/opt/qligent/vision/Conf/XMLCFG/config.xml"""),
		Options(name='taskFileBackup', value="""/opt/qligent/vision/Conf/XMLCFG/config.backup.xml"""),
		Options(name='blockedChannelNames', value='["TST","\.SW\.","TEST","\(","\)",".{30,}"]'),
		Options(name='nameReplaceRules', value="{'+':'_'}"),
		Options(name='startReplaceMarker', value="""<!-- Начало автозамены задач VB -->\n"""),
		Options(name='endReplaceMarker', value="""\n<!-- Конец автозамены задач VB -->"""),
		Options(name='sshUser', value="qladmin"),
		])

	bks=[   
		ControlBlocks(name='01 Киров', ip='10.68.115.178'),
		ControlBlocks(name='02 МарийЭл', ip='10.68.115.179'),
		ControlBlocks(name='03 Саранск', ip='10.68.115.180'),
		ControlBlocks(name='04 Нижний Новгород', ip='10.68.115.181'),
		ControlBlocks(name='05 Оренбург', ip='10.68.115.182'),
		ControlBlocks(name='06 Пенза', ip='10.68.115.183'),
		ControlBlocks(name='07 Самара', ip='10.68.115.184'),
		ControlBlocks(name='08 Саратов', ip='10.68.115.185'),
		ControlBlocks(name='09 Ижевск', ip='10.68.115.186'),
		ControlBlocks(name='10 Ульяновск', ip='10.68.115.187'),
		ControlBlocks(name='11 Чебоксары', ip='10.68.115.188'),
		ControlBlocks(name='12 Казань', ip='10.68.115.189'),
		]

	ControlBlocks.objects.bulk_create(bks)

	Bridges.objects.bulk_create([
		Bridges(name='VLG-KIRV-RF', ip='10.65.66.241', controlblock=bks[1-1]),
		Bridges(name='VLG-JOLA-RF', ip='10.66.65.1', controlblock=bks[2-1]),
		Bridges(name='VLG-SRNK-RF', ip='10.67.24.225', controlblock=bks[3-1]),
		Bridges(name='VLG-NNOV2-MRF', ip='10.82.3.254', controlblock=bks[4-1]),
		Bridges(name='VLG-NNOV-MRF', ip='10.68.15.145', controlblock=bks[4-1]),
		Bridges(name='VLG-NNOV-RF', ip='10.68.15.146', controlblock=bks[4-1]),
		Bridges(name='VLG-ORBG-RF', ip='10.69.64.10', controlblock=bks[5-1]),
		Bridges(name='VLG-PENZ-RF', ip='10.70.31.233', controlblock=bks[6-1]),
		Bridges(name='VLG-SAMR-RF', ip='10.71.249.9', controlblock=bks[7-1]),
		Bridges(name='VLG-SRTV-RF', ip='10.64.41.33', controlblock=bks[8-1]),
		Bridges(name='VLG-IZVK-RF', ip='10.75.8.1', controlblock=bks[9-1]),
		Bridges(name='VLG-ULSK-RF', ip='10.73.0.201', controlblock=bks[10-1]),
		Bridges(name='VLG-CBRK-RF', ip='10.76.63.177', controlblock=bks[11-1]),
		Bridges(name='VLG-KZAN-RF', ip='10.72.8.224', controlblock=bks[12-1]),
		])

class Migration(migrations.Migration):

	dependencies = [
		('vbupdate', '0001_initial'),
	]

	operations = [
		migrations.RunPython(myCustom),
	]