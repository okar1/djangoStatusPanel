# -*- coding: utf-8 -*-
# import sys
# sys.path.append('..')
from . import models
from django.db.models import Q
import re

def getUsers():
	vs=models.Users.objects.order_by('login').values()
	return [{"id":v['id'], "name":v['login']} for v in vs]

def getBoxes(userId):
	b=models.Boxes.objects.filter(user__exact=userId)
	if b.count()>0:
		return list(b.order_by('name').values('id','name'))
	else:
		return []

#return array of new task if rule applied with errors  or queryset else
def _getNewTasks(box,rules):

	addQ=None
	removeQ=None

	for rule in rules:
		ruleText=rule.rule
		if len(ruleText)>=2:
			op=ruleText[0]
			
			#check if boxname processing need in ruletext
			# format {boxname,from,to}
			# ex {boxname,1,3} is replaced to 1-3 symbols of boxname
			# ex {boxname,,3} is replaced to 1-3 symbols of boxname
			# ex {boxname,2,} is replaced to 2- symbols of boxname
			# ex {boxname} is replaced to all symbols of boxname
			match=re.search('{boxname'+
								 ',?' +
								 '((?:[\d]|)*?)' + 
								 ',?' +
								 '((?:\d|)*?)' + 
								 '}',ruleText)
			if match and len(match.groups())==2:
				boxName=box.name
				# print(match.groups())
				sReplaceFrom=match.group(0)
				indexFrom=1 if match.group(1)=='' else int(match.group(1))
				indexTo=2*len(boxName) if match.group(2)=='' else int(match.group(2))
				sReplaceTo=boxName[indexFrom-1:indexTo-len(boxName)]
				if sReplaceTo!='':
					ruleText=ruleText.replace(sReplaceFrom,sReplaceTo)


			q=Q(name__iregex=ruleText[1:])
			if op=='+':
				if addQ==None:
					addQ=q	
				else:
					addQ=addQ|q
			elif op=='-':
				if removeQ==None:
					removeQ=q
				else:
					removeQ=removeQ|q
			else:
				pass

	tasks=models.Tasks.objects

	if addQ!=None:
		tasks=tasks.filter(addQ)
		if removeQ!=None:
			tasks=tasks.exclude(removeQ)
		return tasks
	else:
		return tasks.none()


def getTasks(boxId):

	box=models.Boxes.objects.filter(id__exact=boxId)
	if box.count()!=1:
		return []
	box=box[0]

	rules=box.rules.all()

	if rules.count()==0:
		return list(box.tasks.all().order_by('name').values())

	tasks=_getNewTasks(box,rules)

	#type of tasks is qset - processing changes
	oldTasks=box.tasks.all()

	addedTasks=list(tasks.exclude(id__in=oldTasks).order_by('name').values())
	removedTasks=list(oldTasks.exclude(id__in=tasks).order_by('name').values())
	notChangedTasks=list(tasks.filter(id__in=oldTasks).order_by('name').values())

	# print("add ",addedTasks)
	# print("remove ",removedTasks)
	# print("not change ",notChangedTasks)

	for item in addedTasks:
		item['style']='add'
	for item in removedTasks:
		item['style']='rem'
	for item in notChangedTasks:
		item['style']='none'

	res=addedTasks
	res.extend(removedTasks)
	res.extend(notChangedTasks)

	return res


def updateSingleBox(boxId):
	box=models.Boxes.objects.filter(id__exact=boxId)
	if box.count()!=1:
		return []
	box=box[0]

	rules=box.rules.all()

	if rules.count()==0:
		return list(box.tasks.all().order_by('name').values())

	tasks=_getNewTasks(box,rules)

	box.tasks.set(tasks)


def updateAllBoxes():
	for box in models.Boxes.objects.all():
		rules=box.rules.all()

		if rules.count()!=0:
			tasks=_getNewTasks(box,rules)
			box.tasks.set(tasks)


