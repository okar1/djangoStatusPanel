# -*- coding: utf-8 -*-
import sys
sys.path.append('..')
# from qsettings.rcaDataSource import query
from . import views

def query2viewList(q):
	res=[]
	i=0
	for line in q:
		res.append((str(i),line))
		i+=1
	return res	

def name2num(nameList):
	pass

def num2name(numList)	:
	pass

def pageGen(sessionKey):
	if not hasattr(pageGen,"pageIndex"):
		pageGen.pageIndex=0
		pageGen.selectDone=False        
	if pageGen.pageIndex<0:
		pageGen.pageIndex=0
	# if dialogGenerator.pageIndex>2:
		# dialogGenerator.pageIndex=2

	if type(sessionKey)!=str:
		return views.UnauthorizedDialogForm

	if pageGen.selectDone:
		return views.WeAreReadyToProcess

	index=pageGen.pageIndex
	if index==0:
		views.WelcomeDialogForm.headString="Preved "+sessionKey
		return views.WelcomeDialogForm
	elif index==1:
		return views.SelectRuleDetailDialogForm
	else:    
		return views.SelectRuleItemsDialogForm
	# BaseDialogForm.annotationString=str(dialogGenerator.pageIndex)
	
	# return "WaitDialogForm"
	# return "SelectRuleItemsDialogForm"
	# 

def getPageIndex():
	return pageGen.pageIndex

def incPageIndex(v):
	pageGen.pageIndex+=v

def getPageData(pageShift=0):
	if not hasattr(getPageData,"pageData"):
		getPageData.pageData={}
	i=str(pageGen.pageIndex+pageShift)
	if not i in getPageData.pageData.keys():
		return {}
	return getPageData.pageData[i]

def savePageData(value,pageShift=0):
	if not hasattr(getPageData,"pageData"):
		getPageData.pageData={}
	i=str(pageGen.pageIndex+pageShift)
	if not i in getPageData.pageData:
		getPageData.pageData[i]=value
	else:
		getPageData.pageData[i].update(value)
	
#********************************************
#********************************************
#********************************************
if __name__ == "__main__":
	# q=query()
	#for i in q:
	#	print(i)
	path="d:\\files\\"
	
