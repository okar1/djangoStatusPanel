# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from . import controller
from shared.views import BoxForm,BoxFormView

class CwForm(BoxForm):
	headString="Обновление виджетов ChannelView"
	annotationString="Выберите пользователя из списка, затем щелкните по виджету для отображения списка задач"
	
class MainView(BoxFormView):
	form_class=CwForm

	def getGroups(self):
		return controller.getUsers()

	def getBoxesForGroup(self,userID):
		return controller.getBoxes(userID)

	def getRecordsForBox(self,boxID):
		return controller.getTasks(boxID)

	def applyBt(self,buttonId,selected):
		controller.updateSingleBox(selected)

	def applyAllBt(self,buttonId,selected):
		controller.updateAllBoxes()
		return "Обновление всех плиток завершено"

	buttons=[{"name":"Применить","onclick":applyBt},
			{"name":"Применить все","onclick":applyAllBt}]