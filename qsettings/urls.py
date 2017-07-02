# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.conf.urls import include, url
from .views import MainMenuView

from django.contrib import admin

from django.conf import settings
from django.conf.urls.static import static

# import cvUpdate.views,cvUpdate.config
# import rcaUpdate.views, rcaUpdate.config
# import vbUpdate.views, vbUpdate.config
# import gpolicyUpdate.views, gpolicyUpdate.config
import heartbeat.views, heartbeat.config
# import shared.tests, shared.config

#use verbose app names as main menu headers
mainMenu=[
		 # {"url":r'^cvUpdate/$',"view":cvUpdate.views.MainView,"name":cvUpdate.config.App.verbose_name},
		 # {"url":r'^rcaUpdate/$',"view":rcaUpdate.views.DialogFormView,"name":rcaUpdate.config.App.verbose_name},
		 # {"url":r'^vbUpdate/$',"view":vbUpdate.views.MainView,"name":vbUpdate.config.App.verbose_name},
		 {"url":r'^$',"view":heartbeat.views.MainView,"name":heartbeat.config.App.verbose_name},
		 # {"url":r'^gpolicyUpdate/$',"view":gpolicyUpdate.views.MainView,"name":gpolicyUpdate.config.App.verbose_name},

		 # {"url":r'^test/$',"view":shared.tests.TestBoxView,"name":"тестовый"},
		]

for item in mainMenu:
	#add class string to items
	item["class"]=item['view'].__module__+'.'+item['view'].__name__

# include main menu items
urlpatterns=([
	url(item['url'],item['view'].as_view(),name=item['class']) for item in mainMenu
	])


urlpatterns.extend ([
	#or if you want to redirect to a named view:
	#success_url=reverse('success-url')
	url('^', include('django.contrib.auth.urls')),
	url(r'^admin/', admin.site.urls),
	url(r'^$', MainMenuView.as_view(), name='mainForm'),
	])

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

import threading
from heartbeat.threadPoll import threadPoll
t = threading.Thread(target=threadPoll)
t.start()	
# threadPoll()

# ^login/$ [name='login']
# ^logout/$ [name='logout']
# ^password_change/$ [name='password_change']
# ^password_change/done/$ [name='password_change_done']
# ^password_reset/$ [name='password_reset']
# ^password_reset/done/$ [name='password_reset_done']
# ^reset/(?P<uidb64>[0-9A-Za-z_\-]+)/(?P<token>[0-9A-Za-z]{1,13}-[0-9A-Za-z]{1,20})/$ [name='password_reset_confirm']
# ^reset/done/$ [name='password_reset_complete']