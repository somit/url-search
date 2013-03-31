from django.conf.urls import patterns, url

import views

urlpatterns = patterns('',
    url(r'^$', views.index),
    url(r'^v0/urls/(\d+)$', views.urls),
    url(r'^v0/search/(\d+)$', views.search),
)
