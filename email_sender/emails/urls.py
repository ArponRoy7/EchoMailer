from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('recipients/', views.recipient_list, name='recipient_list'),
    path('recipients/add/', views.add_recipient, name='add_recipient'),
    path('recipients/<int:pk>/edit/', views.edit_recipient, name='edit_recipient'),
    path('recipients/<int:pk>/delete/', views.delete_recipient, name='delete_recipient'),
    path('recipients/import/', views.import_recipients, name='import_recipients'),
    path('templates/', views.template_list, name='template_list'),
    path('templates/add/', views.add_template, name='add_template'),
    path('compose/', views.compose_email, name='compose_email'),
    path('logs/', views.email_logs, name='email_logs'),
    # Email credential management
    path('credentials/', views.credential_list, name='credential_list'),
    path('credentials/add/', views.add_credential, name='add_credential'),
    path('credentials/<int:pk>/edit/', views.edit_credential, name='edit_credential'),
    path('credentials/<int:pk>/delete/', views.delete_credential, name='delete_credential'),
    path('credentials/<int:pk>/activate/', views.activate_credential, name='activate_credential'),
    path('credentials/<int:pk>/test/', views.test_credential, name='test_credential'),
    # Email settings
    path('settings/', views.email_settings, name='email_settings'),
]