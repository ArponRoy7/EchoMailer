from django.contrib import admin
from .models import Recipient, EmailTemplate, EmailLog, EmailCredential, EmailSettings

@admin.register(Recipient)
class RecipientAdmin(admin.ModelAdmin):
    list_display = ['email', 'company', 'created_at']
    list_filter = ['created_at']
    search_fields = ['email', 'company']

@admin.register(EmailTemplate)
class EmailTemplateAdmin(admin.ModelAdmin):
    list_display = ['name', 'subject', 'created_at', 'updated_at']
    search_fields = ['name', 'subject']

@admin.register(EmailLog)
class EmailLogAdmin(admin.ModelAdmin):
    list_display = ['recipient', 'subject', 'status', 'has_attachments', 'attachment_count', 'sent_at', 'created_at']
    list_filter = ['status', 'has_attachments', 'created_at']
    search_fields = ['recipient__email', 'subject']
    readonly_fields = ['created_at', 'sent_at']

@admin.register(EmailCredential)
class EmailCredentialAdmin(admin.ModelAdmin):
    list_display = ['name', 'email_host_user', 'provider', 'is_active', 'created_at']
    list_filter = ['provider', 'is_active', 'created_at']
    search_fields = ['name', 'email_host_user', 'email_host']
    readonly_fields = ['created_at', 'updated_at']
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'provider', 'is_active')
        }),
        ('Email Configuration', {
            'fields': ('email_host_user', 'from_email', 'email_host', 'email_port')
        }),
        ('Security Settings', {
            'fields': ('email_use_tls', 'email_use_ssl', 'email_host_password'),
            'description': 'Password is stored encrypted in the database'
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(EmailSettings)
class EmailSettingsAdmin(admin.ModelAdmin):
    list_display = ['email_delay', 'batch_size', 'batch_delay', 'max_attachments', 'max_attachment_size', 'updated_at']
    readonly_fields = ['created_at', 'updated_at']
    fieldsets = (
        ('Sending Delays', {
            'fields': ('email_delay', 'batch_size', 'batch_delay'),
            'description': 'Configure delays between emails to avoid rate limiting'
        }),
        ('Attachment Limits', {
            'fields': ('max_attachments', 'max_attachment_size'),
            'description': 'Set limits for email attachments'
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def has_add_permission(self, request):
        # Only allow one settings instance
        return not EmailSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        # Don't allow deleting the settings
        return False