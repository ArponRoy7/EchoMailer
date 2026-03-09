from django.db import models
from django.contrib.auth.models import User
from cryptography.fernet import Fernet
from django.conf import settings
import base64

class Recipient(models.Model):
    email = models.EmailField(unique=True)
    company = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['email']
    
    def __str__(self):
        return f"{self.email} ({self.company})" if self.company else self.email

class EmailTemplate(models.Model):
    name = models.CharField(max_length=200)
    subject = models.CharField(max_length=300)
    body = models.TextField(help_text="Use {{email}}, {{company}} for personalization")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.name

class EmailLog(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('sent', 'Sent'),
        ('failed', 'Failed'),
    ]

    recipient = models.ForeignKey(Recipient, on_delete=models.CASCADE)
    template = models.ForeignKey(EmailTemplate, on_delete=models.SET_NULL, null=True)
    subject = models.CharField(max_length=300)
    body = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True)
    has_attachments = models.BooleanField(default=False, help_text="Whether this email had attachments")
    attachment_count = models.IntegerField(default=0, help_text="Number of attachments sent")
    sent_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.recipient.email} - {self.status}"


class EmailCredential(models.Model):
    """
    Stores SMTP email credentials with encryption.
    Only one active credential set should exist at a time.
    """
    PROVIDER_CHOICES = [
        ('gmail', 'Gmail'),
        ('outlook', 'Outlook/Office 365'),
        ('yahoo', 'Yahoo Mail'),
        ('custom', 'Custom SMTP'),
    ]

    name = models.CharField(max_length=200, help_text="Descriptive name for this credential set")
    provider = models.CharField(max_length=50, choices=PROVIDER_CHOICES, default='gmail')
    email_host = models.CharField(max_length=200, default='smtp.gmail.com')
    email_port = models.IntegerField(default=587)
    email_use_tls = models.BooleanField(default=True)
    email_use_ssl = models.BooleanField(default=False)
    email_host_user = models.EmailField(help_text="Your email address")
    email_host_password = models.TextField(help_text="Encrypted password/app password")
    from_email = models.EmailField(help_text="Default sender email (usually same as host user)")
    is_active = models.BooleanField(default=True, help_text="Use this credential for sending emails")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-is_active', '-created_at']

    def __str__(self):
        return f"{self.name} ({self.email_host_user})"

    def save(self, *args, **kwargs):
        # Ensure only one credential is active at a time
        if self.is_active:
            EmailCredential.objects.filter(is_active=True).exclude(pk=self.pk).update(is_active=False)
        super().save(*args, **kwargs)

    def encrypt_password(self, raw_password):
        """Encrypt password using Fernet symmetric encryption"""
        try:
            # Get encryption key from settings or generate one
            key = settings.EMAIL_ENCRYPTION_KEY.encode() if hasattr(settings, 'EMAIL_ENCRYPTION_KEY') else Fernet.generate_key()
            fernet = Fernet(key)
            encrypted = fernet.encrypt(raw_password.encode())
            self.email_host_password = base64.b64encode(encrypted).decode()
        except Exception as e:
            # Fallback: store as-is if encryption fails (not recommended for production)
            self.email_host_password = raw_password

    def decrypt_password(self):
        """Decrypt password for use in email sending"""
        try:
            if hasattr(settings, 'EMAIL_ENCRYPTION_KEY'):
                key = settings.EMAIL_ENCRYPTION_KEY.encode()
                fernet = Fernet(key)
                encrypted = base64.b64decode(self.email_host_password.encode())
                return fernet.decrypt(encrypted).decode()
            else:
                # Fallback: return as-is if no encryption key
                return self.email_host_password
        except Exception:
            # If decryption fails, assume it's plaintext (for backward compatibility)
            return self.email_host_password


class EmailSettings(models.Model):
    """
    Global email sending settings - Singleton model.
    Only one instance should exist.
    """
    email_delay = models.FloatField(
        default=0.0,
        help_text="Delay in seconds between sending each email (0 = no delay, max 60 seconds)"
    )
    batch_size = models.IntegerField(
        default=0,
        help_text="Number of emails to send before taking a longer pause (0 = no batching)"
    )
    batch_delay = models.FloatField(
        default=0.0,
        help_text="Delay in seconds after sending a batch (0 = no delay)"
    )
    max_attachments = models.IntegerField(
        default=5,
        help_text="Maximum number of attachments per email"
    )
    max_attachment_size = models.IntegerField(
        default=10,
        help_text="Maximum size per attachment in MB"
    )
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Email Settings"
        verbose_name_plural = "Email Settings"

    def __str__(self):
        return f"Email Settings (Delay: {self.email_delay}s)"

    def save(self, *args, **kwargs):
        # Ensure only one instance exists (Singleton pattern)
        if not self.pk and EmailSettings.objects.exists():
            # If trying to create a new instance when one exists, update the existing one
            existing = EmailSettings.objects.first()
            self.pk = existing.pk

        # Validate delays
        if self.email_delay < 0:
            self.email_delay = 0
        if self.email_delay > 60:
            self.email_delay = 60

        if self.batch_delay < 0:
            self.batch_delay = 0

        super().save(*args, **kwargs)

    @classmethod
    def get_settings(cls):
        """Get or create the singleton settings instance"""
        settings, created = cls.objects.get_or_create(pk=1)
        return settings