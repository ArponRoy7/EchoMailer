from django import forms
from django.forms.widgets import Input
from .models import Recipient, EmailTemplate, EmailCredential, EmailSettings


class MultipleFileInput(Input):
    """
    Custom widget to handle multiple file uploads
    """
    input_type = 'file'
    template_name = 'django/forms/widgets/file.html'

    def __init__(self, attrs=None):
        if attrs is not None:
            attrs = attrs.copy()
            attrs['multiple'] = True
        else:
            attrs = {'multiple': True}
        super().__init__(attrs)

    def value_omitted_from_data(self, data, files, name):
        return name not in files

class RecipientForm(forms.ModelForm):
    class Meta:
        model = Recipient
        fields = ['email', 'company']
        widgets = {
            'email': forms.EmailInput(attrs={'class': 'form-control'}),
            'company': forms.TextInput(attrs={'class': 'form-control'}),
        }

class BulkRecipientForm(forms.Form):
    csv_file = forms.FileField(
        label='Upload CSV File',
        help_text='CSV format: email, company',
        widget=forms.FileInput(attrs={'class': 'form-control', 'accept': '.csv'})
    )

class EmailTemplateForm(forms.ModelForm):
    class Meta:
        model = EmailTemplate
        fields = ['name', 'subject', 'body']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'subject': forms.TextInput(attrs={'class': 'form-control'}),
            'body': forms.Textarea(attrs={'class': 'form-control', 'rows': 10}),
        }
        help_text = {
            'body': 'Use {{email}}, {{company}} for personalization'
        }

class SendEmailForm(forms.Form):
    template = forms.ModelChoiceField(
        queryset=EmailTemplate.objects.all(),
        widget=forms.Select(attrs={'class': 'form-control'}),
        required=False
    )
    subject = forms.CharField(
        max_length=300,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    body = forms.CharField(
        widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 10})
    )
    recipients = forms.ModelMultipleChoiceField(
        queryset=Recipient.objects.all(),
        widget=forms.CheckboxSelectMultiple,
        required=True
    )
    attachments = forms.FileField(
        widget=MultipleFileInput(attrs={'class': 'form-control'}),
        required=False,
        help_text='Select one or more files to attach (max 5 files, 10MB each)'
    )
    send_immediately = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )

    def clean_attachments(self):
        """Validate attachments"""
        files = self.files.getlist('attachments')

        if files:
            settings = EmailSettings.get_settings()

            # Check number of attachments
            if len(files) > settings.max_attachments:
                raise forms.ValidationError(
                    f'Maximum {settings.max_attachments} attachments allowed. You uploaded {len(files)}.'
                )

            # Check file sizes
            max_size = settings.max_attachment_size * 1024 * 1024  # Convert MB to bytes
            for file in files:
                if file.size > max_size:
                    raise forms.ValidationError(
                        f'File "{file.name}" is too large. Maximum size is {settings.max_attachment_size}MB.'
                    )

        return files


class EmailCredentialForm(forms.ModelForm):
    """Form for adding/editing email credentials"""

    # Add a password field that won't display the encrypted password
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        required=False,
        help_text='Enter your email password or app-specific password. Leave blank to keep existing password.',
        label='Email Password'
    )

    class Meta:
        model = EmailCredential
        fields = ['name', 'provider', 'email_host', 'email_port', 'email_use_tls',
                  'email_use_ssl', 'email_host_user', 'from_email', 'is_active']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g., My Gmail Account'}),
            'provider': forms.Select(attrs={'class': 'form-control', 'id': 'provider-select'}),
            'email_host': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'smtp.gmail.com'}),
            'email_port': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '587'}),
            'email_use_tls': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'email_use_ssl': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'email_host_user': forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'your-email@gmail.com'}),
            'from_email': forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'your-email@gmail.com'}),
            'is_active': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }
        help_texts = {
            'name': 'A descriptive name for this email configuration',
            'provider': 'Select your email provider or custom for other SMTP servers',
            'email_host': 'SMTP server address',
            'email_port': 'SMTP port (usually 587 for TLS, 465 for SSL)',
            'email_use_tls': 'Use TLS encryption (recommended for port 587)',
            'email_use_ssl': 'Use SSL encryption (recommended for port 465)',
            'email_host_user': 'Your email address for authentication',
            'from_email': 'Email address shown as sender (usually same as host user)',
            'is_active': 'Set as the active configuration for sending emails',
        }

    def clean(self):
        cleaned_data = super().clean()
        use_tls = cleaned_data.get('email_use_tls')
        use_ssl = cleaned_data.get('email_use_ssl')

        # Validate that TLS and SSL are not both enabled
        if use_tls and use_ssl:
            raise forms.ValidationError('Cannot enable both TLS and SSL. Please choose one.')

        # Auto-populate from_email if not provided
        if not cleaned_data.get('from_email'):
            cleaned_data['from_email'] = cleaned_data.get('email_host_user')

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=False)

        # Encrypt and save password if provided
        password = self.cleaned_data.get('password')
        if password:
            instance.encrypt_password(password)

        if commit:
            instance.save()
        return instance


class EmailSettingsForm(forms.ModelForm):
    """Form for configuring email sending settings"""

    class Meta:
        model = EmailSettings
        fields = ['email_delay', 'batch_size', 'batch_delay', 'max_attachments', 'max_attachment_size']
        widgets = {
            'email_delay': forms.NumberInput(attrs={
                'class': 'form-control',
                'step': '0.1',
                'min': '0',
                'max': '60',
                'placeholder': '0.0'
            }),
            'batch_size': forms.NumberInput(attrs={
                'class': 'form-control',
                'min': '0',
                'placeholder': '0 (no batching)'
            }),
            'batch_delay': forms.NumberInput(attrs={
                'class': 'form-control',
                'step': '0.1',
                'min': '0',
                'placeholder': '0.0'
            }),
            'max_attachments': forms.NumberInput(attrs={
                'class': 'form-control',
                'min': '1',
                'max': '10'
            }),
            'max_attachment_size': forms.NumberInput(attrs={
                'class': 'form-control',
                'min': '1',
                'max': '25'
            }),
        }
        help_texts = {
            'email_delay': 'Delay between each email in seconds (0-60). Helps avoid rate limiting.',
            'batch_size': 'Send emails in batches. Set to 0 to disable batching.',
            'batch_delay': 'Longer pause after sending a batch (in seconds).',
            'max_attachments': 'Maximum number of attachments allowed per email (1-10).',
            'max_attachment_size': 'Maximum size per attachment in MB (1-25).',
        }
        labels = {
            'email_delay': 'Delay Between Emails (seconds)',
            'batch_size': 'Batch Size',
            'batch_delay': 'Batch Delay (seconds)',
            'max_attachments': 'Max Attachments Per Email',
            'max_attachment_size': 'Max Attachment Size (MB)',
        }

    def clean_email_delay(self):
        delay = self.cleaned_data.get('email_delay')
        if delay < 0:
            raise forms.ValidationError('Delay cannot be negative.')
        if delay > 60:
            raise forms.ValidationError('Maximum delay is 60 seconds.')
        return delay

    def clean_batch_size(self):
        batch_size = self.cleaned_data.get('batch_size')
        if batch_size < 0:
            raise forms.ValidationError('Batch size cannot be negative.')
        return batch_size

    def clean_max_attachments(self):
        max_attachments = self.cleaned_data.get('max_attachments')
        if max_attachments < 1:
            raise forms.ValidationError('Must allow at least 1 attachment.')
        if max_attachments > 10:
            raise forms.ValidationError('Maximum 10 attachments allowed.')
        return max_attachments

    def clean_max_attachment_size(self):
        max_size = self.cleaned_data.get('max_attachment_size')
        if max_size < 1:
            raise forms.ValidationError('Minimum size is 1MB.')
        if max_size > 25:
            raise forms.ValidationError('Maximum size is 25MB.')
        return max_size