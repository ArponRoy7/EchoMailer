from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.db.models import Q, Count
from .models import Recipient, EmailTemplate, EmailLog, EmailCredential, EmailSettings
from .forms import RecipientForm, EmailTemplateForm, SendEmailForm, BulkRecipientForm, EmailCredentialForm, EmailSettingsForm
from .utils import send_bulk_emails, import_recipients_from_csv

def dashboard(request):
    total_recipients = Recipient.objects.count()
    total_templates = EmailTemplate.objects.count()
    total_sent = EmailLog.objects.filter(status='sent').count()
    total_failed = EmailLog.objects.filter(status='failed').count()
    
    recent_logs = EmailLog.objects.all()[:10]
    
    context = {
        'total_recipients': total_recipients,
        'total_templates': total_templates,
        'total_sent': total_sent,
        'total_failed': total_failed,
        'recent_logs': recent_logs,
    }
    return render(request, 'emails/dashboard.html', context)

def recipient_list(request):
    recipients = Recipient.objects.all()
    search = request.GET.get('search', '')
    
    if search:
        recipients = recipients.filter(
            Q(email__icontains=search) |
            Q(company__icontains=search)
        )
    
    context = {'recipients': recipients, 'search': search}
    return render(request, 'emails/recipient_list.html', context)

def add_recipient(request):
    if request.method == 'POST':
        form = RecipientForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Recipient added successfully!')
            return redirect('recipient_list')
    else:
        form = RecipientForm()
    
    return render(request, 'emails/recipient_form.html', {'form': form})

def import_recipients(request):
    if request.method == 'POST':
        form = BulkRecipientForm(request.POST, request.FILES)
        if form.is_valid():
            results = import_recipients_from_csv(request.FILES['csv_file'])
            messages.success(request, f"Imported {results['success']} recipients. Failed: {results['failed']}")
            if results['errors']:
                for error in results['errors'][:5]:
                    messages.warning(request, error)
            return redirect('recipient_list')
    else:
        form = BulkRecipientForm()
    
    return render(request, 'emails/import_recipients.html', {'form': form})

def template_list(request):
    templates = EmailTemplate.objects.all()
    return render(request, 'emails/template_list.html', {'templates': templates})

def add_template(request):
    if request.method == 'POST':
        form = EmailTemplateForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Template created successfully!')
            return redirect('template_list')
    else:
        form = EmailTemplateForm()
    
    return render(request, 'emails/template_form.html', {'form': form})

def compose_email(request):
    if request.method == 'POST':
        form = SendEmailForm(request.POST, request.FILES)
        if form.is_valid():
            subject = form.cleaned_data['subject']
            body = form.cleaned_data['body']
            recipients = form.cleaned_data['recipients']
            template = form.cleaned_data.get('template')
            attachments = request.FILES.getlist('attachments')

            if form.cleaned_data['send_immediately']:
                results = send_bulk_emails(subject, body, recipients, template, attachments)

                # Success message with attachment info
                attachment_info = f" with {len(attachments)} attachment(s)" if attachments else ""
                messages.success(request, f"Sent {results['success']} emails{attachment_info}. Failed: {results['failed']}")

                if results['errors']:
                    for error in results['errors'][:5]:
                        messages.error(request, error)

            return redirect('dashboard')
    else:
        form = SendEmailForm()

    return render(request, 'emails/compose.html', {'form': form})

def email_logs(request):
    logs = EmailLog.objects.all()
    status_filter = request.GET.get('status', '')
    
    if status_filter:
        logs = logs.filter(status=status_filter)
    
    context = {'logs': logs, 'status_filter': status_filter}
    return render(request, 'emails/email_logs.html', context)

def delete_recipient(request, pk):
    recipient = get_object_or_404(Recipient, pk=pk)
    if request.method == 'POST':
        recipient.delete()
        messages.success(request, f'Recipient {recipient.email} deleted successfully!')
        return redirect('recipient_list')
    return redirect('recipient_list')

def edit_recipient(request, pk):
    recipient = get_object_or_404(Recipient, pk=pk)
    if request.method == 'POST':
        form = RecipientForm(request.POST, instance=recipient)
        if form.is_valid():
            form.save()
            messages.success(request, 'Recipient updated successfully!')
            return redirect('recipient_list')
    else:
        form = RecipientForm(instance=recipient)

    return render(request, 'emails/recipient_form.html', {'form': form, 'edit_mode': True})


def credential_list(request):
    """Display all email credentials"""
    credentials = EmailCredential.objects.all()
    active_credential = EmailCredential.objects.filter(is_active=True).first()

    context = {
        'credentials': credentials,
        'active_credential': active_credential,
    }
    return render(request, 'emails/credential_list.html', context)


def add_credential(request):
    """Add a new email credential"""
    if request.method == 'POST':
        form = EmailCredentialForm(request.POST)
        if form.is_valid():
            # Check if password was provided
            if not form.cleaned_data.get('password'):
                messages.error(request, 'Password is required when adding a new credential!')
                return render(request, 'emails/credential_form.html', {'form': form})

            form.save()
            messages.success(request, 'Email credential added successfully!')
            return redirect('credential_list')
    else:
        form = EmailCredentialForm()

    return render(request, 'emails/credential_form.html', {'form': form})


def edit_credential(request, pk):
    """Edit an existing email credential"""
    credential = get_object_or_404(EmailCredential, pk=pk)

    if request.method == 'POST':
        form = EmailCredentialForm(request.POST, instance=credential)
        if form.is_valid():
            form.save()
            messages.success(request, 'Email credential updated successfully!')
            return redirect('credential_list')
    else:
        form = EmailCredentialForm(instance=credential)

    context = {
        'form': form,
        'edit_mode': True,
        'credential': credential,
    }
    return render(request, 'emails/credential_form.html', context)


def delete_credential(request, pk):
    """Delete an email credential"""
    credential = get_object_or_404(EmailCredential, pk=pk)

    if request.method == 'POST':
        credential_name = credential.name
        credential.delete()
        messages.success(request, f'Email credential "{credential_name}" deleted successfully!')
        return redirect('credential_list')

    return redirect('credential_list')


def activate_credential(request, pk):
    """Set a credential as active"""
    credential = get_object_or_404(EmailCredential, pk=pk)

    if request.method == 'POST':
        # Deactivate all other credentials
        EmailCredential.objects.filter(is_active=True).update(is_active=False)
        # Activate this one
        credential.is_active = True
        credential.save()
        messages.success(request, f'Email credential "{credential.name}" is now active!')

    return redirect('credential_list')


def test_credential(request, pk):
    """Test an email credential by sending a test email"""
    credential = get_object_or_404(EmailCredential, pk=pk)

    if request.method == 'POST':
        try:
            from django.core.mail import EmailMessage
            from django.core.mail import get_connection

            # Create a connection with this credential
            connection = get_connection(
                backend='django.core.mail.backends.smtp.EmailBackend',
                host=credential.email_host,
                port=credential.email_port,
                username=credential.email_host_user,
                password=credential.decrypt_password(),
                use_tls=credential.email_use_tls,
                use_ssl=credential.email_use_ssl,
            )

            # Send test email
            email = EmailMessage(
                subject='EchoMailer Test Email',
                body='This is a test email from EchoMailer to verify your email credentials are working correctly.',
                from_email=credential.from_email,
                to=[credential.email_host_user],
                connection=connection,
            )
            email.send()

            messages.success(request, f'Test email sent successfully to {credential.email_host_user}!')
        except Exception as e:
            messages.error(request, f'Failed to send test email: {str(e)}')

    return redirect('credential_list')


def email_settings(request):
    """View and update email sending settings"""
    settings = EmailSettings.get_settings()

    if request.method == 'POST':
        form = EmailSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, 'Email settings updated successfully!')
            return redirect('email_settings')
    else:
        form = EmailSettingsForm(instance=settings)

    context = {
        'form': form,
        'settings': settings,
    }
    return render(request, 'emails/email_settings.html', context)