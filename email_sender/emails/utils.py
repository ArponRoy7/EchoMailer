import csv
import time
from django.core.mail import send_mail, EmailMessage, get_connection
from django.template import Template, Context
from django.conf import settings
from .models import Recipient, EmailLog, EmailCredential, EmailSettings
import logging

logger = logging.getLogger(__name__)


def get_email_connection():
    """
    Get email connection using active credential from database.
    Falls back to settings if no active credential exists.
    """
    try:
        active_credential = EmailCredential.objects.filter(is_active=True).first()

        if active_credential:
            # Use database credential
            connection = get_connection(
                backend='django.core.mail.backends.smtp.EmailBackend',
                host=active_credential.email_host,
                port=active_credential.email_port,
                username=active_credential.email_host_user,
                password=active_credential.decrypt_password(),
                use_tls=active_credential.email_use_tls,
                use_ssl=active_credential.email_use_ssl,
            )
            return connection, active_credential.from_email
        else:
            # Fall back to settings.py configuration
            logger.warning("No active email credential found. Using settings.py configuration.")
            return None, settings.DEFAULT_FROM_EMAIL

    except Exception as e:
        logger.error(f"Error getting email connection: {str(e)}")
        return None, settings.DEFAULT_FROM_EMAIL

def personalize_message(template_text, recipient):
    """Personalize email message with recipient data"""
    template = Template(template_text)
    context = Context({
        'email': recipient.email,
        'company': recipient.company,
    })
    return template.render(context)

def send_bulk_emails(subject, body, recipients, template=None, attachments=None):
    """
    Send personalized emails to multiple recipients using database credentials.
    Supports attachments and configurable delays between emails.

    Args:
        subject: Email subject line
        body: Email body text
        recipients: List of Recipient objects
        template: Optional EmailTemplate object
        attachments: List of file objects to attach

    Returns:
        Dictionary with 'success', 'failed', and 'errors' counts
    """
    results = {
        'success': 0,
        'failed': 0,
        'errors': []
    }

    # Get email connection and from_email
    connection, from_email = get_email_connection()

    # Get email settings for delays and batching
    email_settings = EmailSettings.get_settings()

    # Convert recipients to list to track index
    recipients_list = list(recipients)
    total_recipients = len(recipients_list)

    logger.info(f"Starting bulk email send to {total_recipients} recipients with {email_settings.email_delay}s delay")

    for index, recipient in enumerate(recipients_list, start=1):
        try:
            # Personalize subject and body
            personalized_subject = personalize_message(subject, recipient)
            personalized_body = personalize_message(body, recipient)

            # Create and send email message
            email = EmailMessage(
                subject=personalized_subject,
                body=personalized_body,
                from_email=from_email,
                to=[recipient.email],
                connection=connection,
            )

            # Attach files if provided
            attachment_count = 0
            if attachments:
                for attachment_file in attachments:
                    try:
                        # Read file content
                        attachment_file.seek(0)  # Reset file pointer
                        email.attach(
                            attachment_file.name,
                            attachment_file.read(),
                            attachment_file.content_type
                        )
                        attachment_count += 1
                        logger.debug(f"Attached {attachment_file.name} to email for {recipient.email}")
                    except Exception as attach_error:
                        logger.warning(f"Failed to attach {attachment_file.name}: {str(attach_error)}")

            # Send the email
            email.send(fail_silently=False)

            # Log success
            EmailLog.objects.create(
                recipient=recipient,
                template=template,
                subject=personalized_subject,
                body=personalized_body,
                status='sent',
                has_attachments=(attachment_count > 0),
                attachment_count=attachment_count
            )
            results['success'] += 1
            logger.info(f"Email sent successfully to {recipient.email} ({index}/{total_recipients})")

            # Apply delay between emails (except after the last one)
            if index < total_recipients:
                # Check if we need a batch delay
                if email_settings.batch_size > 0 and index % email_settings.batch_size == 0:
                    if email_settings.batch_delay > 0:
                        logger.info(f"Batch of {email_settings.batch_size} completed. Pausing for {email_settings.batch_delay}s...")
                        time.sleep(email_settings.batch_delay)
                elif email_settings.email_delay > 0:
                    # Regular delay between emails
                    time.sleep(email_settings.email_delay)

        except Exception as e:
            # Log failure
            EmailLog.objects.create(
                recipient=recipient,
                template=template,
                subject=subject,
                body=body,
                status='failed',
                error_message=str(e),
                has_attachments=(attachments is not None and len(attachments) > 0),
                attachment_count=0
            )
            results['failed'] += 1
            results['errors'].append(f"{recipient.email}: {str(e)}")
            logger.error(f"Failed to send email to {recipient.email}: {str(e)}")

            # Still apply delay even after failure to avoid overwhelming the server
            if index < total_recipients and email_settings.email_delay > 0:
                time.sleep(email_settings.email_delay)

    logger.info(f"Bulk email send completed. Success: {results['success']}, Failed: {results['failed']}")
    return results

def import_recipients_from_csv(csv_file):
    """Import recipients from CSV file"""
    results = {
        'success': 0,
        'failed': 0,
        'errors': []
    }
    
    try:
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)
        
        for row in reader:
            try:
                recipient, created = Recipient.objects.get_or_create(
                    email=row['email'],
                    defaults={
                        'company': row.get('company', ''),
                    }
                )
                if created:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{row['email']} already exists")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"Error importing {row.get('email', 'unknown')}: {str(e)}")
    
    except Exception as e:
        results['errors'].append(f"File processing error: {str(e)}")
    
    return results