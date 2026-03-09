# EchoMailer - Email Automation System

A Django-based email automation system for managing recipients, creating email templates, and sending personalized bulk emails.

## Features

- 📧 **Recipient Management**: Store and manage email contacts with company information
- 📝 **Email Templates**: Create reusable email templates with personalization variables
- 🚀 **Bulk Email Sending**: Send personalized emails to multiple recipients
- 📊 **Email Logs**: Track sent, failed, and pending emails
- 📤 **CSV Import**: Bulk import recipients from CSV files
- 🔐 **Email Credential Management**: Configure SMTP settings through web interface
- 🔒 **Encrypted Storage**: Passwords stored securely with Fernet encryption
- 📎 **Email Attachments**: Attach multiple files to your emails (NEW!)
- ⏱️ **Configurable Delays**: Control sending speed to avoid rate limiting (NEW!)
- 🎨 **Modern UI**: Clean and responsive interface with Bootstrap

## Requirements

- Python 3.12+
- Django 5.2.7
- See `requirements.txt` for full list of dependencies

## Installation

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd EchoMailer
```

### 2. Create a virtual environment
```bash
python -m venv env
source env/bin/activate  # On Linux/Mac
# or
env\Scripts\activate  # On Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
Create a `.env` file in the `email_sender` directory:
```env
SECRET_KEY=your-secret-key-here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1

# Email Configuration (Fallback - optional if using credential management)
EMAIL_BACKEND=django.core.mail.backends.console.EmailBackend
# For production, use SMTP:
# EMAIL_BACKEND=django.core.mail.backends.smtp.EmailBackend
# EMAIL_HOST=smtp.gmail.com
# EMAIL_PORT=587
# EMAIL_USE_TLS=True
# EMAIL_HOST_USER=your-email@gmail.com
# EMAIL_HOST_PASSWORD=your-app-password
# DEFAULT_FROM_EMAIL=your-email@gmail.com

# Email Credential Encryption Key (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
EMAIL_ENCRYPTION_KEY=8xvt-R0qZ_KqJfHx7y9dL8N5wGzBkE3mC1_pUoYjLzI=
```

**Note**: You can now configure email credentials through the web interface instead of using `.env` file. See the [Email Credential Management](#email-credential-management-new) section below.

### 5. Run migrations
```bash
cd email_sender
python manage.py migrate
```

### 6. Create a superuser (optional)
```bash
python manage.py createsuperuser
```

### 7. Run the development server
```bash
python manage.py runserver
```

Visit `http://127.0.0.1:8000/` in your browser.

## Usage

### Managing Recipients

1. Navigate to **Recipients** from the dashboard
2. Click **Add Recipient** to add individual contacts
3. Or use **Import CSV** to bulk upload recipients

**CSV Format:**
```csv
email,company
john@example.com,Acme Corp
jane@example.com,Tech Solutions
```

### Creating Email Templates

1. Go to **Templates**
2. Click **Add Template**
3. Use personalization variables:
   - `{{email}}` - Recipient's email address
   - `{{company}}` - Recipient's company name

**Example Template:**
```
Subject: Hello from {{company}}!

Body:
Dear valued customer,

This email is being sent to {{email}}.
We appreciate your business with {{company}}.

Best regards,
The Team
```

### Sending Emails

1. Click **Compose New Email**
2. Select a template (optional) or write custom content
3. Choose recipients
4. Click **Send** to send immediately

### Viewing Email Logs

- Go to **Email Logs** to see all sent, failed, and pending emails
- Filter by status
- View detailed information including error messages

## Email Credential Management (NEW!)

EchoMailer now supports managing SMTP email credentials through a user-friendly web interface instead of manually editing `.env` files.

### Quick Start

1. **Navigate to Email Credentials**
   - Click **Email Credentials** in the sidebar
   - Or visit: `http://127.0.0.1:8000/credentials/`

2. **Add Your First Credential**
   - Click **"Add New Credential"**
   - Select your email provider (Gmail, Outlook, Yahoo, or Custom)
   - Fill in your email and password
   - Check **"Set as the active configuration"**
   - Click **Save**

3. **Test Your Credentials**
   - Click the paper plane icon (Test button)
   - A test email will be sent to verify everything works

### Getting Gmail App Password

1. Go to [Google Account Settings](https://myaccount.google.com/)
2. Navigate to **Security** → **2-Step Verification**
3. Scroll down to **App passwords**
4. Generate a new app password for "Mail"
5. Copy the 16-character password
6. Use it in the credential form

### Features

- 🔐 **Secure Storage**: Passwords encrypted with Fernet encryption
- 🔄 **Multiple Credentials**: Store and switch between different email accounts
- ✅ **Easy Testing**: Send test emails to verify configuration
- 🎯 **Provider Presets**: Auto-filled settings for Gmail, Outlook, and Yahoo
- 🔒 **Active/Inactive**: Only one credential active at a time
- 🔙 **Backward Compatible**: Falls back to `.env` if no active credential

### Supported Providers

| Provider | SMTP Host | Port | Security |
|----------|-----------|------|----------|
| Gmail | smtp.gmail.com | 587 | TLS |
| Outlook | smtp.office365.com | 587 | TLS |
| Yahoo | smtp.mail.yahoo.com | 587 | TLS |
| Custom | Your SMTP server | Custom | TLS/SSL |

### For More Details

See the comprehensive guide: [CREDENTIAL_MANAGEMENT_GUIDE.md](CREDENTIAL_MANAGEMENT_GUIDE.md)

## Project Structure

```
EchoMailer/
├── email_sender/          # Django project directory
│   ├── email_sender/      # Project settings
│   │   ├── settings.py
│   │   ├── urls.py
│   │   └── wsgi.py
│   ├── emails/            # Main application
│   │   ├── models.py      # Data models
│   │   ├── views.py       # View logic
│   │   ├── forms.py       # Form definitions
│   │   ├── urls.py        # URL routing
│   │   ├── utils.py       # Helper functions
│   │   ├── admin.py       # Admin interface
│   │   └── templates/     # HTML templates
│   ├── static/            # Static files (CSS, JS)
│   ├── manage.py          # Django management script
│   └── db.sqlite3         # SQLite database
├── env/                   # Virtual environment
├── .gitignore
├── requirements.txt
└── README.md
```

## Models

### Recipient
- `email` (EmailField, unique)
- `company` (CharField, optional)
- `created_at` (DateTimeField)

### EmailTemplate
- `name` (CharField)
- `subject` (CharField)
- `body` (TextField)
- `created_at` (DateTimeField)
- `updated_at` (DateTimeField)

### EmailLog
- `recipient` (ForeignKey to Recipient)
- `template` (ForeignKey to EmailTemplate)
- `subject` (CharField)
- `body` (TextField)
- `status` (CharField: pending, sent, failed)
- `error_message` (TextField)
- `sent_at` (DateTimeField)
- `created_at` (DateTimeField)

### EmailCredential (NEW!)
- `name` (CharField) - Descriptive name
- `provider` (CharField) - Gmail, Outlook, Yahoo, Custom
- `email_host` (CharField) - SMTP server
- `email_port` (IntegerField) - SMTP port
- `email_use_tls` (BooleanField) - Use TLS
- `email_use_ssl` (BooleanField) - Use SSL
- `email_host_user` (EmailField) - Email address
- `email_host_password` (TextField) - Encrypted password
- `from_email` (EmailField) - Sender email
- `is_active` (BooleanField) - Active status
- `created_at` (DateTimeField)
- `updated_at` (DateTimeField)

## Email Configuration

### Development (Console Backend)
By default, emails are printed to the console for testing.

### Production (SMTP)
Configure your `.env` file with SMTP settings for actual email delivery.

**Gmail Example:**
1. Enable 2-factor authentication
2. Generate an App Password
3. Use the App Password in your `.env` file

## Admin Interface

Access the Django admin at `http://127.0.0.1:8000/admin/` to manage:
- Recipients
- Email Templates
- Email Logs
- Email Credentials (NEW!)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is for academic purposes.

## Support

For issues or questions, please create an issue in the repository.

## Version History

### v1.2.0 (Current)
- ✅ **Email Attachments**: Attach multiple files (PDF, images, documents)
- ✅ **Configurable Delays**: Control sending speed between emails
- ✅ **Batch Sending**: Send in batches with configurable pauses
- ✅ **Attachment Limits**: Set maximum file count and size
- ✅ **Email Settings Page**: Web-based configuration for delays
- ✅ **Attachment Tracking**: Logs show which emails had attachments
- All features from v1.1.0 and v1.0.0

### v1.1.0
- ✅ **Email Credential Management**: Web-based SMTP configuration
- ✅ **Password Encryption**: Fernet symmetric encryption for security
- ✅ **Multi-Provider Support**: Gmail, Outlook, Yahoo, Custom SMTP
- ✅ **Credential Testing**: Send test emails to verify settings
- ✅ **Active/Inactive Toggle**: Easy credential switching
- All features from v1.0.0

### v1.0.0
- Initial release
- Recipient management (email + company only)
- Email template system
- Bulk email sending
- Email logging
- CSV import
- Edit and delete functionality

## Future Enhancements

- [ ] User authentication and authorization
- [ ] Email scheduling
- [ ] Email analytics (open rates, click rates)
- [ ] More personalization variables
- [ ] Different attachments per recipient
- [ ] Advanced filtering and segmentation
- [ ] Export functionality
- [ ] API endpoints for integrations
- [ ] Multiple active credentials per campaign
