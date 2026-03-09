# Changelog

All notable changes to EchoMailer will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.1.0] - 2025-10-31

### Added
- **Email Credential Management System**
  - New `EmailCredential` model for storing SMTP configurations
  - Web-based interface for managing email credentials
  - Support for multiple email providers (Gmail, Outlook, Yahoo, Custom)
  - Fernet symmetric encryption for password security
  - Active/inactive credential toggling
  - Test email functionality to verify credentials
  - Provider-specific presets with auto-fill
  - Six new views for credential CRUD operations
  - Two new templates (`credential_list.html`, `credential_form.html`)
  - Navigation link in sidebar for easy access
  - Django admin integration for credentials

- **Security Features**
  - Password encryption using `cryptography.fernet`
  - Configurable encryption key via settings
  - Secure password input fields (never display plaintext)
  - CSRF protection on all forms

- **Documentation**
  - `CREDENTIAL_MANAGEMENT_GUIDE.md` - Comprehensive feature guide
  - `FEATURE_SUMMARY.md` - Quick feature overview
  - `QUICK_START_CREDENTIALS.md` - 5-minute setup guide
  - `CHANGELOG.md` - This file
  - Updated main `README.md` with new feature section

### Changed
- **Email Sending Logic**
  - `send_bulk_emails()` now uses database credentials by default
  - Falls back to `.env` settings if no active credential exists
  - New `get_email_connection()` helper function
  - Improved error handling and logging

- **Settings Configuration**
  - Added `EMAIL_ENCRYPTION_KEY` setting
  - Updated email configuration comments
  - Added fallback mechanism documentation

- **Dependencies**
  - Added `cryptography==42.0.5` to requirements.txt
  - Installed in virtual environment

### Database
- **New Migration**: `0003_emailcredential.py`
  - Creates `emails_emailcredential` table
  - Stores encrypted SMTP configurations
  - Includes indexes for active status and provider

### Files Modified
```
emails/models.py          - Added EmailCredential model
emails/forms.py           - Added EmailCredentialForm
emails/views.py           - Added 6 credential management views
emails/utils.py           - Updated email sending to use DB credentials
emails/urls.py            - Added 6 credential management routes
emails/admin.py           - Registered EmailCredential model
emails/templates/emails/base.html - Added navigation link
requirements.txt          - Added cryptography package
email_sender/settings.py  - Added EMAIL_ENCRYPTION_KEY
README.md                 - Updated with new features
```

### Files Created
```
emails/templates/emails/credential_list.html
emails/templates/emails/credential_form.html
emails/migrations/0003_emailcredential.py
CREDENTIAL_MANAGEMENT_GUIDE.md
FEATURE_SUMMARY.md
QUICK_START_CREDENTIALS.md
CHANGELOG.md
```

### URLs Added
```
/credentials/                  - List all credentials
/credentials/add/              - Add new credential
/credentials/<id>/edit/        - Edit existing credential
/credentials/<id>/delete/      - Delete credential
/credentials/<id>/activate/    - Activate credential
/credentials/<id>/test/        - Send test email
```

### Backward Compatibility
- ✅ Maintains compatibility with `.env` configuration
- ✅ Existing email sending code works unchanged
- ✅ No breaking changes to existing features
- ✅ Graceful fallback when no DB credential exists

### Security Considerations
- Passwords encrypted at rest in database
- Decryption only happens during email sending
- Passwords never logged or displayed in UI
- CSRF tokens on all forms
- Validation prevents TLS and SSL being enabled simultaneously

---

## [1.0.0] - 2025-10-12

### Added
- Initial release of EchoMailer
- **Recipient Management**
  - Add individual recipients with email and company
  - Edit and delete recipients
  - Search functionality

- **Email Template System**
  - Create reusable email templates
  - Personalization variables: `{{email}}`, `{{company}}`
  - Template list view with search

- **Bulk Email Sending**
  - Send personalized emails to multiple recipients
  - Template-based or custom email composition
  - Immediate sending option

- **Email Logging**
  - Track all sent emails
  - Status tracking (pending, sent, failed)
  - Error message storage
  - Filter by status

- **CSV Import**
  - Bulk import recipients from CSV files
  - Format: `email,company`
  - Duplicate detection

- **User Interface**
  - Modern, responsive design with Bootstrap 5
  - Gradient stat cards on dashboard
  - Fixed sidebar navigation
  - Font Awesome icons
  - Google Fonts (Inter)

- **Django Admin Interface**
  - Full CRUD operations for all models
  - Search and filter capabilities

- **Core Features**
  - SQLite database
  - SMTP email backend support
  - Console backend for development
  - Django 5.2.7 framework
  - Python 3.12+ support

### Database Models
- `Recipient` - Email contacts with company info
- `EmailTemplate` - Reusable email templates
- `EmailLog` - Email sending history

### Configuration
- Environment variable support via `python-decouple`
- `.env` file for configuration
- `.gitignore` for security

---

## Future Releases

### Planned for v1.2.0
- [ ] User authentication system
- [ ] Role-based access control
- [ ] Email scheduling functionality
- [ ] Email attachments support

### Planned for v2.0.0
- [ ] Email analytics dashboard
- [ ] Open rate and click tracking
- [ ] Advanced segmentation
- [ ] API endpoints
- [ ] Webhook support
- [ ] Multiple active credentials

---

## Migration Guide

### From v1.0.0 to v1.1.0

**No breaking changes!** Simply:

1. **Update dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run migrations**:
   ```bash
   python manage.py migrate
   ```

3. **Optional: Add encryption key to `.env`**:
   ```bash
   # Generate key
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

   # Add to .env
   EMAIL_ENCRYPTION_KEY=your-generated-key
   ```

4. **Start using web-based credentials** (optional):
   - Visit `/credentials/`
   - Add your SMTP settings
   - Your existing `.env` settings remain as fallback

---

## Support

For questions or issues related to any version:
- Check the relevant documentation in the repository
- Create an issue on GitHub
- Review the migration guide above

---

## Versioning

EchoMailer follows [Semantic Versioning](https://semver.org/):
- **Major version** (X.0.0): Breaking changes
- **Minor version** (0.X.0): New features, backward compatible
- **Patch version** (0.0.X): Bug fixes, backward compatible
