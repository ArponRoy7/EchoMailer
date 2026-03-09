# Email Credential Management Feature

## Overview

EchoMailer now supports managing email credentials through the web interface instead of using environment variables in the `.env` file. This feature provides a secure, user-friendly way to configure SMTP settings for sending emails.

## Key Features

✅ **Database-Stored Credentials**: Store multiple email configurations in the database
✅ **Encrypted Passwords**: All passwords are encrypted using Fernet symmetric encryption
✅ **Multi-Provider Support**: Pre-configured settings for Gmail, Outlook, Yahoo, and custom SMTP
✅ **Active/Inactive Toggle**: Switch between different credentials easily
✅ **Test Functionality**: Send test emails to verify credentials work correctly
✅ **User-Friendly Interface**: Modern, responsive UI with helpful guides
✅ **Backward Compatible**: Falls back to `.env` configuration if no active credential exists

## Architecture Changes

### New Model: `EmailCredential`

Located in `emails/models.py`, this model stores:
- Name and provider selection
- SMTP host, port, and security settings (TLS/SSL)
- Email address and encrypted password
- Active status (only one can be active at a time)
- Timestamps

### Encryption System

- Uses `cryptography.fernet` for symmetric encryption
- Encryption key stored in settings (`EMAIL_ENCRYPTION_KEY`)
- Passwords encrypted before storage, decrypted only during use
- Never logged or displayed in plaintext

### Updated Email Sending

The `send_bulk_emails()` function in `emails/utils.py` now:
1. Checks for an active credential in the database
2. Uses database credential if available
3. Falls back to `.env` settings if no active credential exists

## How to Use

### 1. Access Credential Management

Navigate to **Email Credentials** from the sidebar menu or visit:
```
http://localhost:8000/credentials/
```

### 2. Add Your First Credential

Click **"Add New Credential"** and fill in:

**For Gmail:**
- Name: "My Gmail Account" (or any descriptive name)
- Provider: Gmail
- Email: your-email@gmail.com
- Password: Your Gmail App Password (see below)
- From Email: (auto-filled)

**Important**: Enable "Set as the active configuration" to use this credential immediately.

### 3. Get Gmail App Password

1. Go to your [Google Account settings](https://myaccount.google.com/)
2. Navigate to **Security** → **2-Step Verification**
3. Scroll to **App passwords** and click it
4. Generate a new app password for "Mail"
5. Copy the 16-character password (e.g., `abcd efgh ijkl mnop`)
6. Use this password in the credential form

### 4. Test Your Credentials

After saving, click the **Test** button (paper plane icon) to send a test email to yourself. This verifies:
- SMTP settings are correct
- Password works
- Email can be sent successfully

### 5. Manage Multiple Credentials

You can store multiple credentials for:
- Different email accounts
- Different providers
- Backup configurations

**Only one credential can be active at a time**. Click the green checkmark button to activate a different credential.

## Configuration Files Changed

### 1. `requirements.txt`
Added:
```python
cryptography==42.0.5
```

Install with:
```bash
source env/bin/activate
pip install -r requirements.txt
```

### 2. `settings.py`
Added encryption key configuration:
```python
EMAIL_ENCRYPTION_KEY = config('EMAIL_ENCRYPTION_KEY', default='8xvt-R0qZ_KqJfHx7y9dL8N5wGzBkE3mC1_pUoYjLzI=')
```

For production, generate a secure key:
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

Add to your `.env` file:
```
EMAIL_ENCRYPTION_KEY=your-generated-key-here
```

### 3. Database Migration

A new migration file was created: `emails/migrations/0003_emailcredential.py`

Apply with:
```bash
python manage.py migrate
```

## URLs Added

| URL | View | Description |
|-----|------|-------------|
| `/credentials/` | `credential_list` | List all credentials |
| `/credentials/add/` | `add_credential` | Add new credential |
| `/credentials/<id>/edit/` | `edit_credential` | Edit existing credential |
| `/credentials/<id>/delete/` | `delete_credential` | Delete credential |
| `/credentials/<id>/activate/` | `activate_credential` | Set as active |
| `/credentials/<id>/test/` | `test_credential` | Send test email |

## Security Considerations

### ✅ Implemented Security Features

1. **Password Encryption**: All passwords encrypted with Fernet before storage
2. **Never Display Passwords**: Encrypted passwords never shown in UI
3. **Secure Forms**: Password fields use `PasswordInput` widget
4. **CSRF Protection**: All forms protected with Django CSRF tokens
5. **Validation**: TLS/SSL mutual exclusion validation
6. **Admin Access**: Credentials manageable through Django admin

### ⚠️ Production Recommendations

1. **Generate Unique Encryption Key**:
   ```python
   from cryptography.fernet import Fernet
   key = Fernet.generate_key()
   print(key.decode())
   ```
   Store in `.env` file (never commit to git)

2. **Use HTTPS**: Always use SSL/TLS in production
3. **Restrict Access**: Implement authentication for credential management
4. **Backup Keys**: Store encryption key securely (losing it = losing passwords)
5. **App Passwords**: Use app-specific passwords, not main account passwords
6. **2FA**: Enable two-factor authentication on email accounts

## Common SMTP Settings

### Gmail
```
Host: smtp.gmail.com
Port: 587 (TLS) or 465 (SSL)
Requires: App Password (2FA must be enabled)
```

### Outlook / Office 365
```
Host: smtp.office365.com
Port: 587 (TLS)
Requires: Regular password or App Password
```

### Yahoo Mail
```
Host: smtp.mail.yahoo.com
Port: 587 (TLS) or 465 (SSL)
Requires: App Password
```

### Custom SMTP
```
Host: your-smtp-server.com
Port: Usually 587 (TLS) or 465 (SSL) or 25
Varies by provider
```

## Troubleshooting

### Issue: "Failed to send test email: Authentication failed"

**Solution**:
- Verify email and password are correct
- For Gmail: Ensure you're using App Password, not regular password
- Check that 2FA is enabled on your email account

### Issue: "Connection timeout"

**Solution**:
- Check SMTP host and port are correct
- Verify TLS/SSL settings match your provider's requirements
- Check firewall isn't blocking outgoing SMTP connections

### Issue: "No active credential found"

**Solution**:
- Go to Email Credentials page
- Click the green checkmark button on a credential to activate it
- Or check the "Set as active" box when creating/editing

### Issue: Encryption errors

**Solution**:
- Verify `cryptography` package is installed: `pip list | grep cryptography`
- Ensure `EMAIL_ENCRYPTION_KEY` is set in settings.py
- Re-save credentials if encryption key was changed

## API Reference

### `get_email_connection()`

Returns SMTP connection using active database credential or falls back to settings.

```python
from emails.utils import get_email_connection

connection, from_email = get_email_connection()
# Use connection to send emails
```

### `EmailCredential.encrypt_password(raw_password)`

Encrypts and stores password.

```python
credential.encrypt_password("my-password")
credential.save()
```

### `EmailCredential.decrypt_password()`

Returns decrypted password for use.

```python
password = credential.decrypt_password()
# Use for SMTP authentication
```

## Backward Compatibility

The system maintains backward compatibility:

1. **No active credential**: Falls back to `.env` configuration
2. **No database credentials**: Uses `settings.py` EMAIL_* settings
3. **Existing code**: All existing email-sending code continues to work

This means you can:
- Gradually migrate from `.env` to database credentials
- Keep `.env` as a backup
- Use both systems simultaneously

## Files Modified/Created

### Created
- `emails/models.py` - Added `EmailCredential` model
- `emails/forms.py` - Added `EmailCredentialForm`
- `emails/views.py` - Added 6 credential management views
- `emails/templates/emails/credential_list.html` - List page
- `emails/templates/emails/credential_form.html` - Add/edit form
- `emails/migrations/0003_emailcredential.py` - Database migration
- `CREDENTIAL_MANAGEMENT_GUIDE.md` - This documentation

### Modified
- `emails/utils.py` - Added `get_email_connection()`, updated `send_bulk_emails()`
- `emails/urls.py` - Added 6 credential management URLs
- `emails/admin.py` - Registered `EmailCredential` in admin
- `emails/templates/emails/base.html` - Added navigation link
- `requirements.txt` - Added `cryptography==42.0.5`
- `email_sender/settings.py` - Added `EMAIL_ENCRYPTION_KEY`

## Next Steps

### Recommended Enhancements

1. **Authentication**: Add user authentication to protect credential management
2. **Role-Based Access**: Restrict credential management to admin users
3. **Audit Logging**: Log credential changes and usage
4. **Multiple Active Credentials**: Support different credentials per campaign
5. **Credential Testing**: Add more robust testing (SMTP connection check)
6. **Import/Export**: Allow credential backup/restore
7. **Email Analytics**: Track which credential sent which emails

### Integration with Existing Features

The credential system is now integrated with:
- ✅ Email composition and sending
- ✅ Bulk email operations
- ✅ Template-based emails
- ✅ Email logging system
- ✅ Django admin interface

## Support

For issues or questions:
1. Check this documentation
2. Review Django logs: `tail -f email_sender/debug.log`
3. Test with simple email first
4. Verify SMTP settings with your email provider

## License

This feature is part of the EchoMailer project and follows the same license.
