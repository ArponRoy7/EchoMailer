# Quick Start: Email Credential Management

## 5-Minute Setup Guide

### Step 1: Start the Server (if not running)
```bash
cd email_sender
source ../env/bin/activate
python manage.py runserver
```

### Step 2: Access Credential Management
Open your browser and visit:
```
http://127.0.0.1:8000/credentials/
```

Or click **"Email Credentials"** in the sidebar navigation.

### Step 3: Get Your Gmail App Password

1. **Enable 2-Factor Authentication**
   - Go to: https://myaccount.google.com/security
   - Turn on 2-Step Verification if not already enabled

2. **Generate App Password**
   - Stay in Security settings
   - Scroll to "App passwords"
   - Select app: **Mail**
   - Select device: **Other (Custom name)**
   - Enter: "EchoMailer"
   - Click **Generate**

3. **Copy the Password**
   - You'll see a 16-character password like: `abcd efgh ijkl mnop`
   - Copy this password (no spaces needed)

### Step 4: Add Credential in EchoMailer

1. Click **"Add New Credential"** button
2. Fill in the form:
   ```
   Name: My Gmail Account
   Provider: Gmail (auto-fills SMTP settings)
   Email: your-email@gmail.com
   Password: [paste app password from Step 3]
   From Email: your-email@gmail.com (auto-filled)
   ```
3. Check: ✅ **"Set as the active configuration"**
4. Click **"Save Credential"**

### Step 5: Test Your Credential

1. You'll see your credential in the list
2. Click the **paper plane icon** (Test button)
3. Wait a few seconds
4. Check your email inbox
5. You should receive: "EchoMailer Test Email"

### Step 6: Start Sending!

Now you can:
- Go to **"Compose Email"**
- Select recipients
- Write your message
- Click **Send**

Your emails will be sent using the active credential!

---

## Troubleshooting

### "Authentication failed"
- ✅ Make sure you used **App Password**, not regular password
- ✅ Check 2FA is enabled on your Gmail account
- ✅ Verify email address is correct

### "Connection timeout"
- ✅ Check your internet connection
- ✅ Try changing Port to 465 and enable SSL instead of TLS
- ✅ Check if firewall is blocking port 587

### "No active credential found"
- ✅ Go to Email Credentials page
- ✅ Click the green checkmark on your credential to activate it

---

## Other Email Providers

### Outlook/Office 365
```
Provider: Outlook/Office 365
Host: smtp.office365.com
Port: 587
TLS: ✅ Enabled
Email: your-email@outlook.com
Password: Your Outlook password or App Password
```

### Yahoo Mail
```
Provider: Yahoo Mail
Host: smtp.mail.yahoo.com
Port: 587
TLS: ✅ Enabled
Email: your-email@yahoo.com
Password: Yahoo App Password (generate at: https://login.yahoo.com/account/security)
```

### Custom SMTP
```
Provider: Custom SMTP
Host: [Your SMTP server]
Port: 587 (TLS) or 465 (SSL)
TLS/SSL: Enable one
Email: your-email@domain.com
Password: Your SMTP password
```

---

## Key Features

| Feature | Description |
|---------|-------------|
| 🔐 Encryption | Passwords encrypted before storage |
| 🔄 Multiple | Store multiple email accounts |
| ✅ Testing | Verify credentials work |
| 🎯 Presets | Auto-fill for Gmail/Outlook/Yahoo |
| 🔒 Active | Only one credential active at a time |

---

## Quick Commands

### Generate Encryption Key (for production)
```python
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Check Credentials in Database
```bash
python manage.py shell
>>> from emails.models import EmailCredential
>>> EmailCredential.objects.all()
```

### Run Migrations (if needed)
```bash
python manage.py makemigrations
python manage.py migrate
```

---

## Security Best Practices

✅ **Always use App Passwords** (not main account password)
✅ **Enable 2FA** on email accounts
✅ **Generate unique encryption key** for production
✅ **Never commit** `.env` file to git
✅ **Use HTTPS** in production
✅ **Restrict admin access** with authentication

---

## Need More Help?

- 📖 Full Guide: [CREDENTIAL_MANAGEMENT_GUIDE.md](CREDENTIAL_MANAGEMENT_GUIDE.md)
- 📋 Feature Summary: [FEATURE_SUMMARY.md](FEATURE_SUMMARY.md)
- 📝 Main Docs: [README.md](README.md)

---

**That's it! You're ready to send emails through EchoMailer! 🚀**
