# Quick Setup Guide

## Prerequisites
- Python 3.12 or higher
- pip (Python package manager)
- Git (optional, for version control)

## Step-by-Step Setup

### 1. Navigate to Project Directory
```bash
cd "/media/nikhil/DATA/MTECH/ACADEMICS/MAIN PROJECT/EchoMailer"
```

### 2. Activate Virtual Environment
```bash
source env/bin/activate
```

### 3. Verify Installation
```bash
pip list
```
You should see:
- Django (5.2.7)
- django-crispy-forms (2.4)
- python-decouple (3.8)
- And other dependencies

### 4. Configure Environment (First Time Only)
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your settings (optional for development)
nano .env  # or use your preferred editor
```

### 5. Navigate to Django Project
```bash
cd email_sender
```

### 6. Run Migrations (If Not Already Done)
```bash
python manage.py migrate
```

### 7. Create Superuser (Optional, for Admin Access)
```bash
python manage.py createsuperuser
```

### 8. Start Development Server
```bash
python manage.py runserver
```

### 9. Access the Application
Open your browser and go to:
- **Application**: http://127.0.0.1:8000/
- **Admin Panel**: http://127.0.0.1:8000/admin/

## Common Commands

### Start Server
```bash
cd email_sender
python manage.py runserver
```

### Stop Server
Press `CTRL + C` in the terminal

### Create New Admin User
```bash
python manage.py createsuperuser
```

### Make Database Changes
```bash
# After modifying models.py
python manage.py makemigrations
python manage.py migrate
```

### Check for Issues
```bash
python manage.py check
```

### Collect Static Files (for production)
```bash
python manage.py collectstatic
```

## Installing on a New System

If you're setting up on a new computer:

```bash
# 1. Clone or copy the project
cd "/path/to/EchoMailer"

# 2. Create virtual environment
python3 -m venv env

# 3. Activate virtual environment
source env/bin/activate  # Linux/Mac
# or
env\Scripts\activate  # Windows

# 4. Install dependencies
pip install -r requirements.txt

# 5. Copy and configure environment
cp .env.example .env
# Edit .env as needed

# 6. Run migrations
cd email_sender
python manage.py migrate

# 7. Create superuser
python manage.py createsuperuser

# 8. Start server
python manage.py runserver
```

## Troubleshooting

### Port Already in Use
```bash
# Use a different port
python manage.py runserver 8080
```

### Database Locked
```bash
# Stop all Django processes and try again
pkill -f runserver
python manage.py runserver
```

### Missing Dependencies
```bash
pip install -r requirements.txt --upgrade
```

### Reset Database (WARNING: Deletes all data)
```bash
cd email_sender
rm db.sqlite3
python manage.py migrate
python manage.py createsuperuser
```

## Email Testing

### Console Backend (Default)
Emails are printed to the terminal - check the console output when sending emails.

### Gmail SMTP Setup
1. Go to Google Account Settings
2. Enable 2-Factor Authentication
3. Generate App Password (Security → 2-Step Verification → App Passwords)
4. Update `.env`:
   ```env
   EMAIL_BACKEND=django.core.mail.backends.smtp.EmailBackend
   EMAIL_HOST=smtp.gmail.com
   EMAIL_PORT=587
   EMAIL_USE_TLS=True
   EMAIL_HOST_USER=your-email@gmail.com
   EMAIL_HOST_PASSWORD=your-16-digit-app-password
   DEFAULT_FROM_EMAIL=your-email@gmail.com
   ```

## Git Setup (Optional)

### Initialize Repository
```bash
cd "/media/nikhil/DATA/MTECH/ACADEMICS/MAIN PROJECT/EchoMailer"
git init
git add .
git commit -m "Initial commit"
```

### Add Remote and Push
```bash
git remote add origin <your-repo-url>
git branch -M main
git push -u origin main
```

## Project Status
✅ Recipient model simplified (email + company)
✅ Delete functionality implemented
✅ Edit functionality implemented
✅ CSV import working
✅ Email templates functional
✅ Email logging active
✅ Admin interface configured

## Need Help?
- Check the README.md for detailed documentation
- Review CHANGES_SUMMARY.md for recent modifications
- See DELETE_EDIT_IMPLEMENTATION.md for edit/delete details
