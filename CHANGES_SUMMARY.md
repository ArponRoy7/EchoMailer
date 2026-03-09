# Recipient Model Simplification - Changes Summary

## Overview
The Recipient model has been simplified to only include `email` and `company` fields. All references to removed fields (`name`, `tags`, `is_active`) have been updated throughout the application.

## Files Modified

### 1. Models (`emails/models.py`)
**Changes:**
- Removed fields: `name`, `tags`, `is_active`
- Kept fields: `email` (unique), `company` (optional), `created_at`
- Updated `__str__()` method to display email and company
- Changed ordering from `name` to `email`
- Updated `EmailTemplate.body` help text to only mention `{{email}}` and `{{company}}`

### 2. Forms (`emails/forms.py`)
**Changes:**
- `RecipientForm`: Reduced fields to only `email` and `company`
- `BulkRecipientForm`: Updated CSV format help text to `email, company`
- `SendEmailForm`: Changed recipients queryset from `filter(is_active=True)` to `all()`
- `EmailTemplateForm`: Added help text about personalization variables

### 3. Views (`emails/views.py`)
**Changes:**
- `dashboard()`: Changed from `filter(is_active=True).count()` to `count()`
- `recipient_list()`: Updated search filter to only search by `email` and `company` (removed `name`)

### 4. Utils (`emails/utils.py`)
**Changes:**
- `personalize_message()`: Removed `name` from context, now only uses `email` and `company`
- `import_recipients_from_csv()`: Removed `name` and `tags` from defaults, only imports `company`

### 5. Admin (`emails/admin.py`)
**Changes:**
- `RecipientAdmin`: Updated `list_display` to `['email', 'company', 'created_at']`
- Removed `is_active` from `list_filter`
- Updated `search_fields` to only `['email', 'company']`

### 6. Templates

#### `templates/emails/recipient_form.html`
- Removed form fields: `name`, `tags`, `is_active` checkbox
- Only displays: `email` and `company` fields

#### `templates/emails/recipient_list.html`
- Updated search placeholder from "Search by name, email, or company..." to "Search by email or company..."
- Removed table columns: `Name`, `Tags`, `Status`
- Table now shows: `Email`, `Company`, `Actions`
- Updated avatar circle to use first letter of email instead of name
- Changed colspan in empty state from 6 to 3

#### `templates/emails/dashboard.html`
- Changed recipient display from name/email to email/company
- Updated to show email as primary with company as secondary info

#### `templates/emails/email_logs.html`
- Changed recipient display from name/email to email/company
- Updated modal to show email with optional company in parentheses

### 7. Database Migration
**File:** `emails/migrations/0002_alter_recipient_options_remove_recipient_is_active_and_more.py`

**Changes:**
- Changed Meta options on Recipient (ordering)
- Removed field: `is_active`
- Removed field: `name`
- Removed field: `tags`
- Altered field: `EmailTemplate.body` help text

## CSV Import Format
The new CSV format for bulk import is:
```csv
email,company
user@example.com,Company Name
another@example.com,Another Company
```

## Template Variables for Personalization
When creating email templates, you can now use:
- `{{email}}` - Recipient's email address
- `{{company}}` - Recipient's company name

## Database Changes Applied
✓ Migration created: `0002_alter_recipient_options_remove_recipient_is_active_and_more.py`
✓ Migration applied successfully
✓ No system check errors

## Testing Performed
- ✓ Django system check passed with no issues
- ✓ Migration created and applied successfully
- ✓ No Python syntax or import errors

## Notes
- All existing recipient data's `name`, `tags`, and `is_active` fields have been removed from the database
- The email field remains unique, so no duplicate emails are allowed
- Company field is optional (can be blank)
- Recipients are now ordered alphabetically by email address
