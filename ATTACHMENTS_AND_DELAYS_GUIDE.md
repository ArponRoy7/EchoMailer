# Email Attachments & Delay Settings - Feature Guide

## Overview

EchoMailer now supports **email attachments** and **configurable sending delays** to help you send professional emails while avoiding rate limiting and spam filters.

## New Features

### 1. Email Attachments
- Attach multiple files to your emails
- Support for various file types (PDF, images, documents, etc.)
- Configurable attachment limits
- Automatic validation of file size and count

### 2. Email Delay Settings
- Add delays between individual emails
- Batch sending with longer pauses
- Prevent rate limiting from email providers
- Avoid spam filter triggers

## Quick Start

### Using Attachments

1. **Navigate to Compose Email**
   ```
   http://127.0.0.1:8000/compose/
   ```

2. **Fill in your email details**
   - Subject and body
   - Select recipients
   - **Click "Choose Files"** under Attachments section

3. **Select Multiple Files**
   - You can select up to 5 files (default)
   - Each file can be up to 10MB (default)
   - Hold Ctrl (Windows/Linux) or Cmd (Mac) to select multiple files

4. **Send Your Email**
   - Click "Send Emails"
   - All recipients will receive the same attachments

### Configuring Email Delays

1. **Navigate to Email Settings**
   ```
   http://127.0.0.1:8000/settings/
   ```

2. **Configure Delay Settings**
   - **Email Delay**: Time between each email (0-60 seconds)
   - **Batch Size**: Number of emails before taking a longer pause
   - **Batch Delay**: Longer pause duration after a batch

3. **Example Configurations**

   **Conservative (Avoid Rate Limiting)**
   ```
   Email Delay: 2 seconds
   Batch Size: 50 emails
   Batch Delay: 60 seconds
   ```

   **Moderate (Balanced)**
   ```
   Email Delay: 1 second
   Batch Size: 100 emails
   Batch Delay: 30 seconds
   ```

   **Fast (No Rate Limiting Concerns)**
   ```
   Email Delay: 0 seconds
   Batch Size: 0 (disabled)
   Batch Delay: 0 seconds
   ```

4. **Configure Attachment Limits**
   - **Max Attachments**: Maximum files per email (1-10)
   - **Max Size**: Maximum size per file in MB (1-25)

## Features in Detail

### Email Attachments

#### How It Works
1. Files are uploaded with the form
2. System validates file count and sizes
3. Files are attached to each email sent
4. **Same attachments** are sent to all recipients
5. Email logs track whether attachments were included

#### Supported File Types
- **Documents**: PDF, DOC, DOCX, TXT, RTF
- **Images**: JPG, PNG, GIF, BMP, SVG
- **Spreadsheets**: XLS, XLSX, CSV
- **Archives**: ZIP, RAR, 7Z
- **Presentations**: PPT, PPTX
- **Other**: Any file type your email provider accepts

#### Limitations
- **Default Limits**:
  - Max 5 files per email
  - Max 10MB per file
  - Total size should stay under 25MB (Gmail limit)

- **Configurable** via Email Settings page

#### Best Practices
1. **Compress Large Files**: Use ZIP for multiple documents
2. **Optimize Images**: Reduce file sizes before uploading
3. **Test First**: Send test email with attachments to yourself
4. **Check Provider Limits**: Gmail, Outlook have different limits
5. **Use Links for Large Files**: Consider cloud storage links for large files

### Email Delay Settings

#### Why Use Delays?

**Avoid Rate Limiting**
- Gmail: ~100-500 emails/day for free accounts
- Outlook: ~300 emails/day for free accounts
- Adding delays spreads out sends over time

**Prevent Spam Filters**
- Sudden bursts of emails trigger spam filters
- Gradual sending appears more natural
- Improves deliverability

**Server Protection**
- Prevents overwhelming your SMTP server
- Reduces chance of temporary blocks
- Maintains good sender reputation

#### Understanding the Settings

**1. Email Delay**
```
Time between each email (seconds)
Range: 0-60 seconds
```

**Use Cases**:
- `0s` - No delay, send as fast as possible
- `1-2s` - Light rate limiting protection
- `3-5s` - Moderate protection, recommended for most users
- `10s+` - Heavy protection, very safe but slower

**2. Batch Size**
```
Number of emails before longer pause
Set to 0 to disable batching
```

**Use Cases**:
- `0` - No batching (disabled)
- `50` - Good for small campaigns
- `100` - Standard batch size
- `500` - Large campaigns

**3. Batch Delay**
```
Longer pause after completing a batch (seconds)
```

**Use Cases**:
- `30s` - Short break
- `60s` - Standard break (recommended)
- `300s (5min)` - Long break for safety
- `600s (10min)` - Maximum safety

#### Calculating Send Time

**Formula**:
```
Time = (Recipients × Email Delay) + (Batches × Batch Delay)
```

**Examples**:

**Example 1: 100 Recipients**
```
Settings:
- Email Delay: 2s
- Batch Size: 50
- Batch Delay: 60s

Calculation:
- (100 × 2s) + (2 batches × 60s)
- 200s + 120s = 320s
- Total: ~5 minutes 20 seconds
```

**Example 2: 500 Recipients**
```
Settings:
- Email Delay: 1s
- Batch Size: 100
- Batch Delay: 30s

Calculation:
- (500 × 1s) + (5 batches × 30s)
- 500s + 150s = 650s
- Total: ~10 minutes 50 seconds
```

**Example 3: 1000 Recipients (Fast)**
```
Settings:
- Email Delay: 0.5s
- Batch Size: 200
- Batch Delay: 15s

Calculation:
- (1000 × 0.5s) + (5 batches × 15s)
- 500s + 75s = 575s
- Total: ~9 minutes 35 seconds
```

## Technical Implementation

### Database Models

**EmailLog Updates**
```python
has_attachments = BooleanField(default=False)
attachment_count = IntegerField(default=0)
```

**New EmailSettings Model (Singleton)**
```python
email_delay = FloatField(default=0.0)  # 0-60 seconds
batch_size = IntegerField(default=0)    # 0 = disabled
batch_delay = FloatField(default=0.0)   # seconds
max_attachments = IntegerField(default=5)
max_attachment_size = IntegerField(default=10)  # MB
```

### Email Sending Flow

```python
1. Get EmailSettings from database
2. Get attachments from form
3. For each recipient:
   a. Personalize email content
   b. Attach files if provided
   c. Send email via SMTP
   d. Log result with attachment info
   e. Apply email_delay (if not last email)
   f. Check if batch completed
   g. Apply batch_delay if needed
```

### Code Example

```python
from emails.models import EmailSettings
from emails.utils import send_bulk_emails

# Get settings
settings = EmailSettings.get_settings()

# Send with attachments and delays
results = send_bulk_emails(
    subject="Your Subject",
    body="Your message",
    recipients=recipient_list,
    attachments=file_list  # List of uploaded files
)

# Delays are applied automatically
```

## URL Routes

| Route | View | Description |
|-------|------|-------------|
| `/settings/` | `email_settings` | Configure delays and attachment limits |
| `/compose/` | `compose_email` | Compose and send emails with attachments |

## Admin Interface

Access via Django admin: `http://127.0.0.1:8000/admin/`

**Email Settings Admin**
- View/edit delay configuration
- Modify attachment limits
- Only one settings instance allowed (Singleton)
- Cannot be deleted

**Email Log Admin**
- View which emails had attachments
- See attachment count per email
- Filter by has_attachments

## Provider-Specific Limits

### Gmail
- **Daily Limit**: 100-500 emails (varies by account type)
- **Max Attachment**: 25MB total per email
- **Recommended Delay**: 2-5 seconds
- **Batch Size**: 50-100 emails

### Outlook/Office 365
- **Daily Limit**: ~300 emails
- **Max Attachment**: 20MB per file, 25MB total
- **Recommended Delay**: 2-3 seconds
- **Batch Size**: 50 emails

### Yahoo Mail
- **Daily Limit**: ~500 emails
- **Max Attachment**: 25MB total
- **Recommended Delay**: 2-5 seconds
- **Batch Size**: 50-100 emails

### Custom SMTP
- Depends on your provider
- Check your provider's documentation
- Start conservative and adjust based on results

## Troubleshooting

### Attachments Not Sending

**Issue**: Attachments not included in emails

**Solutions**:
1. Check form has `enctype="multipart/form-data"`
2. Verify file size doesn't exceed limits
3. Check number of files doesn't exceed max_attachments
4. Review server logs for errors
5. Test with smaller files first

### Validation Errors

**Issue**: "Maximum X attachments allowed"

**Solution**:
- Go to Email Settings
- Increase max_attachments value
- Or reduce number of files you're uploading

**Issue**: "File too large. Maximum size is XMB"

**Solution**:
- Compress the file
- Go to Email Settings to increase max_attachment_size
- Use cloud storage link instead

### Rate Limiting

**Issue**: "Too many emails sent" or blocked by provider

**Solutions**:
1. Increase email_delay to 3-5 seconds
2. Enable batching (batch_size: 50)
3. Add batch_delay of 60-120 seconds
4. Spread campaign over multiple days
5. Verify account with email provider

### Slow Sending

**Issue**: Emails taking too long to send

**Solutions**:
1. Reduce email_delay
2. Increase batch_size
3. Reduce batch_delay
4. Disable batching completely (batch_size: 0)

**Trade-off**: Faster sending = higher risk of rate limiting

## Security Considerations

### File Upload Security
- Validate file types on server
- Scan for malware if handling user uploads
- Set reasonable size limits
- Don't store uploads permanently unless needed

### Email Delays
- Delays add time but improve reliability
- Start conservative, then optimize
- Monitor for bounces and blocks
- Keep good sender reputation

## Best Practices

### For Attachments
1. ✅ **Test First**: Send to yourself before bulk sending
2. ✅ **Keep Small**: Compress files when possible
3. ✅ **Use Common Formats**: PDF, JPG, PNG work everywhere
4. ✅ **Name Files Clearly**: Use descriptive filenames
5. ✅ **Check Provider Limits**: Different providers, different limits

### For Delays
1. ✅ **Start Conservative**: Use 2-5 second delays initially
2. ✅ **Enable Batching**: For campaigns over 100 emails
3. ✅ **Monitor Results**: Track bounce rates and blocks
4. ✅ **Adjust Gradually**: Increase speed slowly based on success
5. ✅ **Respect Limits**: Don't try to circumvent provider limits

### For Both
1. ✅ **Use Test Credentials**: Test with different email providers
2. ✅ **Log Everything**: Review EmailLog for issues
3. ✅ **Document Settings**: Keep notes on what works
4. ✅ **Plan Campaigns**: Schedule large sends strategically
5. ✅ **Monitor Reputation**: Check sender score regularly

## Migration Guide

### From v1.1.0 to v1.2.0

**No Breaking Changes!** Simply:

1. **Pull latest code**

2. **Apply migrations**:
   ```bash
   python manage.py migrate
   ```

3. **Configure settings** (optional):
   - Visit `/settings/`
   - Set your preferred delays
   - Adjust attachment limits

4. **Start using attachments**:
   - Go to Compose Email
   - Select files under Attachments section
   - Send as normal

## FAQ

**Q: Can I send different attachments to different recipients?**
A: Not currently. All recipients receive the same attachments. This is a potential future enhancement.

**Q: Are attachments stored on the server?**
A: No, attachments are sent directly and not stored permanently.

**Q: What happens if one attachment fails?**
A: The email is still sent without that specific attachment. Check logs for details.

**Q: Can I disable delays temporarily?**
A: Yes, set all delay values to 0 in Email Settings.

**Q: Do delays affect test emails?**
A: No, the test credential function sends immediately without delays.

**Q: What's the maximum file size?**
A: Default is 10MB per file, configurable up to 25MB. Provider limits may be lower.

**Q: Can I attach files larger than 25MB?**
A: No, most email providers reject emails larger than 25MB total. Use cloud storage links instead.

**Q: How do I know my delays are working?**
A: Check the server logs (console output) which shows real-time sending progress with delays.

## Support

For issues or questions:
- Check this documentation
- Review server logs
- Test with a single recipient first
- Check your email provider's documentation

## Version History

**v1.2.0** - Added Features
- Email attachment support
- Configurable email delays
- Batch sending with pauses
- Email Settings page
- Attachment tracking in logs
- Custom multiple file upload widget

**v1.1.0** - Previous Version
- Email credential management
- Password encryption
- Multi-provider support

---

**Status**: ✅ Complete and ready for use

**Compatibility**: Django 5.2.7, Python 3.9+

**Dependencies**: No new dependencies required
