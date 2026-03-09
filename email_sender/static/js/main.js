// Auto-hide alerts after 5 seconds
document.addEventListener('DOMContentLoaded', function() {
    const alerts = document.querySelectorAll('.alert:not(.alert-permanent)');
    alerts.forEach(function(alert) {
        setTimeout(function() {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }, 5000);
    });
});

// Template preview on selection
const templateSelect = document.querySelector('select[name="template"]');
if (templateSelect) {
    templateSelect.addEventListener('change', function() {
        const selectedOption = this.options[this.selectedIndex];
        if (selectedOption.value) {
            // Fetch template data via AJAX and populate subject/body
            // This would require a Django view to return template data
            console.log('Template selected:', selectedOption.value);
        }
    });
}

// Character counter for subject and body
const subjectInput = document.querySelector('input[name="subject"]');
const bodyTextarea = document.querySelector('textarea[name="body"]');

if (subjectInput) {
    subjectInput.addEventListener('input', function() {
        console.log('Subject length:', this.value.length);
    });
}

if (bodyTextarea) {
    bodyTextarea.addEventListener('input', function() {
        console.log('Body length:', this.value.length);
    });
}

// Confirmation for bulk email sending
const sendEmailForm = document.querySelector('form[action*="compose"]');
if (sendEmailForm) {
    sendEmailForm.addEventListener('submit', function(e) {
        const checkedRecipients = document.querySelectorAll('input[name="recipients"]:checked');
        if (checkedRecipients.length > 10) {
            if (!confirm(`You are about to send emails to ${checkedRecipients.length} recipients. Continue?`)) {
                e.preventDefault();
            }
        }
    });
}