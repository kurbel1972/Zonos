import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional


def send_email_with_attachments(
    subject: str,
    body: str,
    attachments: Optional[List[str]] = None
) -> bool:
    """
    Send an email with optional attachments.
    
    Args:
        subject: Email subject
        body: Email body text
        attachments: List of file paths to attach
        
    Returns:
        True if email sent successfully, False otherwise
    """
    # Get email configuration from environment variables
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO")  # Comma-separated list of recipients
    
    # Validate required environment variables
    if not all([smtp_server, smtp_user, smtp_password, email_from, email_to]):
        print("Error: Missing required email configuration in environment variables")
        return False
    
    # Parse recipients
    recipients = [email.strip() for email in email_to.split(",")]
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Subject"] = subject
        
        # Attach body
        msg.attach(MIMEText(body, "plain"))
        
        # Attach files
        if attachments:
            for file_path in attachments:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as attachment:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(attachment.read())
                    
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {os.path.basename(file_path)}"
                    )
                    msg.attach(part)
                else:
                    print(f"Warning: Attachment not found: {file_path}")
        
        # Connect to SMTP server and send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        print(f"Email sent successfully to: {email_to}")
        return True
        
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False


def send_processing_notification(
    input_filename: Optional[str] = None,
    output_file: Optional[str] = None,
    log_file: Optional[str] = None,
    total_processed: int = 0,
    total_skipped: int = 0
) -> bool:
    """
    Send notification email about file processing results.
    
    Args:
        input_filename: Name of the input file that was processed
        output_file: Path to the generated output file
        log_file: Path to the skipped rows log file
        total_processed: Number of successfully processed rows
        total_skipped: Number of skipped rows
        
    Returns:
        True if email sent successfully, False otherwise
    """
    subject = "Zonos Postal Data Processing Report"
    
    body = f"""Processing completed successfully: {input_filename if input_filename else 'N/A'}

Summary:
- Total rows processed: {total_processed}
- Total rows skipped: {total_skipped}

Files:
"""
    
    if output_file:
        body += f"- Output file: {os.path.basename(output_file)}\n"
    
    if log_file:
        body += f"- Skipped rows log: {os.path.basename(log_file)}\n"
    
    body += "\nPlease find the attached files for details.\n\nBest regards,\nZonos Automation File Processing System"
    
    # Prepare attachments
    attachments = []
    if output_file and os.path.exists(output_file):
        attachments.append(output_file)
    if log_file and os.path.exists(log_file):
        attachments.append(log_file)
    
    return send_email_with_attachments(subject, body, attachments)
