# src/email_notifier.py
"""
Email Notifier Module
Sends email notifications for manual review requests
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmailNotifier:
    """Handles email notifications"""
    
    def __init__(self):
        """Initialize email notifier"""
        # Email configuration - using Gmail SMTP
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.sender_password = os.getenv('SENDER_PASSWORD')
        
        # Recipient
        self.recipient_email = os.getenv('RECIPIENT_EMAIL', 'sirkristhestreamer@gmail.com')
        
        if not self.sender_email or not self.sender_password:
            logger.warning('⚠️  Email credentials not configured')
            logger.warning('   Set SENDER_EMAIL and SENDER_PASSWORD in .env')
            logger.warning('   For Gmail, use an App Password: https://support.google.com/accounts/answer/185833')
        else:
            logger.info(f'📧 Email notifier initialized')
            logger.info(f'   Recipient: {self.recipient_email}')
    
    def send_email(self, subject: str, body: str, body_html: Optional[str] = None) -> bool:
        """
        Send an email
        
        Args:
            subject: Email subject
            body: Plain text body
            body_html: Optional HTML body
        
        Returns:
            bool: True if sent successfully
        """
        if not self.sender_email or not self.sender_password:
            logger.error('❌ Email not configured')
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = self.recipient_email
            
            # Attach plain text
            text_part = MIMEText(body, 'plain')
            msg.attach(text_part)
            
            # Attach HTML if provided
            if body_html:
                html_part = MIMEText(body_html, 'html')
                msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            logger.info(f'✅ Email sent: {subject}')
            return True
            
        except Exception as e:
            logger.error(f'❌ Failed to send email: {e}')
            return False
    
    def send_metadata_failure_alert(self, stream_title: str, game_name: str, 
                                     twitch_vod_id: str, youtube_url: Optional[str] = None) -> bool:
        """
        Send alert for metadata fetch failure
        
        Args:
            stream_title: Title of the stream
            game_name: Game name that failed
            twitch_vod_id: Twitch VOD ID
            youtube_url: YouTube video URL (if uploaded)
        
        Returns:
            bool: True if sent successfully
        """
        subject = f'⚠️ Manual Review Needed: {game_name}'
        
        # Plain text version
        body = f"""
Hi Sir_Kris,

The automation couldn't find game metadata for one of your streams.

Stream Details:
- Title: {stream_title}
- Game: {game_name}
- Twitch VOD: https://www.twitch.tv/videos/{twitch_vod_id}
"""
        
        if youtube_url:
            body += f"- YouTube Video: {youtube_url}\n"
            body += "\nThe video has been uploaded to YouTube as PRIVATE.\n"
            body += "Please edit the video description and tags manually, then publish it.\n"
        else:
            body += "\nThe video will be uploaded as PRIVATE once available.\n"
        
        body += f"""
What to do:
1. Go to YouTube Studio
2. Edit the video description and tags
3. Change visibility to Public when ready

Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

- Your Content Automation Bot
"""
        
        # HTML version
        body_html = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <h2 style="color: #ff6b6b;">⚠️ Manual Review Needed</h2>
    
    <p>Hi Sir_Kris,</p>
    
    <p>The automation couldn't find game metadata for one of your streams.</p>
    
    <div style="background-color: #f5f5f5; padding: 15px; border-left: 4px solid #6441a5; margin: 20px 0;">
        <h3 style="margin-top: 0;">Stream Details</h3>
        <p><strong>Title:</strong> {stream_title}</p>
        <p><strong>Game:</strong> {game_name}</p>
        <p><strong>Twitch VOD:</strong> <a href="https://www.twitch.tv/videos/{twitch_vod_id}">View on Twitch</a></p>
"""
        
        if youtube_url:
            body_html += f"""
        <p><strong>YouTube Video:</strong> <a href="{youtube_url}">View on YouTube</a></p>
    </div>
    
    <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
        <p><strong>📹 The video has been uploaded to YouTube as PRIVATE.</strong></p>
    </div>
"""
        else:
            body_html += """
    </div>
    
    <div style="background-color: #d1ecf1; padding: 15px; border-left: 4px solid #17a2b8; margin: 20px 0;">
        <p>The video will be uploaded as PRIVATE once available.</p>
    </div>
"""
        
        body_html += """
    <div style="background-color: #e7f3ff; padding: 15px; border-left: 4px solid #2196F3; margin: 20px 0;">
        <h3 style="margin-top: 0;">What to do:</h3>
        <ol>
            <li>Go to <a href="https://studio.youtube.com">YouTube Studio</a></li>
            <li>Find the video and click Edit</li>
            <li>Update the description with game info</li>
            <li>Add relevant tags</li>
            <li>Change visibility to Public when ready</li>
        </ol>
    </div>
    
    <p style="color: #888; font-size: 12px; margin-top: 30px;">
        Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
        - Your Content Automation Bot 🤖
    </p>
</body>
</html>
"""
        
        return self.send_email(subject, body, body_html)
    
    def send_test_email(self) -> bool:
        """
        Send a test email to verify configuration
        
        Returns:
            bool: True if sent successfully
        """
        subject = '✅ Test Email from Content Automation'
        body = f"""
Hi Sir_Kris,

This is a test email from your content automation system.

If you're receiving this, your email notifications are working correctly!

Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

- Your Content Automation Bot
"""
        
        body_html = """
<html>
<body style="font-family: Arial, sans-serif;">
    <h2 style="color: #4CAF50;">✅ Test Email</h2>
    <p>Hi Sir_Kris,</p>
    <p>This is a test email from your content automation system.</p>
    <p><strong>If you're receiving this, your email notifications are working correctly!</strong></p>
    <p style="color: #888; font-size: 12px; margin-top: 30px;">
        - Your Content Automation Bot 🤖
    </p>
</body>
</html>
"""
        
        return self.send_email(subject, body, body_html)


# Example usage and testing
def main():
    """Test email notifier"""
    print('\n' + '='*60)
    print('Testing Email Notifier')
    print('='*60)
    
    notifier = EmailNotifier()
    
    print('\n1. Sending test email...')
    success = notifier.send_test_email()
    
    if success:
        print('✅ Test email sent successfully!')
        print(f'   Check {notifier.recipient_email}')
    else:
        print('❌ Failed to send test email')
        print('   Make sure SENDER_EMAIL and SENDER_PASSWORD are set in .env')
    
    print('\n2. Testing metadata failure alert...')
    success = notifier.send_metadata_failure_alert(
        stream_title='Warframe - New War Quest',
        game_name='Warframe',
        twitch_vod_id='1234567890',
        youtube_url='https://youtube.com/watch?v=example'
    )
    
    if success:
        print('✅ Alert email sent successfully!')
    else:
        print('❌ Failed to send alert email')
    
    print('\n' + '='*60)
    print('Testing complete!')
    print('='*60 + '\n')


if __name__ == '__main__':
    main()