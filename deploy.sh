#!/bin/bash

# Content Automation System - DigitalOcean Deployment Script
# Phase 4: Background Service Setup

set -e  # Exit on any error

echo "🚀 Deploying Content Automation System to DigitalOcean..."

# Configuration
APP_DIR="/var/www/contentautomation"
SERVICE_NAME="content-automation"
USER="www-data"
GROUP="www-data"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run as root (use sudo)"
   exit 1
fi

print_status "Step 1: System Updates and Dependencies"

# Update system
apt update && apt upgrade -y

# Install required system packages
apt install -y python3 python3-pip python3-venv ffmpeg git nginx supervisor

# Install latest yt-dlp
curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp
chmod a+rx /usr/local/bin/yt-dlp

print_status "Step 2: Application Directory Setup"

# Create application directory
mkdir -p $APP_DIR
mkdir -p $APP_DIR/downloads
mkdir -p $APP_DIR/temp
mkdir -p $APP_DIR/logs

# Set up proper ownership
chown -R $USER:$GROUP $APP_DIR

print_status "Step 3: Python Virtual Environment"

# Create virtual environment
sudo -u $USER python3 -m venv $APP_DIR/venv

# Activate and install dependencies
sudo -u $USER $APP_DIR/venv/bin/pip install --upgrade pip
sudo -u $USER $APP_DIR/venv/bin/pip install -r $APP_DIR/requirements.txt

print_status "Step 4: Environment Configuration"

# Check if .env file exists
if [ ! -f "$APP_DIR/.env" ]; then
    print_warning ".env file not found. Creating template..."
    cat > $APP_DIR/.env << 'EOF'
# Database & APIs
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
TWITCH_CLIENT_ID=your_twitch_client_id
TWITCH_CLIENT_SECRET=your_twitch_client_secret
TWITCH_USER_LOGIN=sir_kris

# Storage
DO_SPACES_ENDPOINT=https://twitchvodautomation.nyc3.digitaloceanspaces.com
DO_SPACES_BUCKET=twitchvodautomation

# Processing Settings
VOD_DOWNLOAD_DIR=downloads
VOD_TEMP_DIR=temp
VOD_MAX_SIZE_GB=10
VOD_TARGET_SIZE_GB=8
UPLOAD_SCAN_INTERVAL_MINUTES=30
UPLOAD_PUBLISH_DELAY_HOURS=2
CLEANUP_KEEP_DAYS=7
POLL_INTERVAL_SECONDS=120

# System
LOG_LEVEL=INFO
MAX_CONCURRENT_UPLOADS=2
EOF
    chown $USER:$GROUP $APP_DIR/.env
    chmod 600 $APP_DIR/.env
    print_warning "Please edit $APP_DIR/.env with your actual credentials"
fi

print_status "Step 5: Systemd Service Setup"

# Create systemd service file
cat > /etc/systemd/system/$SERVICE_NAME.service << EOF
[Unit]
Description=Content Automation System - Twitch to YouTube Pipeline
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$GROUP
WorkingDirectory=$APP_DIR
Environment=PATH=/usr/bin:/usr/local/bin:$APP_DIR/venv/bin
Environment=PYTHONPATH=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Resource limits
LimitNOFILE=65536
MemoryMax=2G
CPUQuota=80%

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$APP_DIR/downloads
ReadWritePaths=$APP_DIR/temp
ReadWritePaths=$APP_DIR/logs

[Install]
WantedBy=multi-user.target
EOF

print_status "Step 6: Logging Setup"

# Create log rotation config
cat > /etc/logrotate.d/$SERVICE_NAME << EOF
$APP_DIR/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    copytruncate
    su $USER $GROUP
}
EOF

print_status "Step 7: Service Configuration"

# Reload systemd and enable service
systemctl daemon-reload
systemctl enable $SERVICE_NAME

# Create startup script for manual management
cat > $APP_DIR/manage.sh << 'EOF'
#!/bin/bash

SERVICE_NAME="content-automation"
APP_DIR="/var/www/contentautomation"

case "$1" in
    start)
        echo "🚀 Starting Content Automation System..."
        sudo systemctl start $SERVICE_NAME
        sleep 2
        sudo systemctl status $SERVICE_NAME --no-pager
        ;;
    stop)
        echo "🛑 Stopping Content Automation System..."
        sudo systemctl stop $SERVICE_NAME
        ;;
    restart)
        echo "🔄 Restarting Content Automation System..."
        sudo systemctl restart $SERVICE_NAME
        sleep 2
        sudo systemctl status $SERVICE_NAME --no-pager
        ;;
    status)
        echo "📊 Content Automation System Status:"
        sudo systemctl status $SERVICE_NAME --no-pager
        echo ""
        echo "📋 Recent logs:"
        sudo journalctl -u $SERVICE_NAME -n 20 --no-pager
        ;;
    logs)
        echo "📋 Live logs (Ctrl+C to exit):"
        sudo journalctl -u $SERVICE_NAME -f
        ;;
    enable)
        echo "⚙️  Enabling auto-start on boot..."
        sudo systemctl enable $SERVICE_NAME
        ;;
    disable)
        echo "⚙️  Disabling auto-start on boot..."
        sudo systemctl disable $SERVICE_NAME
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|enable|disable}"
        echo ""
        echo "Examples:"
        echo "  ./manage.sh start    - Start the service"
        echo "  ./manage.sh status   - Check service status"
        echo "  ./manage.sh logs     - View live logs"
        echo ""
        exit 1
        ;;
esac
EOF

chmod +x $APP_DIR/manage.sh
chown $USER:$GROUP $APP_DIR/manage.sh

print_status "Step 8: Firewall Configuration"

# Configure UFW if it's installed
if command -v ufw &> /dev/null; then
    print_status "Configuring firewall..."
    ufw allow ssh
    ufw allow 80/tcp  # HTTP for dashboard
    ufw allow 443/tcp # HTTPS for dashboard
    ufw --force enable
else
    print_warning "UFW not installed, skipping firewall configuration"
fi

print_status "🎉 Deployment Complete!"

echo ""
echo "=================================================================="
echo "📋 Next Steps:"
echo "=================================================================="
echo "1. Edit configuration file:"
echo "   nano $APP_DIR/.env"
echo ""
echo "2. Copy your application files to:"
echo "   $APP_DIR"
echo ""
echo "3. Start the service:"
echo "   $APP_DIR/manage.sh start"
echo ""
echo "4. Check service status:"
echo "   $APP_DIR/manage.sh status"
echo ""
echo "5. View live logs:"
echo "   $APP_DIR/manage.sh logs"
echo ""
echo "=================================================================="
echo "🔧 Service Management Commands:"
echo "=================================================================="
echo "Start:    sudo systemctl start $SERVICE_NAME"
echo "Stop:     sudo systemctl stop $SERVICE_NAME"
echo "Restart:  sudo systemctl restart $SERVICE_NAME"
echo "Status:   sudo systemctl status $SERVICE_NAME"
echo "Logs:     sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "Or use the convenient script: $APP_DIR/manage.sh [command]"
echo ""

if [ ! -f "$APP_DIR/.env" ] || grep -q "your_supabase_project_url" "$APP_DIR/.env"; then
    print_warning "Remember to configure your .env file before starting the service!"
fi

echo "✅ The Content Automation System is ready to run in the background!"
echo "   It will automatically start on boot and restart if it crashes."