"""
Web Dashboard for Content Automation System
Phase 4: Monitoring and Analytics
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

from flask import Flask, render_template, jsonify, request, redirect, url_for
from flask_socketio import SocketIO, emit
import threading
import time

from src.config import Config
from src.database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentAutomationDashboard:
    def __init__(self):
        """Initialize the web dashboard"""
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'your-secret-key-change-this'
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        # Initialize backend services
        self.config = Config()
        self.db = SupabaseClient()
        
        # Dashboard state
        self.is_monitoring = False
        self.monitor_thread = None
        
        self.setup_routes()
        self.setup_socketio()
        
        logger.info("Content Automation Dashboard initialized")
    
    def setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/')
        def dashboard():
            """Main dashboard page"""
            return render_template('dashboard.html')
        
        @self.app.route('/api/status')
        def get_status():
            """Get system status API endpoint"""
            try:
                status = self.get_system_status()
                return jsonify(status)
            except Exception as e:
                logger.error(f"Error getting status: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/streams')
        def get_streams():
            """Get recent streams"""
            try:
                # Get recent streams from database
                streams = self.db.supabase.table('streams').select('*').order('created_at', desc=True).limit(20).execute()
                return jsonify(streams.data)
            except Exception as e:
                logger.error(f"Error getting streams: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/uploads')
        def get_uploads():
            """Get recent uploads"""
            try:
                uploads = self.db.supabase.table('uploads').select('*').order('created_at', desc=True).limit(50).execute()
                return jsonify(uploads.data)
            except Exception as e:
                logger.error(f"Error getting uploads: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/analytics')
        def get_analytics():
            """Get analytics data"""
            try:
                analytics = self.get_analytics_data()
                return jsonify(analytics)
            except Exception as e:
                logger.error(f"Error getting analytics: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/logs')
        def get_logs():
            """Get recent logs"""
            try:
                # Read recent log entries
                log_file = Path('automation.log')
                if log_file.exists():
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        # Get last 100 lines
                        recent_logs = lines[-100:] if len(lines) > 100 else lines
                        return jsonify({'logs': recent_logs})
                else:
                    return jsonify({'logs': []})
            except Exception as e:
                logger.error(f"Error getting logs: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/schedule')
        def get_schedule():
            """Get publishing schedule"""
            try:
                # Get scheduled content for next 7 days
                end_date = datetime.now() + timedelta(days=7)
                
                uploads = self.db.supabase.table('uploads').select('*').not_.is_('scheduled_publish_at', 'null').lte('scheduled_publish_at', end_date.isoformat()).order('scheduled_publish_at').execute()
                
                return jsonify(uploads.data)
            except Exception as e:
                logger.error(f"Error getting schedule: {e}")
                return jsonify({'error': str(e)}), 500
    
    def setup_socketio(self):
        """Setup Socket.IO for real-time updates"""
        
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            logger.info('Client connected to dashboard')
            emit('status', {'message': 'Connected to dashboard'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection"""
            logger.info('Client disconnected from dashboard')
        
        @self.socketio.on('request_update')
        def handle_update_request():
            """Handle request for system update"""
            try:
                status = self.get_system_status()
                emit('system_update', status)
            except Exception as e:
                emit('error', {'message': str(e)})
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        try:
            # Get basic system info
            status = {
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'running': True,  # Dashboard is running if this is called
                    'uptime': self.get_uptime(),
                    'version': '4.0.0'
                }
            }
            
            # Get stream stats
            streams_today = self.db.supabase.table('streams').select('*').gte('created_at', datetime.now().date().isoformat()).execute()
            total_streams = self.db.supabase.table('streams').select('id', count='exact').execute()
            
            status['streams'] = {
                'today': len(streams_today.data),
                'total': total_streams.count,
                'last_stream': streams_today.data[0] if streams_today.data else None
            }
            
            # Get upload stats
            uploads_today = self.db.supabase.table('uploads').select('*').gte('created_at', datetime.now().date().isoformat()).execute()
            pending_uploads = self.db.supabase.table('uploads').select('*').eq('status', 'ready_for_upload').execute()
            
            status['uploads'] = {
                'today': len(uploads_today.data),
                'pending': len(pending_uploads.data),
                'queue_size': len(pending_uploads.data)
            }
            
            # Get processing jobs
            pending_jobs = self.db.supabase.table('processing_jobs').select('*').neq('status', 'completed').execute()
            
            status['processing'] = {
                'pending_jobs': len(pending_jobs.data),
                'active': len([j for j in pending_jobs.data if j.get('status') == 'processing'])
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_analytics_data(self) -> Dict:
        """Get analytics and reporting data"""
        try:
            now = datetime.now()
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)
            
            # Stream analytics
            streams_week = self.db.supabase.table('streams').select('*').gte('created_at', week_ago.isoformat()).execute()
            streams_month = self.db.supabase.table('streams').select('*').gte('created_at', month_ago.isoformat()).execute()
            
            # Upload analytics  
            uploads_week = self.db.supabase.table('uploads').select('*').gte('created_at', week_ago.isoformat()).execute()
            uploads_month = self.db.supabase.table('uploads').select('*').gte('created_at', month_ago.isoformat()).execute()
            
            # Success rates
            successful_uploads_week = [u for u in uploads_week.data if u.get('status') == 'published']
            successful_uploads_month = [u for u in uploads_month.data if u.get('status') == 'published']
            
            return {
                'streams': {
                    'week': len(streams_week.data),
                    'month': len(streams_month.data),
                    'avg_per_week': len(streams_month.data) / 4 if streams_month.data else 0
                },
                'uploads': {
                    'week': len(uploads_week.data),
                    'month': len(uploads_month.data),
                    'success_rate_week': len(successful_uploads_week) / len(uploads_week.data) * 100 if uploads_week.data else 0,
                    'success_rate_month': len(successful_uploads_month) / len(uploads_month.data) * 100 if uploads_month.data else 0
                },
                'performance': {
                    'automation_uptime': 95.5,  # Placeholder - could calculate from logs
                    'avg_processing_time': 12.3  # Placeholder - could calculate from processing jobs
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting analytics: {e}")
            return {'error': str(e)}
    
    def get_uptime(self) -> str:
        """Get system uptime (simplified)"""
        # This is a placeholder - in a real system you'd track start time
        return "2h 34m"
    
    def start_monitoring(self):
        """Start real-time monitoring"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Started real-time monitoring")
    
    def stop_monitoring(self):
        """Stop real-time monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Stopped real-time monitoring")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.is_monitoring:
            try:
                # Get system status and broadcast to connected clients
                status = self.get_system_status()
                self.socketio.emit('system_update', status)
                
                # Wait before next update
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Wait longer on error
    
    def run(self, host='0.0.0.0', port=5000, debug=False):
        """Run the dashboard"""
        logger.info(f"Starting Content Automation Dashboard on http://{host}:{port}")
        
        # Start monitoring
        self.start_monitoring()
        
        try:
            self.socketio.run(self.app, host=host, port=port, debug=debug)
        except KeyboardInterrupt:
            logger.info("Shutting down dashboard...")
        finally:
            self.stop_monitoring()

def create_dashboard_template():
    """Create the dashboard HTML template"""
    template_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Content Automation Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; }
        
        .header { background: #2563eb; color: white; padding: 1rem 2rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .header h1 { font-size: 1.5rem; font-weight: 600; }
        .header .status { font-size: 0.9rem; opacity: 0.9; margin-top: 0.25rem; }
        
        .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1.5rem; }
        
        .card { background: white; border-radius: 8px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .card h3 { color: #374151; margin-bottom: 1rem; font-size: 1.1rem; }
        
        .stat { display: flex; justify-content: space-between; align-items: center; padding: 0.75rem 0; border-bottom: 1px solid #e5e7eb; }
        .stat:last-child { border-bottom: none; }
        .stat-label { color: #6b7280; }
        .stat-value { font-weight: 600; color: #111827; }
        
        .status-indicator { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.5rem; }
        .status-online { background: #10b981; }
        .status-offline { background: #ef4444; }
        
        .log-container { max-height: 300px; overflow-y: auto; background: #1f2937; color: #e5e7eb; padding: 1rem; border-radius: 6px; font-family: monospace; font-size: 0.875rem; }
        .log-entry { margin-bottom: 0.25rem; }
        
        .btn { background: #2563eb; color: white; padding: 0.5rem 1rem; border: none; border-radius: 4px; cursor: pointer; }
        .btn:hover { background: #1d4ed8; }
        .btn-secondary { background: #6b7280; }
        .btn-secondary:hover { background: #4b5563; }
        
        .schedule-item { background: #f9fafb; padding: 0.75rem; border-radius: 4px; margin-bottom: 0.5rem; border-left: 3px solid #2563eb; }
        .schedule-time { font-weight: 600; color: #2563eb; font-size: 0.875rem; }
        .schedule-title { color: #374151; margin-top: 0.25rem; }
        
        @media (max-width: 768px) {
            .grid { grid-template-columns: 1fr; }
            .container { padding: 0 0.5rem; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Content Automation Dashboard</h1>
        <div class="status">
            <span class="status-indicator status-online"></span>
            <span id="connection-status">Connected</span> • 
            <span id="last-update">Loading...</span>
        </div>
    </div>
    
    <div class="container">
        <div class="grid">
            <!-- System Status -->
            <div class="card">
                <h3>System Status</h3>
                <div class="stat">
                    <span class="stat-label">System</span>
                    <span class="stat-value" id="system-status">Loading...</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Uptime</span>
                    <span class="stat-value" id="uptime">Loading...</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Last Stream</span>
                    <span class="stat-value" id="last-stream">Loading...</span>
                </div>
            </div>
            
            <!-- Stream Stats -->
            <div class="card">
                <h3>Stream Statistics</h3>
                <div class="stat">
                    <span class="stat-label">Streams Today</span>
                    <span class="stat-value" id="streams-today">0</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Total Streams</span>
                    <span class="stat-value" id="total-streams">0</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Processing Queue</span>
                    <span class="stat-value" id="processing-queue">0</span>
                </div>
            </div>
            
            <!-- Upload Stats -->
            <div class="card">
                <h3>Upload Statistics</h3>
                <div class="stat">
                    <span class="stat-label">Uploads Today</span>
                    <span class="stat-value" id="uploads-today">0</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Pending Uploads</span>
                    <span class="stat-value" id="pending-uploads">0</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Queue Size</span>
                    <span class="stat-value" id="queue-size">0</span>
                </div>
            </div>
        </div>
        
        <div class="grid" style="margin-top: 2rem;">
            <!-- Publishing Schedule -->
            <div class="card">
                <h3>Publishing Schedule</h3>
                <div id="schedule-container">
                    <div class="schedule-item">
                        <div class="schedule-time">Loading schedule...</div>
                    </div>
                </div>
            </div>
            
            <!-- Recent Activity Logs -->
            <div class="card">
                <h3>Recent Activity</h3>
                <div class="log-container" id="logs-container">
                    <div class="log-entry">Loading logs...</div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Initialize Socket.IO
        const socket = io();
        
        // Connection status
        socket.on('connect', () => {
            document.getElementById('connection-status').textContent = 'Connected';
            document.querySelector('.status-indicator').className = 'status-indicator status-online';
        });
        
        socket.on('disconnect', () => {
            document.getElementById('connection-status').textContent = 'Disconnected';
            document.querySelector('.status-indicator').className = 'status-indicator status-offline';
        });
        
        // System updates
        socket.on('system_update', (data) => {
            updateDashboard(data);
        });
        
        // Request initial update
        socket.emit('request_update');
        
        // Update dashboard with new data
        function updateDashboard(data) {
            document.getElementById('last-update').textContent = `Updated: ${new Date().toLocaleTimeString()}`;
            
            if (data.error) {
                console.error('Dashboard error:', data.error);
                return;
            }
            
            // System status
            document.getElementById('system-status').textContent = data.system?.running ? 'Online' : 'Offline';
            document.getElementById('uptime').textContent = data.system?.uptime || 'Unknown';
            
            // Stream stats
            document.getElementById('streams-today').textContent = data.streams?.today || 0;
            document.getElementById('total-streams').textContent = data.streams?.total || 0;
            document.getElementById('processing-queue').textContent = data.processing?.pending_jobs || 0;
            
            // Upload stats
            document.getElementById('uploads-today').textContent = data.uploads?.today || 0;
            document.getElementById('pending-uploads').textContent = data.uploads?.pending || 0;
            document.getElementById('queue-size').textContent = data.uploads?.queue_size || 0;
            
            // Last stream info
            const lastStream = data.streams?.last_stream;
            document.getElementById('last-stream').textContent = lastStream ? 
                new Date(lastStream.created_at).toLocaleString() : 'None today';
        }
        
        // Load schedule
        function loadSchedule() {
            fetch('/api/schedule')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('schedule-container');
                    container.innerHTML = '';
                    
                    if (data.length === 0) {
                        container.innerHTML = '<div class="schedule-item"><div class="schedule-time">No scheduled content</div></div>';
                        return;
                    }
                    
                    data.slice(0, 5).forEach(item => {
                        const scheduleItem = document.createElement('div');
                        scheduleItem.className = 'schedule-item';
                        
                        const time = new Date(item.scheduled_publish_at).toLocaleString();
                        const title = item.youtube_title || item.title || 'Untitled';
                        
                        scheduleItem.innerHTML = `
                            <div class="schedule-time">${time}</div>
                            <div class="schedule-title">${title.substring(0, 50)}${title.length > 50 ? '...' : ''}</div>
                        `;
                        
                        container.appendChild(scheduleItem);
                    });
                })
                .catch(error => {
                    console.error('Error loading schedule:', error);
                    document.getElementById('schedule-container').innerHTML = 
                        '<div class="schedule-item"><div class="schedule-time">Error loading schedule</div></div>';
                });
        }
        
        // Load logs
        function loadLogs() {
            fetch('/api/logs')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('logs-container');
                    container.innerHTML = '';
                    
                    if (!data.logs || data.logs.length === 0) {
                        container.innerHTML = '<div class="log-entry">No recent logs</div>';
                        return;
                    }
                    
                    // Show last 20 log entries
                    data.logs.slice(-20).reverse().forEach(log => {
                        const logEntry = document.createElement('div');
                        logEntry.className = 'log-entry';
                        logEntry.textContent = log.trim();
                        container.appendChild(logEntry);
                    });
                    
                    // Auto-scroll to top of logs
                    container.scrollTop = 0;
                })
                .catch(error => {
                    console.error('Error loading logs:', error);
                    document.getElementById('logs-container').innerHTML = 
                        '<div class="log-entry">Error loading logs</div>';
                });
        }
        
        // Initial data load
        loadSchedule();
        loadLogs();
        
        // Refresh data periodically
        setInterval(() => {
            loadSchedule();
            loadLogs();
        }, 60000); // Every minute
        
        // Request system updates every 30 seconds
        setInterval(() => {
            socket.emit('request_update');
        }, 30000);
    </script>
</body>
</html>'''
    
    # Create templates directory and save template
    templates_dir = Path('templates')
    templates_dir.mkdir(exist_ok=True)
    
    with open(templates_dir / 'dashboard.html', 'w', encoding='utf-8') as f:
        f.write(template_content)
    
    logger.info("Dashboard template created successfully")

def create_dashboard_app():
    """Create and configure the dashboard Flask app"""
    dashboard = ContentAutomationDashboard()
    
    # Create dashboard template
    create_dashboard_template()
    
    return dashboard

if __name__ == "__main__":
    # Run the dashboard
    dashboard = create_dashboard_app()
    
    # Get port from config or use default
    port = getattr(dashboard.config, 'DASHBOARD_PORT', 5000)
    
    print(f"Starting Content Automation Dashboard...")
    print(f"Access dashboard at: http://localhost:{port}")
    print("Press Ctrl+C to stop")
    
    dashboard.run(host='0.0.0.0', port=port, debug=False)