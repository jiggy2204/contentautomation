# fix_template.py
from pathlib import Path

# Delete existing template
templates_dir = Path('templates')
template_file = templates_dir / 'dashboard.html'

if template_file.exists():
    template_file.unlink()
    print("Deleted old template file")

# Recreate with proper encoding
templates_dir.mkdir(exist_ok=True)

template_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Content Automation Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; }
        .header { background: #2563eb; color: white; padding: 1rem 2rem; }
        .header h1 { font-size: 1.5rem; font-weight: 600; }
        .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
        .card { background: white; border-radius: 8px; padding: 1.5rem; margin-bottom: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .stat { display: flex; justify-content: space-between; padding: 0.5rem 0; }
        .stat-label { color: #6b7280; }
        .stat-value { font-weight: 600; }
        .status-indicator { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.5rem; background: #10b981; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Content Automation Dashboard</h1>
        <div>
            <span class="status-indicator"></span>
            <span id="connection-status">Connected</span>
        </div>
    </div>
    
    <div class="container">
        <div class="card">
            <h3>System Status</h3>
            <div class="stat">
                <span class="stat-label">System</span>
                <span class="stat-value" id="system-status">Online</span>
            </div>
            <div class="stat">
                <span class="stat-label">Streams Today</span>
                <span class="stat-value" id="streams-today">0</span>
            </div>
            <div class="stat">
                <span class="stat-label">Uploads Today</span>
                <span class="stat-value" id="uploads-today">0</span>
            </div>
        </div>
    </div>
    
    <script>
        const socket = io();
        
        socket.on('connect', () => {
            document.getElementById('connection-status').textContent = 'Connected';
        });
        
        socket.on('system_update', (data) => {
            if (data.streams) {
                document.getElementById('streams-today').textContent = data.streams.today || 0;
            }
            if (data.uploads) {
                document.getElementById('uploads-today').textContent = data.uploads.today || 0;
            }
        });
        
        socket.emit('request_update');
    </script>
</body>
</html>'''

with open(template_file, 'w', encoding='utf-8') as f:
    f.write(template_content)

print(f"Created new template file: {template_file}")
print("Template file created with UTF-8 encoding")