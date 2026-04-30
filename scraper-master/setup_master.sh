#!/bin/bash
# ── Master VPS Setup Script ──────────────────────────────────────────────────
# Run on your master Ubuntu VPS
# Usage: chmod +x setup_master.sh && ./setup_master.sh

set -e

echo "⚡ Setting up Scraper Master with PostgreSQL..."

# System deps
sudo apt-get update -qq
sudo apt-get install -y python3 python3-pip python3-venv postgresql postgresql-contrib libpq-dev

# Setup PostgreSQL
echo "📊 Setting up PostgreSQL..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql -c "CREATE USER scraper WITH PASSWORD 'scraper123';" 2>/dev/null || echo "User may already exist"
sudo -u postgres psql -c "CREATE DATABASE scraperdb OWNER scraper;" 2>/dev/null || echo "Database may already exist"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE scraperdb TO scraper;" 2>/dev/null || true

# Set environment variable for database
echo "export DATABASE_URL=postgresql://scraper:scraper123@localhost/scraperdb" | sudo tee /etc/default/scraper-master > /dev/null

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install Python deps
pip install --upgrade pip
pip install -r requirements.txt

# Create systemd service
sudo tee /etc/systemd/system/scraper-master.service > /dev/null <<EOF
[Unit]
Description=Scraper Master Dashboard
After=network.target postgresql.service

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/venv/bin/python -m uvicorn master:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=DATABASE_URL=postgresql://scraper:scraper123@localhost/scraperdb

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable scraper-master
sudo systemctl start scraper-master

# Open firewall
sudo ufw allow 8000/tcp 2>/dev/null || true

echo ""
echo "✅ Master is running!"
echo "🌐 Dashboard: http://$(hostname -I | awk '{print $1}'):8000"
echo ""
echo "📋 Commands:"
echo "   sudo systemctl status scraper-master"
echo "   sudo journalctl -u scraper-master -f"
echo "   sudo systemctl restart scraper-master"
