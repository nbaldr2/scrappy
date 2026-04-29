#!/bin/bash
# ── Slave VPS Setup Script ───────────────────────────────────────────────────
# Run on each slave Ubuntu VPS
# Usage: chmod +x setup_slave.sh && ./setup_slave.sh <MASTER_IP> [SLAVE_PORT] [SLAVE_NAME]
#
# Example:
#   ./setup_slave.sh 10.0.0.1 8001 VPS-Paris-01
#   ./setup_slave.sh 10.0.0.1 8001 VPS-London-02

set -e

MASTER_IP="${1:?Usage: ./setup_slave.sh <MASTER_IP> [PORT] [NAME]}"
SLAVE_PORT="${2:-8001}"
SLAVE_NAME="${3:-Slave-$(hostname)}"
SLAVE_ID=$(cat /proc/sys/kernel/random/uuid | cut -d'-' -f1)

echo "⚡ Setting up Scraper Slave: $SLAVE_NAME"
echo "   Master: http://$MASTER_IP:8000"
echo "   Port:   $SLAVE_PORT"

# System deps
sudo apt-get update -qq
sudo apt-get install -y python3 python3-pip python3-venv

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install Python deps
pip install --upgrade pip
pip install -r requirements.txt

# Get this machine's IP
MY_IP=$(hostname -I | awk '{print $1}')

# Create systemd service
sudo tee /etc/systemd/system/scraper-slave.service > /dev/null <<EOF
[Unit]
Description=Scraper Slave Agent - $SLAVE_NAME
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/venv/bin/python -m uvicorn slave:app --host 0.0.0.0 --port $SLAVE_PORT
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=SLAVE_ID=$SLAVE_ID
Environment=SLAVE_PORT=$SLAVE_PORT
Environment=MASTER_URL=http://$MASTER_IP:8000

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable scraper-slave
sudo systemctl start scraper-slave

# Open firewall
sudo ufw allow $SLAVE_PORT/tcp 2>/dev/null || true

# Register with master
sleep 2
echo ""
echo "📡 Registering with master..."
curl -s -X POST "http://$MASTER_IP:8000/api/slaves" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$SLAVE_ID\",\"url\":\"http://$MY_IP:$SLAVE_PORT\",\"name\":\"$SLAVE_NAME\"}" \
  && echo " ✅ Registered!" \
  || echo " ⚠ Could not reach master — register manually from the dashboard"

echo ""
echo "✅ Slave is running!"
echo "   ID:   $SLAVE_ID"
echo "   URL:  http://$MY_IP:$SLAVE_PORT"
echo ""
echo "📋 Commands:"
echo "   sudo systemctl status scraper-slave"
echo "   sudo journalctl -u scraper-slave -f"
echo "   sudo systemctl restart scraper-slave"
