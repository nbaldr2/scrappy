#!/usr/bin/env python3
"""Deploy updated code to master VPS."""

import paramiko
import os

MASTER_IP = "159.203.180.79"
MASTER_PASS = "Yhv5qg2UYvt2TEbU"
LOCAL_DIR = "/Users/soufianerochdi/Documents/workspace/scrappers"
REMOTE_DIR = "/root/scrappy"

print("Connecting to master VPS...")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(MASTER_IP, port=22, username="root", password=MASTER_PASS, timeout=15)

# Copy files via SFTP
print("Copying master.py...")
sftp = ssh.open_sftp()
sftp.put(
    f"{LOCAL_DIR}/scraper-master/master.py",
    f"{REMOTE_DIR}/scraper-master/master.py"
)

print("Copying database.py...")
sftp.put(
    f"{LOCAL_DIR}/scraper-master/database.py",
    f"{REMOTE_DIR}/scraper-master/database.py"
)

print("Copying dashboard.html...")
sftp.put(
    f"{LOCAL_DIR}/scraper-master/templates/dashboard.html",
    f"{REMOTE_DIR}/scraper-master/templates/dashboard.html"
)

print("Copying slave.py (with fixes)...")
sftp.put(
    f"{LOCAL_DIR}/scraper-slave/slave.py",
    f"{REMOTE_DIR}/scraper-slave/slave.py"
)

sftp.close()

# Restart master service
print("Restarting master service...")
stdin, stdout, stderr = ssh.exec_command("systemctl restart scraper-master")
exit_code = stdout.channel.recv_exit_status()

if exit_code == 0:
    print("✓ Master restarted successfully!")
else:
    err = stderr.read().decode()
    print(f"✗ Master restart failed: {err}")

# Also sync slave code to all slaves
print("\nSyncing slave.py to all slaves (for the service restart/reboot fixes)...")
stdin, stdout, stderr = ssh.exec_command(f"cd {REMOTE_DIR} && ls -la uploads/*/domains.txt 2>/dev/null | head -1 || echo 'No active jobs'")
output = stdout.read().decode().strip()

if "domains.txt" in output:
    print("  Note: Active jobs detected. Slave code will be updated on next provision/restart.")
else:
    print("  No active jobs - slaves can be updated anytime.")

ssh.close()
print("\n✓ Deploy complete!")
print(f"Dashboard: http://{MASTER_IP}:8000/")