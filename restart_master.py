import paramiko
import sys

ip = "159.203.180.79"
user = "root"
password = "Yhv5qg2UYvt2TEbU"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ip, port=22, username=user, password=password, timeout=15)

# Pull latest code, install dependencies, restart service
commands = [
    "cd /root/scrappy && git pull",
    "cd /root/scrappy/scraper-master && venv/bin/pip install -r requirements.txt --quiet",
    "systemctl restart scraper-master"
]

for cmd in commands:
    print(f"Running: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out)
    if err:
        print(f"Stderr: {err}", file=sys.stderr)

print("✓ Master updated and restarted!")
ssh.close()
