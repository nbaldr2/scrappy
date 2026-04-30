import paramiko
import sys

ip = "159.203.180.79"
user = "root"
password = "Yhv5qg2UYvt2TEbU"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ip, port=22, username=user, password=password, timeout=15)

# Check service status and logs
commands = [
    "systemctl status scraper-master --no-pager",
    "journalctl -u scraper-master -n 100 --no-pager",
]

for cmd in commands:
    print(f"\n=== Running: {cmd} ===")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out)
    if err:
        print(f"Stderr: {err}", file=sys.stderr)

ssh.close()