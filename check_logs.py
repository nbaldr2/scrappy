import paramiko
import sys

ip = "159.203.180.79"
user = "root"
password = "Yhv5qg2UYvt2TEbU"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ip, port=22, username=user, password=password, timeout=15)
stdin, stdout, stderr = ssh.exec_command("journalctl -u scraper-master -n 50 --no-pager")
print(stdout.read().decode())
ssh.close()
