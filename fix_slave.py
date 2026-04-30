import paramiko
import sys

ip = "37.114.35.110"
user = "root"
password = "mQ#=&ihc9gYn-KZ0"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ip, port=22, username=user, password=password, timeout=15)

def run(cmd, timeout=300):
    print(f"\n>>> {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out[-1000:])
    if err:
        print(f"STDERR: {err[-500:]}")
    print(f"exit={exit_code}")
    return out, err, exit_code

# 1. Stop service
run("systemctl stop scraper-slave")

# 2. Install python3-venv (includes python3.12-venv on Ubuntu 24.04)
run("apt-get update -qq && apt-get install -y python3-venv python3.12-venv --no-install-recommends -qq", timeout=300)

# 3. Recreate venv
run("rm -rf /opt/scraper-slave/venv")
run("cd /opt/scraper-slave && python3 -m venv venv")

# 3. Install deps
run("cd /opt/scraper-slave && venv/bin/pip install --upgrade pip")
run("cd /opt/scraper-slave && venv/bin/pip install --no-cache-dir -r requirements.txt")

# 4. Verify imports
run("/opt/scraper-slave/venv/bin/python -c 'import fastapi,uvicorn,httpx,requests,lxml,cssselect,colorama,pydantic; print(\"ALL_IMPORTS_OK\")'")

# 5. Start service
run("systemctl daemon-reload && systemctl start scraper-slave")

# 6. Wait and check
import time
time.sleep(3)
run("systemctl status scraper-slave --no-pager | head -15")

# 7. Health check
run("curl -s http://localhost:8001/api/health")

ssh.close()
print("\nDone!")
