import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('95.179.131.195', port=22, username='root', password='!2tS5dXYZNh7fHV3', timeout=15)

print("Stopping service...")
ssh.exec_command('systemctl stop scraper-slave')

print("Installing python3.12-venv...")
stdin, stdout, stderr = ssh.exec_command('apt-get update -qq && apt-get install -y python3.12-venv -qq', timeout=300)
print('apt out:', stdout.read().decode()[-500:])
print('apt err:', stderr.read().decode()[-500:])

print("Recreating venv...")
stdin, stdout, stderr = ssh.exec_command('rm -rf /opt/scraper-slave/venv && cd /opt/scraper-slave && python3 -m venv venv', timeout=60)
print('venv:', stdout.read().decode(), stderr.read().decode())

print("Installing packages...")
stdin, stdout, stderr = ssh.exec_command('cd /opt/scraper-slave && venv/bin/pip install --upgrade pip -q && venv/bin/pip install --no-cache-dir -r requirements.txt -q', timeout=300)
print('pip out:', stdout.read().decode()[-500:])
print('pip err:', stderr.read().decode()[-500:])

print("Verifying...")
stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python -c "import fastapi,uvicorn; print(\"OK\")"')
print('Verify:', stdout.read().decode(), stderr.read().decode())

print("Starting service...")
ssh.exec_command('systemctl daemon-reload && systemctl start scraper-slave')

import time
time.sleep(3)

stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager | head -10')
print('Status:', stdout.read().decode())

stdin, stdout, stderr = ssh.exec_command('curl -s http://localhost:8001/api/health')
print('Health:', stdout.read().decode())

ssh.close()
print("Done!")
