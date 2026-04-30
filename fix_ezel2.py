import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('23.26.4.228', port=22, username='root', password='tvhaDEPlYFvXHnQ8', timeout=15)
print('SSH connected')

# Install python3.12-venv
print('\nInstalling python3.12-venv...')
stdin, stdout, stderr = ssh.exec_command('apt-get update -qq && apt-get install -y python3.12-venv --no-install-recommends -qq 2>&1', timeout=300)
print('apt output:', stdout.read().decode()[-1000:])
print('apt errors:', stderr.read().decode()[-500:])

# Now recreate venv
print('\nRecreating venv...')
stdin, stdout, stderr = ssh.exec_command('rm -rf /opt/scraper-slave/venv && cd /opt/scraper-slave && python3 -m venv venv 2>&1')
print('venv:', stdout.read().decode()[:500], stderr.read().decode()[:500])

# Install packages
print('\nInstalling packages...')
stdin, stdout, stderr = ssh.exec_command('cd /opt/scraper-slave && venv/bin/pip install --upgrade pip -q 2>&1', timeout=120)
print('pip upgrade:', stdout.read().decode()[-200:], stderr.read().decode()[-200:])

stdin, stdout, stderr = ssh.exec_command('cd /opt/scraper-slave && venv/bin/pip install --no-cache-dir -r requirements.txt -q 2>&1', timeout=300)
print('pip install:', stdout.read().decode()[-200:], stderr.read().decode()[-200:])

# Verify
stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python -c "import fastapi,uvicorn; print(\"IMPORTS_OK\")" 2>&1')
result = stdout.read().decode().strip()
print('\nVerification:', result)

# Start service
print('\nStarting service...')
ssh.exec_command('systemctl daemon-reload')
ssh.exec_command('systemctl stop scraper-slave')
ssh.exec_command('pkill -f uvicorn')
ssh.exec_command('systemctl start scraper-slave')

import time
time.sleep(3)

stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager | head -8')
print('Status:', stdout.read().decode())

stdin, stdout, stderr = ssh.exec_command('curl -s http://localhost:8001/api/health')
health = stdout.read().decode()
print('Health:', health[:200] if health else 'No response')

ssh.close()
print('\nDone!')
