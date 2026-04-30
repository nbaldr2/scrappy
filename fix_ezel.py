import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('23.26.4.228', port=22, username='root', password='tvhaDEPlYFvXHnQ8', timeout=15)
print('SSH connected')

# Stop service
print('\nStopping service...')
ssh.exec_command('systemctl stop scraper-slave')
ssh.exec_command('pkill -f uvicorn')

# Check current state
stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/venv/bin/pip 2>&1')
print('pip exists:', 'yes' if 'No such' not in stdout.read().decode() else 'NO')

# Recreate venv properly
print('\nRecreating venv...')
stdin, stdout, stderr = ssh.exec_command('rm -rf /opt/scraper-slave/venv && cd /opt/scraper-slave && python3 -m venv venv 2>&1')
print('venv output:', stdout.read().decode()[-500:], stderr.read().decode()[-500:])

# Install packages
print('\nInstalling packages...')
stdin, stdout, stderr = ssh.exec_command('cd /opt/scraper-slave && venv/bin/pip install --upgrade pip -q && venv/bin/pip install --no-cache-dir -r requirements.txt -q 2>&1', timeout=300)
print('pip output:', stdout.read().decode()[-500:], stderr.read().decode()[-500:])

# Verify
stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python -c "import fastapi,uvicorn; print(\"OK\")" 2>&1')
result = stdout.read().decode().strip()
print('\nVerification:', result)

# Start service
print('\nStarting service...')
ssh.exec_command('systemctl daemon-reload && systemctl start scraper-slave')

import time
time.sleep(3)

stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager | head -10')
print('Status:', stdout.read().decode())

stdin, stdout, stderr = ssh.exec_command('curl -s http://localhost:8001/api/health')
print('Health:', stdout.read().decode())

ssh.close()
print('\nDone!')
