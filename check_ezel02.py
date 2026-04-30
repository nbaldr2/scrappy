import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    ssh.connect('23.26.4.182', port=22, username='root', password='AO,2,hCpIarSvr8_', timeout=15)
    print('SSH connected')

    # Check service
    stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager 2>&1')
    print('Service:', stdout.read().decode()[:1500])

    # Check port
    stdin, stdout, stderr = ssh.exec_command('ss -tlnp | grep 8001 || echo "not listening"')
    print('Port:', stdout.read().decode().strip())

    # Check journal
    stdin, stdout, stderr = ssh.exec_command('journalctl -u scraper-slave -n 10 --no-pager 2>&1')
    print('Logs:', stdout.read().decode()[-1000:])

    # Check venv
    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/venv/bin/python 2>&1')
    print('Python:', stdout.read().decode().strip())

    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/venv/bin/pip 2>&1')
    print('Pip:', stdout.read().decode().strip())

    # Check imports
    stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python -c "import uvicorn" 2>&1')
    err = stderr.read().decode()
    if err:
        print('Uvicorn import error:', err[:200])

    ssh.close()
except Exception as e:
    print(f'SSH Error: {type(e).__name__}: {e}')
