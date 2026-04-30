import paramiko

# Check the failing VPS JDIIIID
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    ssh.connect('95.179.131.195', port=22, username='root', password='!2tS5dXYZNh7fHV3', timeout=15)
    print('SSH connected OK')
    
    # Check service status
    stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager 2>&1')
    out = stdout.read().decode()
    print('\nService status:')
    print(out[:2000])
    
    # Check if port is listening
    stdin, stdout, stderr = ssh.exec_command('ss -tlnp | grep 8001 || echo "Port not listening"')
    print('\nPort 8001:', stdout.read().decode().strip())
    
    # Check logs
    stdin, stdout, stderr = ssh.exec_command('journalctl -u scraper-slave -n 30 --no-pager 2>&1')
    logs = stdout.read().decode()
    print('\nJournal logs (last 30):')
    print(logs[-2000:])
    
    # Check files
    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/ 2>&1')
    print('\nFiles in /opt/scraper-slave/:')
    print(stdout.read().decode())
    
    # Check venv
    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/venv/bin/python 2>&1')
    print('\nPython in venv:', stdout.read().decode().strip())
    
    # Try to manually start and see error
    stdin, stdout, stderr = ssh.exec_command('cd /opt/scraper-slave && venv/bin/python -c "import slave" 2>&1')
    err = stderr.read().decode()
    if err:
        print('\nImport error:', err[:500])
    
    ssh.close()
except Exception as e:
    print(f'SSH Error: {type(e).__name__}: {e}')
