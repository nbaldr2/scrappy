import paramiko

# Check the failing VPS
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # Try the password from earlier
    ssh.connect('95.179.131.195', port=22, username='root', password='aGuUirEVjqkPhLtH', timeout=15)
    print('SSH connected')
    
    # Check service status
    stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager 2>&1')
    out = stdout.read().decode()
    print('Service status:')
    print(out[:2000])
    
    # Check if port is listening
    stdin, stdout, stderr = ssh.exec_command('ss -tlnp | grep 8001 || echo "Port not listening"')
    print('\nPort check:', stdout.read().decode().strip())
    
    # Check logs
    stdin, stdout, stderr = ssh.exec_command('journalctl -u scraper-slave -n 30 --no-pager 2>&1')
    logs = stdout.read().decode()
    print('\nJournal logs (last 30 lines):')
    print(logs[-2000:])
    
    # Check if files exist
    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/ 2>&1')
    print('\nFiles in /opt/scraper-slave/:')
    print(stdout.read().decode())
    
    # Check venv
    stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/venv/bin/ 2>&1 | head -10')
    print('\nVenv bin:')
    print(stdout.read().decode())
    
    ssh.close()
except Exception as e:
    print(f'SSH Error: {type(e).__name__}: {e}')
