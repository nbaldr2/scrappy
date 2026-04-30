import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh.connect('23.26.4.228', port=22, username='root', password='tvhaDEPlYFvXHnQ8', timeout=15)
print('SSH connected')

# Check service status
stdin, stdout, stderr = ssh.exec_command('systemctl status scraper-slave --no-pager 2>&1')
out = stdout.read().decode()
print('\nService status:')
print(out[:2000])

# Check port
stdin, stdout, stderr = ssh.exec_command('ss -tlnp | grep 8001 || netstat -tlnp | grep 8001 || echo "Port not listening"')
print('\nPort 8001:', stdout.read().decode().strip())

# Check logs
stdin, stdout, stderr = ssh.exec_command('journalctl -u scraper-slave -n 20 --no-pager 2>&1')
print('\nJournal logs:')
print(stdout.read().decode()[-1500:])

# Check if files exist
stdin, stdout, stderr = ssh.exec_command('ls -la /opt/scraper-slave/ 2>&1')
print('\nFiles:')
print(stdout.read().decode())

# Check venv python
stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python --version 2>&1')
print('\nPython version:', stdout.read().decode().strip())

# Check if packages work
stdin, stdout, stderr = ssh.exec_command('/opt/scraper-slave/venv/bin/python -c "import fastapi,uvicorn" 2>&1')
err = stderr.read().decode()
if err:
    print('\nImport error:', err[:500])
else:
    print('\nImports OK')

ssh.close()
print('\nDone!')
