import paramiko
import requests
import time

# Fix the service file on the VPS
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('37.114.35.110', port=22, username='root', password='mQ#=&ihc9gYn-KZ0', timeout=15)

# Stop everything
print("Stopping old service...")
ssh.exec_command('systemctl stop scraper-slave')
ssh.exec_command('pkill -f uvicorn')
time.sleep(2)

# Check current health
stdin, stdout, stderr = ssh.exec_command('curl -s http://localhost:8001/api/health || curl -s http://localhost:8002/api/health')
print('Health check:', stdout.read().decode())

ssh.close()

# Now re-provision via master
print("\nRe-provisioning via master...")
r = requests.post('http://159.203.180.79:8000/api/slaves/provision', json={
    'ip': '37.114.35.110',
    'user': 'root',
    'password': 'mQ#=&ihc9gYn-KZ0',
    'ssh_port': 22,
    'slave_port': 8001,
    'name': 'vultr-DA',
    'master_url': 'http://159.203.180.79:8000'
}, timeout=15)
print('Provision status:', r.status_code)
print('Response:', r.json())

# Poll for completion
sid = r.json().get('slave_id')
if sid:
    for i in range(10):
        time.sleep(3)
        r2 = requests.get(f'http://159.203.180.79:8000/api/slaves/{sid}/logs')
        logs = r2.json().get('logs', [])
        if logs:
            print(f"\n--- Logs ({i+1}) ---")
            for log in logs[-3:]:
                print(log)
        
        r3 = requests.get('http://159.203.180.79:8000/api/slaves')
        for s in r3.json():
            if s['id'] == sid:
                print(f"Status: {s['status']}")
                if s['status'] == 'idle':
                    print("\n✓ Slave is ready!")
                    exit(0)
                break

print("\nCheck dashboard for final status")
