import paramiko
import time
import sys

ip = "159.203.180.79"
user = "root"
password = "Yhv5qg2UYvt2TEbU"
repo = "https://github.com/nbaldr2/scrappy"

print(f"Connecting to {user}@{ip}...")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    ssh.connect(ip, port=22, username=user, password=password, timeout=15)
    print("✓ SSH connected")

    def run_cmd(cmd):
        print(f"Running: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        # Read line by line
        for line in iter(stdout.readline, ""):
            print(line, end="")
        
        err = stderr.read().decode()
        if err:
            print(f"Stderr: {err}", file=sys.stderr)
            
        status = stdout.channel.recv_exit_status()
        print(f"Exit status: {status}\n")
        return status

    # 1. Install git if not present
    run_cmd("apt-get update -qq && apt-get install -y git")

    # 2. Clone repo
    run_cmd(f"rm -rf scrappy && git clone {repo} scrappy")

    # 3. Setup Master
    # The user pushed the master code, so we go into scrappy/scraper-master and run setup_master.sh
    run_cmd("cd scrappy/scraper-master && chmod +x setup_master.sh && ./setup_master.sh")

except Exception as e:
    print(f"Error: {e}")
finally:
    ssh.close()
