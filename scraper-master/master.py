"""
Scraper Master — FastAPI server with dashboard, domain cleaning, DNS filtering,
SSH auto-provisioning, slave monitoring/heartbeat, and email collection.
"""

import asyncio
import io
import json
import os
import re
import socket
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import paramiko
from fastapi import FastAPI, File, Form, UploadFile, Request, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Try to use aiodns for async DNS resolution (much faster)
try:
    import aiodns
    HAS_AIODNS = True
except ImportError:
    HAS_AIODNS = False

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
UPLOAD_DIR  = BASE_DIR / "uploads"
RESULT_DIR  = BASE_DIR / "results"
SLAVE_DIR   = BASE_DIR.parent / "scraper-slave"
UPLOAD_DIR.mkdir(exist_ok=True)
RESULT_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Scraper Master", version="2.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ── In-memory state ────────────────────────────────────────────────────────────

# Slave registry:  {slave_id: {url, name, status, last_seen, ...}}
slaves: dict[str, dict] = {}

# Jobs:  {job_id: {status, domains_total, domains_cleaned, domains_live,
#                  domains_per_slave, progress, emails, ...}}
jobs: dict[str, dict] = {}

# Provisioning logs:  {slave_id: [log_lines]}
provision_logs: dict[str, list] = {}

# ── Persistence ───────────────────────────────────────────────────────────────

STATE_FILE = BASE_DIR / "state.json"

def save_state():
    """Persist jobs and slaves to JSON file."""
    try:
        state = {
            "jobs": {k: v for k, v in jobs.items()},
            "slaves": {k: v for k, v in slaves.items()},
        }
        STATE_FILE.write_text(json.dumps(state, default=str), encoding="utf-8")
    except Exception as e:
        print(f"Warning: Failed to save state: {e}")

def load_state():
    """Load jobs and slaves from JSON file on startup."""
    global jobs, slaves
    try:
        if STATE_FILE.exists():
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            # Load slaves
            for sid, s in state.get("slaves", {}).items():
                slaves[sid] = s
                # Reset status for slaves that were active before restart
                if s.get("status") in ("scraping", "processing"):
                    slaves[sid]["status"] = "idle"
            # Load jobs
            for jid, j in state.get("jobs", {}).items():
                jobs[jid] = j
                # Reset status for jobs that were active before restart
                if j.get("status") in ("cleaning", "dns_filtering", "scraping", "processing"):
                    jobs[jid]["status"] = "failed"
                    jobs[jid]["error"] = "Job interrupted by server restart"
            print(f"Loaded {len(slaves)} slaves and {len(jobs)} jobs from state file")
    except Exception as e:
        print(f"Warning: Failed to load state: {e}")

# ── Domain Cleaning (ported from pyscrap.py) ───────────────────────────────────

CCTLD_SECOND_LEVEL = {
    'co.uk','org.uk','net.uk','ac.uk','gov.uk','nhs.uk',
    'com.au','net.au','org.au','gov.au','edu.au',
    'co.nz','org.nz','net.nz','govt.nz',
    'co.jp','or.jp','ne.jp','go.jp','ac.jp',
    'com.br','net.br','org.br','gov.br','edu.br',
    'co.za','org.za','net.za','gov.za',
    'com.cn','net.cn','org.cn','gov.cn','edu.cn',
    'com.sg','net.sg','org.sg','gov.sg',
    'co.in','net.in','org.in','gov.in','ac.in',
}

COMMON_SUBDOMAINS = {
    'www','mail','ftp','blog','shop','store','news','admin','api','app',
    'assets','cdn','cloud','cms','crm','dev','download','email','en',
    'forum','help','img','images','jobs','login','media','mobile','my',
    'newsletter','old','panel','partner','pay','portal','remote','secure',
    'server','service','staging','static','support','test','uat','video',
    'vpn','web','webmail','wiki','ww1','ww2','ww3','www1','www2','www3',
}


def extract_domain_parts(domain: str):
    domain = domain.lower().strip().replace('https://','').replace('http://','')
    domain = domain.split('/')[0].split(':')[0]
    parts = domain.split('.')
    if len(parts) < 2:
        return ('', domain)
    if len(parts) >= 3:
        last3 = '.'.join(parts[-3:])
        for cctld in CCTLD_SECOND_LEVEL:
            if last3.endswith('.' + cctld) or last3 == cctld:
                effective = '.'.join(parts[-3:])
                subdomain = '.'.join(parts[:-3]) if len(parts) > 3 else ''
                return (subdomain, effective)
    effective = '.'.join(parts[-2:])
    subdomain = '.'.join(parts[:-2]) if len(parts) > 2 else ''
    return (subdomain, effective)


def clean_domains(raw_lines: list[str], phase2: bool = True) -> tuple[list[str], dict]:
    """Full cleaning pipeline: dedupe, strip subdomains, optional phase2."""
    raw = []
    for line in raw_lines:
        line = line.strip()
        if line and not line.startswith('#'):
            line = line.replace('https://','').replace('http://','')
            line = line.split('/')[0].split(':')[0]
            if line and '.' in line:
                raw.append(line.lower())

    stats = {'original': len(raw), 'phase1_dupes': 0, 'www_stripped': 0,
             'phase2_dupes': 0, 'subdomains_removed': 0}

    # Phase 1
    seen, p1 = set(), []
    for d in raw:
        sub, eff = extract_domain_parts(d)
        if eff in seen:
            stats['phase1_dupes'] += 1
            continue
        if sub in COMMON_SUBDOMAINS or not sub:
            if sub == 'www' or (sub and sub.startswith('www.')):
                stats['www_stripped'] += 1
            p1.append(eff)
            seen.add(eff)
        else:
            full = f"{sub}.{eff}".lstrip('.')
            if full not in seen:
                p1.append(full)
                seen.add(full)

    cleaned = p1
    if phase2:
        seen2, p2 = set(), []
        for d in p1:
            _, eff = extract_domain_parts(d)
            if eff in seen2:
                stats['phase2_dupes'] += 1
                continue
            if _ :
                stats['subdomains_removed'] += 1
            p2.append(eff)
            seen2.add(eff)
        cleaned = p2

    stats['final'] = len(cleaned)
    stats['removed'] = stats['original'] - len(cleaned)
    return cleaned, stats


# ── DNS Pre-Filter (Robust Multi-Record-Type with Retry) ──────────────────────

DNS_TIMEOUT = 5.0  # 5 seconds timeout for DNS resolution (was 3s, too aggressive)
DNS_MAX_WORKERS = 500  # Reduced from 2000 to avoid overwhelming DNS resolvers
DNS_MAX_RETRIES = 2  # Retry failed lookups once
DNS_RETRY_DELAY = 0.5  # Delay between retries

# Global cancellation flags for jobs
job_cancel_flags: dict[str, bool] = {}

# DNS statistics for debugging
dns_stats = {"total": 0, "a_records": 0, "aaaa_records": 0, "cname_records": 0, "retried": 0, "failed": 0}

async def _dns_query_with_retry(resolver, host: str, qtype: str) -> list:
    """Query DNS with retry logic for transient failures."""
    global dns_stats
    
    for attempt in range(DNS_MAX_RETRIES + 1):
        try:
            if HAS_AIODNS and resolver:
                result = await asyncio.wait_for(
                    resolver.query(host, qtype),
                    timeout=DNS_TIMEOUT
                )
                if result:
                    return result
            else:
                # Fallback to dnspython or socket
                return []
        except asyncio.TimeoutError:
            if attempt < DNS_MAX_RETRIES:
                dns_stats["retried"] += 1
                await asyncio.sleep(DNS_RETRY_DELAY * (attempt + 1))
            continue
        except Exception:
            if attempt < DNS_MAX_RETRIES:
                dns_stats["retried"] += 1
                await asyncio.sleep(DNS_RETRY_DELAY * (attempt + 1))
            continue
    
    return []

async def _dns_check_async_robust(host: str, resolver=None) -> bool:
    """
    Robust async DNS check - tries A, AAAA, and CNAME records.
    Returns True if ANY record type resolves.
    """
    global dns_stats
    dns_stats["total"] += 1
    
    # Try A record (IPv4)
    try:
        a_result = await _dns_query_with_retry(resolver, host, 'A')
        if a_result:
            dns_stats["a_records"] += 1
            return True
    except Exception:
        pass
    
    # Try AAAA record (IPv6) - many modern sites are IPv6-only
    try:
        aaaa_result = await _dns_query_with_retry(resolver, host, 'AAAA')
        if aaaa_result:
            dns_stats["aaaa_records"] += 1
            return True
    except Exception:
        pass
    
    # Try CNAME - some domains only have CNAME records
    try:
        cname_result = await _dns_query_with_retry(resolver, host, 'CNAME')
        if cname_result:
            dns_stats["cname_records"] += 1
            return True
    except Exception:
        pass
    
    dns_stats["failed"] += 1
    return False

def _dns_check_sync_robust(host: str) -> bool:
    """
    Synchronous robust DNS check using socket.
    Tries both IPv4 and IPv6, and handles edge cases better.
    """
    orig = socket.getdefaulttimeout()
    
    # Clean the hostname - remove any accidental protocols or paths
    host = host.strip().lower()
    if host.startswith(('http://', 'https://')):
        host = host.split('://', 1)[1].split('/')[0]
    if ':' in host:
        host = host.split(':')[0]  # Remove port if present
    
    # Skip invalid hostnames
    if not host or '.' not in host or len(host) > 253:
        return False
    
    try:
        socket.setdefaulttimeout(DNS_TIMEOUT)
        
        # Try both IPv4 and IPv6
        for family in [socket.AF_INET, socket.AF_INET6]:
            try:
                socket.getaddrinfo(host, None, family=family, type=socket.SOCK_STREAM)
                return True
            except socket.gaierror:
                continue
            except Exception:
                continue
        
        # Final fallback - try without specifying family (system default)
        try:
            socket.getaddrinfo(host, 80, proto=socket.IPPROTO_TCP)
            return True
        except Exception:
            pass
            
    except Exception:
        pass
    finally:
        socket.setdefaulttimeout(orig)
    
    return False

async def _dns_check_async(host: str, resolver=None) -> bool:
    """Async DNS check with fallback to sync method."""
    # First try the robust async method
    if HAS_AIODNS and resolver:
        try:
            result = await _dns_check_async_robust(host, resolver)
            if result:
                return True
        except Exception:
            pass
    
    # Fallback to sync socket method in thread pool
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _dns_check_sync_robust, host)

def _dns_check_sync(host: str) -> bool:
    """Synchronous DNS check - wrapper for robust version."""
    return _dns_check_sync_robust(host)

async def dns_filter_async(domains: list[str], job_id: str = None) -> tuple[list[str], int]:
    """
    Robust async DNS filter with multi-record-type support and retry logic.
    Uses reduced concurrency (500) to avoid overwhelming DNS resolvers.
    """
    global job_cancel_flags, dns_stats
    
    if not domains:
        return [], 0
    
    # Reset stats
    dns_stats = {"total": 0, "a_records": 0, "aaaa_records": 0, "cname_records": 0, "retried": 0, "failed": 0}
    
    live = []
    dead = 0
    total = len(domains)
    checked = 0
    live_count = 0
    
    # Check if job was cancelled
    if job_id and job_cancel_flags.get(job_id, False):
        return [], 0
    
    # Create resolvers with rotating nameservers for better reliability
    resolvers = []
    if HAS_AIODNS:
        try:
            # Create multiple resolvers with different timeout settings
            for i in range(min(20, (len(domains) // 100) + 1)):
                resolvers.append(aiodns.DNSResolver(timeout=DNS_TIMEOUT))
        except Exception:
            resolvers = []
    
    # Use more conservative concurrency
    semaphore = asyncio.Semaphore(DNS_MAX_WORKERS)
    lock = asyncio.Lock()
    
    async def check_one(domain: str, resolver_idx: int) -> tuple[bool, str]:
        nonlocal checked, live_count
        async with semaphore:
            # Check for cancellation
            if job_id and job_cancel_flags.get(job_id, False):
                return False, domain
            
            resolver = resolvers[resolver_idx % len(resolvers)] if resolvers else None
            result = await _dns_check_async(domain, resolver)
            
            async with lock:
                checked += 1
                if result:
                    live_count += 1
            
            return result, domain
    
    # Process in smaller batches to avoid overwhelming the system
    BATCH_SIZE = 1000
    all_tasks = []
    
    for i in range(0, len(domains), BATCH_SIZE):
        batch = domains[i:i + BATCH_SIZE]
        batch_tasks = [check_one(d, j) for j, d in enumerate(batch)]
        all_tasks.extend(batch_tasks)
        
        # Brief pause between batch creation to allow DNS resolver recovery
        if i + BATCH_SIZE < len(domains):
            await asyncio.sleep(0.1)
    
    # Run with progress updates
    async def update_progress():
        while checked < total:
            await asyncio.sleep(0.5)
            if job_id and job_id in jobs:
                jobs[job_id]["dns_progress"] = {
                    "checked": checked, 
                    "total": total, 
                    "live": live_count,
                    "percent": round((checked / total) * 100, 1) if total > 0 else 0
                }
    
    # Start progress updater
    progress_task = asyncio.create_task(update_progress()) if job_id else None
    
    try:
        # Run all DNS checks with return_exceptions to capture all results
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        for result in results:
            if job_id and job_cancel_flags.get(job_id, False):
                if progress_task:
                    progress_task.cancel()
                return live, dead
            
            if isinstance(result, Exception):
                # Log exception details for debugging
                dead += 1
            elif isinstance(result, tuple):
                is_live, domain = result
                if is_live:
                    live.append(domain)
                else:
                    dead += 1
            else:
                dead += 1
                
    except Exception as e:
        pass
    finally:
        if progress_task:
            try:
                progress_task.cancel()
                await progress_task
            except Exception:
                pass
    
    # Log DNS statistics for debugging
    print(f"DNS Stats: {dns_stats}")
    
    return live, dead


def dns_filter_sync(domains: list[str], max_workers: int = 100) -> tuple[list[str], int]:
    """Synchronous wrapper for DNS filter with reduced concurrency."""
    live, dead = [], 0
    with ThreadPoolExecutor(max_workers=min(max_workers, len(domains) or 1)) as ex:
        futures = {ex.submit(_dns_check_sync_robust, d): d for d in domains}
        for fut in futures:
            if fut.result():
                live.append(futures[fut])
            else:
                dead += 1
    return live, dead


# ── SSH Auto-Provisioning ─────────────────────────────────────────────────────

SLAVE_FILES = ["slave.py", "requirements.txt"]
REMOTE_DIR  = "/opt/scraper-slave"


def _provision_slave_ssh(sid: str, ip: str, user: str, password: str,
                         port: int, slave_port: int, slave_name: str,
                         master_url: str):
    """SSH into slave VPS, upload code, install deps, start service.
    Runs in a background thread — logs to provision_logs[sid]."""
    logs = provision_logs.setdefault(sid, [])

    def log(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        logs.append(f"[{ts}] {msg}")

    log(f"Connecting to {user}@{ip}:{port}...")
    slaves[sid]["status"] = "provisioning"
    slaves[sid]["provision_progress"] = "connecting"

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(ip, port=port, username=user, password=password,
                    timeout=15, banner_timeout=15, auth_timeout=15)
        log("✓ SSH connected")
        slaves[sid]["provision_progress"] = "connected"

        # 1. Create remote directory
        log(f"Creating {REMOTE_DIR}...")
        _exec(ssh, f"mkdir -p {REMOTE_DIR}")

        # 2. Upload slave files via SFTP
        log("Uploading slave code...")
        slaves[sid]["provision_progress"] = "uploading"
        sftp = ssh.open_sftp()
        for fname in SLAVE_FILES:
            local_path = SLAVE_DIR / fname
            if local_path.exists():
                sftp.put(str(local_path), f"{REMOTE_DIR}/{fname}")
                log(f"  ↑ {fname}")
            else:
                log(f"  ⚠ {fname} not found locally")
        sftp.close()

        # 3. Install system deps + Python venv
        log("Installing system packages...")
        slaves[sid]["provision_progress"] = "installing_system"
        _exec(ssh, "apt-get update -qq && apt-get install -y python3 python3-pip python3-venv",
              timeout=120)
        log("✓ System packages installed")

        # 4. Create venv + install Python deps
        log("Creating Python venv...")
        slaves[sid]["provision_progress"] = "installing_python"
        _exec(ssh, f"cd {REMOTE_DIR} && python3 -m venv venv", timeout=30)
        log("Installing Python packages...")
        _exec(ssh, f"cd {REMOTE_DIR} && venv/bin/pip install --upgrade pip -q && "
                    f"venv/bin/pip install -r requirements.txt -q",
              timeout=120)
        log("✓ Python dependencies installed")

        # 5. Create systemd service
        log("Configuring systemd service...")
        slaves[sid]["provision_progress"] = "configuring"
        service_content = f"""[Unit]
Description=Scraper Slave Agent - {slave_name}
After=network.target

[Service]
Type=simple
User={user}
WorkingDirectory={REMOTE_DIR}
ExecStart={REMOTE_DIR}/venv/bin/python -m uvicorn slave:app --host 0.0.0.0 --port {slave_port}
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=SLAVE_ID={sid}
Environment=SLAVE_PORT={slave_port}
Environment=MASTER_URL={master_url}

[Install]
WantedBy=multi-user.target
"""
        _exec(ssh, f"cat > /etc/systemd/system/scraper-slave.service << 'SERVICEEOF'\n"
                    f"{service_content}SERVICEEOF")
        _exec(ssh, "systemctl daemon-reload && systemctl enable scraper-slave && "
                    "systemctl restart scraper-slave")
        log("✓ Service started")

        # 6. Open firewall port
        log("Opening firewall...")
        _exec(ssh, f"ufw allow {slave_port}/tcp 2>/dev/null || true", timeout=10)

        # 7. Wait for slave to come up and verify health
        log(f"Waiting for slave to come online (port {slave_port})...")
        slaves[sid]["provision_progress"] = "verifying"
        time.sleep(3)

        # Quick health check from master side
        try:
            import requests
            r = requests.get(f"http://{ip}:{slave_port}/api/health", timeout=10)
            if r.status_code == 200:
                log("✓ Slave health check passed!")
            else:
                log(f"⚠ Health check returned {r.status_code}")
        except Exception as he:
            log(f"⚠ Health check failed (slave may still be starting): {str(he)[:60]}")

        # 8. Mark as ready
        slaves[sid]["status"] = "idle"
        slaves[sid]["provision_progress"] = "done"
        slaves[sid]["last_seen"] = datetime.now().isoformat()
        log("✅ Provisioning complete!")

    except paramiko.AuthenticationException:
        log("✗ Authentication failed — check user/password")
        slaves[sid]["status"] = "error"
        slaves[sid]["provision_progress"] = "auth_failed"
    except paramiko.SSHException as e:
        log(f"✗ SSH error: {str(e)[:100]}")
        slaves[sid]["status"] = "error"
        slaves[sid]["provision_progress"] = "ssh_error"
    except Exception as e:
        log(f"✗ Error: {str(e)[:150]}")
        slaves[sid]["status"] = "error"
        slaves[sid]["provision_progress"] = "failed"
    finally:
        ssh.close()


def _exec(ssh: paramiko.SSHClient, cmd: str, timeout: int = 30) -> str:
    """Execute command via SSH, return stdout. Raises on non-zero exit."""
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    if exit_code != 0 and err.strip():
        # Log stderr but don't raise for non-critical warnings
        pass
    return out


# ── Slave Heartbeat Monitor ───────────────────────────────────────────────────

HEARTBEAT_INTERVAL = 15   # seconds between health checks
HEARTBEAT_TIMEOUT  = 45   # mark dead after this many seconds without response
_monitor_task = None


async def _heartbeat_monitor():
    """Background task: pings all registered slaves every HEARTBEAT_INTERVAL."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        if not slaves:
            continue

        async with httpx.AsyncClient(timeout=8.0) as client:
            for sid, s in list(slaves.items()):
                if s.get("status") == "provisioning":
                    continue  # Don't ping during provisioning
                try:
                    r = await client.get(f"{s['url']}/api/health")
                    data = r.json()
                    s["last_seen"] = datetime.now().isoformat()
                    s["system_stats"] = data.get("system_stats", {})

                    # If it was marked dead/error but responded, revive
                    if s["status"] in ("dead", "offline", "error"):
                        s["status"] = "idle"

                except Exception:
                    # Calculate time since last seen
                    try:
                        last = datetime.fromisoformat(s.get("last_seen", ""))
                        elapsed = (datetime.now() - last).total_seconds()
                    except Exception:
                        elapsed = 999

                    if elapsed > HEARTBEAT_TIMEOUT:
                        s["status"] = "offline"


@app.on_event("startup")
async def start_monitor():
    global _monitor_task
    # Load persisted state on startup
    load_state()
    _monitor_task = asyncio.create_task(_heartbeat_monitor())


# ── Slave Management ───────────────────────────────────────────────────────────

@app.post("/api/slaves")
async def register_slave(data: dict):
    """Register a slave (manual registration or from provisioning)."""
    sid = data.get("id") or str(uuid.uuid4())[:8]
    slaves[sid] = {
        "id": sid,
        "url": data["url"].rstrip("/"),
        "name": data.get("name", f"Slave-{sid[:4]}"),
        "ip": data.get("ip", ""),
        "status": "idle",
        "last_seen": datetime.now().isoformat(),
        "domains_assigned": 0,
        "domains_done": 0,
        "emails_found": 0,
        "system_stats": {},
        "provision_progress": "manual",
    }
    save_state()
    return {"ok": True, "slave_id": sid}


@app.post("/api/slaves/provision")
async def provision_slave(data: dict):
    """Auto-provision a slave VPS: SSH in, upload code, install, start."""
    ip = data.get("ip", "").strip()
    user = data.get("user", "root").strip()
    password = data.get("password", "").strip()
    ssh_port = int(data.get("ssh_port", 22))
    slave_port = int(data.get("slave_port", 8001))
    name = data.get("name", f"Slave-{ip.split('.')[-1] if ip else 'new'}").strip()

    if not ip or not password:
        raise HTTPException(400, "IP and password are required")

    sid = str(uuid.uuid4())[:8]
    master_url = data.get("master_url", "").strip()
    if not master_url:
        # Auto-detect: assume master is accessible from slave at this IP
        master_url = f"http://{data.get('master_ip', ip)}:8000"

    # Register slave immediately (status=provisioning)
    slaves[sid] = {
        "id": sid,
        "url": f"http://{ip}:{slave_port}",
        "name": name,
        "ip": ip,
        "status": "provisioning",
        "last_seen": datetime.now().isoformat(),
        "domains_assigned": 0,
        "domains_done": 0,
        "emails_found": 0,
        "system_stats": {},
        "provision_progress": "queued",
    }

    # Run provisioning in background thread (blocking SSH calls)
    thread = threading.Thread(
        target=_provision_slave_ssh,
        args=(sid, ip, user, password, ssh_port, slave_port, name, master_url),
        daemon=True,
    )
    thread.start()

    return {"ok": True, "slave_id": sid, "status": "provisioning"}


@app.get("/api/slaves/{slave_id}/logs")
async def get_provision_logs(slave_id: str):
    """Get provisioning log for a slave."""
    return {"logs": provision_logs.get(slave_id, [])}


@app.delete("/api/slaves/{slave_id}")
async def remove_slave(slave_id: str):
    if slave_id in slaves:
        del slaves[slave_id]
        provision_logs.pop(slave_id, None)
        save_state()
        return {"ok": True}
    raise HTTPException(404, "Slave not found")


@app.get("/api/slaves")
async def list_slaves():
    return list(slaves.values())


@app.put("/api/slaves/{slave_id}")
async def update_slave(slave_id: str, data: dict):
    """Update slave settings (name)."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    if "name" in data:
        slaves[slave_id]["name"] = data["name"].strip()
    save_state()
    return {"ok": True, "slave": slaves[slave_id]}


@app.post("/api/slaves/{slave_id}/reboot")
async def reboot_slave(slave_id: str):
    """Reboot the slave VPS."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    slave = slaves[slave_id]
    slave_url = slave.get("url", "")
    slave_ip = slave.get("ip", "")
    
    # Try to trigger reboot via SSH or API
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Try to call reboot endpoint on slave
            r = await client.post(f"{slave_url}/api/system/reboot")
            if r.status_code == 200:
                slave["status"] = "offline"
                return {"ok": True, "message": "Reboot initiated"}
    except Exception:
        pass
    
    # If we have IP, try SSH reboot
    if slave_ip:
        try:
            import paramiko
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # This would require stored credentials - for now just mark status
            slave["status"] = "offline"
            return {"ok": True, "message": "Manual reboot required - SSH credentials not stored"}
        except Exception:
            pass
    
    return {"ok": False, "message": "Could not reboot slave"}


@app.post("/api/slaves/{slave_id}/restart-service")
async def restart_slave_service(slave_id: str):
    """Restart the scraper-slave service (clears memory)."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    slave = slaves[slave_id]
    slave_url = slave.get("url", "")
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(f"{slave_url}/api/system/restart-service")
            if r.status_code == 200:
                return {"ok": True, "message": "Service restarted"}
    except Exception as e:
        return {"ok": False, "message": f"Failed to restart: {str(e)[:50]}"}
    
    return {"ok": False, "message": "Slave not responding"}


@app.get("/api/slaves/{slave_id}/jobs")
async def get_slave_jobs(slave_id: str):
    """Get jobs assigned to this slave."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    assigned_jobs = []
    for jid, job in jobs.items():
        if slave_id in job.get("domains_per_slave", {}):
            assigned_jobs.append({
                "id": jid,
                "name": job.get("name", jid),
                "status": job["status"],
                "domains_assigned": job["domains_per_slave"].get(slave_id, 0),
                "emails_found": slaves[slave_id].get("emails_found", 0),
                "domains_done": slaves[slave_id].get("domains_done", 0)
            })
    
    return {"jobs": assigned_jobs}


@app.post("/api/slaves/{slave_id}/pause")
async def pause_slave(slave_id: str):
    """Pause all jobs on a slave."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    slave = slaves[slave_id]
    slave_url = slave.get("url", "")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{slave_url}/api/system/pause")
            if r.status_code == 200:
                slave["status"] = "paused"
                save_state()
                return {"ok": True, "message": "Slave paused"}
    except Exception as e:
        return {"ok": False, "message": f"Failed: {str(e)[:50]}"}
    
    return {"ok": False, "message": "Slave not responding"}


@app.post("/api/slaves/{slave_id}/resume")
async def resume_slave(slave_id: str):
    """Resume all jobs on a slave."""
    if slave_id not in slaves:
        raise HTTPException(404, "Slave not found")
    
    slave = slaves[slave_id]
    slave_url = slave.get("url", "")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{slave_url}/api/system/resume")
            if r.status_code == 200:
                slave["status"] = "idle"
                save_state()
                return {"ok": True, "message": "Slave resumed"}
    except Exception as e:
        return {"ok": False, "message": f"Failed: {str(e)[:50]}"}
    
    return {"ok": False, "message": "Slave not responding"}


@app.post("/api/slaves/{slave_id}/heartbeat")
async def slave_heartbeat(slave_id: str, data: dict):
    """Slave calls this periodically to report progress + system metrics."""
    if slave_id not in slaves:
        raise HTTPException(404, "Unknown slave")
    s = slaves[slave_id]
    s["last_seen"] = datetime.now().isoformat()
    s["status"] = data.get("status", s["status"])
    s["domains_done"] = data.get("domains_done", s["domains_done"])
    s["emails_found"] = data.get("emails_found", s["emails_found"])
    if "system_stats" in data:
        s["system_stats"] = data["system_stats"]
    return {"ok": True}


# ── Job Orchestration ──────────────────────────────────────────────────────────

@app.post("/api/jobs/upload")
async def upload_domains(file: UploadFile = File(...), name: str = Form(None)):
    """Upload domain list — stored for processing."""
    content = (await file.read()).decode("utf-8", errors="ignore")
    lines = content.strip().splitlines()
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:6]
    fpath = UPLOAD_DIR / f"{job_id}.txt"
    fpath.write_text(content, encoding="utf-8")
    
    # Generate name from filename if not provided
    if not name:
        name = file.filename.rsplit('.', 1)[0] if file.filename else f"Job {job_id[:13]}"
    
    jobs[job_id] = {
        "id": job_id,
        "name": name,
        "status": "uploaded",
        "file": str(fpath),
        "domains_raw": len([l for l in lines if l.strip() and not l.startswith('#')]),
        "domains_cleaned": 0,
        "domains_live": 0,
        "domains_per_slave": {},
        "emails": [],
        "emails_count": 0,
        "created_at": datetime.now().isoformat(),
        "started_at": None,
        "finished_at": None,
        "clean_stats": {},
        "dns_stats": {},
        "dns_progress": None,
        "error": None,
    }
    save_state()
    return {"ok": True, "job_id": job_id, "name": name, "domains_raw": jobs[job_id]["domains_raw"]}


@app.post("/api/jobs/{job_id}/start")
async def start_job(job_id: str, workers: int = 12, turbo: bool = True,
                    dns_on: bool = True):
    """Clean, DNS filter, split and dispatch to slaves."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    job = jobs[job_id]
    if job["status"] not in ("uploaded", "failed"):
        raise HTTPException(400, f"Job is {job['status']}")

    active_slaves = {k: v for k, v in slaves.items()
                     if v["status"] not in ("dead", "offline", "provisioning", "error")}
    if not active_slaves:
        raise HTTPException(400, "No active slaves available — provision or check connections")

    job["status"] = "processing"
    job["started_at"] = datetime.now().isoformat()
    job["error"] = None

    asyncio.create_task(_run_job(job_id, workers, turbo, dns_on))
    return {"ok": True, "status": "processing"}


async def _run_job(job_id: str, workers: int, turbo: bool, dns_on: bool):
    job = jobs[job_id]
    try:
        # 1. Load raw domains
        raw_lines = Path(job["file"]).read_text(encoding="utf-8").splitlines()

        # 2. Clean
        job["status"] = "cleaning"
        cleaned, cstats = clean_domains(raw_lines, phase2=True)
        job["domains_cleaned"] = len(cleaned)
        job["clean_stats"] = cstats

        # 3. DNS filter
        if dns_on and cleaned:
            job["status"] = "dns_filtering"
            live, dead = await dns_filter_async(cleaned, job_id)  # Pass job_id for cancellation support
            job["domains_live"] = len(live)
            job["dns_stats"] = {"alive": len(live), "dead": dead}
        else:
            live = cleaned
            job["domains_live"] = len(live)

        if not live:
            job["status"] = "completed"
            job["finished_at"] = datetime.now().isoformat()
            return

        # 4. Split across active slaves
        active = {k: v for k, v in slaves.items()
                  if v["status"] not in ("dead", "offline", "provisioning", "error")}
        slave_ids = list(active.keys())
        n = len(slave_ids)
        chunk_size = len(live) // n
        remainder = len(live) % n

        chunks = {}
        idx = 0
        for i, sid in enumerate(slave_ids):
            size = chunk_size + (1 if i < remainder else 0)
            chunks[sid] = live[idx:idx+size]
            idx += size
            job["domains_per_slave"][sid] = len(chunks[sid])
            active[sid]["domains_assigned"] = len(chunks[sid])
            active[sid]["domains_done"] = 0
            active[sid]["emails_found"] = 0
            active[sid]["status"] = "scraping"

        # 5. Dispatch to slaves
        job["status"] = "scraping"
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = []
            for sid, chunk in chunks.items():
                url = active[sid]["url"]
                tasks.append(
                    client.post(f"{url}/api/scrape", json={
                        "job_id": job_id,
                        "master_url": "",
                        "domains": chunk,
                        "workers": workers,
                        "turbo": turbo,
                    })
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                sid = slave_ids[i]
                if isinstance(r, Exception):
                    slaves[sid]["status"] = "error"
                    job["error"] = f"Slave {sid}: {str(r)[:100]}"

        # 6. Poll slaves until done
        await _poll_until_done(job_id)

    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)


async def _poll_until_done(job_id: str):
    """Poll slave progress until all done with progress tracking."""
    global job_cancel_flags
    
    job = jobs[job_id]
    active = {k: v for k, v in slaves.items()
              if k in job["domains_per_slave"]}
    
    # Track progress
    total_domains = sum(job["domains_per_slave"].values())
    job["progress"] = {"percent": 0, "elapsed_seconds": 0, "estimated_seconds": 0, "rate": 0}
    start_time = datetime.now()
    
    while True:
        await asyncio.sleep(5)
        
        # Check if job was cancelled
        if job_cancel_flags.get(job_id, False) or job.get("status") == "cancelled":
            job["status"] = "cancelled"
            save_state()
            return
        
        all_done = True
        total_emails = []
        total_done = 0
        any_scraping = False

        async with httpx.AsyncClient(timeout=15.0) as client:
            for sid, sinfo in active.items():
                try:
                    r = await client.get(f"{sinfo['url']}/api/status/{job_id}")
                    data = r.json()
                    sinfo["status"] = data.get("status", "unknown")
                    sinfo["domains_done"] = data.get("domains_done", 0)
                    sinfo["emails_found"] = data.get("emails_found", 0)
                    sinfo["last_seen"] = datetime.now().isoformat()
                    total_done += data.get("domains_done", 0)

                    status = data.get("status", "unknown")
                    if status not in ("completed", "failed", "cancelled"):
                        all_done = False
                    if status == "scraping":
                        any_scraping = True
                    if data.get("emails"):
                        total_emails.extend(data["emails"])
                except Exception:
                    all_done = False

        unique = list(set(total_emails))
        job["emails"] = unique
        job["emails_count"] = len(unique)
        
        # Calculate progress
        elapsed = (datetime.now() - start_time).total_seconds()
        if total_domains > 0 and total_done > 0:
            percent = min(100, (total_done / total_domains) * 100)
            rate = total_done / elapsed if elapsed > 0 else 0
            remaining = total_domains - total_done
            estimated = remaining / rate if rate > 0 else 0
            job["progress"] = {
                "percent": round(percent, 1),
                "elapsed_seconds": round(elapsed),
                "estimated_seconds": round(estimated),
                "rate": round(rate, 1),
                "domains_done": total_done,
                "domains_total": total_domains
            }
        
        save_state()  # Save progress periodically

        # If all slaves report done/cancelled/failed, exit
        if all_done and not any_scraping:
            break

    # Only mark as completed if not cancelled
    if job.get("status") != "cancelled":
        job["status"] = "completed"
    job["finished_at"] = datetime.now().isoformat()

    result_file = RESULT_DIR / f"{job_id}_emails.txt"
    result_file.write_text("\n".join(sorted(set(job["emails"]))), encoding="utf-8")


# ── Slave email collection endpoint ───────────────────────────────────────────

@app.post("/api/jobs/{job_id}/emails")
async def collect_emails(job_id: str, data: dict):
    """Slave POSTs scraped emails here."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    new_emails = data.get("emails", [])
    slave_id = data.get("slave_id", "unknown")
    job = jobs[job_id]
    existing = set(job["emails"])
    existing.update(new_emails)
    job["emails"] = list(existing)
    job["emails_count"] = len(existing)

    if slave_id in slaves:
        slaves[slave_id]["emails_found"] = data.get("total_emails", len(new_emails))
        slaves[slave_id]["domains_done"] = data.get("domains_done", 0)

    result_file = RESULT_DIR / f"{job_id}_emails.txt"
    result_file.write_text("\n".join(sorted(existing)), encoding="utf-8")
    return {"ok": True, "total": len(existing)}


# ── Download results ───────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}/download")
async def download_emails(job_id: str):
    fpath = RESULT_DIR / f"{job_id}_emails.txt"
    if not fpath.exists():
        if job_id in jobs and jobs[job_id]["emails"]:
            fpath.write_text("\n".join(sorted(set(jobs[job_id]["emails"]))), encoding="utf-8")
        else:
            raise HTTPException(404, "No results yet")
    return FileResponse(str(fpath), filename=f"emails_{job_id}.txt",
                        media_type="text/plain")


# ── API: Jobs listing ──────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs(search: str = None, page: int = 1, limit: int = 50):
    """List jobs with search and pagination. Returns most recent first."""
    all_jobs = [
        {k: v for k, v in j.items() if k != "emails"}
        for j in jobs.values()
    ]
    
    # Filter by search term
    if search:
        search_lower = search.lower()
        all_jobs = [j for j in all_jobs if 
                    search_lower in j.get("name", "").lower() or
                    search_lower in j.get("id", "").lower() or
                    search_lower in j.get("status", "").lower()]
    
    # Sort by created_at descending (most recent first)
    all_jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    # Pagination
    total = len(all_jobs)
    start = (page - 1) * limit
    end = start + limit
    paginated = all_jobs[start:end]
    
    return {
        "jobs": paginated,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404)
    j = dict(jobs[job_id])
    j["emails_preview"] = j["emails"][:20]
    j["emails"] = None
    return j


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job and notify all slaves to stop."""
    global job_cancel_flags
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    job = jobs[job_id]
    if job["status"] in ("completed", "failed", "cancelled"):
        raise HTTPException(400, f"Cannot cancel job with status: {job['status']}")
    
    # Set cancellation flag
    job_cancel_flags[job_id] = True
    job["status"] = "cancelled"
    job["finished_at"] = datetime.now().isoformat()
    job["error"] = "Job cancelled by user"
    
    # Notify all slaves to cancel this job
    await _notify_slaves_cancel(job_id)
    
    return {"ok": True, "status": "cancelled"}


async def _notify_slaves_cancel(job_id: str):
    """Send cancellation signal to all slaves working on a job."""
    job = jobs.get(job_id)
    if not job:
        return
    
    # Get slaves assigned to this job
    assigned_slaves = list(job.get("domains_per_slave", {}).keys())
    if not assigned_slaves:
        return
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = []
        for sid in assigned_slaves:
            if sid in slaves:
                slave_url = slaves[sid].get("url")
                if slave_url:
                    tasks.append(
                        client.post(f"{slave_url}/api/jobs/{job_id}/cancel")
                    )
        
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass  # Ignore errors - slaves may be unreachable


@app.put("/api/jobs/{job_id}/rename")
async def rename_job(job_id: str, data: dict):
    """Rename a job."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    new_name = data.get("name", "").strip()
    if not new_name:
        raise HTTPException(400, "Name is required")
    
    jobs[job_id]["name"] = new_name
    return {"ok": True, "name": new_name}


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "slaves": list(slaves.values()),
        "jobs": list(jobs.values()),
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=8000, reload=True)
