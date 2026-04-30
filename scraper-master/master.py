"""
Scraper Master — FastAPI server with dashboard, domain cleaning, DNS filtering,
SSH auto-provisioning, slave monitoring/heartbeat, and email collection.
Uses PostgreSQL for persistent storage.
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
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
import paramiko
from fastapi import FastAPI, File, Form, UploadFile, Request, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Database
import database as db

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
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)

# ── Provisioning logs:  {slave_id: [log_lines]} ────────────────────────────────
provision_logs: dict[str, list] = {}

# ── In-memory slaves cache for provisioning (synced with database) ─────────────
slaves: dict[str, dict] = {}

# ── Main event loop reference (for thread-safe database access) ────────────────
_main_loop: asyncio.AbstractEventLoop = None

# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown events."""
    global _monitor_task
    # Startup
    print("Initializing database...")
    await db.init_database()
    print("Database connected and tables created")
    # Store main event loop reference for thread-safe DB access
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    # Load slaves from database into memory on startup
    db_slaves = await db.list_slaves()
    for s in db_slaves:
        slaves[s["id"]] = s
    print(f"Loaded {len(db_slaves)} slaves from database")
    # Start heartbeat monitor
    _monitor_task = asyncio.create_task(_heartbeat_monitor())
    print("Heartbeat monitor started")
    yield
    # Shutdown
    print("Stopping heartbeat monitor...")
    if _monitor_task:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
    print("Closing database connection...")
    await db.close_database()

app = FastAPI(title="Scraper Master", version="2.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

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

DNS_TIMEOUT = 3.0  # 3 seconds timeout for DNS resolution
DNS_MAX_WORKERS = 1000  # 1000 concurrent workers
DNS_MAX_RETRIES = 1  # 1 retry for failed lookups
DNS_RETRY_DELAY = 0.3  # 300ms delay between retries
BATCH_SIZE = 2000  # Process in 2000-domain batches

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
    Tries both IPv4 and IPv6.
    """
    orig = socket.getdefaulttimeout()
    
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
    
    # Process in batches
    all_tasks = []
    
    for i in range(0, len(domains), BATCH_SIZE):
        batch = domains[i:i + BATCH_SIZE]
        batch_tasks = [check_one(d, j) for j, d in enumerate(batch)]
        all_tasks.extend(batch_tasks)
        
        # Brief pause between batch creation to allow DNS resolver recovery
        if i + BATCH_SIZE < len(domains):
            await asyncio.sleep(0.05)
    
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

        # 3. Check if python3 + venv already installed — skip apt if present
        slaves[sid]["provision_progress"] = "installing_system"
        has_python = _exec(ssh, "which python3 2>/dev/null && python3 --version", timeout=10).strip()
        if has_python and "Python 3" in has_python:
            log(f"✓ Python3 already installed: {has_python.split()[-1]}")
            # Still ensure python3-venv is available (often missing on minimal VPS)
            py_ver = _exec(ssh, "python3 -c 'import sys; print(f\"python3.{sys.version_info.minor}-venv\")'", timeout=10).strip()
            has_venv = _exec(ssh, f"dpkg -l {py_ver} 2>/dev/null | grep -q ^ii && echo YES || echo NO", timeout=10).strip()
            if has_venv != "YES":
                log(f"Installing {py_ver} (may take 30-60s)...")
                _exec(ssh, f"apt-get update -qq && apt-get install -y {py_ver} --no-install-recommends -qq", timeout=300)
                log(f"✓ {py_ver} installed")
        else:
            log("Installing python3 + venv (may take 60-120s on fresh VPS)...")
            _exec(ssh, "apt-get update -qq && apt-get install -y python3 python3-venv python3.12-venv --no-install-recommends -qq",
                  timeout=300)
            log("✓ Python3 installed")

        # 4. Create venv + install Python deps (skip if venv already has everything)
        log("Setting up Python environment...")
        slaves[sid]["provision_progress"] = "installing_python"
        venv_ok = _exec(ssh, f"test -f {REMOTE_DIR}/venv/bin/python && {REMOTE_DIR}/venv/bin/python -c 'import fastapi,uvicorn,httpx,requests,lxml,cssselect,colorama,pydantic' 2>/dev/null && echo OK", timeout=10).strip()
        if venv_ok.endswith("OK"):
            log("✓ Python venv and packages already installed — skipping")
        else:
            # Try to create venv with error checking
            try:
                _exec(ssh, f"cd {REMOTE_DIR} && python3 -m venv venv --clear", timeout=60, check=True)
            except RuntimeError as e:
                log(f"⚠ venv creation failed: {str(e)[:100]}")
                # Likely missing python3.X-venv package, try to install it
                log("Installing python3-venv package...")
                py_ver = _exec(ssh, "python3 -c 'import sys; print(f\"python3.{sys.version_info.minor}-venv\")'", timeout=10).strip()
                try:
                    _exec(ssh, f"apt-get update -qq && apt-get install -y {py_ver} --no-install-recommends -qq", timeout=300, check=True)
                    log(f"✓ {py_ver} installed, retrying venv creation...")
                    _exec(ssh, f"cd {REMOTE_DIR} && python3 -m venv venv --clear", timeout=60, check=True)
                except RuntimeError as apt_err:
                    log(f"✗ Failed to install {py_ver}: {str(apt_err)[:100]}")
                    raise
            
            # Verify venv was created successfully
            venv_check = _exec(ssh, f"test -f {REMOTE_DIR}/venv/bin/pip && echo OK || echo FAIL", timeout=10).strip()
            if venv_check != "OK":
                log("⚠ pip not found in venv, trying --without-pip fallback...")
                _exec(ssh, f"rm -rf {REMOTE_DIR}/venv && cd {REMOTE_DIR} && python3 -m venv venv --without-pip", timeout=60)
                # Install pip via get-pip.py
                _exec(ssh, f"cd {REMOTE_DIR} && curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py && {REMOTE_DIR}/venv/bin/python get-pip.py && rm get-pip.py", timeout=120)
            
            # Final verification before installing packages
            final_check = _exec(ssh, f"test -f {REMOTE_DIR}/venv/bin/pip && echo OK || echo FAIL", timeout=10).strip()
            if final_check != "OK":
                raise RuntimeError("venv/bin/pip does not exist after all attempts")
            
            log("Installing Python packages (may take 30-90s)...")
            _exec(ssh, f"cd {REMOTE_DIR} && venv/bin/pip install --upgrade pip -q && "
                        f"venv/bin/pip install --no-cache-dir -r requirements.txt -q",
                  timeout=300)
            log("✓ Python dependencies installed")

        # 5. Create systemd service
        log("Configuring systemd service...")
        slaves[sid]["provision_progress"] = "configuring"
        
        # Stop any existing service first to clear old SLAVE_ID
        _exec(ssh, "systemctl stop scraper-slave 2>/dev/null || true")
        # Kill any leftover python processes on the slave port
        _exec(ssh, f"pkill -f 'uvicorn.*slave:app.*{slave_port}' 2>/dev/null || true")
        
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
                    "systemctl start scraper-slave")
        log("✓ Service started with fresh SLAVE_ID")

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

        # 8. Mark as ready in memory
        slaves[sid]["status"] = "idle"
        slaves[sid]["provision_progress"] = "done"
        slaves[sid]["last_seen"] = datetime.now().isoformat()
        log("✅ Provisioning complete!")
        
        # 9. Update slave status in database via main event loop
        log("Updating slave status in database...")
        try:
            if _main_loop and not _main_loop.is_closed():
                future = asyncio.run_coroutine_threadsafe(
                    db.update_slave(sid, status="idle"),
                    _main_loop
                )
                future.result(timeout=10)  # Wait up to 10s for the update
                log("✓ Slave status updated in database")
            else:
                log("⚠ Main event loop not available, skipping DB update")
        except Exception as db_err:
            log(f"⚠ Database update failed: {str(db_err)[:100]}")

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


def _exec(ssh: paramiko.SSHClient, cmd: str, timeout: int = 30, check: bool = False) -> str:
    """Execute command via SSH, return stdout. If check=True, raises on non-zero exit."""
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    if check and exit_code != 0:
        raise RuntimeError(f"Command failed (exit={exit_code}): {cmd}\nstderr: {err[:200]}")
    return out


# ── Slave Heartbeat Monitor ───────────────────────────────────────────────────

HEARTBEAT_INTERVAL = 15   # seconds between health checks
HEARTBEAT_TIMEOUT  = 45   # mark dead after this many seconds without response
_monitor_task = None


def _parse_last_seen(last_seen) -> datetime:
    """Parse last_seen value - handles both datetime objects and ISO strings."""
    if last_seen is None:
        return None
    if isinstance(last_seen, datetime):
        return last_seen
    try:
        return datetime.fromisoformat(str(last_seen).replace('Z', '+00:00'))
    except Exception:
        return None


async def _heartbeat_monitor():
    """Background task: pings all registered slaves every HEARTBEAT_INTERVAL.
    Updates both in-memory cache AND database with system stats."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        
        # Add new slaves from database (don't overwrite existing in-memory data)
        try:
            db_slaves = await db.list_slaves()
            for s in db_slaves:
                sid = s["id"]
                if sid not in slaves:
                    # New slave from DB - add to memory
                    slaves[sid] = s
                elif slaves[sid].get("status") == "provisioning":
                    # Sync provisioning progress from thread
                    slaves[sid]["provision_progress"] = s.get("provision_progress", "")
        except Exception as e:
            print(f"[HEARTBEAT] Failed to load slaves from DB: {e}")
        
        if not slaves:
            continue

        async with httpx.AsyncClient(timeout=8.0) as client:
            for sid, s in list(slaves.items()):
                if s.get("status") == "provisioning":
                    continue  # Don't ping during provisioning
                
                slave_url = s.get("url", "")
                if not slave_url:
                    continue
                    
                try:
                    r = await client.get(f"{slave_url}/api/health")
                    data = r.json()
                    now_dt = datetime.now()
                    system_stats = data.get("system_stats", {})
                    
                    # Update in-memory (keep datetime object, not string)
                    slaves[sid]["last_seen"] = now_dt
                    slaves[sid]["system_stats"] = system_stats

                    # Update database
                    update_data = {
                        "last_seen": now_dt,
                        "system_stats": system_stats
                    }
                    
                    # If it was marked dead/error but responded, revive
                    if s.get("status") in ("dead", "offline", "error"):
                        slaves[sid]["status"] = "idle"
                        update_data["status"] = "idle"
                    
                    try:
                        await db.update_slave(sid, **update_data)
                    except Exception as e:
                        print(f"[HEARTBEAT] Failed to update slave {sid} in DB: {e}")

                except Exception as e:
                    # Calculate time since last seen
                    last = _parse_last_seen(s.get("last_seen"))
                    if last is None:
                        elapsed = HEARTBEAT_TIMEOUT + 1  # Force offline if never seen
                    else:
                        elapsed = (datetime.now() - last).total_seconds()

                    if elapsed > HEARTBEAT_TIMEOUT:
                        if s.get("status") != "offline":
                            print(f"[HEARTBEAT] Slave {sid} marked offline (no response for {int(elapsed)}s)")
                            slaves[sid]["status"] = "offline"
                            try:
                                await db.update_slave(sid, status="offline")
                            except Exception:
                                pass


# ── Slave Management ───────────────────────────────────────────────────────────

@app.post("/api/slaves")
async def register_slave(data: dict):
    """Register a slave (manual registration or from provisioning)."""
    sid = data.get("id") or str(uuid.uuid4())[:8]
    url = data.get("url", "").strip()
    name = data.get("name", f"Slave {sid}").strip()
    ip = data.get("ip", "").strip()
    
    if not url:
        raise HTTPException(400, "URL is required")
    
    slave = await db.register_slave(sid, url, name, ip)
    return {"ok": True, "slave_id": sid, "slave": slave}


@app.post("/api/slaves/provision")
async def provision_slave(data: dict, request: Request):
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
        # Auto-detect master URL from request host
        master_url = str(request.base_url).rstrip("/")

    # Register slave immediately in memory and database (status=provisioning)
    slave_url = f"http://{ip}:{slave_port}"
    slaves[sid] = {
        "id": sid,
        "url": slave_url,
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
    
    # Register in database from async context (before thread starts)
    await db.register_slave(sid, slave_url, name, ip)
    await db.update_slave(sid, status="provisioning")

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
    deleted = await db.delete_slave(slave_id)
    if deleted:
        return {"ok": True}
    raise HTTPException(404, "Slave not found")


@app.get("/api/slaves")
async def list_slaves_endpoint():
    slaves = await db.list_slaves()
    return slaves


@app.put("/api/slaves/{slave_id}")
async def update_slave_endpoint(slave_id: str, data: dict):
    """Update slave settings (name)."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
    name = data.get("name", "").strip()
    if name:
        await db.update_slave(slave_id, name=name)
    
    updated = await db.get_slave(slave_id)
    return {"ok": True, "slave": updated}


@app.post("/api/slaves/{slave_id}/reboot")
async def reboot_slave(slave_id: str):
    """Reboot the slave VPS."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
    slave_url = slave.get("url", "")
    ip = slave.get("ip", "")
    
    # Try system reboot endpoint first
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{slave_url}/api/system/reboot")
            if r.status_code == 200:
                await db.update_slave(slave_id, status="rebooting")
                return {"ok": True, "message": "Reboot initiated"}
    except Exception:
        pass
    
    # Fallback: Manual reboot required
    if ip:
        await db.update_slave(slave_id, status="offline")
        return {"ok": True, "message": "Manual reboot required - SSH credentials not stored"}
    
    return {"ok": False, "message": "Could not reboot slave"}


@app.post("/api/slaves/{slave_id}/restart-service")
async def restart_slave_service(slave_id: str):
    """Restart the scraper-slave service (clears memory)."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
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
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
    assignments = await db.get_job_assignments_for_slave(slave_id)
    return {"jobs": assignments}


@app.post("/api/slaves/{slave_id}/pause")
async def pause_slave(slave_id: str):
    """Pause all jobs on a slave."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
    slave_url = slave.get("url", "")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{slave_url}/api/system/pause")
            if r.status_code == 200:
                await db.update_slave(slave_id, status="paused")
                return {"ok": True, "message": "Slave paused"}
    except Exception as e:
        return {"ok": False, "message": f"Failed: {str(e)[:50]}"}
    
    return {"ok": False, "message": "Slave not responding"}


@app.post("/api/slaves/{slave_id}/resume")
async def resume_slave(slave_id: str):
    """Resume all jobs on a slave."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Slave not found")
    
    slave_url = slave.get("url", "")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{slave_url}/api/system/resume")
            if r.status_code == 200:
                await db.update_slave(slave_id, status="idle")
                return {"ok": True, "message": "Slave resumed"}
    except Exception as e:
        return {"ok": False, "message": f"Failed: {str(e)[:50]}"}
    
    return {"ok": False, "message": "Slave not responding"}


@app.post("/api/slaves/{slave_id}/heartbeat")
async def slave_heartbeat(slave_id: str, data: dict):
    """Slave calls this periodically to report progress + system metrics."""
    slave = await db.get_slave(slave_id)
    if not slave:
        raise HTTPException(404, "Unknown slave")
    
    now_dt = datetime.now()
    
    # Update slave status
    update_fields = {
        "last_seen": now_dt,
        "status": data.get("status", slave.get("status", "idle")),
        "domains_done": data.get("domains_done", slave.get("domains_done", 0)),
        "emails_found": data.get("emails_found", slave.get("emails_found", 0)),
    }
    
    if data.get("system_stats"):
        update_fields["system_stats"] = data.get("system_stats")
    
    # Update database
    await db.update_slave(slave_id, **update_fields)
    
    # Update in-memory cache for immediate UI sync
    if slave_id in slaves:
        slaves[slave_id].update(update_fields)
        slaves[slave_id]["last_seen"] = now_dt
    
    return {"ok": True}


# ── Job Orchestration ──────────────────────────────────────────────────────────

@app.post("/api/jobs/upload")
async def upload_domains(file: UploadFile = File(...), name: str = Form(None)):
    """Upload domain list — stored for processing."""
    try:
        # Ensure upload directory exists
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        
        content = (await file.read()).decode("utf-8", errors="ignore")
        lines = content.strip().splitlines()
        job_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:6]
        job_dir = UPLOAD_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        fpath = job_dir / "domains.txt"
        fpath.write_text(content, encoding="utf-8")
        
        # Generate name from filename if not provided
        if not name:
            name = file.filename.rsplit('.', 1)[0] if file.filename else f"Job {job_id[:13]}"
        
        # Create job in database
        domains_raw = len([l for l in lines if l.strip() and not l.startswith('#')])
        await db.create_job(job_id, name, str(fpath))
        await db.update_job(job_id, domains_total=domains_raw)
        
        return {"ok": True, "job_id": job_id, "name": name, "domains_raw": domains_raw}
    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] Upload failed: {error_detail}")
        raise HTTPException(500, f"Upload failed: {str(e)}")


@app.post("/api/jobs/{job_id}/start")
async def start_job(request: Request, job_id: str, workers: int = 12, turbo: bool = True,
                    dns_on: bool = True):
    """Clean, DNS filter, split and dispatch to slaves."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    if job["status"] not in ("uploaded", "failed"):
        raise HTTPException(400, f"Job is {job['status']}")

    # Get active slaves from database
    active_slaves = await db.list_slaves()
    active_slaves = {s["id"]: s for s in active_slaves 
                     if s.get("status") not in ("dead", "offline", "provisioning", "error")}
    
    if not active_slaves:
        raise HTTPException(400, "No active slaves available — provision or check connections")

    await db.update_job(job_id, status="processing", started_at=datetime.now(), error=None)

    master_url = str(request.base_url).rstrip("/")

    asyncio.create_task(_run_job(job_id, workers, turbo, dns_on, master_url))
    return {"ok": True, "status": "processing"}


async def _run_job(job_id: str, workers: int, turbo: bool, dns_on: bool, master_url: str):
    job = await db.get_job(job_id)
    if not job:
        print(f"[ERROR] Job {job_id} not found in database")
        return
    
    try:
        print(f"[JOB {job_id}] Starting job processing...")
        
        # 1. Load raw domains
        file_path = job.get("file_path")
        
        # Try multiple path formats (old: JOB_ID.txt, new: JOB_ID/domains.txt)
        candidates = []
        if file_path:
            candidates.append(Path(file_path))
        # New format: uploads/JOB_ID/domains.txt
        candidates.append(UPLOAD_DIR / job_id / "domains.txt")
        # Old format: uploads/JOB_ID.txt
        candidates.append(UPLOAD_DIR / f"{job_id}.txt")
        
        resolved_path = None
        for p in candidates:
            if p.exists():
                resolved_path = p
                break
        
        if not resolved_path:
            raise FileNotFoundError(f"Job file not found. Tried: {[str(p) for p in candidates]}")
        
        # Update file_path in DB if it changed
        if str(resolved_path) != file_path:
            await db.update_job(job_id, file_path=str(resolved_path))
        
        print(f"[JOB {job_id}] Loading domains from {resolved_path}")
        raw_lines = resolved_path.read_text(encoding="utf-8").splitlines()
        print(f"[JOB {job_id}] Loaded {len(raw_lines)} raw lines")

        # 2. Clean
        await db.update_job(job_id, status="cleaning")
        print(f"[JOB {job_id}] Cleaning domains...")
        cleaned, cstats = clean_domains(raw_lines, phase2=True)
        print(f"[JOB {job_id}] Cleaned: {len(cleaned)} domains. Stats: {cstats}")
        await db.update_job(job_id, domains_cleaned=len(cleaned), clean_stats=cstats)

        # 3. DNS filter
        live = cleaned
        if dns_on and cleaned:
            await db.update_job(job_id, status="dns_filtering")
            print(f"[JOB {job_id}] Starting DNS filtering on {len(cleaned)} domains...")
            live, dead = await dns_filter_async(cleaned, job_id)
            print(f"[JOB {job_id}] DNS filtering complete: {len(live)} live, {dead} dead")
            await db.update_job(job_id, domains_live=len(live), dns_stats={"alive": len(live), "dead": dead})
        else:
            print(f"[JOB {job_id}] Skipping DNS filter (dns_on={dns_on})")
            await db.update_job(job_id, domains_live=len(live))

        if not live:
            print(f"[JOB {job_id}] No live domains, completing job")
            await db.update_job(job_id, status="completed", finished_at=datetime.now())
            return

        # 4. Split across active slaves
        slaves_list = await db.list_slaves()
        active = {s["id"]: s for s in slaves_list
                  if s.get("status") not in ("dead", "offline", "provisioning", "error")}
        slave_ids = list(active.keys())
        
        if not slave_ids:
            await db.update_job(job_id, status="failed", error="No active slaves available")
            return
        
        n = len(slave_ids)
        chunk_size = len(live) // n
        remainder = len(live) % n

        chunks = {}
        domains_per_slave = {}
        idx = 0
        for i, sid in enumerate(slave_ids):
            size = chunk_size + (1 if i < remainder else 0)
            chunks[sid] = live[idx:idx+size]
            idx += size
            domains_per_slave[sid] = len(chunks[sid])
            # Create assignment
            await db.assign_job_to_slave(job_id, sid, len(chunks[sid]))
            # Update slave status
            await db.update_slave(sid, status="scraping", domains_assigned=len(chunks[sid]), 
                                  domains_done=0, emails_found=0)

        await db.update_job(job_id, status="scraping", domains_per_slave=domains_per_slave)

        # 5. Dispatch to slaves
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = []
            for sid, chunk in chunks.items():
                url = active[sid]["url"]
                tasks.append(
                    client.post(f"{url}/api/scrape", json={
                        "job_id": job_id,
                        "master_url": master_url,
                        "domains": chunk,
                        "workers": workers,
                        "turbo": turbo,
                    })
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                sid = slave_ids[i]
                if isinstance(r, Exception):
                    await db.update_slave(sid, status="error")
                    await db.update_job(job_id, error=f"Slave {sid}: {str(r)[:100]}")

        # 6. Poll slaves until done
        await _poll_until_done(job_id)

    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] Job {job_id} failed: {error_msg}")
        await db.update_job(job_id, status="failed", error=str(e)[:500])
        await db.log_activity("error", "jobs", f"Job {job_id} failed: {str(e)[:100]}", {"job_id": job_id})


async def _poll_until_done(job_id: str):
    """Poll slave progress until all done with progress tracking."""
    global job_cancel_flags
    
    job = await db.get_job(job_id)
    if not job:
        return
    
    # Get assigned slaves
    assignments = await db.get_job_assignments(job_id)
    active_slaves = {a["slave_id"]: a for a in assignments}
    slave_ids = list(active_slaves.keys())
    
    # Get slave URLs
    slaves_data = {}
    for sid in slave_ids:
        slave = await db.get_slave(sid)
        if slave:
            slaves_data[sid] = slave
    
    # Track progress
    domains_per_slave = job.get("domains_per_slave") or {}
    total_domains = sum(domains_per_slave.values()) if isinstance(domains_per_slave, dict) else 0
    start_time = datetime.now()
    
    while True:
        await asyncio.sleep(5)
        
        # Check if job was cancelled
        if job_cancel_flags.get(job_id, False) or job.get("status") == "cancelled":
            await db.update_job(job_id, status="cancelled")
            return
        
        all_done = True
        total_emails = []
        total_done = 0
        any_scraping = False

        async with httpx.AsyncClient(timeout=15.0) as client:
            for sid in slave_ids:
                sinfo = slaves_data.get(sid, {})
                url = sinfo.get("url", "")
                if not url:
                    continue
                    
                try:
                    r = await client.get(f"{url}/api/status/{job_id}")
                    data = r.json()
                    
                    # Update slave status in database
                    await db.update_slave(
                        sid,
                        status=data.get("status", "unknown"),
                        domains_done=data.get("domains_done", 0),
                        emails_found=data.get("emails_found", 0),
                        last_seen=datetime.now(),
                    )
                    
                    # Update assignment
                    await db.update_assignment(job_id, sid,
                        domains_done=data.get("domains_done", 0),
                        emails_found=data.get("emails_found", 0),
                        status=data.get("status", "unknown")
                    )
                    
                    total_done += data.get("domains_done", 0)

                    status = data.get("status", "unknown")
                    if status not in ("completed", "failed", "cancelled"):
                        all_done = False
                    if status == "scraping":
                        any_scraping = True
                    if data.get("emails"):
                        total_emails.extend(data["emails"])
                        # Save emails to database
                        await db.save_emails(job_id, data["emails"])
                except Exception:
                    all_done = False

        # Get unique email count from database
        email_count_result = await db.db.fetch_one(
            "SELECT COUNT(*) FROM emails WHERE job_id = :job_id",
            {"job_id": job_id}
        )
        total_emails_count = email_count_result[0] if email_count_result else 0
        
        # Calculate progress
        elapsed = (datetime.now() - start_time).total_seconds()
        progress = None
        if total_domains > 0 and total_done > 0:
            percent = min(100, (total_done / total_domains) * 100)
            rate = total_done / elapsed if elapsed > 0 else 0
            remaining = total_domains - total_done
            estimated = remaining / rate if rate > 0 else 0
            progress = {
                "percent": round(percent, 1),
                "elapsed_seconds": round(elapsed),
                "estimated_seconds": round(estimated),
                "rate": round(rate, 1),
                "domains_done": total_done,
                "domains_total": total_domains
            }
            await db.update_job(job_id, domains_done=total_done, progress=progress)

        # If all slaves report done/cancelled/failed, exit
        if all_done and not any_scraping:
            break

    # Only mark as completed if not cancelled
    job_status = job.get("status")
    if job_status != "cancelled":
        await db.update_job(job_id, status="completed")
    await db.update_job(job_id, finished_at=datetime.now())
    
    # Save final emails to file
    emails = await db.get_emails(job_id)
    (RESULT_DIR / job_id).mkdir(parents=True, exist_ok=True)
    result_file = RESULT_DIR / job_id / "emails.txt"
    result_file.write_text("\n".join(sorted(emails)), encoding="utf-8")


# ── Slave email collection endpoint ───────────────────────────────────────────

@app.post("/api/jobs/{job_id}/emails")
async def collect_emails(job_id: str, data: dict):
    """Slave POSTs scraped emails here."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    new_emails = data.get("emails", [])
    slave_id = data.get("slave_id", "unknown")
    
    # Save emails to database
    if new_emails:
        await db.save_emails(job_id, new_emails)
    
    # Update slave stats
    if slave_id:
        await db.update_slave(slave_id,
            emails_found=data.get("total_emails", len(new_emails)),
            domains_done=data.get("domains_done", 0)
        )
    
    # Get total count
    count_result = await db.db.fetch_one(
        "SELECT COUNT(*) FROM emails WHERE job_id = :job_id",
        {"job_id": job_id}
    )
    total = count_result[0] if count_result else 0
    
    return {"ok": True, "total": total}


# ── Download results ───────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}/download")
async def download_emails(job_id: str):
    fpath = RESULT_DIR / job_id / "emails.txt"
    if not fpath.exists():
        # Generate file from database
        (RESULT_DIR / job_id).mkdir(parents=True, exist_ok=True)
        emails = await db.get_emails(job_id)
        if emails:
            fpath.write_text("\n".join(sorted(emails)), encoding="utf-8")
        else:
            raise HTTPException(404, "No results yet")
    return FileResponse(str(fpath), filename=f"emails_{job_id}.txt",
                        media_type="text/plain")


# ── API: Jobs listing ──────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs_endpoint(search: str = None, page: int = 1, limit: int = 50):
    """List jobs with search and pagination. Returns most recent first."""
    result = await db.list_jobs(search=search, page=page, limit=limit)
    return result


@app.get("/api/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    # Get preview of emails
    emails_data = await db.get_emails_paginated(job_id, page=1, limit=20)
    job["emails_preview"] = [e["email"] for e in emails_data.get("emails", [])]
    job["emails"] = None  # Don't include full email list
    
    return job


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job and notify all slaves to stop."""
    global job_cancel_flags
    
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    if job["status"] in ("completed", "failed", "cancelled"):
        raise HTTPException(400, f"Cannot cancel job with status: {job['status']}")
    
    # Set cancellation flag
    job_cancel_flags[job_id] = True
    await db.update_job(
        job_id,
        status="cancelled",
        finished_at=datetime.now(),
        error="Job cancelled by user",
    )
    
    # Log the cancellation
    await db.log_activity("info", "jobs", f"Job {job_id} cancelled by user", {"job_id": job_id})
    
    # Notify all slaves to cancel this job
    await _notify_slaves_cancel(job_id)
    
    return {"ok": True, "status": "cancelled"}


@app.post("/api/jobs/{job_id}/retry")
async def retry_incomplete_job(request: Request, job_id: str, workers: int = 12, turbo: bool = True):
    """Reassign unfinished domains from offline/failed slaves to active slaves."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    if job["status"] not in ("failed", "completed", "scraping", "processing"):
        raise HTTPException(400, f"Cannot retry job in '{job['status']}' status")

    # Get assignments and find incomplete ones
    assignments = await db.get_job_assignments(job_id)
    incomplete = []
    for a in assignments:
        remaining = (a.get("domains_assigned", 0) or 0) - (a.get("domains_done", 0) or 0)
        if remaining > 0:
            incomplete.append({
                "slave_id": a["slave_id"],
                "slave_name": a.get("slave_name", ""),
                "slave_url": a.get("slave_url", ""),
                "remaining": remaining,
            })
    
    if not incomplete:
        raise HTTPException(400, "No incomplete domains found — all slaves finished their work")

    total_remaining = sum(a["remaining"] for a in incomplete)
    print(f"[RETRY] Job {job_id}: {total_remaining} domains remaining across {len(incomplete)} slaves")

    # Load the original domain list
    file_path = job.get("file_path")
    if not file_path or not Path(file_path).exists():
        raise HTTPException(400, "Original domain file not found — cannot determine which domains were unfinished")

    # Re-read and clean domains (same pipeline as original _run_job)
    raw_lines = Path(file_path).read_text(encoding="utf-8").splitlines()
    cleaned, _ = clean_domains(raw_lines, phase2=True)
    
    # DNS filter if it was used originally
    dns_stats = job.get("dns_stats") or {}
    if dns_stats.get("alive") is not None:
        live, _ = await dns_filter_async(cleaned, job_id)
    else:
        live = cleaned

    # Figure out which domains each slave was assigned
    # We need to reconstruct the chunks to find the unfinished ones
    domains_per_slave = job.get("domains_per_slave") or {}
    slave_ids_original = list(domains_per_slave.keys())
    
    # Rebuild the original chunks
    chunk_size = len(live) // len(slave_ids_original) if slave_ids_original else 0
    remainder = len(live) % len(slave_ids_original) if slave_ids_original else 0
    
    chunks_original = {}
    idx = 0
    for i, sid in enumerate(slave_ids_original):
        size = chunk_size + (1 if i < remainder else 0)
        chunks_original[sid] = live[idx:idx+size]
        idx += size
    
    # Collect unfinished domains
    unfinished_domains = []
    for a in incomplete:
        sid = a["slave_id"]
        chunk = chunks_original.get(sid, [])
        done = (a.get("domains_assigned", 0) or 0) - a["remaining"]
        # Take the domains that weren't processed
        unfinished_domains.extend(chunk[done:])
    
    if not unfinished_domains:
        raise HTTPException(400, "Could not determine unfinished domains")

    print(f"[RETRY] Job {job_id}: {len(unfinished_domains)} unfinished domains to reassign")

    # Get currently active slaves
    slaves_list = await db.list_slaves()
    active = {s["id"]: s for s in slaves_list
              if s.get("status") not in ("dead", "offline", "provisioning", "error")}
    
    if not active:
        raise HTTPException(400, "No active slaves available — bring slaves online first")

    # Split unfinished domains across active slaves
    slave_ids = list(active.keys())
    n = len(slave_ids)
    chunk_size_new = len(unfinished_domains) // n
    remainder_new = len(unfinished_domains) % n
    
    new_chunks = {}
    new_domains_per_slave = {}
    idx2 = 0
    for i, sid in enumerate(slave_ids):
        size = chunk_size_new + (1 if i < remainder_new else 0)
        new_chunks[sid] = unfinished_domains[idx2:idx2+size]
        idx2 += size
        new_domains_per_slave[sid] = len(new_chunks[sid])
        # Create new assignment
        await db.assign_job_to_slave(job_id, sid, len(new_chunks[sid]))
        # Update slave status
        await db.update_slave(sid, status="scraping", domains_assigned=len(new_chunks[sid]),
                              domains_done=0, emails_found=0)

    # Update job status
    await db.update_job(job_id, status="scraping", domains_per_slave=new_domains_per_slave, error=None)
    await db.log_activity("info", "jobs", f"Job {job_id} retry: {len(unfinished_domains)} domains reassigned to {len(active)} slaves", {"job_id": job_id})

    # Dispatch to slaves
    master_url = str(request.base_url).rstrip("/")
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = []
        for sid, chunk in new_chunks.items():
            url = active[sid]["url"]
            tasks.append(
                client.post(f"{url}/api/scrape", json={
                    "job_id": job_id,
                    "master_url": master_url,
                    "domains": chunk,
                    "workers": workers,
                    "turbo": turbo,
                })
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, r in enumerate(results):
            sid = slave_ids[i]
            if isinstance(r, Exception):
                await db.update_slave(sid, status="error")
                await db.update_job(job_id, error=f"Slave {sid}: {str(r)[:100]}")

    # Poll until done
    asyncio.create_task(_poll_until_done(job_id))
    
    return {"ok": True, "status": "scraping", "domains_reassigned": len(unfinished_domains), "slaves_used": len(active)}


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    """Delete a job and all associated data."""
    deleted = await db.delete_job(job_id)
    if deleted:
        # Also delete result file if exists
        # Delete job directory and all files
        for d in [UPLOAD_DIR / job_id, RESULT_DIR / job_id]:
            if d.exists():
                shutil.rmtree(d)
        return {"ok": True, "message": "Job deleted"}
    raise HTTPException(404, "Job not found")


async def _notify_slaves_cancel(job_id: str):
    """Send cancellation signal to all slaves working on a job."""
    job = await db.get_job(job_id)
    if not job:
        return
    
    # Get slaves assigned to this job
    assignments = await db.get_job_assignments(job_id)
    assigned_slaves = [a["slave_id"] for a in assignments]
    if not assigned_slaves:
        return
    
    # Get slave URLs
    slaves_data = {}
    for sid in assigned_slaves:
        slave = await db.get_slave(sid)
        if slave:
            slaves_data[sid] = slave
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = []
        for sid in assigned_slaves:
            slave = slaves_data.get(sid, {})
            slave_url = slave.get("url")
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
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    
    new_name = data.get("name", "").strip()
    if not new_name:
        raise HTTPException(400, "Name is required")
    
    await db.update_job(job_id, name=new_name)
    await db.log_activity("info", "jobs", f"Job {job_id} renamed to '{new_name}'", {"job_id": job_id})
    return {"ok": True, "name": new_name}


# ── Activity Logs API ─────────────────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(limit: int = 200, offset: int = 0, category: str = None, level: str = None):
    """Get activity logs from DB (newest first)."""
    logs = await db.get_activity_logs(
        category=category, level=level, limit=limit, offset=offset
    )
    return {"ok": True, "logs": logs, "total": len(logs)}


@app.post("/api/logs")
async def add_log(data: dict):
    """Save a single log entry from the UI."""
    message = data.get("message", "")
    level = data.get("level", "info")
    category = data.get("category", "ui")
    if message:
        await db.log_activity(level, category, message)
    return {"ok": True}


@app.delete("/api/logs")
async def clear_logs():
    """Clear all activity logs from DB."""
    await db.db.execute("DELETE FROM activity_logs")
    return {"ok": True, "message": "All activity logs cleared"}


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    slaves = await db.list_slaves()
    jobs_result = await db.list_jobs(page=1, limit=50)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "slaves": slaves,
        "jobs": jobs_result.get("jobs", []),
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=8000, reload=True)
