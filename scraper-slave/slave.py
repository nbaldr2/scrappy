"""
Scraper Slave Agent — Receives domains from master, scrapes emails using the
pyscrap engine logic, reports progress and results back.
"""

import os
import re
import random
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from lxml.html import fromstring as lxml_fromstring
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="Scraper Slave")

# ── Config ─────────────────────────────────────────────────────────────────────
SLAVE_ID = os.environ.get("SLAVE_ID", str(uuid.uuid4())[:8])
MASTER_URL = os.environ.get("MASTER_URL", "http://localhost:8000")
CONNECT_TIMEOUT = 4
READ_TIMEOUT = 6
MAX_CONTENT_KB = 512
MAX_PAGES = 20
EARLY_EXIT = 10
DNS_TIMEOUT = 2
EMAIL_BATCH_SIZE = 50  # Send emails to master every N new ones

# ── Thread-local state ─────────────────────────────────────────────────────────
_thread_local = threading.local()
_robots_cache = {}
_domain_backoff = {}
robots_lock = threading.Lock()
backoff_lock = threading.Lock()
shutdown_event = threading.Event()

# ── Active jobs ────────────────────────────────────────────────────────────────
active_jobs: dict[str, dict] = {}

# ── Job cancellation flags ─────────────────────────────────────────────────────
job_cancelled: dict[str, bool] = {}
job_cancel_lock = threading.Lock()

# ── UA Pool ────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
ACCEPT_LANGS = ["en-US,en;q=0.9", "en-GB,en;q=0.9", "fr-FR,fr;q=0.9,en;q=0.8"]
REFERERS = ["https://www.google.com/", "https://www.bing.com/", "https://duckduckgo.com/"]

_HIGH_PRIORITY = ("contact", "about", "team", "staff", "reach", "imprint", "impressum")
_LOW_PRIORITY = ("blog", "news", "product", "shop", "cart", "checkout", "login", "register", "faq", "privacy", "terms")
_SKIP_EXTS = (".pdf", ".zip", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".mp4", ".mp3", ".woff")

_REAL_TLDS = {
    "com","net","org","edu","gov","mil","int","biz","info","name","pro","aero",
    "coop","museum","mobi","tel","travel","jobs","cat","post","xxx","app","dev",
    "io","ai","co","me","tv","fm","am","to","so","ly","gg","vc",
    "ac","ad","ae","af","ag","al","am","ao","ar","at","au","aw","az",
    "ba","bb","bd","be","bf","bg","bh","bi","bj","bn","bo","br","bs","bt","bw","by","bz",
    "ca","cd","cf","cg","ch","ci","ck","cl","cm","cn","cr","cu","cv","cy","cz",
    "de","dj","dk","dm","do","dz","ec","ee","eg","er","es","et","eu",
    "fi","fj","fk","fm","fo","fr","ga","gb","gd","ge","gh","gi","gl","gm","gn",
    "gp","gq","gr","gt","gu","gw","gy","hk","hm","hn","hr","ht","hu",
    "id","ie","il","im","in","iq","ir","is","it","je","jm","jo","jp",
    "ke","kg","kh","ki","km","kn","kp","kr","kw","ky","kz",
    "la","lb","lc","li","lk","lr","ls","lt","lu","lv","ly",
    "ma","mc","md","me","mg","mh","mk","ml","mm","mn","mo","mp","mq","mr","ms","mt","mu","mv","mw","mx","my","mz",
    "na","nc","ne","nf","ng","ni","nl","no","np","nr","nu","nz",
    "om","pa","pe","pf","pg","ph","pk","pl","pm","pn","pr","ps","pt","pw","py",
    "qa","re","ro","rs","ru","rw","sa","sb","sc","sd","se","sg","sh","si","sk","sl","sm","sn","so","sr","ss","st","sv","sy","sz",
    "tc","td","tf","tg","th","tj","tk","tl","tm","tn","to","tr","tt","tv","tw","tz",
    "ua","ug","uk","us","uy","uz","va","vc","ve","vg","vi","vn","vu",
    "wf","ws","ye","yt","za","zm","zw",
    "agency","blog","boutique","cafe","camera","capital","care","center","church",
    "city","clinic","cloud","club","coach","codes","company","consulting","design",
    "digital","email","energy","estate","events","expert","finance","fitness",
    "foundation","fund","gallery","global","group","guru","health","holdings",
    "house","immo","institute","international","kitchen","law","legal","life",
    "limited","link","live","llc","ltd","management","marketing","media","money",
    "network","ninja","online","partners","photography","photos","plus","press",
    "productions","properties","property","pub","realty","run","school","services",
    "shop","site","social","software","solutions","space","store","studio","support",
    "systems","tech","technology","today","tools","training","ventures","video",
    "vision","web","works","world","zone","technologies",
}

_JS_LOCAL_BLACKLIST = {
    "loc","navig","organiz","situ","domin","posit","distribut","orient","communic",
    "separ","decor","illustr","migr","administr","configur","integr","collabor",
    "registr","generat","represent","document","window","navigator","location",
    "origin","protocol","jquery","angular","react","webpack","rollup","vite",
    "final_uri","order",
}
_JS_TLD_BLACKLIST = {
    "useragent","href","search","hash","protocol","hostname","pathname","host",
    "port","origin","assign","replace","reload","min","js","css","map","json",
    "ts","jsx","tsx","vue","svelte","whether","there","since","matter","static","ic","gst",
}
_EXCL_SUBSTRINGS = [
    ".png",".jpg",".jpeg",".webp",".gif",".svg",".ico",".woff",".ttf",".eot",
    ".otf",".mp4",".mp3",".pdf",".zip",
    "@sentry.","sentry.io","@example.","@test.","@localhost",
    "amazonaws.com","googleapis.com","cloudfront.net","gstatic.com",
    "wixpress.com","shopify.com","squarespace.com",
    "noreply","no-reply","donotreply","do-not-reply",
    "tracking.","notify.","alerts.","mailer-daemon",
    "wordpress@","wp-admin@","postmaster@","hostmaster@",
    "abuse@","support@example","info@example","gst@","fonts.gst",
]


# ── Scraping engine (ported from pyscrap.py) ───────────────────────────────────

def url_score(url: str) -> int:
    path = urlparse(url).path.lower()
    if any(path.endswith(e) for e in _SKIP_EXTS):
        return -10
    score = 0
    for kw in _HIGH_PRIORITY:
        if kw in path: score += 10
    for kw in _LOW_PRIORITY:
        if kw in path: score -= 5
    return score


def get_random_headers(referer=True):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1", "Connection": "keep-alive", "Cache-Control": "no-cache",
    }
    if referer:
        h["Referer"] = random.choice(REFERERS)
    return h


def get_session(turbo=False):
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        retry = Retry(total=0 if turbo else 1, backoff_factor=0.3,
                      status_forcelist=[] if turbo else [500,502,503,504],
                      raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"Connection": "keep-alive"})
        _thread_local.session = s
    return _thread_local.session


def should_backoff(host):
    with backoff_lock:
        return max(0.0, _domain_backoff.get(host, 0) - time.time())

def set_backoff(host, seconds):
    with backoff_lock:
        _domain_backoff[host] = time.time() + seconds


def robots_allows(session, url):
    parsed = urlparse(url)
    host = parsed.netloc
    with robots_lock:
        if host in _robots_cache:
            rp = _robots_cache[host]
            return True if rp is None else rp.can_fetch("*", url)
    robots_url = f"{parsed.scheme}://{host}/robots.txt"
    rp = RobotFileParser()
    try:
        r = session.get(robots_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                        headers=get_random_headers(referer=False), verify=False)
        if r.status_code == 200:
            rp.parse(r.text.splitlines())
        else:
            rp = None
    except Exception:
        rp = None
    with robots_lock:
        _robots_cache[host] = rp
    return True if rp is None else rp.can_fetch("*", url)


def check_website(session, domain):
    host = domain.replace("https://","").replace("http://","").strip("/").split("/")[0]
    w = should_backoff(host)
    if w > 0: time.sleep(min(w, 2))
    for prefix in ("https://", "http://"):
        if shutdown_event.is_set(): return None
        url = prefix + domain.replace("https://","").replace("http://","")
        try:
            r = session.head(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                             allow_redirects=True, headers=get_random_headers(), verify=False)
            if r.status_code == 429: set_backoff(host, 12)
            if r.status_code < 500: return r.url
        except Exception: pass
        try:
            r = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                            allow_redirects=True, headers=get_random_headers(), verify=False)
            if r.status_code == 429: set_backoff(host, 12)
            if r.status_code < 500: return r.url
        except Exception: pass
    return None


def fetch_page(session, url):
    if shutdown_event.is_set(): return None
    host = urlparse(url).netloc
    w = should_backoff(host)
    if w > 0: time.sleep(min(w, 2))
    try:
        head = session.head(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                            headers=get_random_headers(), verify=False, allow_redirects=True)
        if head.status_code in (429,): set_backoff(host, 15); return None
        if head.status_code == 503: set_backoff(host, 6); return None
        ct = head.headers.get("Content-Type", "")
        if ct and "text/html" not in ct: return None
        cl = head.headers.get("Content-Length", "")
        if cl.isdigit() and int(cl) > MAX_CONTENT_KB * 1024: return None
    except Exception: pass
    try:
        r = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                        headers=get_random_headers(), verify=False)
        if r.status_code == 429: set_backoff(host, 15); return None
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type",""):
            if len(r.content) > MAX_CONTENT_KB * 1024: return None
            return r.text
    except Exception: pass
    return None


def parse_links(html, base_url, base_domain):
    links = []
    try:
        if LXML_AVAILABLE:
            tree = lxml_fromstring(html)
            tree.make_links_absolute(base_url)
            hrefs = [a.get("href","") for a in tree.cssselect("a[href]")]
        else:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            hrefs = [a.get("href","") for a in soup.find_all("a", href=True)]
        seen = set()
        for href in hrefs:
            full = urljoin(base_url, href)
            p = urlparse(full)
            if p.netloc != base_domain: continue
            clean = f"{p.scheme}://{p.netloc}{p.path}"
            if clean in seen or url_score(clean) < 0: continue
            seen.add(clean)
            links.append(clean)
        links.sort(key=url_score, reverse=True)
    except Exception: pass
    return links


def _clean_html_for_extraction(html: str) -> str:
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<!--.*?-->", " ", html, flags=re.DOTALL)
    return html


def _is_real_tld(tld: str) -> bool:
    t = tld.lower().strip(".")
    if not re.match(r"^[a-z]{2,12}$", t):
        return False
    return t in _REAL_TLDS


def _local_part_ok(local: str) -> bool:
    l = local.lower().rstrip(".")
    if len(l) < 2:
        return False
    if l in _JS_LOCAL_BLACKLIST:
        return False
    for bad in _JS_LOCAL_BLACKLIST:
        if l == bad or l.startswith(bad + ".") or l.endswith("." + bad):
            return False
    if re.match(r"^[a-f0-9\-]{20,}$", l):
        return False
    return True


def _domain_ok(domain: str, tld: str) -> bool:
    d = domain.lower()
    t = tld.lower()
    if t in _JS_TLD_BLACKLIST:
        return False
    sld = d.split(".")[0]
    if len(sld) < 2 or not re.match(r"^[a-z0-9\-]+$", sld):
        return False
    return True


def is_valid_email(e: str) -> bool:
    e = e.strip().lower()
    m = re.match(
        r"^([a-z0-9]([a-z0-9_.+\-]*[a-z0-9])?)@([a-z0-9][a-z0-9.\-]*[a-z0-9])\.([a-z]{2,12})$",
        e,
    )
    if not m:
        return False
    local, domain, tld = m.group(1), m.group(3), m.group(4)
    if not _is_real_tld(tld):
        return False
    if not _local_part_ok(local):
        return False
    if not _domain_ok(domain, tld):
        return False
    if ".." in e:
        return False
    return True


def extract_emails(html: str) -> set:
    clean = _clean_html_for_extraction(html)
    raw: set = set()

    raw.update(
        re.findall(
            r"[a-zA-Z0-9][a-zA-Z0-9_.+\-]*@[a-zA-Z0-9][a-zA-Z0-9.\-]*\.[a-zA-Z]{2,12}",
            clean,
        )
    )

    raw.update(
        re.findall(
            r"mailto:([a-zA-Z0-9_.+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,12})",
            html,
            re.IGNORECASE,
        )
    )

    for m in re.findall(
        r"([a-zA-Z0-9_.+\-]{2,})(?:&#64;|%40)([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,12})",
        html,
        re.IGNORECASE,
    ):
        raw.add(f"{m[0]}@{m[1]}")

    for m in re.findall(
        r"(?<![a-zA-Z])([a-zA-Z0-9_.+\-]{3,})"
        r"\s*[\[\(]?\s*(?:at|AT)\s*[\]\)]?\s*"
        r"([a-zA-Z0-9\-]{2,})"
        r"\s*[\[\(]?\s*(?:dot|DOT|\.)\s*[\]\)]?\s*"
        r"([a-zA-Z]{2,12})"
        r"(?![a-zA-Z])",
        html,
    ):
        raw.add(f"{m[0]}@{m[1]}.{m[2]}")

    out: set = set()
    for e in raw:
        el = e.lower().strip()
        if any(kw in el for kw in _EXCL_SUBSTRINGS):
            continue
        if is_valid_email(el):
            out.add(el)
    return out


def crawl(session, site_url, max_pages=MAX_PAGES, early_exit=EARLY_EXIT, turbo=False):
    parsed = urlparse(site_url)
    base_domain = parsed.netloc
    base_url = f"{parsed.scheme}://{base_domain}"
    priority_paths = ["/contact","/contact-us","/about","/about-us","/team","/staff","/impressum","/imprint"]
    candidates = sorted([base_url + p for p in priority_paths] + [site_url], key=url_score, reverse=True)
    visited, found_emails, pages_done = set(), set(), 0
    while candidates and pages_done < max_pages:
        if shutdown_event.is_set(): break
        url = candidates.pop(0)
        if url in visited: continue
        visited.add(url)
        if not robots_allows(session, url): continue
        if not turbo: time.sleep(random.uniform(0.05, 0.15))
        html = fetch_page(session, url)
        if not html: continue
        pages_done += 1
        found_emails.update(extract_emails(html))
        if len(found_emails) >= early_exit: break
        if pages_done <= 2:
            for link in parse_links(html, base_url, base_domain)[:5]:
                if link not in visited: candidates.append(link)
            candidates.sort(key=url_score, reverse=True)
    return found_emails


def is_job_cancelled(job_id: str) -> bool:
    """Check if a job has been cancelled."""
    with job_cancel_lock:
        return job_cancelled.get(job_id, False)


def set_job_cancelled(job_id: str, cancelled: bool = True):
    """Set cancellation flag for a job."""
    with job_cancel_lock:
        job_cancelled[job_id] = cancelled


def process_domain(domain, turbo=False):
    session = get_session(turbo=turbo)
    try:
        site_url = check_website(session, domain)
        if not site_url: return {"domain": domain, "emails": [], "success": False}
        emails = crawl(session, site_url, turbo=turbo)
        return {"domain": domain, "emails": list(emails), "success": bool(emails)}
    except Exception:
        return {"domain": domain, "emails": [], "success": False}


# ── Scrape orchestrator ────────────────────────────────────────────────────────

def run_scrape_job(job_id: str, domains: list, workers: int, turbo: bool):
    """Run the scrape job in a background thread."""
    job = active_jobs[job_id]
    job["status"] = "scraping"
    all_emails = set()
    done_count = 0
    total = len(domains)
    email_buffer = []

    # Reset cancellation flag when starting
    set_job_cancelled(job_id, False)

    def _report_to_master(emails_batch, done, force=False):
        """Send batch of emails to master."""
        if not emails_batch and not force:
            return
        try:
            httpx.post(
                f"{MASTER_URL}/api/jobs/{job_id}/emails",
                json={
                    "slave_id": SLAVE_ID,
                    "emails": emails_batch,
                    "domains_done": done,
                    "total_emails": len(all_emails),
                },
                timeout=10.0,
            )
        except Exception:
            pass  # Master might be temporarily unreachable

    MAX_PENDING = workers * 2
    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scraper") as ex:
            pending = {}
            it = iter(domains)
            exhausted = False

            def fill():
                nonlocal exhausted
                while len(pending) < MAX_PENDING and not exhausted and not shutdown_event.is_set():
                    # Check for cancellation
                    if is_job_cancelled(job_id):
                        exhausted = True
                        return
                    try:
                        d = next(it)
                        pending[ex.submit(process_domain, d, turbo)] = d
                    except StopIteration:
                        exhausted = True

            fill()
            while pending and not shutdown_event.is_set() and not is_job_cancelled(job_id):
                done_set, _ = wait(list(pending.keys()), timeout=2, return_when=FIRST_COMPLETED)
                for fut in done_set:
                    pending.pop(fut)
                    try:
                        res = fut.result(timeout=18)
                        done_count += 1
                        if res.get("emails"):
                            new_emails = [e for e in res["emails"] if e not in all_emails]
                            all_emails.update(res["emails"])
                            email_buffer.extend(new_emails)
                    except Exception:
                        done_count += 1

                    # Batch send to master
                    if len(email_buffer) >= EMAIL_BATCH_SIZE:
                        _report_to_master(email_buffer[:], done_count)
                        email_buffer.clear()

                job["domains_done"] = done_count
                job["emails_found"] = len(all_emails)
                job["emails"] = list(all_emails)
                
                # Check for cancellation before filling
                if is_job_cancelled(job_id):
                    break
                    
                if not shutdown_event.is_set():
                    fill()

            # Cancel any remaining pending futures
            for f in pending:
                f.cancel()

    except Exception as e:
        job["error"] = str(e)

    # Final report
    _report_to_master(email_buffer[:], done_count, force=True)
    email_buffer.clear()

    # Set final status based on cancellation
    if is_job_cancelled(job_id):
        job["status"] = "cancelled"
        job["error"] = "Job cancelled by master"
    else:
        job["status"] = "completed"
    
    job["domains_done"] = done_count
    job["emails_found"] = len(all_emails)
    job["emails"] = list(all_emails)


# ── API Endpoints ──────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    job_id: str
    master_url: str = ""
    domains: list[str]
    workers: int = 12
    turbo: bool = True


@app.post("/api/scrape")
async def start_scrape(req: ScrapeRequest):
    global MASTER_URL
    if req.master_url:
        MASTER_URL = req.master_url

    active_jobs[req.job_id] = {
        "job_id": req.job_id,
        "status": "starting",
        "domains_total": len(req.domains),
        "domains_done": 0,
        "emails_found": 0,
        "emails": [],
        "error": None,
    }

    t = threading.Thread(
        target=run_scrape_job,
        args=(req.job_id, req.domains, req.workers, req.turbo),
        daemon=True,
    )
    t.start()
    return {"ok": True, "slave_id": SLAVE_ID, "domains": len(req.domains)}


@app.get("/api/status/{job_id}")
async def job_status(job_id: str):
    if job_id not in active_jobs:
        raise HTTPException(404, "Job not found on this slave")
    j = active_jobs[job_id]
    return {
        "slave_id": SLAVE_ID,
        "status": j["status"],
        "domains_total": j["domains_total"],
        "domains_done": j["domains_done"],
        "emails_found": j["emails_found"],
        "emails": j["emails"],
        "error": j.get("error"),
    }


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job on this slave."""
    if job_id not in active_jobs:
        raise HTTPException(404, "Job not found on this slave")
    
    job = active_jobs[job_id]
    
    # Only allow cancelling jobs that are actively scraping
    if job["status"] not in ("starting", "scraping"):
        return {"ok": False, "message": f"Job is {job['status']}, cannot cancel"}
    
    # Set the cancellation flag - this will be picked up by run_scrape_job
    set_job_cancelled(job_id, True)
    
    return {"ok": True, "message": "Cancellation signal sent"}


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "slave_id": SLAVE_ID,
        "active_jobs": len(active_jobs),
        "system_stats": _get_system_stats(),
    }


# ── System Stats (Linux /proc) ─────────────────────────────────────────────────

def _get_system_stats() -> dict:
    """Collect CPU, RAM, disk stats from /proc (Linux only)."""
    stats = {}
    try:
        # CPU load (1min average)
        with open("/proc/loadavg") as f:
            parts = f.read().split()
            stats["cpu_load_1m"] = float(parts[0])
            stats["cpu_load_5m"] = float(parts[1])

        # Memory from /proc/meminfo
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    meminfo[key] = int(val)  # kB

        total_mb = meminfo.get("MemTotal", 0) / 1024
        avail_mb = meminfo.get("MemAvailable", meminfo.get("MemFree", 0)) / 1024
        used_mb = total_mb - avail_mb
        stats["ram_total_mb"] = round(total_mb)
        stats["ram_used_mb"] = round(used_mb)
        stats["ram_percent"] = round(used_mb / total_mb * 100, 1) if total_mb > 0 else 0

        # Disk usage via statvfs
        st = os.statvfs("/")
        disk_total = st.f_blocks * st.f_frsize / (1024**3)
        disk_free = st.f_bavail * st.f_frsize / (1024**3)
        disk_used = disk_total - disk_free
        stats["disk_total_gb"] = round(disk_total, 1)
        stats["disk_used_gb"] = round(disk_used, 1)
        stats["disk_percent"] = round(disk_used / disk_total * 100, 1) if disk_total > 0 else 0

    except Exception:
        pass  # Not Linux or /proc unavailable

    return stats


# ── Heartbeat to Master ───────────────────────────────────────────────────────

HEARTBEAT_INTERVAL = 15  # seconds


def _heartbeat_loop():
    """Background thread: reports status + system stats to master every interval."""
    while not shutdown_event.is_set():
        time.sleep(HEARTBEAT_INTERVAL)
        if shutdown_event.is_set():
            break

        # Determine current status from active (in-progress) jobs only
        active = [j for j in active_jobs.values() if j.get("status") in ("starting", "scraping")]
        if active:
            scraping = any(j.get("status") == "scraping" for j in active)
            status = "scraping" if scraping else "idle"
            total_done = sum(j.get("domains_done", 0) for j in active)
            total_emails = sum(j.get("emails_found", 0) for j in active)
        else:
            status = "idle"
            total_done = 0
            total_emails = 0

        payload = {
            "status": status,
            "domains_done": total_done,
            "emails_found": total_emails,
            "system_stats": _get_system_stats(),
        }

        try:
            httpx.post(
                f"{MASTER_URL}/api/slaves/{SLAVE_ID}/heartbeat",
                json=payload,
                timeout=8.0,
            )
        except Exception:
            pass  # Master unreachable — will retry next interval


# ── System Control Endpoints ────────────────────────────────────────────────────

_pause_flag = threading.Event()

@app.post("/api/system/restart-service")
async def restart_service():
    """Restart the scraper-slave service (clears memory)."""
    import subprocess
    import os
    try:
        # Check if running as root
        is_root = os.getuid() == 0
        cmd = ["systemctl", "restart", "scraper-slave"] if is_root else ["sudo", "systemctl", "restart", "scraper-slave"]
        
        # Schedule restart after returning response
        def do_restart():
            time.sleep(1)
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"[RESTART] Failed: {result.stderr}")
        
        threading.Thread(target=do_restart, daemon=True).start()
        return {"ok": True, "message": "Service restart initiated"}
    except Exception as e:
        return {"ok": False, "message": str(e)[:100]}


@app.post("/api/system/pause")
async def pause_jobs():
    """Pause all active jobs."""
    global _pause_flag
    _pause_flag.set()
    return {"ok": True, "message": "Jobs paused"}


@app.post("/api/system/resume")
async def resume_jobs():
    """Resume all paused jobs."""
    global _pause_flag
    _pause_flag.clear()
    return {"ok": True, "message": "Jobs resumed"}


@app.post("/api/system/reboot")
async def reboot_vps():
    """Reboot the VPS."""
    import subprocess
    import os
    try:
        # Check if running as root
        is_root = os.getuid() == 0
        cmd = ["reboot"] if is_root else ["sudo", "reboot"]
        subprocess.run(cmd, check=False)
        return {"ok": True, "message": "Reboot initiated"}
    except Exception as e:
        return {"ok": False, "message": str(e)[:100]}


@app.get("/api/system/status")
async def system_status():
    """Get system status including pause state."""
    return {
        "ok": True,
        "slave_id": SLAVE_ID,
        "paused": _pause_flag.is_set(),
        "active_jobs": len(active_jobs),
        "system_stats": _get_system_stats(),
    }


@app.on_event("startup")
async def start_heartbeat():
    t = threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat")
    t.start()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("SLAVE_PORT", 8001))
    print(f"🤖 Slave {SLAVE_ID} starting on port {port}")
    uvicorn.run("slave:app", host="0.0.0.0", port=port, reload=False)
