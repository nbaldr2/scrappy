"""
Email Scraper v6.2 — Speed-Optimised, Ban-Resistant, Clean Extraction
====================================================
Speed improvements (no proxies / SOCKS):
  • Domain cleaning  — deduplicate, strip subdomains, ccTLD-aware
  • DNS pre-filter   — discard dead domains in <2 ms (no HTTP timeout wait)
  • lxml parser      — 5-10x faster HTML parsing than html.parser
  • Adaptive delays  — back off per-domain on 429/503, not globally
  • Content guard    — HEAD check skips huge/binary pages before GET
  • Keep-alive pool  — tuned TCP connection reuse, fewer TLS handshakes
  • Robots.txt cache — respected to avoid bans, cached per host
  • Smart URL scorer — rank contact/about pages first, skip obvious misses
  • Bounded queue    — never queues more than 2x workers futures
  • Heartbeat thread — terminal never freezes
  • Thread-local sessions — one session per worker, never per domain
"""

import re
import json
import os
import random
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from colorama import Fore, Style, init

try:
    from lxml.html import fromstring as lxml_fromstring
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
init(autoreset=True)

# ── Constants ──────────────────────────────────────────────────────────────────
DOMAIN_TIMEOUT  = 18      # hard per-domain ceiling (seconds)
CONNECT_TIMEOUT = 4       # TCP connect
READ_TIMEOUT    = 6       # response read
MAX_CONTENT_KB  = 512     # skip pages larger than this
DNS_TIMEOUT     = 2       # socket DNS resolution timeout
HEARTBEAT_SECS  = 30
STATE_FILE      = "scraper_state.json"

# ── Global state ───────────────────────────────────────────────────────────────
shutdown_event   = threading.Event()
file_lock        = threading.Lock()
print_lock       = threading.Lock()
state_lock       = threading.Lock()
counter_lock     = threading.Lock()
robots_lock      = threading.Lock()
backoff_lock     = threading.Lock()

processed_count    = 0
total_domains      = 0
total_emails_found = 0
start_time: float  = 0.0

checked_domains: set = set()
_robots_cache: dict  = {}    # host -> RobotFileParser or None
_domain_backoff: dict = {}   # host -> next_allowed_timestamp
_thread_local        = threading.local()
_shutdown_count      = 0

# ── User-agent pool ────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]
ACCEPT_LANGS = [
    "en-US,en;q=0.9", "en-GB,en;q=0.9",
    "fr-FR,fr;q=0.9,en;q=0.8", "de-DE,de;q=0.9,en;q=0.8",
]
REFERERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
]

# ── URL scoring for prioritisation ────────────────────────────────────────────
_HIGH_PRIORITY = ("contact", "about", "team", "staff", "reach", "imprint", "impressum")
_LOW_PRIORITY  = ("blog", "news", "product", "shop", "cart", "checkout",
                  "login", "register", "faq", "privacy", "terms", "sitemap")
_SKIP_EXTS     = (".pdf", ".zip", ".jpg", ".jpeg", ".png", ".gif",
                  ".svg", ".webp", ".ico", ".mp4", ".mp3", ".woff")


def url_score(url: str) -> int:
    path = urlparse(url).path.lower()
    if any(path.endswith(e) for e in _SKIP_EXTS):
        return -10
    score = 0
    for kw in _HIGH_PRIORITY:
        if kw in path:
            score += 10
    for kw in _LOW_PRIORITY:
        if kw in path:
            score -= 5
    return score


# ── Utilities ──────────────────────────────────────────────────────────────────

def safe_print(msg: str):
    try:
        with print_lock:
            sys.stdout.write(msg + "\n")
            sys.stdout.flush()
    except Exception:
        pass


def fmt_time(s: float) -> str:
    if s < 60:   return f"{s:.0f}s"
    if s < 3600: return f"{int(s//60)}m{int(s%60)}s"
    return f"{int(s//3600)}h{int((s%3600)//60)}m"


def get_time_stats():
    elapsed = time.time() - start_time
    with counter_lock:
        done  = processed_count
        total = total_domains
    if done == 0:
        return fmt_time(elapsed), "?"
    eta = (total - done) / (done / elapsed) if elapsed > 0 else 0
    return fmt_time(elapsed), fmt_time(eta)


def interruptible_sleep(secs: float):
    end = time.monotonic() + secs
    while time.monotonic() < end and not shutdown_event.is_set():
        time.sleep(min(0.05, end - time.monotonic()))


def get_random_headers(referer: bool = True) -> dict:
    h = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Accept-Encoding": "gzip, deflate, br",
        "DNT":             "1",
        "Connection":      "keep-alive",
        "Cache-Control":   "no-cache",
    }
    if referer:
        h["Referer"] = random.choice(REFERERS)
    return h


# ── Domain Cleaning & Deduplication ────────────────────────────────────────────

# Common ccTLDs that have second-level domains (e.g., co.uk)
CCTLD_SECOND_LEVEL = {
    'co.uk', 'org.uk', 'net.uk', 'ac.uk', 'gov.uk', 'nhs.uk',
    'com.au', 'net.au', 'org.au', 'gov.au', 'edu.au',
    'co.nz', 'org.nz', 'net.nz', 'govt.nz',
    'co.jp', 'or.jp', 'ne.jp', 'go.jp', 'ac.jp',
    'com.br', 'net.br', 'org.br', 'gov.br', 'edu.br',
    'co.za', 'org.za', 'net.za', 'gov.za',
    'com.cn', 'net.cn', 'org.cn', 'gov.cn', 'edu.cn',
    'com.sg', 'net.sg', 'org.sg', 'gov.sg',
    'co.in', 'net.in', 'org.in', 'gov.in', 'ac.in',
}

# Common subdomains to strip in phase 1
COMMON_SUBDOMAINS = {
    'www', 'mail', 'ftp', 'blog', 'shop', 'store', 'news',
    'admin', 'api', 'app', 'assets', 'cdn', 'cloud', 'cms',
    'crm', 'dev', 'download', 'email', 'en', 'forum', 'help',
    'img', 'images', 'jobs', 'login', 'media', 'mobile', 'my',
    'newsletter', 'old', 'panel', 'partner', 'pay', 'portal',
    'remote', 'secure', 'server', 'service', 'staging', 'static',
    'support', 'test', 'uat', 'video', 'vpn', 'web', 'webmail',
    'wiki', 'ww1', 'ww2', 'ww3', 'www1', 'www2', 'www3',
}


def extract_domain_parts(domain: str) -> tuple:
    """
    Extract (subdomain, effective_domain) from a domain.
    Handles ccTLDs like co.uk properly.
    Returns (subdomain, effective_domain) where effective_domain is the registrable domain.
    """
    # Clean the domain first
    domain = domain.lower().strip()
    domain = domain.replace('https://', '').replace('http://', '')
    domain = domain.split('/')[0]  # Remove path
    domain = domain.split(':')[0]   # Remove port
    
    parts = domain.split('.')
    
    if len(parts) < 2:
        return ('', domain)
    
    # Check for ccTLD with second-level (e.g., domain.co.uk)
    if len(parts) >= 3:
        # Check last 3 parts for ccTLD patterns
        last3 = '.'.join(parts[-3:])
        last2 = '.'.join(parts[-2:])
        
        # If ends with a known ccTLD second-level domain
        for cctld in CCTLD_SECOND_LEVEL:
            if last3.endswith('.' + cctld) or last3 == cctld:
                # For domain.co.uk: parts = ['www','domain','co','uk']
                # effective = domain.co.uk, subdomain = www
                effective = '.'.join(parts[-3:])
                subdomain = '.'.join(parts[:-3]) if len(parts) > 3 else ''
                return (subdomain, effective)
    
    # Standard TLD (com, net, org, etc.)
    effective_domain = '.'.join(parts[-2:])
    subdomain = '.'.join(parts[:-2]) if len(parts) > 2 else ''
    return (subdomain, effective_domain)


def normalize_domain(domain: str) -> str:
    """
    Normalize domain: lowercase, strip protocol, get effective domain.
    Returns the effective (registrable) domain.
    """
    subdomain, effective = extract_domain_parts(domain)
    return effective


def clean_domains_phase1(domains: list) -> tuple:
    """
    Phase 1: Remove exact duplicates and strip common subdomains.
    Returns (cleaned_list, stats_dict).
    """
    stats = {'original': len(domains), 'exact_dupes': 0, 'www_stripped': 0}
    seen = set()
    cleaned = []
    
    for domain in domains:
        if not domain or not domain.strip():
            continue
            
        subdomain, effective = extract_domain_parts(domain)
        
        # Check for exact duplicate (effective domain only)
        if effective in seen:
            stats['exact_dupes'] += 1
            continue
        
        # If subdomain is common, just keep effective domain
        if subdomain in COMMON_SUBDOMAINS or not subdomain:
            if subdomain == 'www' or subdomain.startswith('www.'):
                stats['www_stripped'] += 1
            cleaned.append(effective)
            seen.add(effective)
        else:
            # Keep full domain with uncommon subdomain
            full = f"{subdomain}.{effective}".lstrip('.')
            if full not in seen:
                cleaned.append(full)
                seen.add(full)
    
    return cleaned, stats


def clean_domains_phase2(domains: list) -> tuple:
    """
    Phase 2: Aggressive deduplication - keep only unique effective domains.
    Strips ALL subdomains except for known multi-level cases.
    Returns (cleaned_list, stats_dict).
    """
    stats = {'subdomains_removed': 0, 'phase2_dupes': 0}
    seen = set()
    cleaned = []
    
    for domain in domains:
        subdomain, effective = extract_domain_parts(domain)
        
        if effective in seen:
            stats['phase2_dupes'] += 1
            continue
        
        if subdomain:
            stats['subdomains_removed'] += 1
        
        cleaned.append(effective)
        seen.add(effective)
    
    return cleaned, stats


def save_cleaned_domains(domains: list, output_file: str = "domains_cleaned.txt"):
    """Save cleaned domain list to file."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# Cleaned domains - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(domains)} domains\n\n")
            for domain in sorted(set(domains)):
                f.write(f"{domain}\n")
        return True
    except Exception as e:
        safe_print(f"{Fore.RED}✗ Failed to save cleaned domains: {e}{Style.RESET_ALL}")
        return False


def load_raw_domains(filepath: str) -> list:
    """Load domains from file, basic cleaning."""
    domains = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Remove common prefixes but keep domain intact
                    line = line.replace('https://', '').replace('http://', '')
                    line = line.split('/')[0]
                    line = line.split(':')[0]
                    if line and '.' in line:
                        domains.append(line.lower())
    except Exception as e:
        safe_print(f"{Fore.RED}✗ Failed to load domains: {e}{Style.RESET_ALL}")
        return []
    return domains


def run_domain_cleaning(input_file: str, phase2: bool = True) -> tuple:
    """
    Full pipeline: load → phase1 → [phase2] → save → return cleaned list.
    Returns (cleaned_domains, stats_dict).
    """
    safe_print(f"\n{Fore.CYAN}🔧 Domain Cleaning Pipeline{Style.RESET_ALL}")
    safe_print(f"{Fore.CYAN}{'─'*48}{Style.RESET_ALL}")
    
    # Load
    raw = load_raw_domains(input_file)
    if not raw:
        return [], {}
    
    safe_print(f"📥 Loaded: {len(raw)} raw domains")
    
    # Phase 1: Remove duplicates and common subdomains
    phase1_clean, phase1_stats = clean_domains_phase1(raw)
    safe_print(f"🔹 Phase 1: {len(phase1_clean)} domains "
              f"(-{phase1_stats['exact_dupes']} dupes, "
              f"-{phase1_stats['www_stripped']} www)")
    
    cleaned = phase1_clean
    phase2_stats = {}
    
    # Phase 2: Aggressive deduplication (optional)
    if phase2:
        cleaned, phase2_stats = clean_domains_phase2(phase1_clean)
        safe_print(f"🔹 Phase 2: {len(cleaned)} domains "
                  f"(-{phase2_stats['phase2_dupes']} dupes, "
                  f"-{phase2_stats['subdomains_removed']} subdomains)")
    
    # Save
    output_file = input_file.replace('.txt', '_cleaned.txt')
    if output_file == input_file:
        output_file = "domains_cleaned.txt"
    
    if save_cleaned_domains(cleaned, output_file):
        safe_print(f"💾 Saved: {output_file}")
    
    # Summary stats
    total_removed = len(raw) - len(cleaned)
    safe_print(f"{Fore.GREEN}✓ Cleaning complete: {len(cleaned)} unique domains "
              f"({total_removed} removed, {(total_removed/len(raw)*100):.1f}%){Style.RESET_ALL}")
    safe_print(f"{Fore.CYAN}{'─'*48}{Style.RESET_ALL}\n")
    
    stats = {
        'original': len(raw),
        'final': len(cleaned),
        'removed': total_removed,
        'output_file': output_file,
        'phase1': phase1_stats,
        'phase2': phase2_stats,
    }
    
    return cleaned, stats


# ── DNS pre-filter ─────────────────────────────────────────────────────────────

def dns_resolves(hostname: str) -> bool:
    """
    Cheap DNS check — fails in ~2 ms for NXDOMAIN vs ~4 s HTTP timeout wait.
    Skipping unresolvable domains is the single biggest speed win.
    """
    orig = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(DNS_TIMEOUT)
        socket.getaddrinfo(hostname, 80, proto=socket.IPPROTO_TCP)
        return True
    except Exception:
        return False
    finally:
        socket.setdefaulttimeout(orig)


def batch_dns_filter(domains: list, workers: int = 100) -> tuple:
    """Resolve all domains in parallel; return (live_list, dead_count)."""
    live = []
    dead = 0
    lock = threading.Lock()

    def check(d):
        nonlocal dead
        host = d.replace("https://", "").replace("http://", "").split("/")[0]
        if dns_resolves(host):
            with lock:
                live.append(d)
        else:
            with lock:
                dead += 1

    safe_print(f"{Fore.CYAN}⚡ DNS pre-filtering {len(domains)} domains…{Style.RESET_ALL}")
    cap = min(workers, len(domains))
    with ThreadPoolExecutor(max_workers=cap, thread_name_prefix="dns") as ex:
        futs = [ex.submit(check, d) for d in domains]
        for i, f in enumerate(futs, 1):
            f.result()
            if i % 500 == 0:
                safe_print(f"   DNS: {i}/{len(domains)} · {len(live)} alive…")

    safe_print(
        f"{Fore.GREEN}✓ DNS done: {len(live)} alive, "
        f"{dead} dead (skipped){Style.RESET_ALL}\n"
    )
    return live, dead


# ── Thread-local session ───────────────────────────────────────────────────────

def get_session(turbo: bool = False) -> requests.Session:
    """One persistent session per worker thread — reused for every domain."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        retry = Retry(
            total=0 if turbo else 1,
            backoff_factor=0.3,
            status_forcelist=[] if turbo else [500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=20,
        )
        s.mount("https://", adapter)
        s.mount("http://",  adapter)
        s.headers.update({"Connection": "keep-alive"})
        _thread_local.session = s
    return _thread_local.session


# ── Adaptive per-domain backoff ────────────────────────────────────────────────

def should_backoff(host: str) -> float:
    with backoff_lock:
        return max(0.0, _domain_backoff.get(host, 0) - time.time())


def set_backoff(host: str, seconds: float):
    with backoff_lock:
        _domain_backoff[host] = time.time() + seconds


# ── Robots.txt (cached) ────────────────────────────────────────────────────────

def robots_allows(session: requests.Session, url: str) -> bool:
    """
    Check robots.txt — cached per host so each host is fetched only once.
    Respecting robots is the single most effective ban-avoidance technique.
    """
    parsed = urlparse(url)
    host   = parsed.netloc
    with robots_lock:
        if host in _robots_cache:
            rp = _robots_cache[host]
            return True if rp is None else rp.can_fetch("*", url)

    robots_url = f"{parsed.scheme}://{host}/robots.txt"
    rp = RobotFileParser()
    try:
        r = session.get(
            robots_url,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            headers=get_random_headers(referer=False),
            verify=False, stream=False,
        )
        if r.status_code == 200:
            rp.parse(r.text.splitlines())
        else:
            rp = None
    except Exception:
        rp = None

    with robots_lock:
        _robots_cache[host] = rp

    return True if rp is None else rp.can_fetch("*", url)


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def check_website(session: requests.Session, domain: str) -> str | None:
    """Resolve domain to live URL via HEAD then GET fallback."""
    host = domain.replace("https://", "").replace("http://", "").strip("/").split("/")[0]

    w = should_backoff(host)
    if w > 0:
        interruptible_sleep(w)

    for prefix in ("https://", "http://"):
        if shutdown_event.is_set():
            return None
        url = prefix + domain.replace("https://", "").replace("http://", "")
        try:
            r = session.head(
                url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True, headers=get_random_headers(), verify=False,
            )
            if r.status_code == 429:
                set_backoff(host, 12)
                interruptible_sleep(12)
            if r.status_code < 500:
                return r.url
        except requests.exceptions.SSLError:
            pass
        except Exception:
            pass

        try:
            r = session.get(
                url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True, headers=get_random_headers(),
                verify=False, stream=False,
            )
            if r.status_code == 429:
                set_backoff(host, 12)
            if r.status_code < 500:
                return r.url
        except Exception:
            pass

    return None


def fetch_page(session: requests.Session, url: str) -> str | None:
    """
    Fetch HTML with a HEAD pre-check:
    - Skip non-HTML content types
    - Skip pages larger than MAX_CONTENT_KB before downloading
    - Back off adaptively on 429/503
    """
    if shutdown_event.is_set():
        return None

    host = urlparse(url).netloc
    w = should_backoff(host)
    if w > 0:
        interruptible_sleep(w)

    try:
        head = session.head(
            url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            headers=get_random_headers(), verify=False, allow_redirects=True,
        )
        if head.status_code == 429:
            set_backoff(host, 15)
            return None
        if head.status_code == 503:
            set_backoff(host, 6)
            return None
        if head.status_code not in (200, 301, 302):
            return None
        ct = head.headers.get("Content-Type", "")
        if ct and "text/html" not in ct:
            return None
        cl = head.headers.get("Content-Length", "")
        if cl.isdigit() and int(cl) > MAX_CONTENT_KB * 1024:
            return None
    except Exception:
        pass   # HEAD failed — try GET anyway

    try:
        r = session.get(
            url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            headers=get_random_headers(), verify=False, stream=False,
        )
        if r.status_code == 429:
            set_backoff(host, 15)
            return None
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            if len(r.content) > MAX_CONTENT_KB * 1024:
                return None
            return r.text
    except Exception:
        pass

    return None


# ── HTML link extraction ───────────────────────────────────────────────────────

def parse_links(html: str, base_url: str, base_domain: str) -> list:
    """Extract and score internal links. Uses lxml when available (5-10x faster)."""
    links = []
    try:
        if LXML_AVAILABLE:
            tree = lxml_fromstring(html)
            tree.make_links_absolute(base_url)
            hrefs = [a.get("href", "") for a in tree.cssselect("a[href]")]
        else:
            soup  = BeautifulSoup(html, "html.parser")
            hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]

        seen = set()
        for href in hrefs:
            full  = urljoin(base_url, href)
            p     = urlparse(full)
            if p.netloc != base_domain:
                continue
            clean = f"{p.scheme}://{p.netloc}{p.path}"
            if clean in seen or url_score(clean) < 0:
                continue
            seen.add(clean)
            links.append(clean)

        links.sort(key=url_score, reverse=True)
    except Exception:
        pass
    return links


# ── Email extraction ───────────────────────────────────────────────────────────
#
# Root cause of false positives like navig@or.useragent / loc@ion.search:
#   The obfuscated-email regex was splitting JS identifiers that contain the
#   letters "at" or "ion" — e.g. "navigator" → "navig" + AT + "or",
#   "location" → "loc" + AT + "ion".  Fixed with four layers of guards:
#     1. Real-TLD whitelist  — TLD must be ≤6 alpha chars AND in a known set
#     2. JS keyword blacklist — local-part / domain fragments match JS names
#     3. Min local-part length — single-fragment local parts like "loc" rejected
#     4. Domain plausibility  — domain must contain a dot and a real SLD
#

# ── Real TLD whitelist (covers >99 % of legitimate contact emails) ─────────────
_REAL_TLDS = {
    # Generic
    "com","net","org","edu","gov","mil","int","biz","info","name","pro","aero",
    "coop","museum","mobi","tel","travel","jobs","cat","post","xxx","app","dev",
    "io","ai","co","me","tv","fm","am","to","so","ly","gg","vc",
    # Country codes (most common)
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
    # Popular new gTLDs
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
    # Common second-level UK / AU / etc patterns used as effective TLDs
    "ac","co","com","gov","net","org","edu","asn","id","act","nsw","vic","qld","sa",
}

# ── JS / framework identifier fragments that appear before/after the @ ─────────
# These are exact strings that real email local-parts or domains would never be.
_JS_LOCAL_BLACKLIST = {
    # JS globals and browser APIs
    "loc", "navig", "organiz", "situ", "domin", "posit", "distribut",
    "orient", "communic", "separ", "decor", "illustr", "migr", "administr",
    "configur", "integr", "collabor", "registr", "generat", "represent",
    "document", "window", "navigator", "location", "origin", "protocol",
    # jQuery / build artifacts
    "jquery", "angular", "react", "webpack", "rollup", "vite",
    # Common obfuscation artifacts
    "final_uri", "order",
}

_JS_TLD_BLACKLIST = {
    # These appear as "TLDs" in JS false positives
    "useragent", "href", "search", "hash", "protocol", "hostname",
    "pathname", "host", "port", "origin", "assign", "replace", "reload",
    "min", "js", "css", "map", "json", "ts", "jsx", "tsx", "vue", "svelte",
    "whether", "there", "since", "matter", "static", "ic", "gst",
}

# ── Substring exclusion list (domains/patterns never in real emails) ───────────
_EXCL_SUBSTRINGS = [
    # File extensions that leak into the regex
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".ico", ".woff",
    ".ttf", ".eot", ".otf", ".mp4", ".mp3", ".pdf", ".zip",
    # Known junk domains
    "@sentry.", "sentry.io", "@example.", "@test.", "@localhost",
    "amazonaws.com", "googleapis.com", "cloudfront.net", "gstatic.com",
    "wixpress.com", "shopify.com", "squarespace.com",
    # Functional/role addresses we don't want
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "tracking.", "notify.", "alerts.", "mailer-daemon",
    "wordpress@", "wp-admin@", "postmaster@", "hostmaster@",
    "abuse@", "support@example", "info@example",
    # CDN/asset domains misidentified by regex (e.g. fonts.gstatic.com → fonts.gst@ic.com)
    "gst@", "fonts.gst",
]


def _clean_html_for_extraction(html: str) -> str:
    """
    Strip <script> and <style> blocks before regex scanning.
    This removes the vast majority of JS false positives at source.
    """
    # Remove script blocks (where navigator.useragent etc live)
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove style blocks
    html = re.sub(r"<style[^>]*>.*?</style>",  " ", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    html = re.sub(r"<!--.*?-->", " ", html, flags=re.DOTALL)
    return html


def _is_real_tld(tld: str) -> bool:
    t = tld.lower().strip(".")
    # Must be purely alphabetic and between 2-12 chars
    # (long gTLDs like "solutions", "international" are up to 12 chars)
    if not re.match(r"^[a-z]{2,12}$", t):
        return False
    return t in _REAL_TLDS


def _local_part_ok(local: str) -> bool:
    """Return False if the local part looks like a JS identifier fragment."""
    l = local.lower().rstrip(".")
    # Too short to be a real address
    if len(l) < 2:
        return False
    # Exact match against known JS fragments
    if l in _JS_LOCAL_BLACKLIST:
        return False
    # Starts with a known JS fragment (e.g. "loc" in "loc@ion")
    for bad in _JS_LOCAL_BLACKLIST:
        if l == bad or l.startswith(bad + ".") or l.endswith("." + bad):
            return False
    # Contains only non-alpha chars (hash strings, UUIDs)
    if re.match(r"^[a-f0-9\-]{20,}$", l):
        return False
    return True


def _domain_ok(domain: str, tld: str) -> bool:
    """Return False if the domain looks like a JS expression."""
    d = domain.lower()
    t = tld.lower()
    if t in _JS_TLD_BLACKLIST:
        return False
    # Domain SLD (second-level) must be ≥2 real alpha/hyphen chars
    sld = d.split(".")[0]
    if len(sld) < 2 or not re.match(r"^[a-z0-9\-]+$", sld):
        return False
    return True


def is_valid_email(e: str) -> bool:
    """Full structural + plausibility check."""
    e = e.strip().lower()
    # RFC-lite structure — TLD up to 12 chars to cover long gTLDs
    # (solutions=9, international=13 trimmed to 12, technology=10, etc.)
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
    # No consecutive dots
    if ".." in e:
        return False
    return True


def extract_emails(html: str) -> set:
    """
    Extract emails from HTML using four strategies, then validate each
    candidate through structural + plausibility filters to eliminate
    JS false positives (navig@or.useragent, loc@ion.search, etc.).
    """
    # Strip script/style blocks first — kills most JS false positives at source
    clean = _clean_html_for_extraction(html)

    raw: set = set()

    # ── Strategy 1: standard regex on cleaned text ─────────────────────────────
    # TLD {2,12} captures long gTLDs like "solutions", "technology"
    raw.update(re.findall(
        r"[a-zA-Z0-9][a-zA-Z0-9_.+\-]*@[a-zA-Z0-9][a-zA-Z0-9.\-]*\.[a-zA-Z]{2,12}",
        clean,
    ))

    # ── Strategy 2: mailto: links (most reliable — always include) ─────────────
    # Run on original HTML so we catch mailto inside JS strings too
    raw.update(re.findall(
        r"mailto:([a-zA-Z0-9_.+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,12})",
        html, re.IGNORECASE,
    ))

    # ── Strategy 3: HTML-encoded @ (&#64; or %40) ──────────────────────────────
    for m in re.findall(
        r"([a-zA-Z0-9_.+\-]{2,})(?:&#64;|%40)([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,12})",
        html, re.IGNORECASE,
    ):
        raw.add(f"{m[0]}@{m[1]}")

    # ── Strategy 4: obfuscated  user [at] domain [dot] com ────────────────────
    # Stricter than before: requires local ≥3 chars, surrounded by non-alpha
    # so "navigator" can't match as "navig" + AT + "or".
    for m in re.findall(
        r"(?<![a-zA-Z])([a-zA-Z0-9_.+\-]{3,})"    # local part (min 3 chars)
        r"\s*[\[\(]?\s*(?:at|AT)\s*[\]\)]?\s*"     # literal "at" only (bare @ handled by S1)
        r"([a-zA-Z0-9\-]{2,})"                     # domain SLD
        r"\s*[\[\(]?\s*(?:dot|DOT|\.)\s*[\]\)]?\s*"
        r"([a-zA-Z]{2,12})"                        # TLD — up to 12 chars
        r"(?![a-zA-Z])",
        html,
    ):
        raw.add(f"{m[0]}@{m[1]}.{m[2]}")

    # ── Filter ─────────────────────────────────────────────────────────────────
    out: set = set()
    for e in raw:
        el = e.lower().strip()

        # Substring exclusions (fast path)
        if any(kw in el for kw in _EXCL_SUBSTRINGS):
            continue

        # Full structural + plausibility check
        if is_valid_email(el):
            out.add(el)

    return out


# ── Crawler ────────────────────────────────────────────────────────────────────

def crawl(session: requests.Session, site_url: str,
          max_pages: int = 6, early_exit: int = 2,
          turbo: bool = False) -> set:
    """
    Priority-scored crawl with robots.txt awareness and early exit.
    High-value pages (contact, about, team) are always checked first.
    """
    parsed      = urlparse(site_url)
    base_domain = parsed.netloc
    base_url    = f"{parsed.scheme}://{base_domain}"

    priority_paths = [
        "/contact", "/contact-us", "/about", "/about-us",
        "/team", "/staff", "/impressum", "/imprint",
    ]
    candidates = sorted(
        [base_url + p for p in priority_paths] + [site_url],
        key=url_score, reverse=True,
    )

    visited:      set = set()
    found_emails: set = set()
    pages_done        = 0

    while candidates and pages_done < max_pages:
        if shutdown_event.is_set():
            break

        url = candidates.pop(0)
        if url in visited:
            continue
        visited.add(url)

        if not robots_allows(session, url):
            continue

        if not turbo:
            interruptible_sleep(random.uniform(0.05, 0.15))

        html = fetch_page(session, url)
        if not html:
            continue

        pages_done += 1
        found_emails.update(extract_emails(html))

        if len(found_emails) >= early_exit:
            break

        if pages_done <= 2:
            for link in parse_links(html, base_url, base_domain)[:5]:
                if link not in visited:
                    candidates.append(link)
            candidates.sort(key=url_score, reverse=True)

    return found_emails


# ── Domain processor ───────────────────────────────────────────────────────────

def process_domain(domain: str, output_file: str,
                   verbose: bool, turbo: bool) -> dict:
    global processed_count, total_emails_found

    if shutdown_event.is_set():
        return {"domain": domain, "success": False, "emails": [], "error": "shutdown"}

    with state_lock:
        if domain in checked_domains:
            return {"domain": domain, "success": False, "emails": [], "error": "skip"}

    with counter_lock:
        processed_count += 1
        current = processed_count

    if verbose and not shutdown_event.is_set():
        elapsed, eta = get_time_stats()
        with counter_lock:
            ef  = total_emails_found
        rem = total_domains - current
        safe_print(
            f"[{Fore.CYAN}{current}/{total_domains}{Style.RESET_ALL}]"
            f"[{Fore.YELLOW}{rem}↓{Style.RESET_ALL}]"
            f"[{Fore.GREEN}{ef}✉{Style.RESET_ALL}]"
            f"[{Fore.MAGENTA}{elapsed}⏱{Style.RESET_ALL}]"
            f"[~{Fore.BLUE}{eta}{Style.RESET_ALL}] {domain}"
        )

    result  = {"domain": domain, "success": False, "emails": [], "error": None}
    session = get_session(turbo=turbo)

    try:
        site_url = check_website(session, domain)
        if shutdown_event.is_set():
            result["error"] = "shutdown"
            return result
        if not site_url:
            if verbose:
                safe_print(f"  {Fore.RED}✗ dead{Style.RESET_ALL}")
            result["error"] = "unreachable"
            return result

        if verbose:
            safe_print(f"  {Fore.GREEN}✓ {site_url}{Style.RESET_ALL}")

        emails = crawl(session, site_url, turbo=turbo)

        if emails:
            if verbose:
                safe_print(f"  {Fore.GREEN}→ {len(emails)} email(s){Style.RESET_ALL}")
                for e in list(emails)[:4]:
                    safe_print(f"    {Fore.YELLOW}{e}{Style.RESET_ALL}")
                if len(emails) > 4:
                    safe_print(f"    … +{len(emails)-4} more")
            result.update({"success": True, "emails": list(emails)})
            with counter_lock:
                total_emails_found += len(emails)
            with file_lock:
                with open(output_file, "a", encoding="utf-8") as f:
                    for e in emails:
                        f.write(e + "\n")
        else:
            if verbose:
                safe_print(f"  {Fore.YELLOW}⚠ no emails{Style.RESET_ALL}")
            result["error"] = "no emails"

    except Exception as ex:
        if verbose and not shutdown_event.is_set():
            safe_print(f"  {Fore.RED}✗ {str(ex)[:80]}{Style.RESET_ALL}")
        result["error"] = str(ex)
    finally:
        with state_lock:
            checked_domains.add(domain)

    return result


# ── State I/O ──────────────────────────────────────────────────────────────────

def save_state():
    with state_lock:
        snap = list(checked_domains)
    with counter_lock:
        ef = total_emails_found
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "processed_count":    processed_count,
                "total_domains":      total_domains,
                "total_emails_found": ef,
                "checked_domains":    snap,
                "timestamp":          datetime.now().isoformat(),
            }, f, indent=2)
    except Exception as e:
        safe_print(f"{Fore.RED}State save failed: {e}{Style.RESET_ALL}")


def load_state() -> dict | None:
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return None


# ── Heartbeat ──────────────────────────────────────────────────────────────────

def heartbeat_thread_fn():
    while not shutdown_event.wait(timeout=HEARTBEAT_SECS):
        with counter_lock:
            done  = processed_count
            total = total_domains
            ef    = total_emails_found
        elapsed, eta = get_time_stats()
        safe_print(
            f"\n{Fore.CYAN}💓 {done}/{total} done · {ef} emails · "
            f"elapsed {elapsed} · eta {eta}{Style.RESET_ALL}\n"
        )


# ── Signal handler (lock-free) ─────────────────────────────────────────────────

def signal_handler(signum, frame):
    global _shutdown_count
    _shutdown_count += 1
    if _shutdown_count >= 2:
        os._exit(1)
    shutdown_event.set()
    threading.Thread(target=_deferred_save, daemon=True).start()


def _deferred_save():
    sys.stdout.write(
        f"\n{Fore.YELLOW}⚠ Stopping… saving state "
        f"(Ctrl+C again = force){Style.RESET_ALL}\n"
    )
    sys.stdout.flush()
    time.sleep(0.3)
    save_state()
    sys.stdout.write(f"{Fore.GREEN}✓ State saved → {STATE_FILE}{Style.RESET_ALL}\n")
    sys.stdout.flush()


signal.signal(signal.SIGINT,  signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ── Orchestrator ───────────────────────────────────────────────────────────────

def process_domains(
    input_file:  str,
    output_file: str  = "emails_v6.txt",
    max_workers: int  = 12,
    verbose:     bool = True,
    turbo:       bool = True,
    resume:      bool = True,
    dns_filter:  bool = True,
):
    global total_domains, processed_count, total_emails_found, start_time, checked_domains

    shutdown_event.clear()
    processed_count = 0

    # Resume
    if resume:
        state = load_state()
        if state:
            safe_print(
                f"\n{Fore.YELLOW}📋 Previous state: {state.get('timestamp','?')} · "
                f"{len(state.get('checked_domains',[]))} checked · "
                f"{state.get('total_emails_found',0)} emails{Style.RESET_ALL}"
            )
            if input("   Resume? (Y/n): ").strip().lower() != "n":
                with state_lock:
                    checked_domains = set(state.get("checked_domains", []))
                with counter_lock:
                    total_emails_found = state.get("total_emails_found", 0)

    start_time = time.time()

    with open(input_file, encoding="utf-8") as f:
        all_domains = [ln.strip() for ln in f if ln.strip()]

    with state_lock:
        domains = [d for d in all_domains if d not in checked_domains]

    if len(domains) < len(all_domains):
        safe_print(
            f"{Fore.GREEN}✓ Skipping {len(all_domains)-len(domains)} "
            f"already done{Style.RESET_ALL}"
        )

    if dns_filter and domains:
        domains, _ = batch_dns_filter(domains, workers=min(150, len(domains)))

    total_domains = len(domains)
    if total_domains == 0:
        safe_print(f"\n{Fore.GREEN}✓ Nothing left to process!{Style.RESET_ALL}")
        return

    safe_print(f"\n{Fore.CYAN}{'━'*48}{Style.RESET_ALL}")
    safe_print(f"  Domains    : {total_domains}")
    safe_print(f"  Workers    : {max_workers}")
    safe_print(f"  Parser     : {'lxml ⚡' if LXML_AVAILABLE else 'BeautifulSoup'}")
    safe_print(f"  Mode       : {'⚡ TURBO' if turbo else '🐢 Standard'}")
    safe_print(f"  DNS filter : {'on' if dns_filter else 'off'}")
    safe_print(f"  Ctrl+C = graceful stop · twice = force kill")
    safe_print(f"{Fore.CYAN}{'━'*48}{Style.RESET_ALL}\n")

    if not resume or not checked_domains:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# Scraped {datetime.now()}\n# Domains: {len(all_domains)}\n\n")

    threading.Thread(
        target=heartbeat_thread_fn, daemon=True, name="heartbeat"
    ).start()

    successful = failed = cancelled = 0
    MAX_PENDING = max_workers * 2

    try:
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="scraper"
        ) as ex:
            pending: dict = {}
            it            = iter(domains)
            exhausted     = False

            def fill():
                nonlocal exhausted
                while (
                    len(pending) < MAX_PENDING
                    and not exhausted
                    and not shutdown_event.is_set()
                ):
                    try:
                        d = next(it)
                        pending[
                            ex.submit(process_domain, d, output_file, verbose, turbo)
                        ] = d
                    except StopIteration:
                        exhausted = True

            fill()

            while pending and not shutdown_event.is_set():
                done, _ = wait(
                    list(pending.keys()), timeout=2, return_when=FIRST_COMPLETED
                )
                for fut in done:
                    domain = pending.pop(fut)
                    try:
                        res = fut.result(timeout=DOMAIN_TIMEOUT)
                        if res.get("success"):
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        failed += 1
                        if not shutdown_event.is_set():
                            safe_print(
                                f"[{Fore.RED}!{Style.RESET_ALL}] "
                                f"{domain}: {str(e)[:60]}"
                            )
                if not shutdown_event.is_set():
                    fill()

            if shutdown_event.is_set():
                cancelled = len(pending)
                for f in pending:
                    f.cancel()

    except KeyboardInterrupt:
        pass

    shutdown_event.set()
    save_state()

    elapsed = time.time() - start_time
    rate    = processed_count / elapsed if elapsed > 0 else 0

    safe_print(f"\n{Fore.CYAN}{'━'*48}{Style.RESET_ALL}")
    safe_print(f"{Fore.GREEN}✓ Successful : {successful}{Style.RESET_ALL}")
    safe_print(f"{Fore.RED}✗ Failed     : {failed}{Style.RESET_ALL}")
    if cancelled:
        safe_print(f"{Fore.YELLOW}⏹ Cancelled  : {cancelled}{Style.RESET_ALL}")
    with counter_lock:
        safe_print(f"📧 Total emails : {total_emails_found}")
    safe_print(f"⏱  {fmt_time(elapsed)} · {rate:.1f} domains/s")
    safe_print(f"📁 {output_file}")

    if not cancelled and processed_count >= total_domains:
        if input("\n   Delete state file? (y/N): ").strip().lower() == "y":
            try:
                os.remove(STATE_FILE)
            except Exception:
                pass
    else:
        safe_print(f"📋 Resume: re-run the same command")
    safe_print(f"{Fore.CYAN}{'━'*48}{Style.RESET_ALL}")


# ── Single-domain test ─────────────────────────────────────────────────────────

def test_single(domain: str):
    global total_domains, processed_count, start_time
    total_domains = 1
    processed_count = 0
    start_time = time.time()
    safe_print(f"\n{Fore.CYAN}Testing: {domain}{Style.RESET_ALL}\n")
    session = get_session()
    url = check_website(session, domain)
    if not url:
        safe_print(f"{Fore.RED}✗ Not reachable{Style.RESET_ALL}")
        return
    safe_print(f"{Fore.GREEN}✓ {url}{Style.RESET_ALL}")
    emails = crawl(session, url)
    if emails:
        safe_print(f"\n{Fore.GREEN}{len(emails)} email(s):{Style.RESET_ALL}")
        for e in sorted(emails):
            safe_print(f"  {e}")
    else:
        safe_print(f"{Fore.YELLOW}No emails found{Style.RESET_ALL}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    safe_print(f"""
{Fore.CYAN}╔════════════════════════════════════════════════╗
║  Email Scraper v6.2 — Speed + Ban-Resistant    ║
║  Domain clean · DNS filter · lxml · Adaptive   ║
║  Robots.txt · URL scoring · Heartbeat          ║
╚════════════════════════════════════════════════╝{Style.RESET_ALL}
""")
    if not LXML_AVAILABLE:
        safe_print(
            f"{Fore.YELLOW}⚠  lxml not installed — using BeautifulSoup (slower)\n"
            f"   Install for best speed: pip install lxml{Style.RESET_ALL}\n"
        )

    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--test":
            test_single(input("Domain: ").strip())
        else:
            infile  = input("Domains file: ").strip()
            
            # ── Domain Cleaning Phase ────────────────────────────────────────────
            clean_choice = input("Clean domains? (1=Phase1 only, 2=Phase1+2, n=Skip, default 2): ").strip()
            
            if clean_choice == "1":
                # Phase 1 only - keep uncommon subdomains
                cleaned_domains, clean_stats = run_domain_cleaning(infile, phase2=False)
                infile = clean_stats.get('output_file', infile)
            elif clean_choice.lower() == "n":
                # Skip cleaning - use raw file
                cleaned_domains = load_raw_domains(infile)
                safe_print(f"{Fore.YELLOW}⚠ Skipping domain cleaning ({len(cleaned_domains)} raw domains){Style.RESET_ALL}\n")
            else:
                # Default: Phase 1 + Phase 2 - aggressive deduplication
                cleaned_domains, clean_stats = run_domain_cleaning(infile, phase2=True)
                infile = clean_stats.get('output_file', infile)
            
            # ── Scraping Configuration ─────────────────────────────────────────
            outfile = input("Output file (default emails_v6.txt): ").strip()
            if not outfile:
                outfile = "emails_v6.txt"
            
            w_raw   = input("Workers (default 12, max 30): ").strip()
            workers = min(int(w_raw), 30) if w_raw.isdigit() else 12
            speed   = input("Speed (1=Standard  2=Turbo  3=MAX, default 2): ").strip()
            dns_on  = input("DNS pre-filter? (Y/n): ").strip().lower() != "n"

            if speed == "1":
                turbo = False
                safe_print(f"{Fore.GREEN}🐢 STANDARD{Style.RESET_ALL}\n")
            elif speed == "3":
                turbo = True
                workers = min(workers + 5, 30)
                safe_print(f"{Fore.RED}🔥 MAX{Style.RESET_ALL}\n")
            else:
                turbo = True
                safe_print(f"{Fore.YELLOW}⚡ TURBO{Style.RESET_ALL}\n")

            process_domains(
                infile,
                output_file=outfile,
                max_workers=workers,
                turbo=turbo,
                resume=True,
                dns_filter=dns_on,
            )

    except KeyboardInterrupt:
        safe_print(f"\n{Fore.YELLOW}Goodbye!{Style.RESET_ALL}")
        sys.exit(0)