#!/usr/bin/env python3
"""
DNS Filter - Fast domain liveness checker with Cloudflare NS detection
=======================================================================
Features:
  • Fast parallel DNS resolution (100+ workers)
  • Detects Cloudflare nameservers and removes those domains
  • Generates live.txt and bad.txt output files
  • Uses same fast DNS method as pyscrap.py
"""

import re
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False
    print("WARNING: dnspython not installed. Cloudflare NS detection disabled.")
    print("Install with: pip install dnspython")

# ── Constants ──────────────────────────────────────────────────────────────────
DNS_TIMEOUT = 2  # seconds for DNS resolution
WORKERS = 1000   # parallel DNS workers (more = better throughput)
BATCH_SIZE = 100 # results batch size to reduce lock contention

# Cloudflare nameservers to detect
CLOUDFLARE_NS = {
    'dana.ns.cloudflare.com',
    'kai.ns.cloudflare.com',
    'lily.ns.cloudflare.com',
    'mark.ns.cloudflare.com',
    'noah.ns.cloudflare.com',
    'norm.ns.cloudflare.com',
    'sara.ns.cloudflare.com',
    'tom.ns.cloudflare.com',
    'uma.ns.cloudflare.com',
    'walt.ns.cloudflare.com',
    'zeke.ns.cloudflare.com',
    'zoe.ns.cloudflare.com',
    'bob.ns.cloudflare.com',
    'abby.ns.cloudflare.com',
    'adam.ns.cloudflare.com',
    'alan.ns.cloudflare.com',
    'amy.ns.cloudflare.com',
    'andy.ns.cloudflare.com',
    'anna.ns.cloudflare.com',
    'bella.ns.cloudflare.com',
    'ben.ns.cloudflare.com',
    'betty.ns.cloudflare.com',
    'brad.ns.cloudflare.com',
    'brian.ns.cloudflare.com',
    'cara.ns.cloudflare.com',
    'cathy.ns.cloudflare.com',
    'charlie.ns.cloudflare.com',
    'clara.ns.cloudflare.com',
    'cody.ns.cloudflare.com',
    'cole.ns.cloudflare.com',
    'dana.ns.cloudflare.com',
    'dave.ns.cloudflare.com',
    'dave.ns.cloudflare.com',
    'diana.ns.cloudflare.com',
    'doug.ns.cloudflare.com',
    'drew.ns.cloudflare.com',
    'earl.ns.cloudflare.com',
    'elle.ns.cloudflare.com',
    'eric.ns.cloudflare.com',
    'evan.ns.cloudflare.com',
    'faye.ns.cloudflare.com',
    'felix.ns.cloudflare.com',
    'fiona.ns.cloudflare.com',
    'gabe.ns.cloudflare.com',
    'gina.ns.cloudflare.com',
    'grace.ns.cloudflare.com',
    'gus.ns.cloudflare.com',
    'hank.ns.cloudflare.com',
    'heidi.ns.cloudflare.com',
    'hugh.ns.cloudflare.com',
    'ian.ns.cloudflare.com',
    'irma.ns.cloudflare.com',
    'jack.ns.cloudflare.com',
    'jake.ns.cloudflare.com',
    'jane.ns.cloudflare.com',
    'jason.ns.cloudflare.com',
    'jeff.ns.cloudflare.com',
    'jenna.ns.cloudflare.com',
    'jess.ns.cloudflare.com',
    'john.ns.cloudflare.com',
    'josh.ns.cloudflare.com',
    'julia.ns.cloudflare.com',
    'justin.ns.cloudflare.com',
    'karen.ns.cloudflare.com',
    'karl.ns.cloudflare.com',
    'kate.ns.cloudflare.com',
    'katy.ns.cloudflare.com',
    'keith.ns.cloudflare.com',
    'kelly.ns.cloudflare.com',
    'kent.ns.cloudflare.com',
    'kevin.ns.cloudflare.com',
    'kim.ns.cloudflare.com',
    'kris.ns.cloudflare.com',
    'kyle.ns.cloudflare.com',
    'laura.ns.cloudflare.com',
    'leah.ns.cloudflare.com',
    'leo.ns.cloudflare.com',
    'lexi.ns.cloudflare.com',
    'lily.ns.cloudflare.com',
    'logan.ns.cloudflare.com',
    'lola.ns.cloudflare.com',
    'lucy.ns.cloudflare.com',
    'luke.ns.cloudflare.com',
    'lynn.ns.cloudflare.com',
    'maddy.ns.cloudflare.com',
    'maria.ns.cloudflare.com',
    'mark.ns.cloudflare.com',
    'martin.ns.cloudflare.com',
    'mary.ns.cloudflare.com',
    'matt.ns.cloudflare.com',
    'max.ns.cloudflare.com',
    'maya.ns.cloudflare.com',
    'megan.ns.cloudflare.com',
    'mike.ns.cloudflare.com',
    'molly.ns.cloudflare.com',
    'nancy.ns.cloudflare.com',
    'natalie.ns.cloudflare.com',
    'nathan.ns.cloudflare.com',
    'neil.ns.cloudflare.com',
    'nora.ns.cloudflare.com',
    'norm.ns.cloudflare.com',
    'olivia.ns.cloudflare.com',
    'oscar.ns.cloudflare.com',
    'owen.ns.cloudflare.com',
    'pam.ns.cloudflare.com',
    'parker.ns.cloudflare.com',
    'pat.ns.cloudflare.com',
    'paul.ns.cloudflare.com',
    'peter.ns.cloudflare.com',
    'phil.ns.cloudflare.com',
    'phoebe.ns.cloudflare.com',
    'rachel.ns.cloudflare.com',
    'raquel.ns.cloudflare.com',
    'ray.ns.cloudflare.com',
    'rebecca.ns.cloudflare.com',
    'rich.ns.cloudflare.com',
    'rick.ns.cloudflare.com',
    'rita.ns.cloudflare.com',
    'robert.ns.cloudflare.com',
    'robin.ns.cloudflare.com',
    'ron.ns.cloudflare.com',
    'rose.ns.cloudflare.com',
    'ryan.ns.cloudflare.com',
    'sage.ns.cloudflare.com',
    'sally.ns.cloudflare.com',
    'sam.ns.cloudflare.com',
    'sara.ns.cloudflare.com',
    'scott.ns.cloudflare.com',
    'sean.ns.cloudflare.com',
    'sharon.ns.cloudflare.com',
    'shawn.ns.cloudflare.com',
    'simon.ns.cloudflare.com',
    'skyler.ns.cloudflare.com',
    'sofia.ns.cloudflare.com',
    'spencer.ns.cloudflare.com',
    'stacy.ns.cloudflare.com',
    'stephanie.ns.cloudflare.com',
    'steve.ns.cloudflare.com',
    'tara.ns.cloudflare.com',
    'ted.ns.cloudflare.com',
    'terry.ns.cloudflare.com',
    'tina.ns.cloudflare.com',
    'todd.ns.cloudflare.com',
    'tom.ns.cloudflare.com',
    'tony.ns.cloudflare.com',
    'tracy.ns.cloudflare.com',
    'travis.ns.cloudflare.com',
    'tyler.ns.cloudflare.com',
    'uma.ns.cloudflare.com',
    'ursula.ns.cloudflare.com',
    'vanessa.ns.cloudflare.com',
    'victor.ns.cloudflare.com',
    'vince.ns.cloudflare.com',
    'vinny.ns.cloudflare.com',
    'violet.ns.cloudflare.com',
    'wayne.ns.cloudflare.com',
    'will.ns.cloudflare.com',
    'william.ns.cloudflare.com',
    'xavier.ns.cloudflare.com',
    'yara.ns.cloudflare.com',
    'yvonne.ns.cloudflare.com',
    'zach.ns.cloudflare.com',
    'zara.ns.cloudflare.com',
}

# Common ccTLDs that have second-level domains
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

# ── Domain Cleaning ────────────────────────────────────────────────────────────

def extract_domain_parts(domain: str) -> tuple:
    """Extract (subdomain, effective_domain) from a domain."""
    domain = domain.lower().strip()
    domain = domain.replace('https://', '').replace('http://', '')
    domain = domain.split('/')[0]
    domain = domain.split(':')[0]
    
    parts = domain.split('.')
    
    if len(parts) < 2:
        return ('', domain)
    
    # Check for ccTLD with second-level
    if len(parts) >= 3:
        last3 = '.'.join(parts[-3:])
        last2 = '.'.join(parts[-2:])
        
        for cctld in CCTLD_SECOND_LEVEL:
            if last3.endswith('.' + cctld) or last3 == cctld:
                effective = '.'.join(parts[-3:])
                subdomain = '.'.join(parts[:-3]) if len(parts) > 3 else ''
                return (subdomain, effective)
    
    # Standard TLD
    effective_domain = '.'.join(parts[-2:])
    subdomain = '.'.join(parts[:-2]) if len(parts) > 2 else ''
    return (subdomain, effective_domain)


def normalize_domain(domain: str) -> str:
    """Normalize domain: lowercase, strip protocol, get effective domain."""
    subdomain, effective = extract_domain_parts(domain)
    return effective


def clean_domains(domains: list) -> tuple:
    """Clean and deduplicate domains."""
    seen = set()
    cleaned = []
    
    for domain in domains:
        if not domain or not domain.strip():
            continue
        
        subdomain, effective = extract_domain_parts(domain)
        
        if effective in seen:
            continue
        
        # Strip common subdomains
        if subdomain in COMMON_SUBDOMAINS or not subdomain:
            cleaned.append(effective)
        else:
            full = f"{subdomain}.{effective}".lstrip('.')
            if full not in seen:
                cleaned.append(full)
        
        seen.add(effective)
    
    return cleaned


# ── DNS Resolution ─────────────────────────────────────────────────────────────

_dns_cache = {}

def dns_resolves(hostname: str) -> bool:
    """Cheap DNS check — cached, no global timeout manipulation."""
    # Check cache first
    if hostname in _dns_cache:
        return _dns_cache[hostname]
    
    try:
        # Use socket.getaddrinfo with direct timeout via creating a socket
        socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
        _dns_cache[hostname] = True
        return True
    except Exception:
        _dns_cache[hostname] = False
        return False


def has_cloudflare_ns(domain: str) -> bool:
    """Check if domain uses Cloudflare nameservers."""
    if not DNS_AVAILABLE:
        return False
    
    try:
        # Get NS records
        answers = dns.resolver.resolve(domain, 'NS', lifetime=DNS_TIMEOUT)
        ns_names = [str(rdata).lower().rstrip('.') for rdata in answers]
        
        # Check if any NS is Cloudflare
        for ns in ns_names:
            if ns in CLOUDFLARE_NS:
                return True
        return False
    except Exception:
        return False


def check_domain(domain: str) -> dict:
    """Check if domain is live and not using Cloudflare."""
    result = {
        'domain': domain,
        'live': False,
        'cloudflare': False,
        'reason': ''
    }
    
    # Clean domain
    clean = normalize_domain(domain)
    result['clean_domain'] = clean
    
    # Check DNS resolution
    if not dns_resolves(clean):
        result['reason'] = 'DNS failed'
        return result
    
    result['live'] = True
    
    # Check for Cloudflare NS
    if has_cloudflare_ns(clean):
        result['cloudflare'] = True
        result['live'] = False
        result['reason'] = 'Cloudflare NS'
        return result
    
    return result


# ── Global state for Ctrl+C handling ────────────────────────────────────────────
shutdown_event = threading.Event()
live_domains = []
bad_domains = []
progress_lock = threading.Lock()
current_input_name = None


def handle_interrupt(signum, frame):
    """Handle Ctrl+C - save progress and exit."""
    print("\n\n⚠️ Interrupted by user. Saving progress...")
    shutdown_event.set()
    
    if live_domains or bad_domains and current_input_name:
        save_results(live_domains, bad_domains, current_input_name)
        print(f"✓ Progress saved: {len(live_domains)} live, {len(bad_domains)} bad")
    
    sys.exit(0)


# ── Main Processing ────────────────────────────────────────────────────────────

def process_domains(domains: list, workers: int = WORKERS, input_name: str = None) -> tuple:
    """Process all domains in parallel with batched results."""
    global live_domains, bad_domains, current_input_name
    current_input_name = input_name
    live_domains = []
    bad_domains = []
    
    # Thread-local batch storage to reduce lock contention
    from collections import deque
    results_queue = deque()
    counter_lock = threading.Lock()
    cloudflare_count = [0]
    dns_fail_count = [0]
    processed = [0]
    
    def flush_batch():
        """Flush batched results to global lists."""
        with progress_lock:
            while results_queue:
                item = results_queue.popleft()
                if item['live']:
                    live_domains.append(item['clean_domain'])
                else:
                    bad_domains.append(item['clean_domain'])
                    if item['cloudflare']:
                        cloudflare_count[0] += 1
                    else:
                        dns_fail_count[0] += 1
    
    def process(d):
        if shutdown_event.is_set():
            return None
        
        result = check_domain(d)
        results_queue.append(result)
        
        # Flush when batch is full
        if len(results_queue) >= BATCH_SIZE:
            flush_batch()
        
        # Update progress counter
        with counter_lock:
            processed[0] += 1
            return processed[0]
    
    print(f"⚡ Processing {len(domains)} domains with {workers} workers...")
    print("   Press Ctrl+C to save progress and exit\n")
    start = time.time()
    last_printed = [0]
    
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dns") as executor:
        futures = [executor.submit(process, d) for d in domains]
        
        for future in as_completed(futures):
            if shutdown_event.is_set():
                break
            
            count = future.result()
            if count and count - last_printed[0] >= 500:
                elapsed = time.time() - start
                rate = count / elapsed if elapsed > 0 else 0
                last_printed[0] = count
                print(f"   Progress: {count}/{len(domains)} · Live: {len(live_domains)} · Bad: {len(bad_domains)} · {rate:.0f}/s")
    
    # Final flush of remaining results
    flush_batch()
    
    elapsed = time.time() - start
    total = len(live_domains) + len(bad_domains)
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\n✓ Complete: {len(live_domains)} live, {len(bad_domains)} bad in {elapsed:.1f}s ({rate:.0f} domains/s)")
    print(f"  - DNS failures: {dns_fail_count[0]}")
    print(f"  - Cloudflare NS: {cloudflare_count[0]}")
    
    return live_domains, bad_domains


def load_domains(filepath: str) -> list:
    """Load domains from file."""
    domains = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    line = line.replace('https://', '').replace('http://', '')
                    line = line.split('/')[0]
                    line = line.split(':')[0]
                    if line and '.' in line:
                        domains.append(line.lower())
    except Exception as e:
        print(f"✗ Failed to load domains: {e}")
        sys.exit(1)
    
    return domains


def save_results(live: list, bad: list, input_name: str):
    """Save results to files."""
    # Generate output filenames
    live_file = f"{input_name}_live.txt"
    bad_file = f"{input_name}_bad.txt"
    
    # Save live domains
    try:
        with open(live_file, 'w', encoding='utf-8') as f:
            for domain in sorted(live):
                f.write(f"{domain}\n")
        print(f"💾 Saved live domains: {live_file}")
    except Exception as e:
        print(f"✗ Failed to save live domains: {e}")

    # Save bad domains
    try:
        with open(bad_file, 'w', encoding='utf-8') as f:
            for domain in sorted(bad):
                f.write(f"{domain}\n")
        print(f"💾 Saved bad domains: {bad_file}")
    except Exception as e:
        print(f"✗ Failed to save bad domains: {e}")


def main():
    # Prompt for input file
    print("🔍 DNS Filter - Fast Domain Liveness Checker")
    print(f"{'─' * 50}")
    input_file = input("Enter input file name (e.g., domains.txt): ").strip()
    
    if not input_file:
        print("✗ No input file provided")
        sys.exit(1)
    
    # Prompt for workers (optional)
    workers_input = input(f"Enter number of workers [default: {WORKERS}]: ").strip()
    workers = int(workers_input) if workers_input else WORKERS
    
    # Get input name without extension
    input_name = Path(input_file).stem
    
    # Register Ctrl+C handler
    signal.signal(signal.SIGINT, handle_interrupt)
    
    print(f"Input file: {input_file}")
    print(f"Output: {input_name}_live.txt, {input_name}_bad.txt")
    print(f"Workers: {workers}")
    print(f"{'─' * 50}\n")
    
    # Load domains
    raw = load_domains(input_file)
    print(f"📥 Loaded: {len(raw)} raw domains")
    
    # Clean domains
    cleaned = clean_domains(raw)
    print(f"🔹 Cleaned: {len(cleaned)} unique domains\n")
    
    # Process domains
    live, bad = process_domains(cleaned, workers, input_name)
    
    # Save results
    print()
    save_results(live, bad, input_name)
    
    print()
    print(f"{'─' * 50}")
    print(f"✓ Done! {len(live)} live domains ready for scraping")
    print(f"{'─' * 50}")


if __name__ == "__main__":
    main()
