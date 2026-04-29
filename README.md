# Email Scraper v6.2 — Speed-Optimised, Ban-Resistant

## Overview
A multi-threaded web crawler that extracts email addresses from websites. Features **domain cleaning**, **DNS pre-filtering**, **adaptive backoff**, and **robots.txt respect** for fast, polite scraping.

---

## Features

| Feature | Description |
|---------|-------------|
| **Domain Cleaning** | 2-phase deduplication, subdomain stripping, ccTLD-aware (co.uk, com.au, etc.) |
| **DNS Pre-filter** | Discards dead domains in <2ms before HTTP requests |
| **Concurrent Processing** | `ThreadPoolExecutor` with thread-local sessions |
| **Email Filtering** | Excludes AWS, noreply, MD5 hashes, common false positives |
| **Adaptive Backoff** | Per-domain rate limiting on 429/503 responses |
| **Robots.txt Cache** | Respects robots.txt per host, cached to avoid repeated fetches |
| **Smart URL Scoring** | Prioritizes /contact, /about pages for faster email discovery |
| **Resume Capability** | Saves state to JSON, resume interrupted runs |
| **Graceful Shutdown** | Ctrl+C stops cleanly, saves progress |
| **3 Speed Modes** | Standard / Turbo / MAX for different risk/speed tradeoffs |

---

## Domain Cleaning Pipeline

Before scraping, domains are cleaned in 2 phases:

### Phase 1: Deduplication & Common Subdomain Removal
- Removes exact duplicates
- Strips common subdomains: `www`, `mail`, `ftp`, `blog`, `shop`, `api`, `cdn`, etc.
- Keeps uncommon subdomains (e.g., `api.internal.example.com` → `api.internal.example.com`)

### Phase 2: Aggressive Deduplication (Optional)
- Strips ALL remaining subdomains
- Keeps only effective/registrable domain

### ccTLD Handling
Correctly handles second-level ccTLDs:
| Input | Effective Domain |
|-------|-----------------|
| `www.domain.co.uk` | `domain.co.uk` |
| `shop.domain.com.au` | `domain.com.au` |
| `blog.company.net` | `company.net` |
| `api.sub.example.com` | `sub.example.com` (kept if uncommon) |

### Output
Cleaned domains saved to `domains_cleaned.txt` before scraping begins.

---

## Speed Optimizations (Implemented in `pyscrap.py`)

### Three Speed Modes

| Mode | Workers | Jitter | Pages/Domain | Description |
|------|---------|--------|--------------|-------------|
| **Standard** (1) | 10 | 0.02-0.08s | 5 | Safest, staggered, minimal delays |
| **Turbo** (2) | 10-25 | 0s | 5 | No delays, high concurrency |
| **MAX/Insane** (3) | 15-30 | 0s | 3 | Aggressive, minimal retries |

### Aggressive Optimizations
- **Zero jitter in turbo** - removes all `time.sleep()` calls
- **Early exit at 2 emails** - stop crawling domain immediately
- **Max 3 pages per domain** - homepage + 2 contact pages
- **4 second timeouts** - aggressive fail-fast
- **200 connection pool** - massive reuse in MAX mode
- **Zero retries in MAX** - no backoff on errors
- **No batch delays** - submit all domains instantly

### Usage
```bash
python pyscrap.py

# Interactive prompts:
# Domains file: domains.txt
# Clean domains? (1=Phase1 only, 2=Phase1+2, n=Skip, default 2): 2
# Output file (default emails_v6.txt): my_emails.txt
# Workers: (default 12, max 30): 15
# Speed mode: (1=Standard, 2=Turbo, 3=MAX, default 2): 2
# DNS pre-filter? (Y/n): y
```

### CLI Options
```bash
# Test single domain
python pyscrap.py --test
```

### Expected Speeds (3,500 domains)
| Mode | Est. Time | Rate | Risk |
|------|-----------|------|------|
| Standard | ~90 min | ~0.6/s | Low |
| Turbo | ~45 min | ~1.3/s | Medium |
| MAX | ~25 min | ~2.3/s | High |

---

## Undetectability Improvements

1. **Rotating User-Agents & Headers**
   ```python
   headers = {
       'User-Agent': random.choice(USER_AGENTS),
       'Accept-Language': random.choice(['en-US', 'en-GB', 'fr-FR']),
       'Referer': random.choice(REFERRERS)
   }
   ```

2. **Proxy Rotation**
   - Integrate residential proxy pools (Oxylabs, Smartproxy)
   - Rotate IP per request or per domain

3. **Request Jitter**
   - Add random delays (0.5-3s) between requests
   - Implement exponential backoff on 429 responses

4. **Browser Fingerprinting**
   - Use `playwright` or `selenium-stealth` for JS-heavy sites
   - Rotate viewport, timezone, and WebGL fingerprints

5. **Distributed Requests**
   - Space out requests to same domain over time
   - Respect `Crawl-Delay` from robots.txt

6. **TLS/JA3 Fingerprint Randomization**
   - Use `curl_cffi` or `requests-impersonate` to mimic real browsers

---

## Legal/Ethical Considerations

- **Check robots.txt** before crawling any domain
- Implement `Crawl-delay` respect
- Add opt-out list for domains that request no scraping
- Consider GDPR compliance for EU domains
