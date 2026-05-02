# Scrappy - Distributed Web Scraper

A high-performance distributed web scraping system with a master-slave architecture, built with Python FastAPI, PostgreSQL, and modern async processing.

## Overview

Scrappy is designed to scrape email addresses from large domain lists across multiple VPS instances. It features automatic job distribution, real-time monitoring, pause/resume functionality, and individual slave control.

## Architecture

### Master Node (`scraper-master/`)

The master node serves as the central control panel and job orchestrator:

- **Web Dashboard**: Jinja2-based UI for managing jobs and slaves
- **Job Management**: Upload, start, pause, resume, and monitor scraping jobs
- **Slave Orchestration**: Provision, monitor, and control slave VPS instances
- **Database**: PostgreSQL for persistent storage of jobs, slaves, and results
- **API**: RESTful endpoints for all operations

**Key Features:**
- Multi-file job upload support
- Automatic domain cleaning and deduplication
- Job pause/resume with progress preservation
- Real-time slave health monitoring
- Individual slave enable/disable toggle
- Bulk slave operations (restart all, pause all, resume all, toggle all)
- Activity logging and system notifications

### Slave Nodes (`scraper-slave/`)

Slave nodes are lightweight scraping workers that run on separate VPS instances:

- **HTTP API**: FastAPI-based service for receiving commands
- **Domain Processing**: Parallel async HTTP requests for scraping
- **Health Reporting**: Periodic system stats and heartbeat to master
- **Graceful Shutdown**: Handles SIGTERM/SIGINT for clean job termination

**Key Features:**
- Configurable worker count per job
- Turbo mode for maximum speed
- Automatic retry with exponential backoff
- System resource monitoring (CPU, RAM, disk)
- Support for job pause/resume signals

## Database Schema

### Tables

#### `jobs` - Job Management
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(50) | Unique job ID (timestamp + random) |
| `name` | VARCHAR(255) | Human-readable job name |
| `status` | VARCHAR(50) | Current status: uploaded, cleaning, dns_filtering, scraping, processing, completed, failed, cancelled, paused |
| `domains_total` | INTEGER | Total domains in original file |
| `domains_live` | INTEGER | Domains that passed DNS filtering |
| `domains_done` | INTEGER | Domains successfully processed |
| `emails_found` | INTEGER | Total unique emails discovered |
| `progress_percent` | INTEGER | Completion percentage (0-100) |
| `remaining_domains` | JSONB | Domains remaining when paused (for resume) |
| `live_domains` | JSONB | List of live domains after DNS filtering |
| `dns_stats` | JSONB | DNS filtering statistics |
| `created_at` | TIMESTAMP | Job creation time |
| `started_at` | TIMESTAMP | Job start time |
| `finished_at` | TIMESTAMP | Job completion time |
| `error` | TEXT | Error message if failed |
| `assigned_slaves` | JSONB | List of slave IDs assigned to job |

#### `slaves` - Slave Management
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(50) | Unique slave identifier |
| `name` | VARCHAR(255) | Human-readable slave name |
| `url` | TEXT | Slave API endpoint URL |
| `ip` | VARCHAR(50) | Slave IP address |
| `status` | VARCHAR(50) | Current status: idle, scraping, paused, offline, error, provisioning |
| `domains_assigned` | INTEGER | Total domains assigned |
| `domains_done` | INTEGER | Domains processed by this slave |
| `emails_found` | INTEGER | Emails found by this slave |
| `provision_progress` | TEXT | Current provisioning step |
| `system_stats` | JSONB | CPU, RAM, disk usage, load averages |
| `last_seen` | TIMESTAMP | Last heartbeat timestamp |
| `enabled` | BOOLEAN | Whether slave participates in jobs (default: TRUE) |
| `created_at` | TIMESTAMP | When slave was added |
| `updated_at` | TIMESTAMP | Last update timestamp |

#### `slave_chunks` - Job Distribution Tracking
| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `slave_id` | VARCHAR(50) | Reference to slaves table |
| `job_id` | VARCHAR(50) | Reference to jobs table |
| `chunk_index` | INTEGER | Chunk number for this slave |
| `start_index` | INTEGER | Starting line in domains file |
| `end_index` | INTEGER | Ending line in domains file |
| `total` | INTEGER | Total domains in this chunk |
| `processed` | INTEGER | Domains processed so far |
| `emails` | INTEGER | Emails found in this chunk |
| `created_at` | TIMESTAMP | When chunk was assigned |

#### `results` - Scraped Emails
| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `job_id` | VARCHAR(50) | Reference to jobs table |
| `domain` | TEXT | Domain where email was found |
| `email` | TEXT | Discovered email address |
| `source_url` | TEXT | URL where email was found |
| `created_at` | TIMESTAMP | When email was discovered |

#### `activities` - System Activity Log
| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `level` | VARCHAR(20) | Log level: info, warning, error, success |
| `category` | VARCHAR(50) | Category: system, jobs, slaves |
| `message` | TEXT | Log message |
| `details` | JSONB | Additional structured data |
| `created_at` | TIMESTAMP | When activity was logged |

### Indexes
- `results.job_id` - For fast job result queries
- `results.email` - For email uniqueness checks
- `slave_chunks.job_id, slave_chunks.slave_id` - For chunk lookups
- `activities.created_at` - For log pagination

## API Endpoints

### Job Management
```
POST   /api/jobs                    - Create new job (upload files)
GET    /api/jobs                    - List all jobs
POST   /api/jobs/{id}/start         - Start a job
POST   /api/jobs/{id}/pause         - Pause a running job
POST   /api/jobs/{id}/resume        - Resume a paused job
POST   /api/jobs/{id}/cancel        - Cancel a job
POST   /api/jobs/{id}/retry         - Retry incomplete domains
DELETE /api/jobs/{id}               - Delete a job
GET    /api/jobs/{id}/download       - Download results CSV
```

### Slave Management
```
GET    /api/slaves                  - List all slaves
POST   /api/slaves                  - Add a new slave
GET    /api/slaves/{id}             - Get slave details
POST   /api/slaves/{id}/toggle      - Toggle enabled/disabled
POST   /api/slaves/{id}/pause       - Pause slave operations
POST   /api/slaves/{id}/resume      - Resume slave operations
POST   /api/slaves/{id}/reboot      - Reboot slave VPS
POST   /api/slaves/{id}/update      - Update slave code
DELETE /api/slaves/{id}             - Remove slave
POST   /api/slaves/check-all        - Check all slave health
POST   /api/slaves/provision         - Provision new slave VPS
```

### System
```
GET    /api/stats                   - Dashboard statistics
GET    /api/activities              - Recent activity log
GET    /                           - Web dashboard UI
```

## Job Lifecycle

1. **Uploaded** → Files uploaded, domains extracted
2. **Cleaning** → Deduplication and validation
3. **DNS Filtering** → (Optional) Check domain DNS resolution
4. **Scraping** → Domains distributed to slaves for processing
5. **Processing** → Results being collected and aggregated
6. **Completed** → All domains processed successfully
7. **Failed** → Error occurred during processing
8. **Paused** → Job paused by user, can be resumed
9. **Cancelled** → Job cancelled by user

## Slave Enable/Disable Feature

The `enabled` column in the `slaves` table controls whether a slave participates in scraping:

- **Enabled (default)**: Slave receives domains and participates in all operations
- **Disabled**: Slave is excluded from:
  - New job domain distribution
  - Resume/retry operations
  - Bulk actions (restart all, pause all, resume all)
  - Health check bulk operations

**Use Cases:**
- Temporarily remove a problematic slave without deleting it
- Reserve specific slaves for special jobs
- Maintenance mode - keep slave in system but don't assign work

## Deployment

### Requirements
- Python 3.11+
- PostgreSQL 14+
- VPS with public IP (for master)
- Multiple VPS instances (for slaves)

### Environment Variables
```bash
DATABASE_URL=postgresql://user:pass@localhost/scraperdb
MASTER_HOST=0.0.0.0
MASTER_PORT=8000
SLAVE_PORT=8001
```

### Installation
```bash
# Master node
cd scraper-master
pip install -r requirements.txt
python master.py

# Slave node
cd scraper-slave
pip install -r requirements.txt
python slave.py
```

### Systemd Services
Master and slave include systemd service files for production deployment with auto-start on boot.

## Dashboard UI

The web dashboard provides:
- **Jobs Panel**: Upload, start, monitor, and manage jobs
- **Slaves Panel**: View status, toggle enabled/disabled, bulk operations
- **Stats Cards**: Real-time counters for jobs, slaves, domains, emails
- **Activity Log**: System events and notifications
- **Settings**: Slave configuration and provisioning

## License

MIT License
