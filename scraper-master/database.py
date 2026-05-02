"""
PostgreSQL Database Layer for Scraper Master
Handles all persistent storage: jobs, slaves, settings, activity logs, emails
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import asyncpg
from databases import Database

# Database URL from environment or default
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://scraper:scraper123@localhost/scraperdb"
)

# Global database instance
db: Database = Database(DATABASE_URL)


# ── SQL Schema ─────────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
-- Settings table for app configuration
CREATE TABLE IF NOT EXISTS settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Jobs table - stores all job information
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'uploaded',
    file_path TEXT,
    result_file TEXT,
    domains_total INTEGER DEFAULT 0,
    domains_cleaned INTEGER DEFAULT 0,
    domains_live INTEGER DEFAULT 0,
    domains_done INTEGER DEFAULT 0,
    emails_count INTEGER DEFAULT 0,
    clean_stats JSONB,
    dns_stats JSONB,
    progress JSONB,
    domains_per_slave JSONB,
    remaining_domains JSONB,
    live_domains JSONB,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Slaves table - stores slave/agent information
CREATE TABLE IF NOT EXISTS slaves (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    url TEXT NOT NULL,
    ip VARCHAR(50),
    status VARCHAR(50) DEFAULT 'idle',
    domains_assigned INTEGER DEFAULT 0,
    domains_done INTEGER DEFAULT 0,
    emails_found INTEGER DEFAULT 0,
    provision_progress TEXT,
    system_stats JSONB,
    last_seen TIMESTAMP,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Activity logs - all actions and events
CREATE TABLE IF NOT EXISTS activity_logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL DEFAULT 'info',
    category VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    job_id VARCHAR(50) REFERENCES jobs(id) ON DELETE SET NULL,
    slave_id VARCHAR(50) REFERENCES slaves(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Emails table - stores scraped emails per job
CREATE TABLE IF NOT EXISTS emails (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    domain VARCHAR(255),
    source VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, email)
);

-- Job slave assignments - tracks which slaves work on which jobs
CREATE TABLE IF NOT EXISTS job_slave_assignments (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    slave_id VARCHAR(50) NOT NULL REFERENCES slaves(id) ON DELETE CASCADE,
    domains_assigned INTEGER DEFAULT 0,
    domains_done INTEGER DEFAULT 0,
    emails_found INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'assigned',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, slave_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_slaves_status ON slaves(status);
CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON activity_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_logs_category ON activity_logs(category);
CREATE INDEX IF NOT EXISTS idx_activity_logs_job_id ON activity_logs(job_id);
CREATE INDEX IF NOT EXISTS idx_emails_job_id ON emails(job_id);
CREATE INDEX IF NOT EXISTS idx_emails_email ON emails(email);
"""


# ── Database Lifecycle ───────────────────────────────────────────────────────────

async def init_database():
    """Initialize database connection and create tables."""
    await db.connect()
    
    # Create tables - execute each statement separately (asyncpg doesn't support multi-statement)
    # Split by semicolon and execute each non-empty statement
    statements = [s.strip() for s in CREATE_TABLES_SQL.split(';') if s.strip()]
    for i, statement in enumerate(statements):
        try:
            await db.execute(statement)
        except Exception as e:
            # Ignore "already exists" errors, raise others
            if "already exists" in str(e).lower():
                continue
            print(f"[DB INIT] Failed on statement {i}: {str(e)[:100]}")
            print(f"[DB INIT] Statement: {statement[:200]}")
            raise
    
    # Insert default settings if not exist
    await _init_default_settings()
    
    # Migration: Add enabled column to slaves if it doesn't exist
    await _migrate_add_slave_enabled()
    
    await log_activity("info", "system", "Database initialized and tables created")


async def _migrate_add_slave_enabled():
    """Migration: Add enabled column to slaves table if not exists."""
    try:
        await db.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='slaves' AND column_name='enabled'
                ) THEN
                    ALTER TABLE slaves ADD COLUMN enabled BOOLEAN DEFAULT TRUE;
                END IF;
            END $$;
        """)
    except Exception as e:
        print(f"[MIGRATION] Slave enabled column check: {e}")


async def close_database():
    """Close database connection."""
    await db.disconnect()


async def _init_default_settings():
    """Initialize default application settings."""
    defaults = [
        ("dns_timeout", "3.0", "DNS resolution timeout in seconds"),
        ("dns_workers", "1000", "Number of concurrent DNS workers"),
        ("dns_retries", "1", "Number of DNS retry attempts"),
        ("scrape_workers", "12", "Default number of scraper workers per slave"),
        ("max_slaves", "10", "Maximum number of slaves allowed"),
        ("email_batch_size", "50", "Batch size for sending emails to master"),
        ("heartbeat_interval", "15", "Slave heartbeat interval in seconds"),
        ("auto_cleanup_days", "30", "Days to keep completed jobs before cleanup"),
        ("system_version", "2.1.0", "Current system version"),
    ]
    
    for key, value, description in defaults:
        await db.execute(
            """
            INSERT INTO settings (key, value, description)
            VALUES (:key, :value, :description)
            ON CONFLICT (key) DO NOTHING
            """,
            {"key": key, "value": value, "description": description}
        )


# ── Settings Operations ──────────────────────────────────────────────────────────

async def get_setting(key: str, default: str = None) -> str:
    """Get a setting value by key."""
    result = await db.fetch_one(
        "SELECT value FROM settings WHERE key = :key",
        {"key": key}
    )
    return result["value"] if result else default


async def set_setting(key: str, value: str, description: str = None):
    """Set a setting value."""
    await db.execute(
        """
        INSERT INTO settings (key, value, description, updated_at)
        VALUES (:key, :value, :description, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            description = COALESCE(EXCLUDED.description, settings.description),
            updated_at = CURRENT_TIMESTAMP
        """,
        {"key": key, "value": value, "description": description}
    )


async def get_all_settings() -> List[Dict[str, Any]]:
    """Get all settings."""
    rows = await db.fetch_all(
        "SELECT key, value, description, updated_at FROM settings ORDER BY key"
    )
    return [dict(row) for row in rows]


# ── Jobs Operations ────────────────────────────────────────────────────────────

async def create_job(job_id: str, name: str = None, file_path: str = None) -> Dict[str, Any]:
    """Create a new job record."""
    await db.execute(
        """
        INSERT INTO jobs (id, name, file_path, status, created_at)
        VALUES (:id, :name, :file_path, 'uploaded', CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO NOTHING
        """,
        {"id": job_id, "name": name or f"Job {job_id[:8]}", "file_path": file_path}
    )
    await log_activity("info", "jobs", f"Job {job_id} created", {"job_id": job_id})
    return await get_job(job_id)


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID."""
    row = await db.fetch_one(
        """
        SELECT id, name, status, file_path, result_file,
               domains_total, domains_cleaned, domains_live, domains_done,
               emails_count, clean_stats, dns_stats, progress, domains_per_slave,
               remaining_domains, live_domains, error, created_at, started_at, finished_at, updated_at
        FROM jobs WHERE id = :id
        """,
        {"id": job_id}
    )
    if not row:
        return None
    
    job = dict(row)
    # Parse JSONB fields
    for field in ["clean_stats", "dns_stats", "progress", "domains_per_slave", "remaining_domains", "live_domains"]:
        if job.get(field) and isinstance(job[field], str):
            try:
                job[field] = json.loads(job[field])
            except:
                job[field] = {}
    return job


async def update_job(job_id: str, **fields):
    """Update job fields. Allows None values to clear JSONB fields."""
    if not fields:
        return
    
    # Build dynamic query
    set_clauses = []
    values = {"id": job_id}
    
    # Fields that can be updated (including live_domains)
    allowed_fields = [
        "name", "status", "file_path", "result_file",
        "domains_total", "domains_cleaned", "domains_live", "domains_done",
        "emails_count", "clean_stats", "dns_stats", "progress",
        "domains_per_slave", "remaining_domains", "live_domains", "error", "started_at", "finished_at"
    ]
    
    for key, value in fields.items():
        if key in allowed_fields:
            set_clauses.append(f"{key} = :{key}")
            # Convert dicts to JSON strings for JSONB fields, allow None to clear
            if isinstance(value, (dict, list)):
                values[key] = json.dumps(value)
            else:
                values[key] = value
    
    if set_clauses:
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        query = f"UPDATE jobs SET {', '.join(set_clauses)} WHERE id = :id"
        await db.execute(query, values)


async def delete_job(job_id: str) -> bool:
    """Delete a job and all associated data (cascades to emails and assignments)."""
    # Check if job exists
    job = await get_job(job_id)
    if not job:
        return False
    
    # Delete job (cascades to emails and job_slave_assignments due to FK constraints)
    await db.execute("DELETE FROM jobs WHERE id = :id", {"id": job_id})
    
    await log_activity("info", "jobs", f"Job {job_id} deleted", {"job_id": job_id})
    return True


async def list_jobs(
    search: str = None,
    status: str = None,
    page: int = 1,
    limit: int = 50
) -> Dict[str, Any]:
    """List jobs with pagination and filtering."""
    where_clauses = []
    values = {}
    
    if search:
        where_clauses.append("(name ILIKE :search OR id ILIKE :search)")
        values["search"] = f"%{search}%"
    
    if status:
        where_clauses.append("status = :status")
        values["status"] = status
    
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    # Count total
    count_query = f"SELECT COUNT(*) FROM jobs {where_sql}"
    total_row = await db.fetch_one(count_query, values)
    total = total_row[0] if total_row else 0
    
    # Fetch jobs
    offset = (page - 1) * limit
    values["limit"] = limit
    values["offset"] = offset
    
    query = f"""
        SELECT id, name, status, domains_total, domains_cleaned, domains_live,
               domains_done, emails_count, progress, remaining_domains, error, created_at,
               started_at, finished_at
        FROM jobs
        {where_sql}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    rows = await db.fetch_all(query, values)
    jobs = []
    for row in rows:
        job = dict(row)
        if job.get("progress") and isinstance(job["progress"], str):
            try:
                job["progress"] = json.loads(job["progress"])
            except:
                job["progress"] = None
        if job.get("remaining_domains") and isinstance(job["remaining_domains"], str):
            try:
                job["remaining_domains"] = json.loads(job["remaining_domains"])
            except:
                job["remaining_domains"] = None
        jobs.append(job)
    
    return {
        "jobs": jobs,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }


# ── Slaves Operations ───────────────────────────────────────────────────────────

async def register_slave(
    slave_id: str,
    url: str,
    name: str = None,
    ip: str = None
) -> Dict[str, Any]:
    """Register or update a slave."""
    await db.execute(
        """
        INSERT INTO slaves (id, name, url, ip, status, last_seen, created_at, updated_at)
        VALUES (:id, :name, :url, :ip, 'idle', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE SET
            url = EXCLUDED.url,
            name = COALESCE(EXCLUDED.name, slaves.name),
            ip = COALESCE(EXCLUDED.ip, slaves.ip),
            updated_at = CURRENT_TIMESTAMP
        """,
        {"id": slave_id, "name": name, "url": url, "ip": ip}
    )
    await log_activity("info", "slaves", f"Slave {slave_id} registered", {"slave_id": slave_id, "url": url})
    return await get_slave(slave_id)


async def get_slave(slave_id: str) -> Optional[Dict[str, Any]]:
    """Get a slave by ID."""
    row = await db.fetch_one(
        "SELECT * FROM slaves WHERE id = :id",
        {"id": slave_id}
    )
    if not row:
        return None
    
    slave = dict(row)
    if slave.get("system_stats") and isinstance(slave["system_stats"], str):
        try:
            slave["system_stats"] = json.loads(slave["system_stats"])
        except:
            slave["system_stats"] = {}
    return slave


async def update_slave(slave_id: str, **fields):
    """Update slave fields."""
    if not fields:
        return
    
    set_clauses = []
    values = {"id": slave_id}
    
    for key, value in fields.items():
        if key in [
            "name", "url", "ip", "status", "domains_assigned",
            "domains_done", "emails_found", "provision_progress",
            "system_stats", "last_seen", "enabled"
        ]:
            set_clauses.append(f"{key} = :{key}")
            if isinstance(value, (dict, list)):
                values[key] = json.dumps(value)
            else:
                values[key] = value
    
    if set_clauses:
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        query = f"UPDATE slaves SET {', '.join(set_clauses)} WHERE id = :id"
        await db.execute(query, values)


async def delete_slave(slave_id: str) -> bool:
    """Delete a slave."""
    slave = await get_slave(slave_id)
    if not slave:
        return False
    
    await db.execute("DELETE FROM slaves WHERE id = :id", {"id": slave_id})
    await log_activity("info", "slaves", f"Slave {slave_id} deleted", {"slave_id": slave_id})
    return True


async def list_slaves(status: str = None) -> List[Dict[str, Any]]:
    """List all slaves, optionally filtered by status."""
    query = "SELECT * FROM slaves"
    values = {}
    
    if status:
        query += " WHERE status = :status"
        values["status"] = status
    
    query += " ORDER BY updated_at DESC"
    
    rows = await db.fetch_all(query, values)
    slaves = []
    for row in rows:
        slave = dict(row)
        if slave.get("system_stats") and isinstance(slave["system_stats"], str):
            try:
                slave["system_stats"] = json.loads(slave["system_stats"])
            except:
                slave["system_stats"] = {}
        slaves.append(slave)
    return slaves


async def get_active_slaves() -> List[Dict[str, Any]]:
    """Get all active (non-dead/offline) slaves."""
    return await list_slaves()


# ── Activity Logs Operations ───────────────────────────────────────────────────

async def log_activity(
    level: str,
    category: str,
    message: str,
    details: Dict = None,
    job_id: str = None,
    slave_id: str = None
):
    """Log an activity/event."""
    try:
        await db.execute(
            """
            INSERT INTO activity_logs (level, category, message, details, job_id, slave_id, created_at)
            VALUES (:level, :category, :message, :details, :job_id, :slave_id, CURRENT_TIMESTAMP)
            """,
            {
                "level": level,
                "category": category,
                "message": message,
                "details": json.dumps(details) if details else None,
                "job_id": job_id,
                "slave_id": slave_id
            }
        )
    except Exception:
        # Don't let logging failures break the app
        pass


async def get_activity_logs(
    category: str = None,
    level: str = None,
    job_id: str = None,
    slave_id: str = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Get activity logs with filtering."""
    where_clauses = []
    values = {}
    
    if category:
        where_clauses.append("category = :category")
        values["category"] = category
    
    if level:
        where_clauses.append("level = :level")
        values["level"] = level
    
    if job_id:
        where_clauses.append("job_id = :job_id")
        values["job_id"] = job_id
    
    if slave_id:
        where_clauses.append("slave_id = :slave_id")
        values["slave_id"] = slave_id
    
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    query = f"""
        SELECT * FROM activity_logs
        {where_sql}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """
    values["limit"] = limit
    values["offset"] = offset
    
    rows = await db.fetch_all(query, values)
    logs = []
    for row in rows:
        log = dict(row)
        if log.get("details") and isinstance(log["details"], str):
            try:
                log["details"] = json.loads(log["details"])
            except:
                log["details"] = None
        logs.append(log)
    return logs


async def get_recent_logs(minutes: int = 60) -> List[Dict[str, Any]]:
    """Get logs from the last N minutes."""
    rows = await db.fetch_all(
        """
        SELECT * FROM activity_logs
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '{} minutes'
        ORDER BY created_at DESC
        LIMIT 500
        """.format(minutes)
    )
    return [dict(row) for row in rows]


# ── Emails Operations ───────────────────────────────────────────────────────────

async def save_emails(job_id: str, emails: List[str], domain: str = None):
    """Save emails for a job (upsert to avoid duplicates)."""
    if not emails:
        return
    
    # Use ON CONFLICT to ignore duplicates
    values_list = [
        {"job_id": job_id, "email": email, "domain": domain}
        for email in emails
    ]
    
    # Batch insert
    await db.execute_many(
        """
        INSERT INTO emails (job_id, email, domain, created_at)
        VALUES (:job_id, :email, :domain, CURRENT_TIMESTAMP)
        ON CONFLICT (job_id, email) DO NOTHING
        """,
        values_list
    )
    
    # Update job email count
    count_result = await db.fetch_one(
        "SELECT COUNT(*) FROM emails WHERE job_id = :job_id",
        {"job_id": job_id}
    )
    count = count_result[0] if count_result else 0
    
    await update_job(job_id, emails_count=count)


async def get_emails(job_id: str) -> List[str]:
    """Get all emails for a job."""
    rows = await db.fetch_all(
        "SELECT email FROM emails WHERE job_id = :job_id ORDER BY email",
        {"job_id": job_id}
    )
    return [row["email"] for row in rows]


async def get_emails_paginated(
    job_id: str,
    page: int = 1,
    limit: int = 1000
) -> Dict[str, Any]:
    """Get emails for a job with pagination."""
    offset = (page - 1) * limit
    
    count_result = await db.fetch_one(
        "SELECT COUNT(*) FROM emails WHERE job_id = :job_id",
        {"job_id": job_id}
    )
    total = count_result[0] if count_result else 0
    
    rows = await db.fetch_all(
        """
        SELECT email, domain, created_at
        FROM emails
        WHERE job_id = :job_id
        ORDER BY email
        LIMIT :limit OFFSET :offset
        """,
        {"job_id": job_id, "limit": limit, "offset": offset}
    )
    
    return {
        "emails": [dict(row) for row in rows],
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }


async def delete_emails(job_id: str):
    """Delete all emails for a job."""
    await db.execute(
        "DELETE FROM emails WHERE job_id = :job_id",
        {"job_id": job_id}
    )


# ── Job-Slave Assignment Operations ────────────────────────────────────────────

async def assign_job_to_slave(job_id: str, slave_id: str, domains_assigned: int = 0):
    """Assign a job to a slave."""
    await db.execute(
        """
        INSERT INTO job_slave_assignments (job_id, slave_id, domains_assigned, status, created_at, updated_at)
        VALUES (:job_id, :slave_id, :domains_assigned, 'assigned', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (job_id, slave_id) DO UPDATE SET
            domains_assigned = EXCLUDED.domains_assigned,
            status = 'assigned',
            updated_at = CURRENT_TIMESTAMP
        """,
        {"job_id": job_id, "slave_id": slave_id, "domains_assigned": domains_assigned}
    )


async def update_assignment(job_id: str, slave_id: str, **fields):
    """Update job-slave assignment."""
    if not fields:
        return
    
    set_clauses = []
    values = {"job_id": job_id, "slave_id": slave_id}
    
    for key, value in fields.items():
        if value is not None and key in ["domains_done", "emails_found", "status"]:
            set_clauses.append(f"{key} = :{key}")
            values[key] = value
    
    if set_clauses:
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        query = f"""
            UPDATE job_slave_assignments
            SET {', '.join(set_clauses)}
            WHERE job_id = :job_id AND slave_id = :slave_id
        """
        await db.execute(query, values)


async def get_job_assignments(job_id: str) -> List[Dict[str, Any]]:
    """Get all slave assignments for a job."""
    rows = await db.fetch_all(
        """
        SELECT jsa.*, s.name as slave_name, s.url as slave_url
        FROM job_slave_assignments jsa
        JOIN slaves s ON jsa.slave_id = s.id
        WHERE jsa.job_id = :job_id
        """,
        {"job_id": job_id}
    )
    return [dict(row) for row in rows]


async def get_job_assignments_for_slave(slave_id: str) -> List[Dict[str, Any]]:
    """Get all job assignments for a slave."""
    rows = await db.fetch_all(
        """
        SELECT jsa.*, j.name as job_name, j.status as job_status
        FROM job_slave_assignments jsa
        JOIN jobs j ON jsa.job_id = j.id
        WHERE jsa.slave_id = :slave_id
        """,
        {"slave_id": slave_id}
    )
    return [dict(row) for row in rows]


# ── Statistics Operations ───────────────────────────────────────────────────────

async def get_system_stats() -> Dict[str, Any]:
    """Get overall system statistics."""
    # Job stats
    job_stats = await db.fetch_one(
        """
        SELECT 
            COUNT(*) as total_jobs,
            COUNT(*) FILTER (WHERE status = 'uploaded') as uploaded,
            COUNT(*) FILTER (WHERE status = 'cleaning') as cleaning,
            COUNT(*) FILTER (WHERE status = 'dns_filtering') as dns_filtering,
            COUNT(*) FILTER (WHERE status = 'scraping') as scraping,
            COUNT(*) FILTER (WHERE status = 'processing') as processing,
            COUNT(*) FILTER (WHERE status = 'paused') as paused,
            COUNT(*) FILTER (WHERE status = 'completed') as completed,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
            SUM(emails_count) as total_emails
        FROM jobs
        """
    )
    
    # Slave stats
    slave_stats = await db.fetch_one(
        """
        SELECT
            COUNT(*) as total_slaves,
            COUNT(*) FILTER (WHERE status = 'idle') as idle,
            COUNT(*) FILTER (WHERE status = 'scraping') as scraping,
            COUNT(*) FILTER (WHERE status = 'error') as error,
            COUNT(*) FILTER (WHERE last_seen > CURRENT_TIMESTAMP - INTERVAL '5 minutes') as active
        FROM slaves
        """
    )
    
    # Recent activity
    recent_logs = await db.fetch_one(
        """
        SELECT COUNT(*) as count
        FROM activity_logs
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
        """
    )
    
    return {
        "jobs": dict(job_stats) if job_stats else {},
        "slaves": dict(slave_stats) if slave_stats else {},
        "recent_logs": recent_logs["count"] if recent_logs else 0,
        "timestamp": datetime.now().isoformat()
    }


# ── Cleanup Operations ───────────────────────────────────────────────────────────

async def cleanup_old_jobs(days: int = None):
    """Delete old completed/failed/cancelled jobs."""
    if days is None:
        days = int(await get_setting("auto_cleanup_days", "30"))
    
    result = await db.execute(
        """
        DELETE FROM jobs
        WHERE status IN ('completed', 'failed', 'cancelled')
        AND finished_at < CURRENT_TIMESTAMP - INTERVAL '{} days'
        """.format(days)
    )
    
    await log_activity("info", "cleanup", f"Cleaned up jobs older than {days} days")
    return result


async def cleanup_old_logs(days: int = 7):
    """Delete logs older than N days."""
    await db.execute(
        """
        DELETE FROM activity_logs
        WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '{} days'
        """.format(days)
    )
