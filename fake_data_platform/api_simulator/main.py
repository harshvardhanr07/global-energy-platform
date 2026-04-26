# api_simulator/main.py
#
# Entry point for the API Simulator service.
# Orchestrates startup sequence in correct order:
#
#   1. Load sites from config/sites.json
#   2. Connect to PostgreSQL (needed for occupancy factors)
#   3. Run backfill — generate 2-year historical Parquet data
#      (skips partitions that already exist — idempotent)
#   4. Start APScheduler — hourly sensor data append job
#   5. Start FastAPI app via uvicorn
#
# Shutdown sequence (on SIGTERM/SIGINT):
#   - Scheduler stopped cleanly before process exits
#   - DB connection closed

import os
import sys
import json
import logging
import psycopg2
import uvicorn
from contextlib import asynccontextmanager
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── DB Connection ──────────────────────────────────────────────────────────

def get_db_connection():
    """
    Creates and returns a fresh psycopg2 connection using env vars.

    Used as:
    - Direct connection during backfill (main thread)
    - Factory function passed to scheduler (background thread)
      Each hourly job creates its own connection via this function.
    """
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST"),
        dbname   = os.getenv("POSTGRES_DB"),
        user     = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        port     = os.getenv("POSTGRES_PORT"),
    )


# ── Sites Loader ───────────────────────────────────────────────────────────

def load_sites():
    """
    Loads the 10 site definitions from config/sites.json.
    Raises on failure — everything depends on site data being available.
    """
    sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')
    try:
        with open(sites_path) as f:
            sites = json.load(f)
        logger.info(f"Loaded {len(sites)} sites from config")
        return sites
    except Exception as e:
        logger.error(f"Failed to load sites.json: {e}")
        raise


# ── Lifespan ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    """
    FastAPI lifespan context manager — handles startup and shutdown.

    Replaces deprecated @app.on_event("startup") / ("shutdown") pattern.

    Startup:
    - Load sites
    - Connect to DB
    - Run backfill (skips existing partitions)
    - Start scheduler

    Shutdown:
    - Stop scheduler cleanly
    - Close DB connection

    Everything before `yield` runs on startup.
    Everything after `yield` runs on shutdown.
    """
    global scheduler, conn, cursor

    logger.info("=" * 60)
    logger.info("  Global Energy IoT Simulator — Starting Up")
    logger.info("=" * 60)

    # ── step 1 — load sites ────────────────────────────────────────────
    sites = load_sites()

    # ── step 2 — connect to DB ─────────────────────────────────────────
    logger.info("Connecting to PostgreSQL...")
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        logger.info("PostgreSQL connection established")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

    # ── step 3 — run backfill ──────────────────────────────────────────
    logger.info("Starting historical data backfill...")
    try:
        from backfill import run_backfill
        run_backfill(sites, cursor)
        logger.info("Backfill complete")
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise

    # ── step 4 — start scheduler ───────────────────────────────────────
    logger.info("Starting hourly scheduler...")
    try:
        from scheduler import start_scheduler
        scheduler = start_scheduler(sites, get_db_connection)
        logger.info("Scheduler started")
    except Exception as e:
        logger.error(f"Scheduler failed to start: {e}")
        raise

    logger.info("=" * 60)
    logger.info("  Startup complete — API ready to serve requests")
    logger.info("=" * 60)

    # ── hand control to FastAPI ────────────────────────────────────────
    yield

    # ── shutdown sequence ──────────────────────────────────────────────
    logger.info("Shutting down...")

    try:
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
    except Exception as e:
        logger.warning(f"Scheduler shutdown error: {e}")

    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("DB connection closed")
    except Exception as e:
        logger.warning(f"DB connection close error: {e}")

    logger.info("Shutdown complete")


# ── App Import + Lifespan Attach ───────────────────────────────────────────

# import app from api.py and attach lifespan
# done here not in api.py to keep api.py clean and testable independently
from api import app
app.router.lifespan_context = lifespan


# ── Entry Point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Starts uvicorn server on 0.0.0.0:8000.

    host=0.0.0.0  → accessible from outside Docker container
    reload=False  → production mode, no file watching
    log_level     → info to match our logging config
    """
    uvicorn.run(
        "main:app",
        host      = "0.0.0.0",
        port      = 8000,
        reload    = False,
        log_level = "info",
    )