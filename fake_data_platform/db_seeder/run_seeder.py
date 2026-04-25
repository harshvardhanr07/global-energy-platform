# db_seeder/run_seeder.py
#
# Master entry point for the full database seeding pipeline.
# Runs all 4 seeders in the correct dependency order.
#
# Execution order is critical:
#   1. create_tables  → tables must exist before any inserts
#   2. seed_sites     → site_profile rows must exist before history/occupancy
#   3. seed_history   → depends on site_profile rows being present
#   4. seed_occupancy → depends on site_profile rows being present

from schema import main as create_tables
from seed_sites import main as seed_sites
from seed_history import main as seed_history
from seed_occupancy import main as seed_occupancy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting database seeder...")
    create_tables()   # step 1 — create all 4 tables
    seed_sites()      # step 2 — insert 10 site profiles
    seed_history()    # step 3 — insert profile + status history
    seed_occupancy()  # step 4 — insert ~7,100 daily occupancy rows
    logger.info("Database seeding completed successfully")