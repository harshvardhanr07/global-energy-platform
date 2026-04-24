from schema import main as create_tables
from seed_sites import main as seed_sites
from seed_history import main as seed_history
from seed_occupancy import main as seed_occupancy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting database seeder...")
    create_tables()
    seed_sites()
    seed_history()
    seed_occupancy()
    logger.info("Database seeding completed successfully")