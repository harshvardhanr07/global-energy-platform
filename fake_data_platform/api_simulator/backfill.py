def is_backfill_needed(site_id)
# checks if oldest partition exists
# returns True if backfill needed

def backfill_site(site, cursor)
# loops month by month for 2 years
# for each month → loops every minute (44,640 iterations)
# generates one row per minute
# writes monthly parquet partition
# logs progress per month

def run_backfill(sites, cursor)
# loops all 10 sites
# calls backfill_site() for each
# skips if already backfilled