def get_partition_path(site_id, year, month)
# returns path: output/parquet/SITE_001/year=2024/month=01/data.parquet

def write_partition(df, site_id, year, month)
# writes DataFrame to partition path
# creates directories if needed

def read_partition(site_id, year, month)
# reads parquet file for given partition
# returns empty DataFrame if file doesn't exist

def append_to_partition(new_rows_df, site_id, year, month)
# reads existing partition
# concats new rows
# deduplicates on timestamp
# sorts by timestamp
# overwrites file

def read_range(site_id, from_ts, to_ts)
# converts unix timestamps to year/month combinations
# reads all relevant partitions
# filters by exact timestamp range
# returns combined DataFrame