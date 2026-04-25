def generate_last_hour(sites, cursor)
# gets current hour's start and end unix timestamps
# for each site → generates 60 minute rows
# calls storage.append_to_partition()

def start_scheduler()
# runs generate_last_hour() every 60 minutes
# use APScheduler library