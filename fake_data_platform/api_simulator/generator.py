def get_temperature(site, timestamp)
# uses climate zone + seasonal factor for the month
# returns avg_outside_temp, degree_day_cooling,
#         degree_day_heating, reference_temp

def get_consumption(site, timestamp, occupancy_factor)
# returns dict of 7 usage type values in kWh
# consumption scales with:
#   - time of day (peak hours 8am-6pm, low at night)
#   - occupancy factor
#   - seasonal factor
#   - random numpy noise

def generate_minute(site, timestamp, occupancy_factor)
# combines temperature + consumption into one row dict
# timestamp is Unix integer