GET /sites
# returns list of all sites from sites.json

GET /site/{site_id}/consumption
# query params: from (unix ts), to (unix ts)
# calls storage.read_range()
# returns JSON with consumption columns

GET /site/{site_id}/temperature
# query params: from (unix ts), to (unix ts)
# calls storage.read_range()
# returns JSON with temperature columns