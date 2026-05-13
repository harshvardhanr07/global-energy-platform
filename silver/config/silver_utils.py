
def enforce_shcema(df, schema):
    """
     cast every column to its declared type, drop columns not in schema, add nulls for missing columns
    """
    