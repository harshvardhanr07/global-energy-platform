class IngestionLogger():

    def __init__(self, conn):
        self.conn = conn

    def log(self, endpoint, site_id, year_month, rows_written, status, error_message, started_at, finished_at):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO
                ingestion_log
                (endpoint, site_id, year_month, rows_written, status, error_message, started_at, finished_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)            
        """, (endpoint, site_id, year_month, rows_written, status, error_message, started_at, finished_at))
        self.conn.commit()
        cursor.close()