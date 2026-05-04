from datetime import datetime

class WatermarkManager():

    def __init__(self, conn):
        self.conn = conn

    def get(self, endpoint, site_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT last_ingested_ts FROM ingestion_watermark
            WHERE
                endpoint = %s
                and site_id = %s
        """, (endpoint, site_id))

        response = cursor.fetchone()
        cursor.close()
        if response:
            return response[0]
        else:
            return datetime(2024, 4, 1, 0, 0, 0)
        
    def update(self, endpoint, site_id, last_ingested_ts ):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO 
                ingestion_watermark (endpoint, site_id, last_ingested_ts, updated_at)
            VALUES
                (%s, %s, %s, NOW())
            ON CONFLICT(endpoint, site_id)
            DO UPDATE SET
                last_ingested_ts = %s,
                updated_at = NOW()
        """, (endpoint, site_id, last_ingested_ts , last_ingested_ts ))

        self.conn.commit()
        cursor.close()