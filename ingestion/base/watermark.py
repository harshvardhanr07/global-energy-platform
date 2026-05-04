class WatermarkManager():

    def __init__(self, conn):
        self.conn = conn

    def get(self, endpoint, site_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT last_year_month FROM ingestion_watermark
            WHERE
                endpoint = %s
                and site_id = %s
        """, (endpoint, site_id))

        response = cursor.fetchone()
        cursor.close()
        if response:
            return response[0]
        else:
            return "2024-04"
        
    def update(self, endpoint, site_id, year_month):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO 
                ingestion_watermark (endpoint, site_id, last_year_month, updated_at)
            VALUES
                (%s, %s, %s, NOW())
            ON CONFLICT(endpoint, site_id)
            DO UPDATE SET
                last_year_month = %s,
                updated_at = NOW()
        """, (endpoint, site_id, year_month, year_month))

        self.conn.commit()
        cursor.close()