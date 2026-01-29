import duckdb
import os
import pandas as pd
from db_connection import db

class CuratedETL:
    # Connect to DuckDB
    def __init__(self):
        self.conn = db.db_open()

    #Creates Curated Table
    def _create_curated_table(self):
        self.conn.execute("DROP TABLE IF EXISTS curated_music_data;")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS curated_music_data(
            listen_id VARCHAR,
            artist_msid VARCHAR,
            artist_name VARCHAR,
            release_msid VARCHAR,
            release_name VARCHAR,
            track_name VARCHAR,
            listened_at_ts TIMESTAMP,
            listened_at_date DATE,
            recording_msid VARCHAR,
            user_name VARCHAR,
            load_id BIGINT,
            load_date DATE,
            load_ts TIMESTAMP);
        """)
        print("[INFO] Curated table created successfully.")

    #Transform raw_music_data to curated_music_data, flatten JSON, handle nulls, and deduplicate using ROW_NUMBER().
    def _transform_raw_to_curated(self):
        
        insert_query = """
        INSERT INTO curated_music_data

        WITH enriched_data AS (
            SELECT 
                COALESCE(listened_at,'UNKNOWN') || ':' || COALESCE(recording_msid,'UNKNOWN') || ':' || COALESCE(user_name,'UNKNOWN') AS listen_id,
                COALESCE(track_metadata['additional_info']['artist_msid']::VARCHAR,'UNKNOWN') AS artist_msid,
                COALESCE(track_metadata['artist_name']::VARCHAR,'UNKNOWN') AS artist_name,
                COALESCE(track_metadata['additional_info']['release_msid']::VARCHAR,'UNKNOWN') AS release_msid,
                COALESCE(track_metadata['release_name']::VARCHAR,'UNKNOWN') AS release_name,
                COALESCE(track_metadata['track_name']::VARCHAR,'UNKNOWN') AS track_name,
                TO_TIMESTAMP(COALESCE(listened_at,0)) AS listened_at_ts,
                DATE(TO_TIMESTAMP(COALESCE(listened_at,0))) AS listened_at_date,
                COALESCE(recording_msid,'UNKNOWN') AS recording_msid,
                COALESCE(user_name,'UNKNOWN') AS user_name,
                load_id,
                load_date,
                load_ts
            FROM raw_music_data
        ),
        deduplicate_data AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY listen_id ORDER BY listened_at_ts DESC) AS row_number
            FROM enriched_data
        )
        SELECT * EXCLUDE(row_number)
        FROM deduplicate_data
        WHERE row_number = 1
        """

        self.conn.execute(insert_query)
        print("[INFO] Raw data transformed to curated data with duplicates removed.")
    
    #Fetch sample rows and count from curated table.
    def _curated_table_count(self):
        df = self.conn.execute(f"SELECT count(*) as total_rows FROM curated_music_data").fetchdf()
        print(f"[INFO] Total rows in curated table: {df['total_rows'][0]}")
        
    #Export curated table to CSV.   
    def _export_to_csv(self, output_file="./etl/output/curated/curated_output.csv"):
        output_dir=os.path.dirname(output_file)
        os.makedirs(output_dir,exist_ok=True)
        df = self.conn.execute("SELECT * FROM curated_music_data").fetchdf()
        df.to_csv(output_file, index=False)
        print(f"[INFO] Curated data exported to {output_file}")

    #Run Curated Process
    def run_curated_process(self):
        self._create_curated_table()
        self._transform_raw_to_curated()
        self._curated_table_count()
        self._export_to_csv()