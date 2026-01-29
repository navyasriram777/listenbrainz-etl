import os
from db_connection import db

class RawETL:
    # Connect to DuckDB
    def __init__(self):
        self.conn = db.db_open()

    # Drop Table if exits
    def _drop_tables(self): 
        self.conn.execute(""" DROP TABLE IF EXISTS raw_music_data""")
        self.conn.execute(""" DROP TABLE IF EXISTS etl_load_control""")
    
    #Creates Table Function
    def _create_tables(self):
        self.conn.execute("""CREATE TABLE IF NOT EXISTS etl_load_control (load_id BIGINT,file_name VARCHAR);""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS raw_music_data(
                            track_metadata JSON,
                            listened_at BIGINT,
                            recording_msid varchar,
                            user_name varchar,
                            load_id BIGINT DEFAULT 999,
                            load_date DATE DEFAULT CURRENT_DATE,
                            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")
        print("[INFO] RAW table created successfully.")
    
    #Generates load_id for each file load_id is generated and traced in table etl_load_control
    def _generate_load_id(self):
        load_id = self.conn.execute("""SELECT COALESCE(MAX(load_id),999) + 1 FROM etl_load_control""").fetchone()[0]
        return load_id
    
    """ Ingest a single JSON/JSONL file into raw_music_data.
    Deduplicates and only inserts new records.
    Returns the number of rows inserted. """
    def _ingest_file(self, file_path):
        if not os.path.exists(file_path):
            print(f"[FILE NOT FOUND]: {file_path}")
            return 0

        load_id = self._generate_load_id()

        insert_query = f"""
        INSERT INTO raw_music_data
        SELECT
            track_metadata,
            listened_at,
            recording_msid,
            user_name,
            {load_id} AS load_id,
            CURRENT_DATE AS load_date,
            CURRENT_TIMESTAMP AS load_ts
        FROM read_json(
            '{file_path}',
            columns={{'track_metadata': 'JSON', 'listened_at': 'BIGINT', 'recording_msid': 'VARCHAR', 'user_name': 'VARCHAR'}},
            ignore_errors=true
        ) src
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_music_data tgt
            WHERE COALESCE(tgt.user_name,'') = COALESCE(src.user_name,'')
              AND COALESCE(tgt.recording_msid,'') = COALESCE(src.recording_msid,'')
              AND tgt.listened_at = src.listened_at
        );
        """
        self.conn.execute(insert_query)

        # Count rows actually inserted
        rows_inserted = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM raw_music_data
            WHERE load_id = {load_id}
        """).fetchone()[0]

        # Only log load_id if rows inserted
        if rows_inserted > 0:
            self.conn.execute(
                "INSERT INTO etl_load_control (load_id, file_name) VALUES (?, ?)",
                [load_id, file_path]
            )
            print(f"[SUCCESS] Inserted {rows_inserted} rows from {os.path.basename(file_path)} (load_id={load_id})")
        else:
            print(f"[SKIPPED] No new rows to insert from {os.path.basename(file_path)}. Load ID not recorded.")

        return rows_inserted
    
    # Ingest multiple files sequentially.
    def _ingest_files(self, file_list):
        total_rows = 0
        for f in file_list:
            total_rows += self._ingest_file(f)
        print(f"[INFO] Total rows inserted from MULTIPLE FILES are: {total_rows}")
        return total_rows
    
    #Total count in raw table
    def _get_raw_table_count(self):
        count_val=self.conn.execute("SELECT count(*) AS total_rows from raw_music_data;").fetchdf()
        print(f"[INFO] Total Number of Records in RAW TABLE are: {count_val['total_rows'][0]}")
    
    #Run Raw  Layer process 
    def run_raw_process(self):
        self._drop_tables()
        self._create_tables()
        # Single file
        self._ingest_file('./data/listens_spotify.json')

        # Multiple files
        files = [
        './data/sample.json',
        './data/test.json',
        './data/test1.json'
             ]
        
        self._ingest_files(files)
        self._get_raw_table_count()