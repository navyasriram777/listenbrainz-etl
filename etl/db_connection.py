import duckdb
class DuckDBConnection:
    def __init__(self, db_path="listenbrainz.db"):
        self.db_path = db_path
        self.conn = None
    
    #Open the DuckDB DataBase Connection
    def db_open(self):
        if self.conn is None:
            self.conn=duckdb.connect(self.db_path)
            print("[INFO] DuckDB connection opened.")
        return self.conn

    #Close the DuckDB DataBase Connection
    def db_close(self):
        if self.conn is not None:
            self.conn.close()
            print("[INFO] DuckDB connection closed.")

# SINGLE shared object
db=DuckDBConnection()