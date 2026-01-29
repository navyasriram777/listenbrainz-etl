from db_connection import db
import json

class TaskA_Analysis:
    def __init__(self,config_file="C:/Users/Public/ScalableCapital/listenbrainz-etl/config/taskA.json"):
        # Reuse DB connection
        self.conn = db.db_open()
        with open(config_file,'r') as f:
            cfg = json.load(f)
        
        # Assign only runtime parameters from config
        self.top_n_user_param = cfg.get("top_n_user_param")
        self.on_date_param = cfg.get("on_date_param")
        self.user_nth_song_param = cfg.get("user_nth_song")
    
    def top_n_user(self,n):
        file_name=f"""C:/Users/Public/ScalableCapital/listenbrainz-etl/etl/output/taskA/top_{n}_user.csv"""
        top_n_user_query=f"""
                            select user_name,count(release_msid) as listen_times
                            from curated_music_data where release_msid!='UNKNOWN' group by user_name
                            order by listen_times desc limit {n} 
                            """
        df=self.conn.execute(top_n_user_query).fetchdf()
        df.to_csv(file_name, index=False)
        print(f"[INFO] Top {n} users are written to file : {file_name}")
        
    def user_count_for_date(self,on_date):
        file_name=f"""user_count_{on_date}.csv"""
        user_count_for_date_query=f"""
                            select count(distinct user_name) as user_count
                            from curated_music_data 
                            where release_msid!='UNKNOWN' and listened_at_date= DATE '{on_date}';
                            """
        df=self.conn.execute(user_count_for_date_query).fetchdf()
        print(f"[INFO] User Count for {on_date} is : {df['user_count'][0]}")


    def user_nth_song(self,n):
        file_name=f"""C:/Users/Public/ScalableCapital/listenbrainz-etl/etl/output/taskA/user_{n}th_song.csv"""
        user_nth_song_query=f"""
        with user_nth_song_cte AS
        (
        select *,
        row_number() over (partition by user_name order by listened_at_ts) as row_num
        from curated_music_data WHERE release_msid!='UNKNOWN'
        )
        select user_name,track_name from user_nth_song_cte where row_num={n} order by user_name;
        """
        df=self.conn.execute(user_nth_song_query).fetchdf()
        df.to_csv(file_name, index=False)
        print(f"[INFO] Users {n}th song is written to {file_name}")
        
    def run_task_A(self):
        self.top_n_user(self.top_n_user_param)
        self.user_count_for_date(self.on_date_param)
        self.user_nth_song(self.user_nth_song_param)