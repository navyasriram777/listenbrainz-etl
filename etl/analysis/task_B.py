import json
import os
from db_connection import db
class TaskB_Analysis:
    def __init__(self,config_file="./config/taskB.json"):
        # Reuse DB connection
        self.conn = db.db_open()
        with open(config_file,'r') as f:
            cfg = json.load(f)
        
        # Assign only runtime parameters from config
        self.user_topn_days_param = cfg.get("user_topn_days_param")

    def user_topn_days(self,n):
        file_name=f"""./etl/output/taskB/user_top{n}_days.csv"""
        output_dir=os.path.dirname(file_name)
        os.makedirs(output_dir,exist_ok=True)
        user_topn_days_query=f"""
        with user_day_listen_count as
        (
        select distinct user_name,listened_at_date,count(release_msid) 
        over(partition by user_name,listened_at_date) as number_of_listens
        from curated_music_data where release_msid!='UNKNOWN'
        ),
        user_top_n_days AS
        (
        select *,dense_rank()
        over(partition by user_name order by number_of_listens DESC,listened_at_date ASC) day_rank
        from user_day_listen_count
        )
        select user_name,number_of_listens,listened_at_date from user_top_n_days where day_rank <= {n} order by user_name,number_of_listens desc
        ;
        """
        df=self.conn.execute(user_topn_days_query).fetchdf()
        df.to_csv(file_name, index=False)
        print(f"[INFO] Users {n} th song is written to {file_name}")

    def run_task_B(self):
        self.user_topn_days(self.user_topn_days_param)