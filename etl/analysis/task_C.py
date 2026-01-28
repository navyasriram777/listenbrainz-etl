from db_connection import db
class TaskC_Analysis:
    def __init__(self):
        # Reuse DB connection
        self.conn = db.db_open()

    def active_users(self):
        file_name='C:/Users/Public/ScalableCapital/listenbrainz-etl/etl/output/taskC/active_users.csv'

        active_users_query=""" 
        with usercount_per_day AS
        (
            select  user_name,listened_at_date,count(*) as day_count from curated_music_data where recording_msid!='UNKNOWN' group by user_name,listened_at_date
        ),
        active_user AS
        (
            select *,
            count(*) OVER (
                PARTITION BY user_name
                ORDER BY listened_at_date
                RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
            ) AS range_count
            from usercount_per_day
        ),
        daily_active_user AS
        (
            SELECT 
            listened_at_date,
            count(distinct user_name) as num_active_users,
            ROUND(count(distinct user_name)/(select count(distinct user_name) from curated_music_data)*100,2) AS active_users_percentage
            FROM active_user where range_count>=1 group by listened_at_date order by listened_at_date
        )
        select * from daily_active_user
        """
        df=self.conn.execute(active_users_query).fetchdf()
        df.to_csv(file_name, index=False)
        print(f"[INFO]  ACTIVE USERS Details written to file : {file_name}")
    
    def run_task_C(self):
        self.active_users()