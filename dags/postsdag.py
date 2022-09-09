

from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import psycopg2


import sys

sys.path.append('/home/rahul/latana')

from modules import utils

with DAG(
  'posts',
  default_args={
 'depends_on_past': False,
        'email': ['kr7168799@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'provide_context':True,
        
        # 'sla': timedelta(hours=2),
     
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        'trigger_rule': 'all_success'
  },
  description='dag for getting 2013 posts from reddit',
    schedule_interval= "@daily",
    start_date=datetime(2022, 9, 9),
    catchup=False,
    
    tags=['example'],
                ) as dag:
    #write all the operaters that are to be used
    
    # pull_posts = PythonOperator(
    #     task_id='pull_posts',
    #     python_callable= utils.pull_data,
        
    #     op_kwargs={
    #         "url":"https://latana-data-eng-challenge.s3.eu-central-1.amazonaws.com/allposts.csv",
    #         "dest_file":"/home/rahul/reddit/allposts.csv"
    #     },
    #     dag=dag
    # )

    transform_posts= PythonOperator(
        task_id = 'transform_posts',
        python_callable= utils.transformdata_raw,
         op_kwargs={
            "src_file":'/home/rahul/reddit/allposts.csv',
            "dtype":{"created_utc":int,'score':int,'ups':int,'downs':int,'permalink':str,'id':str,'subreddit_id':str}
        },

    )

    insert_posts_stg=PythonOperator(
        task_id = 'insert_posts_stg',
        python_callable=utils.insert_to_STG,
         op_kwargs={
            'conn': psycopg2.connect(database="redditdatabase", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432'),
            'src_folder':'/home/rahul/reddit/posts_transformed/'
        },
        dag=dag
    )

    cdc = PythonOperator(
        task_id = 'cdc',
        python_callable=utils.change_data_capture,
        op_kwargs={
            'conn': psycopg2.connect(database="redditdatabase", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432'),
            'columns':["created_utc",'score','ups','downs','permalink','id','subreddit_id'],
           'dest_table':'posts_2013'
        },
        dag=dag
    )

# pull_posts >> 
transform_posts >> insert_posts_stg >> cdc 
    
