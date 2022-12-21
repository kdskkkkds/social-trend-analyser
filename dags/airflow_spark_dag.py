import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from 2_upload_to_s3 import upload 
from 4_move_l0_processed import l0_processed


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

upload_to_s3 = PythonOperator(
    dag=dag,
    task_id="data_to_s3",
    python_callable=upload
)

move_l0_processed = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=l0_processed
)

config1 ={'application':'3_1_user_app.py',
    'master' : 'local[*]',
    'connection':'spark_local',
    'executor_cores': 1,
    'EXECUTORS_MEM': '1G'
}
spark_submit_operator1 = SparkSubmitOperator(
    task_id='spark_submit_job',
    dag=dag,config=config1)


config2 ={'application':'3_2_user_acc.py',
    'master' : 'local[*]',
    'connection':'spark_local',
    'executor_cores': 1,
    'EXECUTORS_MEM': '1G'
}
spark_submit_operator2 = SparkSubmitOperator(
    task_id='spark_submit_job',
    dag=dag,config=config2)


config3 ={'application':'3_3_acc_post.py',
    'master' : 'local[*]',
    'connection':'spark_local',
    'executor_cores': 1,
    'EXECUTORS_MEM': '1G'
}
spark_submit_operator3 = SparkSubmitOperator(
    task_id='spark_submit_job',
    dag=dag,config=config3)

start_data_pipeline >> upload_to_s3 >> [spark_submit_operator1,spark_submit_operator2,spark_submit_operator3] >> move_l0_processed >> end_data_pipeline