from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract.fetch_ads import fetch_ads_data
from src.normalize.normalize_ads import normalize_ads
from src.enrich.enrich_ads import enrich_ads
from src.rank.rank_ads import rank_ads
from src.utils.logger import setup_logging, get_logger

setup_logging(level=logging.INFO, log_file="logs/pipeline.log")
logger = get_logger(__name__)

with DAG(
    "ad_data_pipeline", 
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1), 
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
        "sla": timedelta(minutes=15),
    }
) as dag:
    extract_task = PythonOperator(
        task_id="extract_ads",
        python_callable=fetch_ads_data,
        op_kwargs={
            "mode": "{{ dag_run.conf['mode'] if dag_run and dag_run.conf and 'mode' in dag_run.conf else 'mock' }}"
        }
    )

    normalize_task = PythonOperator(
        task_id="normalize_ads",
        python_callable=normalize_ads,
        op_kwargs={
            "input_path": "{{ task_instance.xcom_pull(task_ids='extract_ads', key='return_value') }}"
        }
    )

    enrich_task = PythonOperator(
        task_id="enrich_ads",
        python_callable=enrich_ads,
        op_kwargs={
            "input_path": "{{ task_instance.xcom_pull(task_ids='normalize_ads', key='return_value') }}"
        }
    )

    rank_task = PythonOperator(
        task_id="rank_ads",
        python_callable=rank_ads,
        op_kwargs={
            "input_path": "{{ task_instance.xcom_pull(task_ids='enrich_ads', key='return_value') }}"
        }
    )

    extract_task >> normalize_task >> enrich_task >> rank_task