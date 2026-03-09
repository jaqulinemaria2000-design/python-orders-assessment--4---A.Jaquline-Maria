import os
import sqlite3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define paths
BASE_DIR = r"c:\Users\Jaquline Maria\Desktop\week 4\assignment"
SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')
LOG_DB_PATH = os.path.join(BASE_DIR, 'data', 'airflow_job_logs.db')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_job_status(task_name, status, **context):
    """Logs the job status into a SQLite table."""
    os.makedirs(os.path.dirname(LOG_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(LOG_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            task_name TEXT,
            status TEXT,
            execution_date TIMESTAMP,
            log_time TIMESTAMP
        )
    """)
    run_id = context['dag_run'].run_id if 'dag_run' in context else 'manual_run'
    exec_date = context['execution_date'].isoformat() if 'execution_date' in context else datetime.now().isoformat()
    now = datetime.now().isoformat()
    
    cursor.execute(
        "INSERT INTO job_logs (run_id, task_name, status, execution_date, log_time) VALUES (?, ?, ?, ?, ?)",
        (run_id, task_name, status, exec_date, now)
    )
    conn.commit()
    conn.close()
    print(f"Logged status {status} for task {task_name}.")

def log_success(context):
    log_job_status(context['task'].task_id, 'SUCCESS', **context)

def log_failure(context):
    log_job_status(context['task'].task_id, 'FAILED', **context)

with DAG(
    'assignment_4_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline from Raw to DWH',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['assignment_4'],
) as dag:

    # Task 1: Setup Source DB (useful for first-time runs or resets)
    setup_source = BashOperator(
        task_id='setup_source_db',
        bash_command=f'python "{os.path.join(SCRIPTS_DIR, "setup_source_db.py")}"',
        on_success_callback=log_success,
        on_failure_callback=log_failure
    )

    # Task 2: Ingestion
    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command=f'python "{os.path.join(SCRIPTS_DIR, "ingestion.py")}"',
        on_success_callback=log_success,
        on_failure_callback=log_failure
    )

    # Task 3: Cleaning
    clean_data = BashOperator(
        task_id='clean_data',
        bash_command=f'python "{os.path.join(SCRIPTS_DIR, "cleaning.py")}"',
        on_success_callback=log_success,
        on_failure_callback=log_failure
    )

    # Task 4: Load OLTP
    load_oltp = BashOperator(
        task_id='load_oltp',
        bash_command=f'python "{os.path.join(SCRIPTS_DIR, "oltp_loader.py")}"',
        on_success_callback=log_success,
        on_failure_callback=log_failure
    )

    # Task 5: Load DWH
    load_dwh = BashOperator(
        task_id='load_dwh',
        bash_command=f'python "{os.path.join(SCRIPTS_DIR, "dwh_loader.py")}"',
        on_success_callback=log_success,
        on_failure_callback=log_failure
    )
    
    # Task 6: Final Pipeline Status Log
    log_pipeline_success = PythonOperator(
        task_id='log_pipeline_success',
        python_callable=log_job_status,
        op_kwargs={'task_name': 'pipeline_execution', 'status': 'SUCCESS'},
    )

    # Define dependencies
    setup_source >> ingest_data >> clean_data >> load_oltp >> load_dwh >> log_pipeline_success
