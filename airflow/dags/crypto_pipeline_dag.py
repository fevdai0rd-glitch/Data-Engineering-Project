from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# 1. Define standard recovery rules for the orchestration tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),  # Historical anchor start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Initialize the Master Orchestrator Blueprint (DAG)
with DAG(
    dag_id='crypto_batch_transformation_pipeline',
    default_args=default_args,
    description='Automated orchestration layer to trigger Spark processing jobs',
    schedule_interval='@daily',  # Trigger automatically every midnight
    catchup=False,               # Skip running historical dates upon activation
    is_paused_upon_creation=False,
    tags=['crypto', 'spark', 'transformation']
) as dag:

    print("⚙️ Registering Pipeline Automation Operators...")

    # 3. Task: Run a quick connection diagnostic check on our core databases
    verify_nodes = BashOperator(
        task_id='verify_infrastructure_nodes',
        bash_command=(
            'echo "🔍 Scanning Docker Cluster Node Ports..." && '
            'nc -z -w 3 de_postgres 5432 && '
            'nc -z -w 3 de_spark_master 7077 && '
            'nc -z -w 3 de_mongodb 27017'
        ),
        retries=2,
        retry_delay=timedelta(seconds=10)
    )
    # 4. Task: Trigger batch spark on spark master container through SSH
    execute_spark_job = SSHOperator(
        task_id='execute_spark_batch_transformation',
        ssh_conn_id='spark_ssh_default',  # The connection name we will link in the UI
        command=(
            'export JAVA_HOME=/opt/java/openjdk && '
            'export SPARK_HOME=/opt/spark && '
            'export PATH=$PATH:$SPARK_HOME/bin && '
            '/opt/spark/bin/spark-submit '
            '--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.6.0 '
            '/workspace/spark_transform.py'
        ),
        cmd_timeout=300
    )


    # 5. Define the strict operational workflow sequence
    verify_nodes >> execute_spark_job
