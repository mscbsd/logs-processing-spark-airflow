from __future__ import annotations

import datetime
import os

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

def _get_dates_to_process(**context):
    """Obtem as datas para processar (pode ser adaptado para diferentes lógicas)."""
    execution_date = context["ds"] # Data de execução da DAG (yyyy-mm-dd)
    # Formata a data para AAAAMMDD
    date_formatted = execution_date.replace("-", "")
    return date_formatted


with DAG(
    dag_id="processamento_logs_s3",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 7, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=["logs", "s3", "pyspark"],
) as dag:
    get_dates_task = PythonOperator(
        task_id="get_dates_to_process",
        python_callable=_get_dates_to_process,
        provide_context=True,
        do_xcom_push=True, # Envia a data para as próximas tasks via XCom
        dag=dag
    )

    processar_logs_task = BashOperator(
        task_id="processar_logs_pyspark",
        bash_command=f"""
            export AWS_ACCESS_KEY_ID={os.environ.get("AWS_ACCESS_KEY_ID")};
            export AWS_SECRET_ACCESS_KEY={os.environ.get("AWS_SECRET_ACCESS_KEY")};
            export AWS_REGION={os.environ.get("AWS_REGION", "us-east-1")};
            export BUCKET_NAME={os.environ.get("BUCKET_NAME")};
            spark-submit \
            --master local[*] \ # ou yarn, spark://master:7077, etc.
            --py-files /path/para/seu/script/processar_logs.py \ # Caminho completo para o script PySpark
            /path/para/seu/script/processar_logs.py {{ env.get('BUCKET_NAME') }} {{ ti.xcom_pull(task_ids='get_dates_to_process') }}
        """,
        dag=dag
    )

    get_dates_task >> processar_logs_task