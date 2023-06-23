import json

import boto3
import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator

with open("../../../lessons/dags/config.json") as config_file:
    config = json.load(config_file)

AWS_ACCESS_KEY_ID = config["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = config["aws_secret_access_key"]
HOST = config["host"]
PORT = config["port"]
USER = config["user"]
PASSWORD = config["password"]
DB = config["db"]

conn_info = {
    "host": HOST,
    "port": PORT,
    "user": USER,
    "password": PASSWORD,
    "database": DB,
    "autocommit": True,
}


def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(Bucket=bucket, Key=key, Filename=f"../../../data/{key}")


def load_currencies_staging(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """copy st23052706__staging.currencies
                (currency_code,currency_code_with,date_update,currency_with_div)
                from local"""
                "'/data/currencies_history.csv' delimiter ','"
                "rejected data '/data/currencies_rejects.txt'"
                "exceptions '/data/currencies_exceptions.txt'",
                buffer_size=65536,
            )
            res = cur.fetchall()
        except vertica_python.errors.Error as e:
            raise Exception(f"An error occurred during currencies data load: {str(e)}")
        return res


def load_transactions_staging(file_num, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """copy st23052706__staging.transactions
                (operation_id,account_number_from,account_number_to,currency_code,
                country,status,transaction_type,amount,transaction_dt)
                from local"""
                f"'/data/transactions_batch_{file_num}.csv' delimiter ','"
                f"rejected data '/data/transactions_batch_{file_num}_rejects.txt'"
                f"exceptions '/data/transactions_batch_{file_num}_exceptions.txt'",
                buffer_size=65536,
            )
            res = cur.fetchall()
        except vertica_python.errors.Error as e:
            raise Exception(
                f"An error occurred during transactions data load: {str(e)}"
            )
        return res


@dag(
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def staging_dag():
    fetch_currencies_task = PythonOperator(
        task_id="fetch_currencies",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "final-project", "key": "currencies_history.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies_staging",
        python_callable=load_currencies_staging,
    )

    fetch_transactions_task = []
    load_transactions_task = []
    for i in range(1, 11):
        fetch_transactions_task.append(
            PythonOperator(
                task_id=f"fetch_transactions_{i}",
                python_callable=fetch_s3_file,
                op_kwargs={
                    "bucket": "final-project",
                    "key": f"transactions_batch_{i}.csv",
                },
            )
        )

        load_transactions_task.append(
            PythonOperator(
                task_id=f"load_transactins_staging_{i}",
                python_callable=load_transactions_staging,
                op_kwargs={"file_num": i},
            )
        )

    (
        fetch_currencies_task
        >> fetch_transactions_task
        >> load_currencies_task
        >> load_transactions_task
    )


_ = staging_dag()
