import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
HOST = Variable.get("HOST")
PORT = Variable.get("PORT")
USER = Variable.get("USER")
PASSWORD = Variable.get("PASSWORD")
DB = Variable.get("DB")

conn_info = {
    "host": HOST,
    "port": PORT,
    "user": USER,
    "password": PASSWORD,
    "database": DB,
    "autocommit": True,
}


def load_global_metrics_dwh(date, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                insert into st23052706__dwh.global_metrics
                with c as (
                select
                    *
                from
                    st23052706__staging.currencies c
                where
                    currency_code_with = 420
                    and date_update = '{date}'::date-1
                ),
                t as (
                select
                    c.date_update,
                    t.currency_code as currency_from,
                    t.account_number_from,
                    (t.amount * c.currency_with_div) as amount
                from
                    st23052706__staging.transactions t
                join c
                on
                    t.transaction_dt::date = c.date_update
                    and t.currency_code = c.currency_code
                where
                    t.status = 'done'
                    and t.transaction_dt::date = '{date}'::date-1
                union all
                select
                    transaction_dt::date as date_update,
                    currency_code as currency_from,
                    account_number_from,
                    amount
                from
                    st23052706__staging.transactions
                where
                    currency_code = 420
                    and status = 'done'
                    and transaction_dt::date = '{date}'::date-1
                )
                select
                    date_update,
                    currency_from,
                    sum(amount) as amount_total,
                    count(*) as cnt_transactions,
                    round(sum(amount)/ count(distinct account_number_from), 2) as avg_transactions_per_account,
                    count(distinct account_number_from) as cnt_accounts_make_transactions
                from
                    t
                group by
                    date_update,
                    currency_from
                ;
                """
            )
            res = cur.fetchall()
        except vertica_python.errors.Error:
            raise
        return res


@dag(schedule_interval="0 12 * * *", start_date=pendulum.parse("2022-10-01"),
     catchup=True)
def dwh_dag():
    load_global_metrics_task = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics_dwh,
        op_kwargs={"date": "{{ ds }}"},
    )

    load_global_metrics_task


_ = dwh_dag()
