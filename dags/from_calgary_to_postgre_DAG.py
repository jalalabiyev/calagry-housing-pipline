from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime

from calgary.fetch_data import fetch_data

def ingest_bronze():
    df = fetch_data()
    df=df.astype(str)
    hook = PostgresHook(postgres_conn_id="bronze_pg")
    engine = create_engine(hook.get_uri())

    df.to_sql( 
        "building_permit_raw",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
        )
    
with DAG(
        dag_id="building_permit_raw",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False,
    ) as dag:
        

        bronze_task = PythonOperator(
            task_id="ingest_bronze",
            python_callable=ingest_bronze,
        )
    



