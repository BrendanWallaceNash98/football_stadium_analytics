from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wiki_pipeline import extract_wiki_data, transform_wikipedia_data, write_wiki_data
dag = DAG(
    dag_id='wiki_flow',
    default_args={
        "owner": "Brendan W",
        "start_date": datetime(2024,1,1),

    },

    schedule_interval=None,
    catchup=False

)

extraction_data_from_wiki = PythonOperator(
    task_id="extract_data_from_wiki",
    python_callable=extract_wiki_data,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag

)

transform_wiki_data = PythonOperator(
    task_id='transform_wiki_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

write_wiki_data = PythonOperator(
    task_id='write_wiki_data',
    provide_context=True,
    python_callable=write_wiki_data,
    dag=dag

)

extraction_data_from_wiki >> transform_wiki_data >> write_wiki_data