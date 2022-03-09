from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dag_report_jilli',
    schedule_interval='0 1 * * *',
    start_date=datetime(2022,3,6),
    catchup=False
) as dag:
    migrate_orders = BashOperator(
    task_id='migrate_orders',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_migrate_orders.py {{ ds }}'
    )
    migrate_order_details = BashOperator(
    task_id='migrate_order_details',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_migrate_order_details.py {{ ds }}'
    )
    migrate_products = BashOperator(
    task_id='migrate_products',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_migrate_products.py {{ ds }}'
    )
    migrate_suppliers = BashOperator(
    task_id='migrate_suppliers',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_migrate_suppliers.py {{ ds }}'
    )
    supplier_report = BashOperator(
    task_id='supplier_report',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_supplier_report.py {{ ds }}'
    )
    supplier_csv_report = BashOperator(
    task_id='supplier_csv_report',
    bash_command='python /home/airflow/gcs/dags/includes/jil/jil_pg_supplier_csv_report.py {{ ds }}'
    )
    
    migrate_orders >> migrate_order_details >> supplier_report
    migrate_products >> supplier_report
    migrate_suppliers >> supplier_report
    supplier_report >> supplier_csv_report

 