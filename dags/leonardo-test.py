from airflow import DAG
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator

with DAG(
    dag_id="Run_Fabric_Item",
    schedule_interval="@daily",
    start_date=datetime(2023, 8, 7),
    catchup=False,
) as dag:

    run_pipeline = FabricRunItemOperator(
        task_id="run_fabric_pipeline",
        workspace_id="<workspace_id>",
        item_id="<item_id>",
        fabric_conn_id="fabric_conn_id",
        job_type="Pipeline",
        wait_for_termination=True,
        deferrable=True,
    )

    run_pipeline