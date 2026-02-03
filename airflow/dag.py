from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="hr_bronze_silver_gold_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["hr", "bronze", "silver", "gold"]
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    bronze_load = SnowflakeOperator(
        task_id="load_bronze_hr",
        snowflake_conn_id="snowflake_default",
        sql="CALL bronze.sp_load_hr_employee();"
    )

    silver_clean = SnowflakeOperator(
        task_id="build_silver_hr",
        snowflake_conn_id="snowflake_default",
        sql="CALL silver.sp_build_hr_employee_clean();"
    )

    gold_dim_employee = SnowflakeOperator(
        task_id="build_gold_dim_employee",
        snowflake_conn_id="snowflake_default",
        sql="CALL gold.sp_build_dim_employee();"
    )

    gold_fact_headcount = SnowflakeOperator(
        task_id="build_gold_fact_headcount",
        snowflake_conn_id="snowflake_default",
        sql="CALL gold.sp_build_fact_headcount();"
    )

    gold_fact_attrition = SnowflakeOperator(
        task_id="build_gold_fact_attrition",
        snowflake_conn_id="snowflake_default",
        sql="CALL gold.sp_build_fact_attrition();"
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> bronze_load >> silver_clean
    silver_clean >> gold_dim_employee
    gold_dim_employee >> gold_fact_headcount >> gold_fact_attrition >> end
