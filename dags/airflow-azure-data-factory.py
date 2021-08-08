import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from include.sensors.adf_pipeline_run_sensor import AzureDataFactoryPipelineRunSensor

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


@task
def get_latest_pipeline_run_status(
    conn_id: str,
    pipeline_name: str,
    factory_name: str,
    check_statuses: Optional[Union[List[str], str]] = ["Succeeded", "Failed", "Cancelled"],
) -> bool:
    from azure.mgmt.datafactory.models import (
        RunQueryFilter,
        RunFilterParameters,
        RunQueryOrder,
        RunQueryOrderBy,
        RunQueryFilterOperand,
        RunQueryFilterOperator,
        RunQueryOrderByField,
    )

    if isinstance(check_statuses, str):
        check_statuses = [check_statuses]

    run_filter = RunQueryFilter(
        operand=RunQueryFilterOperand.PIPELINE_NAME,
        operator=RunQueryFilterOperator.EQUALS,
        values=[pipeline_name],
    )
    run_order = RunQueryOrderBy(order_by=RunQueryOrderByField.RUN_START, order=RunQueryOrder.DESC)
    filter_params = RunFilterParameters(
        last_updated_before=datetime.utcnow(),
        last_updated_after=days_ago(7),
        filters=[run_filter],
        order_by=[run_order],
    )

    logging.info(f"Checking for the latest status for the {pipeline_name} pipeline.")

    hook = AzureDataFactoryHook(conn_id=conn_id)
    query_response = hook.get_conn().pipeline_runs.query_by_factory(
        resource_group_name="adf-tutorial", factory_name=factory_name, filter_parameters=filter_params
    )

    if query_response.value:
        pipeline_status = query_response.value[0].status

        logging.info(
            f"Found the latest pipeline run for {pipeline_name} pipeline within factory {factory_name} has a "
            f"status of {pipeline_status}."
        )

        return pipeline_status
    else:
        pipeline = hook._pipeline_exists(
            pipeline_name=pipeline_name,
            resource_group_name="adf-tutorial",
            factory_name=factory_name,
        )
        if pipeline:
            return "HasNeverRun"
    return "DoesNotExist"


@task(multiple_outputs=True)
def run_adf_pipeline(pipeline_name: str, conn_id: str, factory_name: Optional[str] = None) -> Dict:
    hook = AzureDataFactoryHook(conn_id=conn_id)
    output = hook.run_pipeline(
        pipeline_name=pipeline_name, resource_group_name="adf-tutorial", factory_name=factory_name
    )

    return vars(output)


with DAG(
    dag_id="airflow-adf-integration-demo",
    start_date=datetime(2021, 7, 21),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    default_view="graph",
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    with TaskGroup(
        group_id="extract_data_factory_pipeline",
        tooltip="""
        Tasks to execute an Azure Data Factory pipline to extract currency exchange rate data and load into
        Azure SQL Database.
        """,
    ) as extract_data_factory_pipeline:
        get_latest_extract_pipeline_run_status = get_latest_pipeline_run_status(
            conn_id="azure_data_factory",
            pipeline_name="extractDailyExchangeRates",
            factory_name="airflow-adf-integration",
        )

        is_extract_pipeline_running = ShortCircuitOperator(
            task_id="is_extract_pipeline_running",
            python_callable=lambda x: x in ["Succeeded", "Failed", "Cancelled", "HasNeverRun"],
            op_args=[get_latest_extract_pipeline_run_status],
        )

        run_extract_pipeline = run_adf_pipeline(
            conn_id="azure_data_factory",
            pipeline_name="extractDailyExchangeRates",
            factory_name="airflow-adf-integration",
        )

        get_current_extract_pipeline_run_status = AzureDataFactoryPipelineRunSensor(
            task_id="get_current_extract_pipeline_run_status",
            conn_id="azure_data_factory",
            resource_group_name="adf-tutorial",
            factory_name="airflow-adf-integration",
            run_id=run_extract_pipeline["run_id"],
            poke_interval=10,
        )

        is_extract_pipeline_running >> run_extract_pipeline

        # Task dependencies created via `XComArgs`:
        #   get_latest_extract_pipeline_run_status >> is_extract_pipeline_running
        #   run_extract_pipeline >> get_current_extract_pipeline_run_status

    with TaskGroup(group_id="data_quality_factory_pipeline") as data_quality_factory_pipeline:
        get_latest_dq_pipeline_run_status = get_latest_pipeline_run_status(
            conn_id="azure_data_factory",
            pipeline_name="loadDailyExchangeRates",
            factory_name="airflow-adf-integration",
        )

        is_dq_pipeline_running = ShortCircuitOperator(
            task_id="is_dq_pipeline_running",
            python_callable=lambda x: x in ["Succeeded", "Failed", "Cancelled", "HasNeverRun"],
            op_args=[get_latest_dq_pipeline_run_status],
        )

        run_dq_pipeline = run_adf_pipeline(
            conn_id="azure_data_factory",
            pipeline_name="loadDailyExchangeRates",
            factory_name="airflow-adf-integration",
        )

        get_current_dq_pipeline_run_status = AzureDataFactoryPipelineRunSensor(
            task_id="get_current_dq_pipeline_run_status",
            conn_id="azure_data_factory",
            resource_group_name="adf-tutorial",
            factory_name="airflow-adf-integration",
            run_id=run_dq_pipeline["run_id"],
            poke_interval=10,
        )

        is_dq_pipeline_running >> run_dq_pipeline

    # Task dependencies created via `XComArgs`:
    #   get_latest_dq_pipeline_run_status >> is_dq_pipeline_running
    #   run_dq_pipeline >> get_current_dq_pipeline_run_status

    begin >> extract_data_factory_pipeline >> data_quality_factory_pipeline >> end
