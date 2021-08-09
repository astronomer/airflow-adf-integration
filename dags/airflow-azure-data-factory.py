import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from include.sensors.adf_pipeline_run_sensor import AzureDataFactoryPipelineRunStatusSensor

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
    resource_group_name: str,
) -> bool:
    """Retrieves the status of the latest pipeline run for a give pipeline."""

    from azure.mgmt.datafactory.models import (
        RunQueryFilter,
        RunFilterParameters,
        RunQueryOrder,
        RunQueryOrderBy,
        RunQueryFilterOperand,
        RunQueryFilterOperator,
        RunQueryOrderByField,
    )

    # Create a ``RunQueryFilter`` object checking for a pipeline with the provided name.
    run_filter = RunQueryFilter(
        operand=RunQueryFilterOperand.PIPELINE_NAME,
        operator=RunQueryFilterOperator.EQUALS,
        values=[pipeline_name],
    )
    # Create a ``RunQueryOrderBy`` object to ensure pipeline runs are ordered by descending run-start time.
    run_order = RunQueryOrderBy(order_by=RunQueryOrderByField.RUN_START, order=RunQueryOrder.DESC)
    # Create a ``RunFilterParameters`` object to check pipeline runs within the past 7 days.
    filter_params = RunFilterParameters(
        last_updated_before=datetime.utcnow(),
        last_updated_after=days_ago(7),
        filters=[run_filter],
        order_by=[run_order],
    )

    # Retrieve runs for the given pipeline within a given factory.
    logging.info(f"Checking for the latest status for the {pipeline_name} pipeline.")
    hook = AzureDataFactoryHook(conn_id=conn_id)
    query_response = hook.get_conn().pipeline_runs.query_by_factory(
        resource_group_name=resource_group_name, factory_name=factory_name, filter_parameters=filter_params
    )

    # Check if pipeline runs were found within the filter and date/time parameters.
    if query_response.value:
        pipeline_status = query_response.value[0].status
        logging.info(
            f"Found the latest pipeline run for {pipeline_name} pipeline within factory {factory_name} has a "
            f"status of {pipeline_status}."
        )

        return pipeline_status
    else:
        # It is possible a pipeline exists but has never run before or within the time window. As long as the
        # pipeline exists, a "good" status should still be returned. Otherwise, return a status that signifies
        # the pipeline doesn't exist.
        logging.info(
            f"The pipeline {pipeline_name} does exists but no runs have executed within the specified time "
            "window. Checking if pipeline exists."
        )
        pipeline = hook._pipeline_exists(
            pipeline_name=pipeline_name,
            resource_group_name="adf-tutorial",
            factory_name=factory_name,
        )
        if pipeline:
            logging.info(f"A pipeline named {pipeline_name} does exist.")

            return "NoRunInTimeWindow"
    return "DoesNotExist"


@task(multiple_outputs=True)
def run_adf_pipeline(
    conn_id: str,
    pipeline_name: str,
    factory_name: str,
    resource_group_name: str
) -> Dict:
    """
    Executes an Azure Data Factory pipeline and return ``additional_properties`` and ``run_id`` as separate
    `XCom` keys.
    """

    hook = AzureDataFactoryHook(conn_id=conn_id)
    output = hook.run_pipeline(
        pipeline_name=pipeline_name, resource_group_name=resource_group_name, factory_name=factory_name
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
    dag.doc_md = """
    ### Orchestrating Azure Data Factory Pipelines in Airflow
    This DAG demonstrates orchestrating multiple [Azure Data Factory][1] (ADF) pipelines using Airflow to
    perform classic ELT operators. These ADF pipelines extract daily, currency exchange-rates from an
    [API][2], persist data to a data lake in [Azure Blob Storage][3], perform data-quality checks on staged
    data, and finally load to a daily aggregate table with SCD, Type-2 logic in [Azure SQL Database][4].

    Airflow executes each distinct ADF pipelines with operational checks throughout.

    [1]: https://azure.microsoft.com/en-us/services/data-factory/
    [2]: https://www.exchangerate-api.com/
    [3]: https://azure.microsoft.com/en-us/services/storage/blobs/
    [4]: https://azure.microsoft.com/en-us/products/azure-sql/database/
    """

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # Begin tasks for "extract" activities.
    with TaskGroup(group_id="extract_data_factory_pipeline") as extract_data_factory_pipeline:
        get_latest_extract_pipeline_run_status = get_latest_pipeline_run_status(
            conn_id="azure_data_factory",
            pipeline_name="extractDailyExchangeRates",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
        )

        is_extract_pipeline_running = ShortCircuitOperator(
            task_id="is_extract_pipeline_running",
            python_callable=lambda x: x in ["Succeeded", "Failed", "Cancelled", "NoRunInTimeWindow"],
            op_args=[get_latest_extract_pipeline_run_status],
        )

        run_extract_pipeline = run_adf_pipeline(
            conn_id="azure_data_factory",
            pipeline_name="extractDailyExchangeRates",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
        )

        get_current_extract_pipeline_run_status = AzureDataFactoryPipelineRunStatusSensor(
            task_id="get_current_extract_pipeline_run_status",
            conn_id="azure_data_factory",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
            run_id=run_extract_pipeline["run_id"],
            poke_interval=10,
        )

        is_extract_pipeline_running >> run_extract_pipeline

        # Task dependencies created via `XComArgs`:
        #   get_latest_extract_pipeline_run_status >> is_extract_pipeline_running
        #   run_extract_pipeline >> get_current_extract_pipeline_run_status

    # Begin tasks for "data quality and load" activities.
    with TaskGroup(group_id="data_quality_factory_pipeline") as data_quality_factory_pipeline:
        get_latest_dq_pipeline_run_status = get_latest_pipeline_run_status(
            conn_id="azure_data_factory",
            pipeline_name="loadDailyExchangeRates",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
        )

        is_dq_pipeline_running = ShortCircuitOperator(
            task_id="is_dq_pipeline_running",
            python_callable=lambda x: x in ["Succeeded", "Failed", "Cancelled", "NoRunInTimeWindow"],
            op_args=[get_latest_dq_pipeline_run_status],
        )

        run_dq_pipeline = run_adf_pipeline(
            conn_id="azure_data_factory",
            pipeline_name="loadDailyExchangeRates",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
        )

        get_current_dq_pipeline_run_status = AzureDataFactoryPipelineRunStatusSensor(
            task_id="get_current_dq_pipeline_run_status",
            conn_id="azure_data_factory",
            factory_name="airflow-adf-integration",
            resource_group_name="adf-tutorial",
            run_id=run_dq_pipeline["run_id"],
            poke_interval=10,
        )

        is_dq_pipeline_running >> run_dq_pipeline

        # Task dependencies created via `XComArgs`:
        #   get_latest_dq_pipeline_run_status >> is_dq_pipeline_running
        #   run_dq_pipeline >> get_current_dq_pipeline_run_status

    # Create overall task dependencies for the DAG.
    begin >> extract_data_factory_pipeline >> data_quality_factory_pipeline >> end
