from typing import List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.sensors.base import BaseSensorOperator


class AzureDataFactoryPipelineRunStatusSensor(BaseSensorOperator):
    """
    Checks the status of a pipeline run in Azure Data Factory against expected statuses and known terminal
    statuses.

    :param conn_id: The Connection ID to use for connecting to Azure Data Factory.
    :type conn_id: str
    :param run_id: The pipeline run indentifier.
    :type run_id: str
    :param resource_group_name: The resource group name.
    :type resource_group_name: str
    :param factory_name: The data factory name.
    :type factory_name: str
    :param expected_statuses: The status(es) which are desired for the pipline run.
    :type expected_statuses: str or List[str]
    """

    template_fields = ("run_id", "resource_group_name", "factory_name")

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: str,
        resource_group_name: str,
        factory_name: str,
        expected_statuses: Optional[Union[List[str], str]] = "Succeeded",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.run_id = run_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.expected_statuses = (
            [expected_statuses] if isinstance(expected_statuses, str) else expected_statuses
        )

    def poke(self, context: dict) -> bool:
        self.log.info(
            f"Checking for pipeline run {self.run_id} to be in one of the following statuses: "
            f"{', '.join(self.expected_statuses)}.",
        )

        self.hook = AzureDataFactoryHook(conn_id=self.conn_id)
        pipeline_run = self.hook.get_pipeline_run(
            run_id=self.run_id, factory_name=self.factory_name, resource_group_name=self.resource_group_name
        )
        pipeline_run_status = pipeline_run.status
        self.log.info(f"Current job status for job {self.run_id}: {pipeline_run_status}.")

        if pipeline_run_status in self.expected_statuses:
            return True
        elif pipeline_run_status in ["Failed", "Cancelled"]:
            raise AirflowException(
                f"Pipeline run with ID '{self.run_id}' is in a terminal status: {pipeline_run_status}"
            )

        return False
