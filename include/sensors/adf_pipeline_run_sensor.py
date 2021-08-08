from typing import List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.sensors.base import BaseSensorOperator


class AzureDataFactoryPipelineRunSensor(BaseSensorOperator):
    template_fields = ("run_id", "resource_group_name", "factory_name")

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
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
