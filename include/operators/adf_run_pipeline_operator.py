from typing import List, Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook


class AzureDataFactoryRunPipelineOperator(BaseOperator):
    """
    Executes a pipeline for a given data factory.

    :param conn_id: The Connection ID to use for connecting to Azure Data Factory.
    :type conn_id: str
    :param pipeline_name: The name of the pipeline to execute.
    :type pipeline_name: str
    :param resource_group_name: The resource group name.
    :type resource_group_name: str
    :param factory_name: The data factory name.
    :type factory_name: str
    """

    template_fields = ("resource_group_name", "factory_name")

    def __init__(
        self,
        *,
        conn_id: str,
        pipeline_name: str,
        resource_group_name: str,
        factory_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.pipeline_name = pipeline_name
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name

    def execute(self, context: dict) -> None:
        hook = AzureDataFactoryHook(conn_id=self.conn_id)
        self.log.info("Executing the {pipeline_name} pipeline within the {factory_name} factory.")
        output = hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

        for key, value in vars(output).items():
            context["ti"].xcom_push(key=key, value=value)
