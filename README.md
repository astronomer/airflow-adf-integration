##  Orchestrating Azure Data Factory Pipelines in Airflow
This DAG demonstrates orchestrating multiple Azure Data Factory (ADF) pipelines using Airflow to perform classic ELT operators. These ADF pipelines extract daily, currency exchange-rates from an API, persist data to a data lake in Azure Blob Storage, perform data-quality checks on staged data, and finally load to a daily aggregate table with SCD, Type-2 logic in Azure SQL Database.

> **Note:** A custom [`AzureDataFactoryPipelineRunStatusSensor`](https://github.com/astronomer/modern-elt-salesforce-to-snowflake/blob/main/include/operators/salesforce_to_s3.py) was created for this DAG to check the status of ADF pipelines runs as an operational checkpoint -- soon to be a part of the Microsoft Azure provider in Airflow.

</br>

**Airflow Version**

   `2.1.0`

**Providers**

  ```
  apache-airflow-providers-microsoft-azure==3.0.0
  ```

**Connections Required**
  - Azure Data Factory
