##  Orchestrating Azure Data Factory Pipelines in Airflow
This DAG demonstrates orchestrating multiple Azure Data Factory (ADF) pipelines using Airflow to perform classic ELT operations. These ADF pipelines extract daily, currency exchange-rates from an API, persist data to a data lake in Azure Blob Storage, perform data-quality checks on staged data, and finally load to a daily aggregate table with SCD, Type-2 logic in Azure SQL Database.  There are two ADF pipelines, `extractDailyExchangeRates` and `loadDailyExchangeRates`, which perform the ELT.

</br>

The `extractDailyExchangeRates` ADF pipeline will extract the data from the open [Exchange Rate API](https://www.exchangerate-api.com/) for the USD and EUR currencies and initially store the response data in a "landing" container within Azure Blob Storage, then copy the extracted data to a "data-lake" container, load the landed data to a staging table in Azure SQL Database via a T-SQL stored procedure, and finally delete the landed data file.
![image](https://user-images.githubusercontent.com/48934154/128788608-db87cd3d-1408-4423-bbc0-692de57820dd.png)

The `loadDailyExchangeRates` ADF pipeline performs a data quality check against the ingested currency codes relative to a dimensional, reference dataset.  If the data quality check passes, another T-SQL stored procedure will insert the data into a daily, aggregate table of exchange rates comparing the US dollar, Euro, Japanese Yen, and the Swiss Franc.
![image](https://user-images.githubusercontent.com/48934154/128788546-978f724b-3782-4a02-9eaa-bdc99f9fa9db.png)


**Airflow Version**

   `2.2.0`

**Providers**

  ```
  apache-airflow-providers-microsoft-azure==3.2.0
  ```

**Connections Required**
  - Azure Data Factory

For more information on how to connect to Azure Data Factory from Airflow, [see this guide](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/connections/adf.html).
