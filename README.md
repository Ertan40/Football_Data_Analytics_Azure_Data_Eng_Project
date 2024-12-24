# Football_Data_Analytics_Azure_Data_Eng_Project
 This project involves a complete pipeline for extracting football data from Wikipedia, storing it in Azure Data Lake, querying the data through Azure Synapse, and visualizing the results in PowerBI. It's designed to provide comprehensive analytics on football data for enthusiasts and analysts.

1. <b>Architecture Diagram</b>:
    - <p><img src="images/architecture_diagram.jpg" alt="architecture_diagram" width="800px"></p>

## <ins>Current Environment</ins>
- Docker Desktop
- Apache Airflow
- Azure Data Lake Storage Gen2 account
- Azure Databricks workspace (Optional)
- Azure Synapse Analytics workspace
- Powerbi Desktop or Tableau Desktop
<p><img src="images/SQL_DB.jpg" alt="SQL_DB" width="800px"></p>

## <ins>1. Extracting Data with Apache Airflow</ins>

1. Files: wikipedia_pipeline.py within the pipelines directory and wikipedia_flow.py within the dags directory.
2. Establishing a connection between Azure Data Factory and the local SQL Server.
3. Setting up a copy pipeline to transfer all tables from the local SQL server to the Azure Data Lake's "bronze" folder.
<p><img src="images/for_each_activity_successed.jpg" alt="for_each_activity_successed" width="800px"></p>
<p><img src="images/data_ingestion_done.jpg" alt="data_ingestion_done" width="800px"></p>

- Azure Data Lake Gen 2: Used for storage of data. Three containers have been created Bronze, Silver and Gold. The three layers represent the stages of business logic and requirements. The initial data ingested from SQL Server is transformed to parquet format as it provides significant performance, storage, and query optimization benefits in big data processing and analytics scenarios and stored in the bronze container. The next level transformation is performed on the bronze data and stored in the silver container. The final transformation is performed on the silver layer and the data is stored in the gold container. The delta lake abstraction layer has been used on the parquet format files for storing the data in the gold and silver containers to allow for versioning. The data in the gold layer is ideal for reporting and analysis. It can be consumed by Azure Synapse Analytics which in turn can be connected to PowerBI for creating visualizations.
<p><img src="images/resource_group.jpg" alt="resource_group" width="800px"></p>
<p><img src="images/grant_access.jpg" alt="grant_access" width="800px"></p>  
<p><img src="images/containers.jpg" alt="containers" width="800px"></p> 

## <ins>2. Data Transformation</ins>
After ingesting data into the "bronze" folder, it is transformed following the medallion data lake architecture (bronze, silver, gold). Data transitions through bronze, silver, and ultimately gold, suitable for business reporting tools like Power BI.

It was convenient to access the storage using the credential passthrough feature as the email ID being used for this project was already added to the IAM policy of the data lake. The Data Factory could be connected by using an access token generated from Databricks and saving it as a secret in the Key vault.

Azure Databricks, using PySpark, is used for these transformations. Data initially stored in parquet format in the "bronze" folder is converted to the delta format as it progresses to "silver" and "gold." This transformation is carried out through Databricks notebooks:

1. Mount the storage.
2. Transform data from "bronze" to "silver" layer.
3. Further transform data from "silver" to "gold" layer.
<p><img src="images/end to end pipeline_successed.jpg" alt="end to end pipeline_successed" width="800px"></p> 

## <ins>3. Data Loading</ins>
Data from the "gold" folder is loaded into the Business Intelligence reporting application, Power BI. Azure Synapse is used for this purpose. The steps involve:

1. Creating a link from Azure Storage (Gold Folder) to Azure Synapse.
2. Writing stored procedures to extract table information as a SQL view.
3. Storing views within a server-less SQL Database in Synapse.
<p><img src="images/Connected to ServerlessSQLDB.jpg" alt="Connected to ServerlessSQLDB" width="800px"></p>  
<p><img src="images/stored procedure.jpg" alt="stored procedure" width="800px"></p>
<p><img src="images/linked_data lake storage.jpg" alt="linked_data lake storage" width="800px"></p> 
<p><img src="images/all_views_success.jpg" alt="all_views_success" width="800px"></p> 

## <ins>4. Data Reporting</ins>
Power BI connects directly to the cloud pipeline using DirectQuery to dynamically update the database. A Power BI report is developed to visualize AdventureWorks dataset data, including sales, product information, and customer gender.

Database connected and data loaded
<p><img src="images/Load_power BI.jpg" alt="Load_power BI" width="800px"></p>
Manage relationship 
<p><img src="images/create new relationship.jpg" alt="create new relationship" width="800px"></p>
<p><img src="images/dashboard.jpg" alt="dashboard" width="800px"></p>
