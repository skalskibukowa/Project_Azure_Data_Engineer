# Project overview
This data engineering project aims to migrate a company's on-premises database to Azure, leveraging Azure Data Factory for data ingestion, transformation, and storage. The project will implement a three-stage storage strategy, consisting of bronze, silver, and gold data layers (Medalion architecture). Bronze data will represent raw data extracted from the source database, silver data will undergo data cleansing, transformation, and enrichment, while gold data will serve as the aggregated and standardized data source for Power BI analytics. Azure Databricks will be employed for data transformation tasks.

## Used Service/Tool in Azure
1. Azure data factory
2. Storage Account
3. Azure Databricks
4. Azure key vault
5. Power BI 

## Key
 Objectives
Migrate data from on-premises database to Azure: Utilize Azure Data Factory to seamlessly transfer data from the on-premises database to Azure storage accounts.
Implement a three-stage data storage strategy: Establish a bronze, silver, and gold data layer to handle raw, transformed, and aggregated data, respectively.
Leverage Azure Databricks for data transformation: Employ Azure Databricks' Apache Spark engine to perform data cleansing, transformation, and enrichment tasks.
Prepare data for Power BI analytics: Ensure that the gold data layer is in a format suitable for loading into Power BI dashboards and reports.

## Project Methodology
The project will follow a structured methodology, encompassing the following phases:
1. Data Assessment and Planning: Thoroughly assess the existing on-premises database, identifying data sources, data volumes, and data quality issues. Plan the data migration process, including data storage locations and transformation strategies.
2. Data Ingestion: Utilize Azure Data Factory to establish data pipelines for extracting data from the on-premises database, loading it into Azure storage accounts, and ensuring data consistency and integrity.
3. Data Transformation: Employ Azure Databricks to perform data cleansing, transformation, and enrichment tasks within the silver data layer. This may involve data cleansing, data format conversion, data enrichment with external data sources, and data validation.
4. Data Storage and Aggregation: Move transformed data from the silver layer to the gold layer, which will serve as the centralized data source for Power BI analytics. Aggregate data to improve performance and reduce data volume.
5. Data Quality Monitoring: Establish data quality monitoring processes to ensure the accuracy, consistency, and completeness of data throughout the data lifecycle. Implement alerts and notifications to identify and address data quality issues promptly.
6. Power BI Integration: Prepare the gold data layer for loading into Power BI dashboards and reports. Ensure data alignment with Power BI data models and visualizations.
7. Data Governance and Security: Implement data governance policies to control access to sensitive data, enforce data quality standards, and adhere to regulatory compliance requirements. Utilize Azure Key Vault to securely store and manage data access credentials.

## Project Benefits
1. Reduced Operational Costs: By migrating data to Azure, the company can eliminate the need for on-premises infrastructure and associated maintenance costs.
2. Enhanced Scalability: Azure provides the flexibility to scale data storage and processing capabilities to meet changing business needs.
3. Improved Data Availability: Azure's global infrastructure ensures high availability and data durability, minimizing the risk of data loss or downtime.
4. Enhanced Data Quality: The three-stage data storage approach facilitates data cleansing, transformation, and aggregation, improving data quality for downstream analysis.
5. Accelerated Analytics: Power BI provides a powerful and user-friendly platform for analyzing and visualizing data, enabling informed decision-making.

# Architecture of the project

![DataEngineerProject](https://github.com/skalskibukowa/Azure_Data_Engineer_Project/assets/29678557/eec6b2c1-84e1-4d45-a8ea-354e1091313b)
