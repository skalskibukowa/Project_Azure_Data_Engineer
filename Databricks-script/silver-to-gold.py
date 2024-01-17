# Databricks notebook source
# Display the list of files in the '/mnt/silver/SalesLT/' directory
#display(dbutils.fs.ls('mnt/silver/SalesLT/'))

# Display the list of files in the '/mnt/gold/' directory
#dbutils.fs.ls('mnt/gold/')

# Specify the input path to the 'Address' directory
#input_path = '/mnt/silver/SalesLT/Address/'

# Read the parquet files from the input directory into a Spark DataFrame
#df = spark.read.format('delta').load(input_path)

# Display the DataFrame to inspect its contents
#display(df)

# Import necessary libraries for column renaming and removal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Get the list of column names
#column_names = df.columns

# Iterate through each column name and convert it from ColumnName to Column_Name format
#for old_col_name in column_names:
#    new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

    # Change the column name using withColumnRenamed and regexp_replace
#    df = df.withColumnRenamed(old_col_name, new_col_name)

# Display the transformed DataFrame
#display(df)

# MAGIC %md
# MAGIC ## Performing transformation for all tables in '/mnt/silver/SalesLT/' directory (Changing column names and removing column 'rowguid')

# Create a list of table names from the '/mnt/silver/SalesLT/' directory
table_name = []

# Iterate through each directory in the '/mnt/silver/SalesLT/' directory and extract the table name
for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_name.append(i.name.split('/')[0])

# Print the list of table names
#print(table_name)

# Iterate through each table in the list and perform the following transformations:
#   1. Convert column names from ColumnName to Column_Name format using regexp_replace
#   2. Remove the 'rowguid' column from the DataFrame
#   3. Save the transformed DataFrame to a Delta file in the '/mnt/gold/SalesLT/' directory
for name in table_name:
    path = '/mnt/silver/SalesLT/' + name
    df = spark.read.format('delta').load(path)

    # Change column names from ColumnName to Column_Name format
    for old_col_name in column_names:
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
        df = df.withColumnRenamed(old_col_name, new_col_name)

    # Remove the 'rowguid' column
    df = df.drop('rowguid')

    output_path = '/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode("overwrite").option("overwriteSchema", "true").save(output_path)

# Display the transformed DataFrame
#display(df)