# List all folders from the '/mnt/bronze/SalesLT' directory
#display(dbutils.fs.ls("/mnt/bronze/SalesLT"))

# List all folders from the '/mnt/silver' directory
#dbutils.fs.ls("/mnt/silver")

# Specify the input path to the 'Address.parquet' file
#input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'

# Read the input parquet file into a Spark DataFrame
#df = spark.read.format('parquet').load(input_path)

# Display the DataFrame to inspect its contents
#display(df)

# Import necessary functions for date manipulation
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# Convert the 'ModifiedDate' column to 'yyyy-MM-dd' format
#df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

# Display the updated DataFrame with modified date format
#display(df)

# Use SQL to read a JSON file into a DataFrame
#%sql
#spark.read.format("json").load("abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/data")
#SELECT * FROM json.`abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/data`;

# MAGIC %md
# MAGIC ## Transformation date type in all tables in '/mnt/bronze/SalesLT/' directory

# Create a list of table names from the '/mnt/bronze/SalesLT/' directory
table_name = []

# Iterate through each directory in the '/mnt/bronze/SalesLT/' directory
for i in dbutils.fs.ls("/mnt/bronze/SalesLT/"):
    # Extract the table name from the directory path
    table_name.append(i.name.split('/')[0])

# Print the list of table names
print(table_name)

# Import necessary functions for date manipulation and column selection
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# Iterate through each table in the list
for i in table_name:
    # Construct the full path to the parquet file for the current table
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'

    # Read the parquet file into a Spark DataFrame
    df = spark.read.format('parquet').load(path)

    # Get the list of column names
    column = df.columns

    # Iterate through each column name
    for col in column:
        # Check if the column name contains either "Date" or "date"
        if "Date" in col or "date" in col:
            # Convert the column to 'yyyy-MM-dd' format
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

    # Construct the full output path for the current table
    output_path = '/mnt/silver/SalesLT/' + i + '/'

    # Write the transformed DataFrame to a Delta file in the '/mnt/silver/SalesLT/' directory
    df.write.format('delta').mode("overwrite").save(output_path)
