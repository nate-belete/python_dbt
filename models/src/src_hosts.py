import snowflake.snowpark as snowpark
import pandas as pd

# This function serves as the entry point for your dbt model
def model(dbt, session: snowpark.Session):
    
    # Fetch the Snowpark dataframe from the raw data
    raw_hosts_df = dbt.source("python_airbnb", "hosts")

    # Convert to pandas DataFrame
    pandas_df = raw_hosts_df.to_pandas()

    # Transform the data using pandas, selecting and renaming columns
    pandas_df.columns = ['host_id', 'host_name', 'is_superhost',
                         'created_at' ,'updated_at'
                         ]
    pandas_df.columns = [col.upper() for col in pandas_df.columns]

    
    return pandas_df