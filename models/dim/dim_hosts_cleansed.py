import snowflake.snowpark as snowpark
import pandas as pd

def model(dbt, session: snowpark.Session):

    # Fetch the Snowpark dataframe from the referenced hosts source model
    src_hosts_df = dbt.ref('src_hosts')

    # Convert to pandas DataFrame
    pandas_df = src_hosts_df.to_pandas()

    # Transform the data using pandas
    pandas_df['host_name'] = pandas_df['host_name'].fillna('Anonymous')

    # Select and rename the necessary columns
    transformed_df = pandas_df[[
        'host_id',
        'host_name',
        'is_superhost',
        'created_at',
        'updated_at'
    ]]

    # Return the transformed pandas DataFrame
    return transformed_df