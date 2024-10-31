import snowflake.snowpark as snowpark
import pandas as pd

# This function serves as the entry point for your dbt model
def model(dbt, session: snowpark.Session):
    
    # Fetch the Snowpark dataframe from the raw data
    raw_listings_df = dbt.source("python_airbnb", "listings")

    # Convert to pandas DataFrame
    pandas_df = raw_listings_df.to_pandas()

    # Transform the data using pandas, selecting and renaming columns
    pandas_df.columns = ['listing_id', 'listing_name', 'listing_url', 'room_type', 
                         'minimum_nights', 'host_id', 'price_str', 'created_at', 'updated_at'
                         ]


    return pandas_df