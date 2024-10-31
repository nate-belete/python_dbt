import snowflake.snowpark as snowpark
import pandas as pd

def model(dbt, session: snowpark.Session):

    # Fetch the Snowpark dataframe from the source listings
    src_listings_df = dbt.ref('src_listings')

    # Convert to pandas DataFrame
    pandas_df = src_listings_df.to_pandas()

    # Transform the data using pandas
    pandas_df['minimum_nights'] = pandas_df['minimum_nights'].apply(lambda x: 1 if x == 0 else x)
    pandas_df['price'] = pandas_df['price_str'].str.replace('$', '').astype(float)

    # Selecting and renaming columns as needed
    transformed_df = pandas_df[[
        'listing_id',
        'listing_name',
        'room_type',
        'minimum_nights',
        'host_id',
        'price',
        'created_at',
        'updated_at'
    ]]

    # Return the transformed pandas DataFrame
    return transformed_df