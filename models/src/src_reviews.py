import snowflake.snowpark as snowpark
import pandas as pd

# This function serves as the entry point for your dbt model
def model(dbt, session: snowpark.Session):
    
    # Fetch the Snowpark dataframe from the raw data
    raw_reviews_df = dbt.source("python_airbnb", "reviews")

    # Convert to pandas DataFrame
    pandas_df = raw_reviews_df.to_pandas()

    # Transform the data using pandas, selecting and renaming columns
    pandas_df.columns = ['listing_id', 'review_date', 'reviewer_name',
                         'review_text' ,'review_sentiment'
                         ]
    
    return pandas_df