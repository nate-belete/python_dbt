import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, max as sf_max

def model(dbt, session: snowpark.Session):

    # Access the source table using dbt's ref function
    src_reviews_df = dbt.ref('src_reviews')

    # Convert the Snowpark DataFrame to a pandas DataFrame for manipulation
    pandas_df = src_reviews_df.to_pandas()

    # Filter out null review_text entries
    pandas_df = pandas_df[pandas_df['review_text'].notnull()]

    # Check if incrementing is needed by assessing the current state of the target table
    try:
        # Check if the incremental table already exists
        current_df = session.table(dbt.this).to_pandas()
        max_review_date = current_df['review_date'].max()

        # Filter the new data to include only records with review_date greater than the max date in the table
        pandas_df = pandas_df[pandas_df['review_date'] > max_review_date]
    except:
        pass

    # Return the transformed DataFrame
    return pandas_df