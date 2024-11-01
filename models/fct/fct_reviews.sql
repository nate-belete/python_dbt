{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
SELECT 
  {{ dbt_utils.surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }}
    AS review_id,
  * 
  FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }})
{% endif %}

-- # import pandas as pd
-- # import snowflake.snowpark as snowpark
-- # from snowflake.snowpark.functions import col, max as sf_max

-- # def model(dbt, session: snowpark.Session):

-- #     # Access the source table using dbt's ref function
-- #     src_reviews_df = dbt.ref('src_reviews')

-- #     # Convert the Snowpark DataFrame to a pandas DataFrame for manipulation
-- #     pandas_df = src_reviews_df.to_pandas()
-- #     pandas_df.columns = [col.lower() for col in pandas_df.columns]

-- #     # Filter out null review_text entries
-- #     pandas_df = pandas_df[pandas_df['review_text'].notnull()]

-- #     # Check if incrementing is needed by assessing the current state of the target table
-- #     try:
-- #         # Check if the incremental table already exists
-- #         current_df = session.table(dbt.this).to_pandas()
-- #         max_review_date = current_df['review_date'].max()

-- #         # Filter the new data to include only records with review_date greater than the max date in the table
-- #         pandas_df = pandas_df[pandas_df['review_date'] > max_review_date]
-- #     except:
-- #         pass

-- #     pandas_df.columns = [col.upper() for col in pandas_df.columns]

-- #     # Return the transformed DataFrame
-- #     return pandas_df

