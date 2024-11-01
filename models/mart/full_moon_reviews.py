import snowflake.snowpark as snowpark
import pandas as pd

# This function serves as the entry point for your dbt model
def model(dbt, session: snowpark.Session):
    
    # Fetch the Snowpark dataframes from the existing dbt models (tables/views)
    fct_reviews_df = dbt.ref('fct_reviews')
    full_moon_dates_df = dbt.ref('seed_full_moon_dates')

    # Convert Snowpark DataFrames to pandas DataFrames
    fct_reviews_pd = fct_reviews_df.to_pandas()
    full_moon_dates_pd = full_moon_dates_df.to_pandas()

    fct_reviews_pd.columns = [col.lower() for col in fct_reviews_pd.columns]


    # Convert the review_date to datetime and adjust the full_moon_date
    fct_reviews_pd['review_date'] = pd.to_datetime(fct_reviews_pd['review_date']).dt.date
        # Assuming 'seed_full_moon_dates' has a single column 'full_moon_date'
    full_moon_dates_pd['FULL_MOON_DATE'] = ( pd.to_datetime(full_moon_dates_pd['FULL_MOON_DATE']) +
                                            pd.Timedelta(days=1)
                                            ).dt.date

    # Merge the dataframes on adjusted date
    result_pd = fct_reviews_pd.merge(
        full_moon_dates_pd,
        left_on='review_date',
        right_on='FULL_MOON_DATE',
        how='left'
    )

    # Create a new column to determine if the review was on a full moon
    result_pd['is_full_moon'] = result_pd['FULL_MOON_DATE'].apply(
        lambda x: 'full moon' if pd.notnull(x) else 'not full moon'
    )

    # Drop the 'full_moon_date' column as it's no longer needed
    result_pd.drop(columns=['FULL_MOON_DATE'], inplace=True)

    result_pd.columns = [col.upper() for col in result_pd.columns]


    return result_pd