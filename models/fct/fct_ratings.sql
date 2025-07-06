-- Define the configuration as incremental table
-- This model will be built incrementally, meaning it will only process new or changed data since the last run.
-- The `on_schema_change` option is set to 'fail' to ensure that any schema changes will cause the model to fail, preventing potential issues with data integrity.
-- This is useful for maintaining the integrity of the data model, especially in production environments.
-- The model is set to materialize as an incremental table, which is efficient for large datasets.
{{ 
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
   )
}}

WITH src_ratings AS (
    SELECT * FROM {{ ref('src_ratings') }}
)

SELECT user_id,
       movie_id,
       rating,
       rating_timestamp
FROM src_ratings
WHERE rating IS NOT NULL

{% if is_incremental() %}
    AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}

-- What we are doing above is that whenever a new row is added to the source table (Here, for ex: ratings table), it will be added to the incremental model.
-- This is useful for large datasets where you only want to process new or changed data, rather than reprocessing the entire dataset each time.
-- The `is_incremental()` function checks if the model is being run in incremental mode, allowing us to optimize the query accordingly.
-- In general, the above query updates the `fct_ratings` table with new ratings data with the conditions I have defined above, ensuring that only new or changed ratings are processed during each run     