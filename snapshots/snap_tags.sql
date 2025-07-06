{% snapshot snap_tags %}

{{
    config(
        target_schema='snapshots',
        unique_key=['user_id','movie_id','tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}

SELECT {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tag']) }} AS row_key,
       user_id,
       movie_id,
       tag,
       CAST(tag_timestamp AS TIMESTAMP_NTZ) AS tag_timestamp
FROM {{ ref('src_tags') }}
LIMIT 100

{% endsnapshot %}

-- This snapshot captures the tags associated with movies by users.
-- It uses a timestamp strategy to track changes in the tags.   
-- The unique key is a combination of user_id and movie_id to ensure that each tag is uniquely identified.
-- The tag_timestamp is cast to TIMESTAMP_NTZ for consistency in time zone handling.
-- The LIMIT clause is used to restrict the number of records processed in the snapshot for testing purposes.
-- In a production environment, we would typically remove the LIMIT clause to capture all records.
-- In Data Warehouses, surrogate key is a concept that when the data is coming from RDBMS, APIs, or external system, they have their own keys available but when we go on to format of creating fact and dimension table or the fact table, we have to create our own version of key to keep track and to join different tables together. It's like a own created key which is known as surrogate key.