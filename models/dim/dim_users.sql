WITH ratings AS (
    SELECT DISTINCT user_id FROM {{ ref('src_ratings') }}
),
tags AS (
    SELECT DISTINCT user_id FROM {{ ref('src_tags') }}
)

SELECT DISTINCT user_id
FROM (
    SELECT * FROM ratings 
    UNION
    SELECT * FROM tags
)

-- Here, in the above query what we have done is that we have fetched all of the user_id details from ratings and tags followed by unioning it and getting all the unique user ids.