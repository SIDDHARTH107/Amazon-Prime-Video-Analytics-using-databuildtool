-- Connecting to the AMAZON S3 Bucket using Snowflake Stage or Snowflake Storage Integration (Most secured one)
-- Set defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE AMAZON_PRIME;
USE SCHEMA RAW;

CREATE STAGE amazonprimestage
URL='s3://xyz'
CREDENTIALS=(AWS_KEY_ID='xyz' AWS_SECRET_KEY='xyz');

-- Loading all the data after creating the table
-- 1. movies.csv
CREATE OR REPLACE TABLE raw_movies (
movieId INTEGER,
title STRING,
genres STRING
)

-- Copying movies data from Amazon S3 bucket
COPY INTO raw_movies
FROM '@amazonprimestage/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')

-- 2. links.csv
CREATE OR REPLACE TABLE raw_links (
movieId INTEGER,
imdbId INTEGER,
tmdbId INTEGER
)

-- Copying links data from Amazon S3 bucket
COPY INTO raw_links
FROM '@amazonprimestage/links.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')

-- 4. genome-scores.csv
CREATE OR REPLACE TABLE raw_genome_scores (
movieId INTEGER,
tagId INTEGER,
relevance FLOAT
)

-- Copying genome-tags data from Amazon S3 bucket
COPY INTO raw_genome_scores
FROM '@amazonprimestage/genome-scores.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')

-- 5. ratings.csv
CREATE OR REPLACE TABLE raw_ratings (
userId INTEGER,
movieId INTEGER,
rating FLOAT,
timestamp BIGINT
)

-- Copying ratings data from Amazon S3 bucket
COPY INTO raw_ratings
FROM '@amazonprimestage/ratings.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')

-- 6. tags.csv
CREATE OR REPLACE TABLE raw_tags (
userId INTEGER,
movieId INTEGER,
tag STRING,
timestamp BIGINT
)

-- Copying ratings data from Amazon S3 bucket
COPY INTO raw_tags
FROM '@amazonprimestage/tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE'
-- The above line means we are telling Snowflake that if it encounters any error in any row just skip it and keep on adding other rows



USE WAREHOUSE COMPUTE_WH;
USE DATABASE AMAZON_PRIME;
USE SCHEMA RAW;

DROP TABLE DIM_MOVIES;
DROP VIEW SRC_GENOME_SCORES;
DROP VIEW SRC_GENOME_TAGS;
DROP VIEW SRC_LINKS;
DROP VIEW SRC_MOVIES;
DROP VIEW SRC_RATINGS;
DROP VIEW SRC_TAGS;
DROP VIEW MY_SECOND_DBT_MODEL;
-- Removing all these views and tables to avoid confusion and make the project cleaner



AMAZON_PRIME.SNAPSHOTS.SNAP_TAGSAMAZON_PRIME.SNAPSHOTS.SNAP_TAGSAMAZON_PRIME.SNAPSHOTS.SNAP_TAGSAMAZON_PRIME.SNAPSHOTS.SNAP_TAGSSELECT *
FROM AMAZON_PRIME.DEV.FCT_RATINGS
ORDER BY rating_timestamp DESC
LIMIT 5;

SELECT *
FROM AMAZON_PRIME.DEV.SRC_RATINGS
ORDER BY rating_timestamp DESC
LIMIT 6;

INSERT INTO AMAZON_PRIME.DEV.SRC_RATINGS (user_id, movie_id, rating, rating_timestamp)
VALUES (87587, 7151, 3.9, '2015-03-31 23:40:02.000 -0700')

-- After running dbt run --select fct_ratings command, the new row which I have inserted has been added in the new incremental table also

SELECT *
FROM SNAPSHOTS.SNAP_TAGS
ORDER BY user_id, dbt_valid_from DESC


UPDATE dev.src_tags
SET tag = 'Mark Waters Returns', 
    tag_timestamp = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)
WHERE user_id = 18;

SELECT *
FROM dev.src_tags
WHERE user_id = 18;


SELECT *
FROM SNAPSHOTS.SNAP_TAGS
WHERE user_id = 18
ORDER BY user_id, dbt_valid_from DESC;

------------------------------------------------------

CREATE OR REPLACE TABLE movie_analysis AS
(WITH ratings_summary AS (
    SELECT movie_id,
           AVG(rating) AS average_rating,
           COUNT(*) AS total_ratings
    FROM AMAZON_PRIME.DEV.fct_ratings
    GROUP BY movie_id
    HAVING COUNT(*) > 100 -- Only movies with more than 100 ratings
)
SELECT m.movie_title,
       rs.average_rating,
       rs.total_ratings
FROM ratings_summary rs
JOIN AMAZON_PRIME.DEV.dim_movies m ON m.movie_id = rs.movie_id
ORDER BY rs.average_rating DESC)