-- SNOWFLAKE USER CREATION

-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='AMAZON_PRIME.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

-- Step 5: Create a database and schema for the MovieLens project
CREATE DATABASE IF NOT EXISTS AMAZON_PRIME;
CREATE SCHEMA IF NOT EXISTS AMAZON_PRIME.RAW;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE AMAZON_PRIME TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE AMAZON_PRIME TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AMAZON_PRIME TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA AMAZON_PRIME.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA AMAZON_PRIME.RAW TO ROLE TRANSFORM;


-- Windows
virtualenv venv
venv\Scripts\activate

-- MAC
mkdir course
cd course
virtualenv venv
. venv/bin/activate

-- install dbt snowflake
pip install dbt-snowflake==1.9.0

-- create dbt profile
-- mac
mkdir ~/.dbt

-- windows 
mkdir %userprofile%\.dbt

#initiate dbt project 
dbt init <projectname>

-- Add staging tables -> all will be views
-- Update dbt_project.yaml file to support of tables
dim:
  +materialized: table
--   +schema: dim
-- fct:
--   +materialized: incremental
--   +schema: fct
-- mart:
--   +materialized: table
--   +schema: mart
-- snapshots:
--   +schema: snapshots