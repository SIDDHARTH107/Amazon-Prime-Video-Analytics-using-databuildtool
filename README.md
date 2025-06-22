# Amazon-Prime-Data-Analytics-using-databuildtool (from dbt labs)

**Project Architecture**
![image](https://github.com/user-attachments/assets/e2539855-aba4-4450-8b93-7d34110a5a6f)

**Flowchart of dbt**
![image](https://github.com/user-attachments/assets/7e493142-f768-434e-8e41-705e69224a8d)

**DBT Overview**
![image](https://github.com/user-attachments/assets/c469cdc4-7753-4dc3-9cb4-9339f8303bbd)

**Generic Data Engineering Lifecycle**
![image](https://github.com/user-attachments/assets/c724779d-ee51-4280-b013-c27c900260f2)

**DBT works with data warehouse**
![image](https://github.com/user-attachments/assets/901f1247-dc7d-4d9c-b0c1-ff2032f9cf46)

**Data Warehouse VS Data Lake**
![image](https://github.com/user-attachments/assets/86478bb7-7a41-4663-9eb9-e18246ba2da9)

**ETL VS ELT**
![image](https://github.com/user-attachments/assets/d94199a4-09d5-4601-886a-944c7be2f2a4)

``Dataset source``
https://grouplens.org/datasets/movielens/20m/

- MovieLens 20M movie ratings. 
- Stable benchmark dataset.
- 20 million ratings and 465,000 tag applications applied to 27,000 movies by 138,000 users.
- Includes tag genome data with 12 million relevance scores across 1,100 tags.
- Released on 4/2015
- Updated on 10/2016 to update links.csv and add tag genome data.

``Dataset readme file``
https://files.grouplens.org/datasets/movielens/ml-20m-README.html

## Project workflow
```Step 1```
After downloading all the datasets from the above link, I have extracted all the csv files from the zip folder. Then I uploaded it in the __Amazon S3 Bucket__ as shown in the image below.

![image](https://github.com/user-attachments/assets/87ceef94-4563-4e6d-a731-08f04dcb649b)

```Step 2``` 
Then, I created my __Snowflake__ account to connect it with S3 bucket internally. You can go to CREATE option followed by clicking SQL WORKSHEET. I have created an internal user in order to connect Snowflake to S3.

![image](https://github.com/user-attachments/assets/dea9d0ea-95be-47db-9104-ff245a27045b)

![image](https://github.com/user-attachments/assets/1ce496e1-46c8-4849-96a4-4174c54e90bd)

```Step 3```
In order to create a connection, I created a IAM user to specify the access key and access secret key as credentials in Snowflake.

![image](https://github.com/user-attachments/assets/fd70093c-cb7a-44d4-8e53-a62116a10f3f)

![image](https://github.com/user-attachments/assets/67054fa8-d6d3-4735-8256-21e262e8ac6b)

![image](https://github.com/user-attachments/assets/ac9fd815-f5a1-4b28-b1e2-ca696c3ffdc3)

![image](https://github.com/user-attachments/assets/73759521-4151-43e9-afb6-f4ed560a3f4b)

```Step 4```
Afer this, I used COPY INTO commands in Snowflake to copy all the csv files from S3 bucket to Snowflake Data Warehouse.

![image](https://github.com/user-attachments/assets/3533b8ac-8433-4cda-9e04-ddf45b2f894d)

```Step 5```
Setting up the dbt configuration.

### Installation and Setup 
There are two main ways to use DBT:

__DBT Cloud:__ A web-based service that provides a development environment, job scheduler, and documentation hosting

__DBT Core:__ The open-source command-line version that you can run locally or on your servers (This is used in my project because it is free and open source & there's not much difference between the two)

Open a folder for your project followed by opening command prompt and typing these commands:

![image](https://github.com/user-attachments/assets/83717eb1-b4c0-401e-b89b-a98309934118)

Commands for windows in VS Code:

This will give us direct flexibility for DBT to connect to the Snowflake. It will install the core DBT, the connector to Snowflake / Snowflake adapter and the required dependencies also.

```sql
python -m venv venv
.\venv\Scripts\Activate
pip install dbt-snowflake==1.9.0
