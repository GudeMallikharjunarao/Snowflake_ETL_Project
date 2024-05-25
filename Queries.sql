// Create database and schemas if not exists already
CREATE DATABASE IF NOT EXISTS PROJECT_DB;
CREATE SCHEMA IF NOT EXISTS PROJECT_DB.file_formats;
CREATE SCHEMA IF NOT EXISTS PROJECT_DB.external_stages;

/* Creating the storage integration object in other to have access to 
credentials and connect to  AWS S3.*/

// Create storage integration object
create or replace storage integration s3_int_aws
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::339712796431:role/s3_intg_project'
  STORAGE_ALLOWED_LOCATIONS = ('s3://awss3newmalli/project/csv/')
  COMMENT = 'Integration with aws s3 buckets';

/* Using the DESC INTEGRATION command to retrieve the STORAGE_AWS_IAM_USER_ARN
and STORAGE_AWS_EXTERNAL_ID so we can update it in S3*/

DESC integration s3_int_aws;

-- First, we create a table for the data to be loaded
// Create table first

CREATE TABLE PROJECT_DB.PUBLIC.sales_data (
    Invoice_ID VARCHAR(255) NOT NULL,
    Branch VARCHAR(255) NOT NULL,
    City VARCHAR(255) NOT NULL,
    Customer_type VARCHAR(255) NOT NULL,
    Gender VARCHAR(255) NOT NULL,
    Product_line VARCHAR(255) NOT NULL,
    Unit_price FLOAT NOT NULL,
    Quantity INT NOT NULL,
    Tax_5 FLOAT NOT NULL,
    Total FLOAT NOT NULL,
    Date DATE NOT NULL,
    Time TIME NOT NULL,
    Payment VARCHAR(255) NOT NULL,
    cogs FLOAT NOT NULL,
    gross_margin_percentage FLOAT NOT NULL,
    gross_income FLOAT NOT NULL,
    Rating FLOAT NOT NULL,
    PRIMARY KEY (Invoice_ID)
);

// Create a file format object of csv type
CREATE OR REPLACE file format PROJECT_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE;


 
/* Create a stage(csv_folder) object that references the storage 
integration object and the file format object*/  
CREATE OR REPLACE stage PROJECT_DB.external_stages.stage_aws_project
    URL = 's3://awss3newmalli/project/csv/'
    STORAGE_INTEGRATION = s3_int_aws
    FILE_FORMAT = PROJECT_DB.file_formats.csv_fileformat;

    // List the files in Stage
LIST @project_db.external_stages.stage_aws_project;


// Create a schema to keep pipe objects
CREATE OR REPLACE SCHEMA project_db.pipes;

-- Create a pipe
CREATE OR REPLACE PIPE project_db.pipes.supermarket_pipe
AUTO_INGEST = TRUE
AS
COPY INTO project_db.public.sales_data
FROM @project_db.external_stages.stage_aws_project
PATTERN = '.*supermarket.*';

/*Describe the pipe to see the code to connect the trigger Snowpipe and 
create event notification in S3 Bucket*/

desc pipe supermarket_pipe;


select * from project_db.public.sales_data;

-- 1. Display the first 5 rows from the dataset.

select * from sales_data limit 5;

-- 2. Display the last 5 rows from the dataset.

select * from sales_data order by invoice_id desc limit 5;

-- 3. Display count, min, max, avg, and std values for a column in the dataset.

select count(gross_income),
min(gross_income),
max(gross_income),
avg(gross_income)
from sales_data;

-- 4. Find the number of missing values.
select count(*) from sales_data where Branch is null;

-- 5. Count the number of occurrences of each city.

select City,count(City) from sales_data group by City;

-- 6. Find the most frequently used payment method.

select Payment,count(*) from sales_data group by Payment 
order by count(*) desc;

-- 7. Find the most profitable branch as per gross income.

select Branch,round(sum(gross_income),2) as sum_gross_income 
from sales_data group by Branch order by sum_gross_income desc;


-- 8.  Find the most used payment method city-wise.
select * from sales_data;

select city,
   sum(case when Payment='Cash' then 1 else 0 end) as Cash,
   sum(case when Payment='Ewallet' then 1 else 0 end) as Ewallet,
   sum(case when Payment='Credit card' then 1 else 0 end) as Credit_card
from sales_data group by City;

-- 9. Find the product line purchased in the highest quantity.

select Product_line,sum(Quantity) from sales_data
group by Product_line order by sum(Quantity) desc;

-- 10. Which gender spends more on average?

select Gender,avg(gross_income) from sales_data group by Gender;