-- Create a Snowflake stage for loading data from CSV
CREATE OR REPLACE STAGE my_stage
URL = 's3://your-bucket/path/to/csv/files'
CREDENTIALS = (AWS_KEY_ID = 'your_aws_key_id' AWS_SECRET_KEY = 'your_aws_secret_key')
FILE_FORMAT = (TYPE = CSV);

-- Load data into staging area
COPY INTO my_table
FROM @my_stage/dataset.csv
FILE_FORMAT = (TYPE = CSV)
ON_ERROR = CONTINUE; -- Handle errors as per your requirement


-- Create a target table to store the data
CREATE OR REPLACE TABLE target_table (
    customer_id INT,
    transaction_amount INT
);

-- Load data into the target table
INSERT INTO target_table (customer_id, transaction_amount)
SELECT PARSE_JSON(column1).customer_id AS customer_id,
       PARSE_JSON(column1).transaction_amount AS transaction_amount
FROM (
    SELECT FLATTEN(INPUT => PARSE_JSON(column1)) AS column1
    FROM my_table
);


