-- Create a Snowflake stage for loading data from CSV
CREATE OR REPLACE STAGE my_stage
URL = 'STORAGE_AUTO'
FILE_FORMAT = (TYPE = CSV);

-- Load data into staging area
dataset_path = "/home/ec2-user/environment/65fd04910420750aa248497b-001/2.etl_processsql/dataset.csv"
PUT file://{{dataset_path}} @my_stage;

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
    FROM @my_stage/dataset.csv
);
