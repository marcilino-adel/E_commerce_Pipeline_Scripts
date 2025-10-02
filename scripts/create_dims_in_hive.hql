-- Drop tables if they exist to ensure the script is rerunnable
DROP TABLE IF EXISTS Dim_Customer;
DROP TABLE IF EXISTS Dim_Products;

-- Create the external table for Dim_Customer
CREATE EXTERNAL TABLE Dim_Customer (
  cust_key            BIGINT,
  customer_id         INT,
  age                 INT,
  gender              STRING,
  city                STRING,
  registration_date   BIGINT,   -- Correct, this comes from Sqoop as a number
  RFM_Score           BIGINT
)
STORED AS PARQUET
LOCATION '/user/warehouse/e-commerce/Dim_Customer';

-- Create the external table for Dim_Products
CREATE EXTERNAL TABLE Dim_Products (
  product_key         BIGINT,
  product_id          INT,
  category            STRING,
  sub_category        STRING,
  product_price       STRING,   -- Correct, this comes from Sqoop as a string
  product_description STRING,
  dw_start_date       DATE,     -- CORRECTED back to DATE
  dw_end_date         DATE      -- CORRECTED back to DATE
)
STORED AS PARQUET
LOCATION '/user/warehouse/e-commerce/Dim_Products';
