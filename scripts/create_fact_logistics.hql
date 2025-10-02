DROP TABLE IF EXISTS Fact_Logistics;

CREATE EXTERNAL TABLE Fact_Logistics (
  order_id                    INT,
  delivery_time_delta_days    INT,
  is_late_delivery            BOOLEAN,
  shipping_cost               STRING,
  warehouse_id                INT
)
STORED AS PARQUET
LOCATION '/user/warehouse/e-commerce/fact_logistics';
