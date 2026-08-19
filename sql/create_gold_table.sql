CREATE DATABASE IF NOT EXISTS email_pipeline;

CREATE EXTERNAL TABLE email_pipeline.orders_gold (
  order_id string,
  customer_id string,
  product string,
  category string,
  quantity int,
  unit_price double,
  status string
)
PARTITIONED BY (order_date_clean date)
STORED AS PARQUET
LOCATION 's3://gold-layer-pipline/orders/';

MSCK REPAIR TABLE email_pipeline.orders_gold;
