-- FACT TABLE
CREATE TABLE fact_product_views (
  id UUID PRIMARY KEY,
  time_id INT,
  product_id INT,
  store_id INT,
  device_id INT,
  email VARCHAR,
  ip VARCHAR,
  user_agent_id INT,
  view_count INT default 1,
  country_id INT,
  region_id INT,
  city_id INT
);

-- DIMENSIONS
CREATE TABLE dim_time (
  time_id INT PRIMARY KEY,
  date DATE,
  day INT,
  month INT,
  year INT,
  weekday VARCHAR
);

CREATE TABLE dim_product (
  product_id INT PRIMARY KEY
);

CREATE TABLE dim_store (
  store_id INT PRIMARY KEY,
  country VARCHAR
);

CREATE TABLE dim_user_agent (
  user_agent_id INT PRIMARY KEY,
  user_agent VARCHAR,
  browser VARCHAR,
  os VARCHAR
);

CREATE TABLE dim_country (
  country_id INT PRIMARY KEY,
  country_long VARCHAR,
  country_short VARCHAR
);

CREATE TABLE dim_region (
  region_id INT PRIMARY KEY,
  region VARCHAR
);

CREATE TABLE dim_city (
  city_id INT PRIMARY KEY,
  city VARCHAR
);

CREATE TABLE dim_stg_option (
    log_id UUID PRIMARY KEY,
    option_id INT
)

CREATE TABLE dim_option (
    option_id INT PRIMARY KEY,
    option_label VARCHAR
);

CREATE TABLE dim_referrer (
    referrer_id INT PRIMARY KEY,
    referrer_url VARCHAR
)