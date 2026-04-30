------------------------------------------------------------------------------------------------------
-- NYC Taxi Data Warehouse - Create Tables Script
------------------------------------------------------------------------------------------------------

-- Create schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage')
    EXEC('CREATE SCHEMA stage');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
    EXEC('CREATE SCHEMA control');
GO

------------------------------------------------------------------------------------------------------
-- create stage.yellow_tripdata table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('stage.yellow_tripdata', 'U') IS NOT NULL
DROP TABLE stage.yellow_tripdata;
GO

CREATE TABLE stage.yellow_tripdata (
    VendorID NVARCHAR(50),
    tpep_pickup_datetime NVARCHAR(50),
    tpep_dropoff_datetime NVARCHAR(50),
    passenger_count NVARCHAR(50),
    trip_distance NVARCHAR(50),
    RatecodeID NVARCHAR(50),
    store_and_fwd_flag NVARCHAR(50),
    PULocationID NVARCHAR(50),
    DOLocationID NVARCHAR(50),
    payment_type NVARCHAR(50),
    fare_amount NVARCHAR(50),
    extra NVARCHAR(50),
    mta_tax NVARCHAR(50),
    tip_amount NVARCHAR(50),
    tolls_amount NVARCHAR(50),
    improvement_surcharge NVARCHAR(50),
    total_amount NVARCHAR(50),
    congestion_surcharge NVARCHAR(50),
    Airport_fee NVARCHAR(50),
    cbd_congestion_fee NVARCHAR(50)
);

------------------------------------------------------------------------------------------------------
-- create silver.yellow_tripdata_clean table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('silver.yellow_tripdata_clean', 'U') IS NOT NULL
DROP TABLE silver.yellow_tripdata_clean;
GO

CREATE TABLE silver.yellow_tripdata_clean (
    vendor_id INT,
    pickup_datetime DATETIME,
    dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance DECIMAL(8,2),
    rate_code_id INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type INT,
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    cbd_congestion_fee DECIMAL(10,2),
    trip_key NVARCHAR(200)
);

------------------------------------------------------------------------------------------------------
-- create gold.dim_datetime table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_datetime', 'U') IS NOT NULL
DROP TABLE gold.dim_datetime;
GO

CREATE TABLE gold.dim_datetime (
    datetime_id BIGINT PRIMARY KEY,
    full_datetime DATETIME,
    [date] DATE,
    [year] INT,
    quarter INT,
    [month] INT,
    [day] INT,
    hour INT,
    minute INT,
    day_of_week INT,
    is_weekend BIT
);

------------------------------------------------------------------------------------------------------
-- create gold.dim_location  table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_location', 'U') IS NOT NULL
DROP TABLE gold.dim_location;
GO

CREATE TABLE gold.dim_location (
    location_id INT PRIMARY KEY,
    borough NVARCHAR(100),
    zone NVARCHAR(100),
    service_zone NVARCHAR(100)
);

------------------------------------------------------------------------------------------------------
-- create gold.dim_payment_type table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_payment_type', 'U') IS NOT NULL
DROP TABLE gold.dim_payment_type;
GO

CREATE TABLE gold.dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_type_name NVARCHAR(100)
);

------------------------------------------------------------------------------------------------------
-- create gold.dim_rate_code table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_rate_code', 'U') IS NOT NULL
DROP TABLE gold.dim_rate_code;
GO

CREATE TABLE gold.dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_code_name NVARCHAR(100)
);

------------------------------------------------------------------------------------------------------
-- create gold.fact_trip table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.fact_trip', 'U') IS NOT NULL
DROP TABLE gold.fact_trip;
GO

CREATE TABLE gold.fact_trip (
    trip_id INT IDENTITY(1,1) PRIMARY KEY,
    trip_key NVARCHAR(200) NOT NULL UNIQUE,
    pickup_datetime_id BIGINT,
    dropoff_datetime_id BIGINT,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type_id INT,
    rate_code_id INT,
    vendor_id INT,
    passenger_count INT,
    trip_distance DECIMAL(8,2),
    fare_amount DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    cbd_congestion_fee DECIMAL(10,2),
    total_amount DECIMAL(10,2)
);

------------------------------------------------------------------------------------------------------
-- create gold.fact_trip_summary table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('gold.fact_trip_summary', 'U') IS NOT NULL
DROP TABLE gold.fact_trip_summary;
GO

CREATE TABLE gold.fact_trip_summary (
    [year] INT,
    quarter INT,
    [month] INT,
    pickup_borough NVARCHAR(100),
    dropoff_borough NVARCHAR(100),
    payment_type NVARCHAR(100),

    total_trips INT,
    total_revenue DECIMAL(15,2),
    total_tip DECIMAL(15,2),
    total_distance DECIMAL(15,2)
);

------------------------------------------------------------------------------------------------------
-- create control.etl_loads table:
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('control.etl_loads', 'U') IS NOT NULL
DROP TABLE control.etl_loads;
GO

CREATE TABLE control.etl_loads (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    load_year INT,
    load_month INT,
    [status] NVARCHAR(50),
    rows_loaded INT,
    load_date DATETIME,
    source_file_name NVARCHAR(200),
    error_message NVARCHAR(MAX),

    CONSTRAINT CK_etl_load_status
    CHECK ([status] IN ('RUNNING','SUCCESS','FAILED'))
);
