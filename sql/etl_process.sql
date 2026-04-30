------------------------------------------------------------------------------------------------------
-- NYC Taxi Data Warehouse - ETL Process Script
------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------
-- 1. Load CSV file into stage.yellow_tripdata
------------------------------------------------------------------------------------------------------

TRUNCATE TABLE stage.yellow_tripdata;

BULK INSERT stage.yellow_tripdata
FROM 'C:\Users\yuval\OneDrive\שולחן העבודה\nyc_taxi_pipeline\data\bronze\yellow_csv\yellow_tripdata_2025-01.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

------------------------------------------------------------------------------------------------------
-- 2. Load and clean data into silver.yellow_tripdata_clean
------------------------------------------------------------------------------------------------------

TRUNCATE TABLE silver.yellow_tripdata_clean;

INSERT INTO silver.yellow_tripdata_clean (
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    trip_key
)
SELECT 
    TRY_CAST(VendorID AS INT),
    TRY_CAST(tpep_pickup_datetime AS DATETIME),
    TRY_CAST(tpep_dropoff_datetime AS DATETIME),
    TRY_CAST(TRY_CAST(passenger_count AS FLOAT) AS INT),
    TRY_CAST(trip_distance AS DECIMAL(8,2)),
    TRY_CAST(TRY_CAST(RatecodeID AS FLOAT) AS INT),
    store_and_fwd_flag,
    TRY_CAST(PULocationID AS INT),
    TRY_CAST(DOLocationID AS INT),
    TRY_CAST(payment_type AS INT),
    TRY_CAST(fare_amount AS DECIMAL(10,2)),
    TRY_CAST(extra AS DECIMAL(10,2)),
    TRY_CAST(mta_tax AS DECIMAL(10,2)),
    TRY_CAST(tip_amount AS DECIMAL(10,2)),
    TRY_CAST(tolls_amount AS DECIMAL(10,2)),
    TRY_CAST(improvement_surcharge AS DECIMAL(10,2)),
    TRY_CAST(total_amount AS DECIMAL(10,2)),
    TRY_CAST(congestion_surcharge AS DECIMAL(10,2)),
    TRY_CAST(Airport_fee AS DECIMAL(10,2)),
    TRY_CAST(cbd_congestion_fee AS DECIMAL(10,2)),
    CONCAT(
        tpep_pickup_datetime, '|',
        tpep_dropoff_datetime, '|',
        PULocationID, '|',
        DOLocationID, '|',
        VendorID, '|',
        trip_distance, '|',
        total_amount
    )
FROM stage.yellow_tripdata
WHERE TRY_CAST(tpep_pickup_datetime AS DATETIME) IS NOT NULL
  AND TRY_CAST(tpep_dropoff_datetime AS DATETIME) IS NOT NULL
  AND TRY_CAST(trip_distance AS DECIMAL(8,2)) >= 0
  AND TRY_CAST(total_amount AS DECIMAL(10,2)) >= 0
  AND TRY_CAST(tpep_pickup_datetime AS DATETIME) < TRY_CAST(tpep_dropoff_datetime AS DATETIME)
  AND YEAR(TRY_CAST(tpep_pickup_datetime AS DATETIME)) = 2025;

------------------------------------------------------------------------------------------------------
-- 3. Load CSV file into dim_location 
------------------------------------------------------------------------------------------------------

BULK INSERT gold.dim_location
FROM 'C:\Users\yuval\OneDrive\שולחן העבודה\nyc_taxi_pipeline\taxi_zone_lookup.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

------------------------------------------------------------------------------------------------------
-- 4. Load dim_datetime
------------------------------------------------------------------------------------------------------

INSERT INTO gold.dim_datetime (
    datetime_id,
    full_datetime,
    [date],
    [year],
    [quarter],
    [month],
    [day],
    [hour],
    [minute],
    day_of_week,
    is_weekend
)
SELECT DISTINCT
    CAST(FORMAT(dt, 'yyyyMMddHHmm') AS BIGINT),
    dt,
    CAST(dt AS DATE),
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DAY(dt),
    DATEPART(HOUR, dt),
    DATEPART(MINUTE, dt),
    DATEPART(WEEKDAY, dt),
    CASE
        WHEN DATEPART(WEEKDAY, dt) IN (1,7) THEN 1
        ELSE 0
    END
FROM (
    SELECT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, pickup_datetime), 0) AS dt
    FROM silver.yellow_tripdata_clean
    WHERE pickup_datetime IS NOT NULL

    UNION

    SELECT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, dropoff_datetime), 0) AS dt
    FROM silver.yellow_tripdata_clean
    WHERE dropoff_datetime IS NOT NULL
) t
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.dim_datetime d
    WHERE d.datetime_id = CAST(FORMAT(t.dt, 'yyyyMMddHHmm') AS BIGINT)
);




