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

------------------------------------------------------------------------------------------------------
-- 5. Load fact_trip
------------------------------------------------------------------------------------------------------

WITH dedup AS
(
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY trip_key
			ORDER BY pickup_datetime, dropoff_datetime
		) AS rn
	FROM silver.yellow_tripdata_clean
)
INSERT INTO gold.fact_trip
(
	trip_key,
	pickup_datetime_id,
	dropoff_datetime_id,
	pickup_location_id,
	dropoff_location_id,
	payment_type_id,
	rate_code_id,
	vendor_id,
	passenger_count,
	trip_distance,
	fare_amount,
	tip_amount,
	extra,
	mta_tax,
	tolls_amount,
	improvement_surcharge,
	congestion_surcharge,
	airport_fee,
	cbd_congestion_fee,
	total_amount
)
SELECT
	ytc.trip_key,
	ddp.datetime_id,
	ddd.datetime_id,
	dlp.location_id,
	dld.location_id,
	pt.payment_type_id,
	rc.rate_code_id,
	ytc.vendor_id,
	ytc.passenger_count,
	ytc.trip_distance,
	ytc.fare_amount,
	ytc.tip_amount,
	ytc.extra,
	ytc.mta_tax,
	ytc.tolls_amount,
	ytc.improvement_surcharge,
	ytc.congestion_surcharge,
	ytc.airport_fee,
	ytc.cbd_congestion_fee,
	ytc.total_amount
FROM dedup ytc
JOIN gold.dim_datetime ddp
	ON DATEADD(MINUTE, DATEDIFF(MINUTE, 0, ytc.pickup_datetime), 0) = ddp.full_datetime
JOIN gold.dim_datetime ddd
	ON DATEADD(MINUTE, DATEDIFF(MINUTE, 0, ytc.dropoff_datetime), 0) = ddd.full_datetime
JOIN gold.dim_location dlp
	ON ytc.pickup_location_id = dlp.location_id
JOIN gold.dim_location dld
	ON ytc.dropoff_location_id = dld.location_id
JOIN gold.dim_payment_type pt
	ON ytc.payment_type = pt.payment_type_id
JOIN gold.dim_rate_code rc
	ON ytc.rate_code_id = rc.rate_code_id
WHERE ytc.rn = 1
AND NOT EXISTS (
	SELECT 1
	FROM gold.fact_trip f
	WHERE f.trip_key = ytc.trip_key
);

------------------------------------------------------------------------------------------------------
-- 6. Build Power BI summary table
------------------------------------------------------------------------------------------------------

TRUNCATE TABLE gold.fact_trip_summary;

INSERT INTO gold.fact_trip_summary (
    [year],
    [quarter],
    [month],
    pickup_borough,
    dropoff_borough,
    payment_type,
    total_trips,
    total_revenue,
    total_tip,
    total_distance
)
SELECT
    d.[year],
    d.[quarter],
    d.[month],
    pl.borough AS pickup_borough,
    dl.borough AS dropoff_borough,
    pt.payment_type_name AS payment_type,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.tip_amount) AS total_tip,
    SUM(f.trip_distance) AS total_distance
FROM gold.fact_trip f
JOIN gold.dim_datetime d
    ON f.pickup_datetime_id = d.datetime_id
JOIN gold.dim_location pl
    ON f.pickup_location_id = pl.location_id
JOIN gold.dim_location dl
    ON f.dropoff_location_id = dl.location_id
JOIN gold.dim_payment_type pt
    ON f.payment_type_id = pt.payment_type_id
GROUP BY
    d.[year],
    d.[quarter],
    d.[month],
    pl.borough,
    dl.borough,
    pt.payment_type_name;
