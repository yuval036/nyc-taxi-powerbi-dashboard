# NYC Taxi Data Warehouse & Power BI Dashboard
## Project Overview
This project demonstrates an end-to-end data pipeline for analyzing NYC Taxi data.

The pipeline includes:
- Data ingestion from raw CSV files
- Data cleaning and transformation (ETL)
- Building a Data Warehouse (Stage → Silver → Gold)
- Creating an aggregated dataset for BI
- Developing an interactive Power BI dashboard

---

## Architecture

### Stage Layer
- Raw data loaded using `BULK INSERT`
- No transformations applied

### Silver Layer
- Data cleaning using `TRY_CAST`
- Filtering invalid records:
  - Negative values
  - Invalid timestamps
  - Trips outside 2025
- Creation of `trip_key` for deduplication

### Gold Layer

#### Dimensions:
- `dim_datetime`
- `dim_location`
- `dim_payment_type`
- `dim_rate_code`

#### Fact Table:
- `fact_trip`

#### Aggregation Table:
- `fact_trip_summary`
- Optimized for Power BI performance

---

## ETL Process

Implemented using:
- SQL scripts (`etl_process.sql`)
- Stored procedures:
  - `sp_load_yellow_taxi_month`
  - `sp_load_yellow_taxi_2025_all_months`

### Features:
- Automated monthly data loading
- Error handling using TRY/CATCH
- Logging using `control.etl_loads`

---

## Key Highlights

- End-to-end Data Engineering project
- Real-world dataset (NYC Taxi)
- Data modeling using Star Schema
- ETL automation with stored procedures
- Performance optimization using aggregation tables
- Interactive BI dashboard

## Insights Example

- Manhattan dominates both pickup and dropoff activity
- Majority of payments are done via credit card
- Trip distance and tipping behavior vary by borough

