# NYC Taxi Analytics Project with Microsoft Fabric

## Overview

This project demonstrates an end-to-end analytics solution using Microsoft Fabric to process, transform, analyze, and interact with NYC Taxi trip data.

The solution includes:

* Data ingestion using Fabric Pipelines
* Staging and presentation data layers
* Data quality and metadata logging
* Data transformation using Dataflow Gen2
* Reporting through Semantic Models
* Conversational analytics using a Fabric Data Agent

---

## Architecture

```text
Parquet Files + Lookup CSV
            │
            ▼
      Fabric Lakehouse
            │
            ▼
      Staging Pipelines
            │
            ▼
      Data Warehouse
            │
            ▼
      Dataflow Gen2
            │
            ▼
      Presentation Layer
            │
            ▼
   Semantic Model & Reports
            │
            ▼
       NYC TaxiBot Agent
```

---

# Prerequisites

* Microsoft Fabric Capacity
* Fabric Workspace access
* NYC Taxi Parquet files
* NYC Taxi Zone Lookup CSV file
* Data Warehouse permissions
* Data Agent capability enabled

---

# Step 1: Create Workspace and Storage

## Workspace

Create a workspace:

```text
NYCTaxiProject
```

Verify your access to the workspace.

---

## Lakehouse

Create a Lakehouse:

```text
NYCLakehouse
```

Create the following folders:

```text
Files/
├── nyctaxi_yellow/
└── nyctaxi_lookup_zones/
```

Upload:

* Yellow Taxi Parquet files into `nyctaxi_yellow`
* Taxi Zone Lookup CSV into `nyctaxi_lookup_zones`

---

## Warehouse

Create a Warehouse:

```text
NYCWarehouse
```

---

# Step 2: Create Staging Pipeline for Lookup Data

Create a workspace folder:

```text
NYC Taxi Data Pipelines
```

Create pipeline:

```text
pl_stg_lookup
```

### Copy Activity Configuration

| Setting           | Value             |
| ----------------- | ----------------- |
| Activity Name     | Copy Lookup Table |
| Source            | NYCLakehouse      |
| Root Folder       | Files             |
| File Type         | DelimitedText     |
| Destination       | NYCWarehouse      |
| Auto Create Table | Enabled           |
| Schema            | stg               |
| Table             | taxi_zone_lookup  |

Run and validate the pipeline.

---

# Step 3: Create Taxi Staging Pipeline

Create pipeline:

```text
pl_stg_process_nyctaxi
```

### Copy Activity

| Setting       | Value           |
| ------------- | --------------- |
| Activity Name | Copy to Staging |
| Source        | NYCLakehouse    |
| Format        | Parquet         |
| Destination   | NYCWarehouse    |
| Schema        | stg             |
| Table         | nyctaxi_yellow  |

### Dynamic File Name

Variable:

```text
v_date
```

Expression:

```text
@concat('yellow_tripdata_',variables('v_date'),'.parquet')
```

Example value:

```text
2025-01
```

Validate data load by checking:

```sql
SELECT
MIN(tpep_pickup_datetime),
MAX(tpep_pickup_datetime)
FROM stg.nyctaxi_yellow;
```

---

# Step 4: Data Quality Cleanup

Create stored procedure:

```sql
CREATE OR ALTER PROCEDURE stg.data_cleaning_stg
    @end_date DATETIME2,
    @start_date DATETIME2
AS

DELETE FROM stg.nyctaxi_yellow
WHERE tpep_pickup_datetime < @start_date
   OR tpep_pickup_datetime > @end_date;
```

Pipeline parameters:

```text
end_date   : @variables('v_end_date')
start_date : @concat(variables('v_date'),'-01')
```

Create variable:

```text
v_end_date
```

Expression:

```text
@addToTime(concat(variables('v_date'),'-01'),1,'Month')
```

---

# Step 5: Staging Metadata Logging

Create stored procedure:

```sql
CREATE OR ALTER PROCEDURE metadata.insert_staging_metadata
(
    @pipeline_run_id VARCHAR(255),
    @table_name VARCHAR(255),
    @processed_date DATETIME
)
AS
BEGIN

INSERT INTO metadata.processing_log
(
    pipeline_run_id,
    table_processed,
    rows_processed,
    latest_processed_pickup,
    processed_datetime
)
SELECT
    @pipeline_run_id,
    @table_name,
    COUNT(*),
    MAX(tpep_pickup_datetime),
    @processed_date
FROM stg.nyctaxi_yellow;

END
```

Pass parameters:

```text
@pipeline().RunId
@pipeline().TriggerTime
staging_nyctaxi_yellow
```

---

# Step 6: Incremental Processing Logic

Script Activity:

```sql
SELECT TOP 1
    latest_processed_pickup
FROM metadata.processing_log
WHERE table_processed='staging_nyctaxi_yellow'
ORDER BY latest_processed_pickup DESC;
```

Update variable:

```text
v_date
```

Expression:

```text
@formatDateTime(
addToTime(
activity('Latest Processed Data').output.resultSets[0].rows[0].latest_processed_pickup,
1,
'Month'),
'yyyy-MM')
```

---

# Step 7: Create Presentation Table

```sql
CREATE TABLE dbo.nyctaxi_yellow
(
    vendor VARCHAR(50),
    tpep_pickup_datetime DATE,
    tpep_dropoff_datetime DATE,
    pu_borough VARCHAR(100),
    pu_zone VARCHAR(100),
    do_borough VARCHAR(100),
    do_zone VARCHAR(100),
    payment_method VARCHAR(50),
    passenger_count INT,
    trip_distance FLOAT,
    total_amount FLOAT
);
```

---

# Step 8: Dataflow Gen2 Transformation

Create Dataflow:

```text
df_pres_processing_nyctaxi
```

Source:

```text
stg.nyctaxi_yellow
```

## Transformations

### Remove Columns

Remove:

* RatecodeID
* store_and_fwd_flag
* fare_amount
* improvement_surcharge
* congestion_surcharge
* airport_fee

### Vendor Mapping

Create a conditional column:

```text
vendor
```

Replace:

```text
VendorID
```

### Payment Method Mapping

Create a conditional column:

```text
payment_method
```

Replace:

```text
payment_type
```

### Rename Columns

```text
PULocationID → pu_location_id
DOLocationID → do_location_id
```

### Convert DateTime to Date

Convert:

```text
tpep_pickup_datetime
tpep_dropoff_datetime
```

to Date only.

### Lookup Enrichment

Merge with:

```text
stg.taxi_zone_lookup
```

For:

* Pickup Location
* Dropoff Location

Populate:

```text
pu_borough
pu_zone
do_borough
do_zone
```

Remove:

```text
pu_location_id
do_location_id
cbd_congestion_fee
```

### Destination

Append to:

```text
dbo.nyctaxi_yellow
```

---

# Step 9: Presentation Pipeline

Create pipeline:

```text
pl_pres_processing
```

Add Dataflow activity:

```text
Process to Presentation
```

Invoke:

```text
df_pres_processing_nyctaxi
```

---

# Step 10: Presentation Metadata Logging

```sql
CREATE OR ALTER PROCEDURE metadata.insert_presentation_metadata
(
    @pipeline_run_id VARCHAR(255),
    @table_name VARCHAR(255),
    @processed_date DATETIME
)
AS
BEGIN

INSERT INTO metadata.processing_log
(
    pipeline_run_id,
    table_processed,
    rows_processed,
    latest_processed_pickup,
    processed_datetime
)
SELECT
    @pipeline_run_id,
    @table_name,
    COUNT(*),
    MAX(tpep_pickup_datetime),
    @processed_date
FROM dbo.nyctaxi_yellow;

END
```

Parameters:

```text
@pipeline().RunId
@pipeline().TriggerTime
presentation_nyctaxi_yellow
```

---

# Step 11: Orchestration Pipeline

Create pipeline:

```text
pl_orchestrate_nyctaxi
```

Pipeline sequence:

```text
Staging Pipeline
        ↓
Presentation Pipeline
```

---

# Step 12: Reporting

Create a Semantic Model from:

```text
dbo.nyctaxi_yellow
```

Build Power BI reports and dashboards.

Suggested reports:

* Revenue Analysis
* Borough Performance
* Trip Distance Analysis
* Payment Method Trends
* Peak Hour Usage
* Route Optimization

---

# Step 13: NYC TaxiBot Data Agent

Create a Fabric Data Agent named:

```text
NYC TaxiBot
```

## Purpose

Provide transportation analytics and operational insights from NYC Taxi trip data.

## Core Capabilities

* Descriptive analytics
* Trend analysis
* Route optimization
* Revenue analysis
* Data quality monitoring
* Borough-level insights
* Time-series analysis

## Key Metrics

* Average trip distance
* Revenue by route
* Revenue per mile
* Passenger behavior
* Peak usage periods
* Payment preferences
* Geographic patterns

## Confidence Framework

| Level  | Threshold |
| ------ | --------- |
| High   | >90%      |
| Medium | 70–90%    |
| Low    | <70%      |

## Visualization Recommendations

* Line Charts
* Bar Charts
* Histograms
* Scatter Plots
* Heat Maps
* Sankey Diagrams

---

# Project Deliverables

✅ Lakehouse

✅ Warehouse

✅ Staging Pipelines

✅ Data Cleaning

✅ Metadata Logging

✅ Incremental Processing

✅ Dataflow Gen2

✅ Presentation Layer

✅ Semantic Model

✅ Power BI Reports

✅ NYC TaxiBot Agent

---

# Future Enhancements

* Real-time streaming ingestion
* Predictive demand forecasting
* Weather integration
* Route optimization recommendations
* Dynamic pricing analysis
* Multi-agent analytics architecture

```
```
