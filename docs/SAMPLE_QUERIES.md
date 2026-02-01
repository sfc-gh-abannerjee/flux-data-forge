# Sample Queries

Validation queries to confirm your Flux Data Forge deployment is working correctly.

## Deployment Validation

### 1. Verify Data Landed

```sql
-- Check that data exists and matches expected counts
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT METER_ID) as unique_meters,
    MIN(READING_TIMESTAMP) as first_reading,
    MAX(READING_TIMESTAMP) as last_reading,
    DATEDIFF('day', MIN(READING_TIMESTAMP), MAX(READING_TIMESTAMP)) + 1 as days_covered
FROM AMI_STREAMING_READINGS;
```

**Expected results by preset:**

| Preset | Rows | Meters | Days |
|--------|------|--------|------|
| Quick Demo | ~67K | 100 | 7 |
| SE Demo | ~8.6M | 1,000 | 90 |
| Enterprise POC | ~86M | 5,000 | 180 |
| ML Training | ~350M | 10,000 | 365 |

### 2. Check Data Quality

```sql
-- Verify no NULL values in required fields
SELECT 
    COUNT_IF(METER_ID IS NULL) as null_meter_ids,
    COUNT_IF(READING_TIMESTAMP IS NULL) as null_timestamps,
    COUNT_IF(USAGE_KWH IS NULL) as null_usage,
    COUNT_IF(CUSTOMER_SEGMENT IS NULL) as null_segments
FROM AMI_STREAMING_READINGS;
-- All counts should be 0
```

### 3. Validate Customer Segments

```sql
-- Check segment distribution (should be ~70/20/10 split)
SELECT 
    CUSTOMER_SEGMENT,
    COUNT(DISTINCT METER_ID) as meter_count,
    ROUND(COUNT(DISTINCT METER_ID) * 100.0 / SUM(COUNT(DISTINCT METER_ID)) OVER(), 1) as pct
FROM AMI_STREAMING_READINGS
GROUP BY CUSTOMER_SEGMENT
ORDER BY meter_count DESC;
```

**Expected distribution:**
- RESIDENTIAL: ~70%
- COMMERCIAL: ~20%
- INDUSTRIAL: ~10%

### 4. Verify Time Intervals

```sql
-- Confirm 15-minute reading intervals
SELECT 
    MINUTE(READING_TIMESTAMP) as minute_value,
    COUNT(*) as reading_count
FROM AMI_STREAMING_READINGS
GROUP BY minute_value
ORDER BY minute_value;
-- Should only see 0, 15, 30, 45
```

### 5. Check Usage Ranges

```sql
-- Verify usage values are realistic by segment
SELECT 
    CUSTOMER_SEGMENT,
    ROUND(MIN(USAGE_KWH), 2) as min_kwh,
    ROUND(AVG(USAGE_KWH), 2) as avg_kwh,
    ROUND(MAX(USAGE_KWH), 2) as max_kwh,
    ROUND(STDDEV(USAGE_KWH), 2) as stddev_kwh
FROM AMI_STREAMING_READINGS
GROUP BY CUSTOMER_SEGMENT;
```

**Expected ranges:**
- RESIDENTIAL: 0.1 - 5 kWh (avg ~1.5)
- COMMERCIAL: 0.5 - 25 kWh (avg ~7)
- INDUSTRIAL: 2 - 75 kWh (avg ~22)

### 6. Validate Voltage Readings

```sql
-- Check voltage distribution (should center around 120V)
SELECT 
    ROUND(VOLTAGE, 0) as voltage_bucket,
    COUNT(*) as reading_count
FROM AMI_STREAMING_READINGS
WHERE VOLTAGE IS NOT NULL
GROUP BY voltage_bucket
ORDER BY reading_count DESC
LIMIT 10;
-- Most readings should be 118-122V
```

### 7. Check Outage Signals

```sql
-- Verify outage flag distribution
SELECT 
    IS_OUTAGE,
    DATA_QUALITY,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
FROM AMI_STREAMING_READINGS
GROUP BY IS_OUTAGE, DATA_QUALITY
ORDER BY count DESC;
-- Outages should be <5% of total readings
```

---

## Infrastructure Validation

### Check Service Status

```sql
-- Verify SPCS service is running
SELECT SYSTEM$GET_SERVICE_STATUS('FLUX_DATA_FORGE_SERVICE');
-- Should return: {"status":"READY",...}
```

### View Service Logs

```sql
-- Get recent application logs
CALL SYSTEM$GET_SERVICE_LOGS('FLUX_DATA_FORGE_SERVICE', '0', 'flux-data-forge', 100);
```

### Check Compute Pool

```sql
-- Verify compute pool is active
SHOW COMPUTE POOLS LIKE 'FLUX%';

-- Get detailed status
DESCRIBE COMPUTE POOL <your_pool_name>;
```

### Verify Image Repository

```sql
-- List images in repository
SHOW IMAGES IN IMAGE REPOSITORY FLUX_DATA_FORGE_REPO;
```

---

## Streaming Validation (Real-time Mode)

If using Snowpipe Streaming SDK:

### Check Recent Inserts

```sql
-- Verify streaming is active (run during streaming)
SELECT 
    DATE_TRUNC('minute', READING_TIMESTAMP) as minute,
    COUNT(*) as rows_inserted
FROM AMI_STREAMING_READINGS
WHERE READING_TIMESTAMP > DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY minute
ORDER BY minute DESC
LIMIT 10;
```

### Measure Streaming Latency

```sql
-- Compare generation time vs current time
SELECT 
    MAX(READING_TIMESTAMP) as latest_reading,
    CURRENT_TIMESTAMP() as current_time,
    DATEDIFF('second', MAX(READING_TIMESTAMP), CURRENT_TIMESTAMP()) as latency_seconds
FROM AMI_STREAMING_READINGS;
-- Latency should be <5 seconds during active streaming
```

---

## Cleanup

### Drop Test Data

```sql
-- Remove all generated data (use with caution!)
TRUNCATE TABLE AMI_STREAMING_READINGS;
```

### Stop Service

```sql
-- Suspend the service when not in use
ALTER SERVICE FLUX_DATA_FORGE_SERVICE SUSPEND;

-- Resume when needed
ALTER SERVICE FLUX_DATA_FORGE_SERVICE RESUME;
```

### Full Cleanup

```sql
-- Remove service entirely
DROP SERVICE IF EXISTS FLUX_DATA_FORGE_SERVICE;

-- Remove compute pool (if dedicated)
DROP COMPUTE POOL IF EXISTS FLUX_COMPUTE_POOL;
```
