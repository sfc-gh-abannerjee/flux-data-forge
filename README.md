# Flux Data Forge

Synthetic AMI (Advanced Metering Infrastructure) data generation platform for Snowflake demos and POCs. Generates realistic smart meter readings at configurable scale (67K to 350M+ rows) with real-time streaming capabilities.

## Features

- **Batch Generation**: Generate historical AMI datasets (7 days to 1 year)
- **Real-Time Streaming**: Sub-5-second latency using Snowpipe Streaming SDK
- **Multiple Data Flows**:
  - Snowflake Table (scheduled via Tasks)
  - Snowflake Table (real-time streaming)
  - S3 External Stage (medallion architecture)
  - Dual Write (Snowflake + Postgres)
- **Realistic Patterns**: Time-of-day usage curves, customer segments, voltage anomalies, outage signals
- **Scale Presets**: Quick Demo (67K rows) → ML Training (350M rows)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Flux Data Forge (SPCS)                      │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   FastAPI   │───▶│  Generator  │───▶│  Streaming  │         │
│  │   Web UI    │    │   Engine    │    │   Workers   │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
└────────────────────────────┬────────────────────────────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
    ┌────────────┐    ┌────────────┐    ┌────────────┐
    │ Snowflake  │    │ S3 Stage   │    │  Postgres  │
    │   Table    │    │ (Bronze)   │    │  (OLTP)    │
    └────────────┘    └────────────┘    └────────────┘
```

## Prerequisites

- Snowflake account with SPCS enabled
- ACCOUNTADMIN role (or equivalent permissions)
- Docker installed locally (for building images)

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/sfc-gh-abannerjee/flux-data-forge.git
cd flux-data-forge/spcs_app
```

Edit the configuration files:
- `service_spec.yaml` - Update database, schema, and credentials
- `build_and_push.sh` - Update registry URL and image path
- `deploy_spcs.sql` - Update configuration variables

### 2. Build and Push Docker Image

```bash
# Login to Snowflake registry
docker login <YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com

# Build and push
./build_and_push.sh
```

### 3. Deploy to SPCS

Run `deploy_spcs.sql` in Snowflake:

```sql
-- Update configuration variables at the top of the script
-- Then execute the entire script
```

### 4. Access the Application

After deployment, get the service URL:

```sql
SHOW ENDPOINTS IN SERVICE FLUX_DATA_FORGE_SERVICE;
```

Navigate to the `ingress_url` in your browser.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_DATABASE` | Target database | Required |
| `SNOWFLAKE_SCHEMA` | Target schema | Required |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse | Required |
| `SNOWFLAKE_ROLE` | Execution role | SYSADMIN |
| `S3_BUCKET` | S3 bucket for external staging | Optional |
| `AWS_ROLE_ARN` | IAM role for S3 access | Optional |

### Secrets (for Snowflake)

Create these secrets before deployment if using advanced features:

```sql
-- For key-pair authentication (Snowpipe Streaming SDK)
CREATE SECRET streaming_key
    TYPE = GENERIC_STRING
    SECRET_STRING = '<your-private-key>';

-- For Postgres dual-write
CREATE SECRET postgres_credentials
    TYPE = PASSWORD
    USERNAME = 'application'
    PASSWORD = '<your-password>';

-- For S3 external staging
CREATE SECRET aws_credentials
    TYPE = PASSWORD
    USERNAME = '<aws-access-key-id>'
    PASSWORD = '<aws-secret-access-key>';
```

## Use Case Templates

| Template | Meters | Days | Rows | Use Case |
|----------|--------|------|------|----------|
| Quick Demo | 100 | 7 | 67K | Fast demos |
| SE Demo | 1,000 | 90 | 8.6M | Cortex Analyst demos |
| Enterprise POC | 5,000 | 180 | 86M | Enterprise evaluations |
| ML Training | 10,000 | 365 | 350M | ML model training |

## Data Schema

The generated AMI data includes:

| Column | Type | Description |
|--------|------|-------------|
| `METER_ID` | VARCHAR | Unique meter identifier |
| `READING_TIMESTAMP` | TIMESTAMP_NTZ | Reading timestamp |
| `USAGE_KWH` | FLOAT | 15-minute energy usage |
| `VOLTAGE` | FLOAT | Voltage reading |
| `TRANSFORMER_ID` | VARCHAR | Associated transformer |
| `CIRCUIT_ID` | VARCHAR | Associated circuit |
| `SUBSTATION_ID` | VARCHAR | Associated substation |
| `CUSTOMER_SEGMENT` | VARCHAR | RESIDENTIAL/COMMERCIAL/INDUSTRIAL |
| `SERVICE_AREA` | VARCHAR | Geographic service territory |
| `IS_OUTAGE` | BOOLEAN | Outage indicator |
| `DATA_QUALITY` | VARCHAR | VALID/ESTIMATED/OUTAGE |

## File Structure

```
flux-data-forge/
├── README.md
├── spcs_app/
│   ├── fastapi_app.py          # Main FastAPI application
│   ├── snowpipe_streaming_impl.py  # Snowpipe Streaming SDK wrapper
│   ├── Dockerfile              # Container definition
│   ├── requirements.txt        # Python dependencies
│   ├── service_spec.yaml       # SPCS service specification
│   ├── deploy_spcs.sql         # Deployment SQL script
│   └── build_and_push.sh       # Build automation script
└── docs/
    └── (architecture diagrams)
```

## License

Internal Snowflake use only. Contact the Flux team for licensing questions.

## Support

For issues or feature requests, contact the Snowflake SE team or open an issue in this repository.
