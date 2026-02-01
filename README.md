# Flux Data Forge

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Synthetic AMI (Advanced Metering Infrastructure) data generation platform for Snowflake demos and POCs. Generates realistic smart meter readings at configurable scale (67K to 350M+ rows) with real-time streaming capabilities.

---

<p align="center">
  <img width="49%" alt="Flux Data Forge - Generation Interface" src="assets/flux_data_forge_generate_1.png" />
  <img width="49%" alt="Flux Data Forge - Data Preview" src="assets/flux_data_forge_generate_2.png" />
</p>

---

> **Quick Deploy**: Run `./scripts/quick_deploy.sh` for guided interactive deployment!

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

### Option A: Use Prebuilt Image (Recommended)

Pull the prebuilt image from GitHub Container Registry:

```bash
# Pull latest image
docker pull ghcr.io/sfc-gh-abannerjee/flux-data-forge:latest

# Tag for your Snowflake registry
docker tag ghcr.io/sfc-gh-abannerjee/flux-data-forge:latest \
  <YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com/<DB>/<SCHEMA>/<REPO>/flux_data_forge:latest

# Login and push to Snowflake
docker login <YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com
docker push <YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com/<DB>/<SCHEMA>/<REPO>/flux_data_forge:latest
```

Then skip to [Step 3: Deploy to SPCS](#3-deploy-to-spcs).

### Option B: Build from Source

#### 1. Clone and Configure

```bash
git clone https://github.com/sfc-gh-abannerjee/flux-data-forge.git
cd flux-data-forge
```

Copy and edit the environment template:
```bash
cp .env.example .env
# Edit .env with your Snowflake configuration
```

Update deployment files in `spcs_app/`:
- `build_and_push.sh` - Set your registry URL
- `deploy_spcs.sql` - Set configuration variables (database, schema, compute pool)

#### 2. Build and Push Docker Image

```bash
cd spcs_app

# Login to Snowflake registry
docker login <YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com

# Build and push
./build_and_push.sh
```

### 3. Deploy to SPCS

Run `deploy_spcs.sql` in Snowflake Worksheets:

```sql
-- 1. First, update the configuration variables at the top
-- 2. Run the PRE-FLIGHT CHECKS section to validate your environment
-- 3. If checks pass, run the remaining sections
```

### 4. Verify Deployment

```sql
-- Check service is running
SELECT SYSTEM$GET_SERVICE_STATUS('FLUX_DATA_FORGE_SERVICE');
-- Should return: {"status":"READY",...}

-- Get the application URL
SHOW ENDPOINTS IN SERVICE FLUX_DATA_FORGE_SERVICE;
-- Copy the ingress_url value
```

### 5. Validate the Application

1. **Open the URL** from step 4 in your browser
2. **Health check**: The UI should load without errors
3. **Test generation**: 
   - Select "Quick Demo" preset (67K rows, ~5 min)
   - Click "Generate"
   - Watch the progress indicator

4. **Verify data landed**:
```sql
SELECT COUNT(*) as ROWS_GENERATED,
       MIN(READING_TIMESTAMP) as FIRST_READING,
       MAX(READING_TIMESTAMP) as LAST_READING,
       COUNT(DISTINCT METER_ID) as UNIQUE_METERS
FROM AMI_STREAMING_READINGS;
```

Expected output for Quick Demo:
```
ROWS_GENERATED | FIRST_READING       | LAST_READING        | UNIQUE_METERS
67,200         | 2026-01-10 00:00:00 | 2026-01-16 23:45:00 | 100
```

## Configuration

### Environment Variables

See [`.env.example`](.env.example) for all available variables.

| Variable | Description | Required |
|----------|-------------|----------|
| `SNOWFLAKE_DATABASE` | Target database | Yes |
| `SNOWFLAKE_SCHEMA` | Target schema | Yes |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse | Yes |
| `SNOWFLAKE_ROLE` | Execution role | No (default: SYSADMIN) |
| `S3_BUCKET` | S3 bucket for external staging | No |
| `AWS_ROLE_ARN` | IAM role for S3 access | No |

### Secrets (for Advanced Features)

Create these Snowflake secrets if using optional features:

```sql
-- For Snowpipe Streaming SDK (key-pair auth)
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

| Template | Meters | Days | Rows | Generation Time | Use Case |
|----------|--------|------|------|-----------------|----------|
| Quick Demo | 100 | 7 | 67K | ~5 min | Fast demos, testing |
| SE Demo | 1,000 | 90 | 8.6M | ~30 min | Cortex Analyst demos |
| Enterprise POC | 5,000 | 180 | 86M | ~3 hours | Enterprise evaluations |
| ML Training | 10,000 | 365 | 350M | ~12 hours | ML model training |

## Data Schema

The generated AMI data includes:

| Column | Type | Description |
|--------|------|-------------|
| `METER_ID` | VARCHAR | Unique meter identifier |
| `READING_TIMESTAMP` | TIMESTAMP_NTZ | Reading timestamp (15-min intervals) |
| `USAGE_KWH` | FLOAT | Energy consumption |
| `VOLTAGE` | FLOAT | Voltage reading |
| `TRANSFORMER_ID` | VARCHAR | Associated transformer |
| `CIRCUIT_ID` | VARCHAR | Associated circuit |
| `SUBSTATION_ID` | VARCHAR | Associated substation |
| `CUSTOMER_SEGMENT` | VARCHAR | RESIDENTIAL / COMMERCIAL / INDUSTRIAL |
| `SERVICE_AREA` | VARCHAR | Geographic service territory |
| `IS_OUTAGE` | BOOLEAN | Outage indicator |
| `DATA_QUALITY` | VARCHAR | VALID / ESTIMATED / OUTAGE |

## File Structure

```
flux-data-forge/
├── README.md               # This file
├── LICENSE                 # Apache 2.0 license
├── SECURITY.md             # Security model and RBAC
├── .env.example            # Environment variable template
├── .gitignore              # Git ignore rules
├── .github/
│   └── workflows/
│       └── ci.yml          # CI/CD pipeline (lint, test, build, deploy)
├── spcs_app/
│   ├── fastapi_app.py      # Main FastAPI application (12K lines)
│   ├── snowpipe_streaming_impl.py  # Snowpipe Streaming SDK wrapper
│   ├── Dockerfile          # Container definition
│   ├── requirements.txt    # Python dependencies
│   ├── service_spec.yaml   # SPCS service specification template
│   ├── deploy_spcs.sql     # Deployment script with pre-flight checks
│   └── build_and_push.sh   # Docker build automation
├── scripts/
│   └── quick_deploy.sh     # Interactive one-click deployment
├── terraform/
│   ├── main.tf             # Infrastructure resources
│   ├── variables.tf        # Input variables
│   ├── outputs.tf          # Output values
│   ├── terraform.tfvars.example  # Example configuration
│   └── README.md           # Terraform usage guide
├── tests/
│   ├── smoke_test.py       # Quick validation tests
│   └── test_unit.py        # Unit tests (pytest)
└── docs/
    ├── ARCHITECTURE.md     # System diagrams (Mermaid)
    ├── SAMPLE_QUERIES.md   # Deployment validation queries
    └── TROUBLESHOOTING.md  # Common issues and solutions
```

## Troubleshooting

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for common issues:

- Service won't start
- Image push fails
- Permission errors
- No data in target table
- S3/Postgres integration issues

## Snowflake Documentation

### Core Technologies

| Technology | Description | Documentation |
|------------|-------------|---------------|
| **Snowpark Container Services** | Run containers in Snowflake | [SPCS Overview](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview) |
| **Snowpipe Streaming** | Real-time data ingestion (<5 sec latency) | [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview) |
| **Image Repository** | Store Docker images in Snowflake | [Image Repository](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository) |
| **Compute Pool** | SPCS compute resources | [Compute Pool](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-compute-pool) |

### Data Pipeline Components

| Component | Use Case | Documentation |
|-----------|----------|---------------|
| **External Stages** | S3/Azure/GCS integration | [External Stages](https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage) |
| **Storage Integration** | Secure cloud storage access | [Storage Integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) |
| **Snowpipe** | Auto-ingest from stages | [Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro) |
| **Tasks** | Scheduled SQL execution | [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro) |
| **Dynamic Tables** | Declarative data pipelines | [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about) |

### Security & Access

| Topic | Documentation |
|-------|---------------|
| **Secrets** | [CREATE SECRET](https://docs.snowflake.com/en/sql-reference/sql/create-secret) |
| **External Access Integration** | [External Network Access](https://docs.snowflake.com/en/developer-guide/external-network-access/external-network-access-overview) |
| **Service Roles** | [SPCS Service Roles](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/additional-considerations-services-jobs#service-roles) |

### Terraform

| Resource | Documentation |
|----------|---------------|
| **Snowflake Provider** | [Terraform Provider](https://registry.terraform.io/providers/Snowflake-Labs/snowflake/latest/docs) |
| **Compute Pool Resource** | [snowflake_compute_pool](https://registry.terraform.io/providers/Snowflake-Labs/snowflake/latest/docs/resources/compute_pool) |

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For issues or feature requests, open an issue in this repository.
