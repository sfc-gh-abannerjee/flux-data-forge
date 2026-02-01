# Architecture

## System Overview

```mermaid
flowchart LR
    subgraph SPCS["Snowpark Container Services"]
        UI["FastAPI Web UI<br/>Port 8080"]
        GEN["Generator Engine"]
        STREAM["Streaming Workers"]
        UI --> GEN
        GEN --> STREAM
    end

    subgraph SNOW["Snowflake"]
        TBL[("AMI_STREAMING<br/>_READINGS")]
        PIPE["Snowpipe"]
        TASK["Snowflake Task"]
    end

    subgraph EXT["External (Optional)"]
        S3[("S3 Stage<br/>Bronze Zone")]
        PG[("Managed<br/>Postgres")]
    end

    STREAM -->|"Direct Insert"| TBL
    STREAM -->|"Snowpipe SDK"| PIPE --> TBL
    STREAM -->|"Task Schedule"| TASK --> TBL
    STREAM -->|"Raw JSON"| S3 -->|"Auto-ingest"| PIPE
    STREAM -->|"Dual Write"| PG
```

## Data Flow Options

```mermaid
flowchart TB
    subgraph INPUT["Configuration"]
        PRESET["Use Case Preset<br/>Quick Demo → ML Training"]
        FLEET["Fleet Size<br/>100 → 100K meters"]
        PATTERN["Emission Pattern<br/>Uniform / Staggered / Degraded"]
    end

    subgraph FLOWS["Data Flow Selection"]
        F1["Snowflake Table<br/>(Scheduled)"]
        F2["Snowflake Table<br/>(Real-time)"]
        F3["Stage Landing<br/>(Medallion)"]
        F4["Dual Write<br/>(SF + Postgres)"]
    end

    subgraph OUTPUT["Generated Data"]
        AMI["AMI Readings<br/>• METER_ID<br/>• TIMESTAMP<br/>• USAGE_KWH<br/>• VOLTAGE<br/>• SEGMENT"]
    end

    INPUT --> FLOWS
    F1 -->|"~1 min latency"| AMI
    F2 -->|"<5 sec latency"| AMI
    F3 -->|"~10 sec latency"| AMI
    F4 -->|"Full stack"| AMI
```

## Deployment Architecture

```mermaid
flowchart TB
    subgraph LOCAL["Local Development"]
        CODE["Source Code"]
        DOCKER["Docker Build"]
        CODE --> DOCKER
    end

    subgraph REGISTRY["Snowflake Registry"]
        IMG["flux_data_forge:latest"]
    end

    subgraph SNOWFLAKE["Snowflake Account"]
        POOL["Compute Pool<br/>CPU_X64_S"]
        SVC["SPCS Service"]
        REPO["Image Repository"]
        POOL --> SVC
        REPO --> SVC
    end

    subgraph ENDPOINTS["Access"]
        URL["https://...snowflakecomputing.app"]
        OAUTH["Snowflake OAuth"]
    end

    DOCKER -->|"docker push"| IMG
    IMG --> REPO
    SVC --> URL
    URL --> OAUTH
```

## Streaming Architecture (Snowpipe SDK)

```mermaid
sequenceDiagram
    participant UI as Web UI
    participant GEN as Generator
    participant SDK as Snowpipe SDK
    participant PIPE as Streaming PIPE
    participant TBL as Target Table

    UI->>GEN: Start Streaming (1000 meters, 100 rows/sec)
    
    loop Every batch interval
        GEN->>GEN: Generate AMI readings
        GEN->>SDK: append_rows(batch)
        SDK->>PIPE: Stream via channel
        PIPE->>TBL: Insert rows
    end

    UI->>GEN: Stop Streaming
    GEN->>SDK: flush() + close()
    SDK->>PIPE: Final flush
    Note over TBL: Data available in <5 seconds
```

## Medallion Architecture (S3 Flow)

```mermaid
flowchart LR
    subgraph BRONZE["Bronze Layer"]
        S3["S3 External Stage<br/>raw/ami/*.json"]
        PIPE["Snowpipe<br/>Auto-ingest"]
        RAW[("BRONZE_AMI<br/>Raw JSON")]
    end

    subgraph SILVER["Silver Layer"]
        STREAM["Dynamic Table<br/>or Stream + Task"]
        CLEAN[("SILVER_AMI<br/>Cleaned, Typed")]
    end

    subgraph GOLD["Gold Layer"]
        AGG["Aggregation"]
        METRICS[("GOLD_METRICS<br/>Hourly/Daily")]
    end

    S3 -->|"Event notification"| PIPE
    PIPE --> RAW
    RAW --> STREAM --> CLEAN
    CLEAN --> AGG --> METRICS
```

## Data Generation Patterns

```mermaid
flowchart TB
    subgraph PATTERNS["Emission Patterns"]
        P1["UNIFORM<br/>100% meters report<br/>No stagger"]
        P2["STAGGERED<br/>100% meters report<br/>Spread over 15 min"]
        P3["PARTIAL<br/>98% meters report<br/>2% comm failures"]
        P4["DEGRADED<br/>85% meters report<br/>Storm conditions"]
    end

    subgraph SEGMENTS["Customer Segments"]
        RES["RESIDENTIAL<br/>~70% of fleet<br/>1x usage multiplier"]
        COM["COMMERCIAL<br/>~20% of fleet<br/>5x usage multiplier"]
        IND["INDUSTRIAL<br/>~10% of fleet<br/>15x usage multiplier"]
    end

    subgraph TIME["Time-of-Day Curves"]
        PEAK["Peak (2-7 PM)<br/>1.5-3.5 kWh base"]
        MORN["Morning (6-9 AM)<br/>1.0-2.5 kWh base"]
        OFF["Off-Peak<br/>0.3-1.5 kWh base"]
    end

    PATTERNS --> SEGMENTS --> TIME
```
