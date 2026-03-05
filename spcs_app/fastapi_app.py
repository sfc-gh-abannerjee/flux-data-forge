"""
FLUX Data Forge - SPCS FastAPI Version
Real-Time AMI Streaming Application for Snowpark Container Services

Uses FastAPI instead of Streamlit for reliable SPCS OAuth token handling.
Enhanced with full Snowpipe Streaming SDK volume configuration.
"""

import os
import logging
import html
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Optional
from contextlib import asynccontextmanager
import json
import io
import math

# Import centralized configuration
from config import DB, SCHEMA_PRODUCTION, SCHEMA_APPLICATIONS, get_table_path

# Import SQL sanitization utilities
from utils.sanitize import validate_identifier, sanitize_sql_value

# Import extracted data generation engine (Step 3A: module extraction)
from data_generators import (
    EMISSION_PATTERNS,
    EVENT_FREQUENCIES,
    GridTopologyState,
    DATA_FORMATS,
    UTILITY_PROFILES,
    SNOWPIPE_SDK_LIMITS,
    generate_ami_reading,
    generate_itron_grid_planning_row,
    generate_symphony_iris_row,
    generate_carto_spatial_row,
    generate_siemens_edge_row,
)

from fastapi import FastAPI, Request, Form, HTTPException, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
import base64
import uvicorn

import snowflake.connector
from snowflake.snowpark import Session
import threading
import time
import random
import uuid

# AWS S3 for raw JSON streaming
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("boto3 not available - Raw JSON S3 streaming disabled")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

snowflake_session: Optional[Session] = None

# Active streaming jobs (for Snowpipe Streaming)
active_streaming_jobs = {}  # job_id -> {thread, status, config, stats}
streaming_lock = threading.Lock()

# PATTERN: Dependency cache for background preloading
# Loads tables, pipes, stages on app startup to improve UX
dependency_cache = {
    'tables': None,       # Cached bronze tables
    'pipes': None,        # Cached Snowpipes from all schemas  
    'stages': None,       # Cached stages (internal + external)
    'databases': None,    # Cached database list
    'last_refresh': None, # Timestamp of last cache refresh
    'lock': threading.Lock()
}

USE_CASE_TEMPLATES = {
    'Quick Demo': {'meters': 100, 'days': 7, 'interval_minutes': 15, 'estimated_rows': '67K',
                   'description': 'Fast 5-minute generation for quick demos', 'icon': 'rocket_launch'},
    'SE Demo': {'meters': 1000, 'days': 90, 'interval_minutes': 15, 'estimated_rows': '8.6M',
                'description': 'Standard size for Cortex Analyst demos', 'icon': 'target'},
    'Enterprise POC': {'meters': 5000, 'days': 180, 'interval_minutes': 15, 'estimated_rows': '86M',
                       'description': '6-month history for enterprise POCs', 'icon': 'business'},
    'ML Training': {'meters': 10000, 'days': 365, 'interval_minutes': 15, 'estimated_rows': '350M',
                    'description': 'Full year dataset for ML model training', 'icon': 'model_training'},
}

FLEET_PRESETS = {
    'Demo (1K)': {'meters': 1000, 'desc': '~67 readings/min'},
    'Substation (10K)': {'meters': 10000, 'desc': '~667 readings/min'},
    'District (50K)': {'meters': 50000, 'desc': '~3.3K readings/min'},
    'Service Area (100K)': {'meters': 100000, 'desc': '~6.7K readings/min'},
}

# Production table sources for real meter data
PRODUCTION_DATA_SOURCES = {
    'METER_INFRASTRUCTURE': {
        'name': 'Meter Infrastructure',
        'table': f'{DB}.{SCHEMA_PRODUCTION}.METER_INFRASTRUCTURE',
        'meter_col': 'METER_ID',
        'transformer_col': 'TRANSFORMER_ID',
        'circuit_col': 'CIRCUIT_ID',
        'segment_col': 'CUSTOMER_SEGMENT_ID',
        'lat_col': 'METER_LATITUDE',
        'lon_col': 'METER_LONGITUDE',
        'substation_col': 'SUBSTATION_ID',
        'desc': '596K meters - Real CenterPoint infrastructure',
        'count': 596906,
    },
    'AMI_METADATA_SEARCH': {
        'name': 'AMI Metadata',
        'table': f'{DB}.{SCHEMA_PRODUCTION}.AMI_METADATA_SEARCH',
        'meter_col': 'METER_ID',
        'transformer_col': 'TRANSFORMER_ID',
        'circuit_col': 'CIRCUIT_ID',
        'segment_col': 'CUSTOMER_SEGMENT',
        'lat_col': 'LATITUDE',
        'lon_col': 'LONGITUDE',
        'substation_col': 'SUBSTATION_ID',
        'desc': '596K meters - Searchable AMI metadata',
        'count': 596906,
    },
    'SYNTHETIC': {
        'name': 'Synthetic (No Production Data)',
        'table': None,
        'desc': 'Generate synthetic meter IDs (for demos without production data)',
        'count': 0,
    },
}

# EMISSION_PATTERNS, EVENT_FREQUENCIES — imported from data_generators


# GridTopologyState — imported from data_generators


# Unified Data Flow options - combines mechanism + destination into logical pipelines
# Simplified to 4 clear options that make sense to users
DATA_FLOWS = {
    'snowflake_task': {
        'name': 'Snowflake Table (Scheduled)',
        'latency': '~1 min',
        'color': '#64748b',
        'desc': 'Snowflake Task generates data every minute. Simple setup, great for batch demos.',
        'icon': 'schedule',
        'mechanism': 'task',
        'dest': 'snowflake',
    },
    'streaming_insert': {
        'name': 'SQL Insert (Real-time)',
        'latency': '< 1 sec',
        'color': '#22c55e',
        'desc': 'Direct SQL INSERT via Snowpark. No PIPE objects or key-pair auth needed.',
        'icon': 'bolt',
        'mechanism': 'snowpipe_classic',
        'dest': 'snowflake',
        'throughput': '~25 MB/s (XL warehouse)',
        'billing': 'Warehouse credits',
        'badge': 'Zero Setup',
    },
    'streaming_hp': {
        'name': 'Snowpipe Streaming (HP SDK)',
        'latency': '5-10 sec',
        'color': '#f59e0b',
        'desc': 'Native HP SDK with server-side validation, in-flight transforms, and offset tracking.',
        'icon': 'rocket_launch',
        'mechanism': 'snowpipe_hp',
        'dest': 'snowflake',
        'throughput': 'Up to 10 GB/s per table',
        'billing': 'Throughput-based',
        'badge': 'Production Grade',
    },
    'stage_landing': {
        'name': 'Stage Landing (Raw JSON)',
        'latency': '~5-10 sec',
        'color': '#0ea5e9',
        'desc': 'Complete medallion architecture: External Stage → Snowpipe auto-ingest → Bronze table. Configure Storage Integration, Stage, and Pipe objects.',
        'icon': 'folder_open',
        'mechanism': 'stage_json',
        'dest': 'stage',
    },
    'dual_write': {
        'name': 'Dual Write (SF + Postgres)',
        'latency': 'Full stack',
        'color': '#a855f7',
        'desc': 'Simultaneous write to Snowflake + Postgres. Analytics + real-time operational database.',
        'icon': 'sync_alt',
        'mechanism': 'snowpipe_classic',
        'dest': 'dual',
    },
}

# Legacy mappings for backward compatibility
STREAMING_MECHANISMS = {
    'task': DATA_FLOWS['snowflake_task'],
    'snowpipe_classic': DATA_FLOWS['streaming_insert'],
    'snowpipe_hp': DATA_FLOWS['streaming_hp'],
    'stage_json': DATA_FLOWS['stage_landing'],
}

TARGET_DESTINATIONS = {
    'snowflake': {'name': 'Snowflake', 'color': '#38bdf8'},
    'postgres': {'name': 'Managed Postgres', 'color': '#10b981'},
    'dual': {'name': 'Dual (Both)', 'color': '#a855f7'},
    'stage': {'name': 'Stage (Landing Zone)', 'color': '#0ea5e9'},
}

# Streaming Architecture Advisor — maps source systems to recommended ingestion paths
STREAMING_ADVISOR = {
    'AMI Head-End': {
        'icon': 'electric_meter',
        'sources': ['Itron, Landis+Gyr, Honeywell', 'Interval reads (15-min)', '100K-1M+ meters'],
        'recommended': 'streaming_hp',
        'alt': 'streaming_insert',
        'rationale': 'High-volume, steady-state meter reads benefit from HP SDK offset tracking and throughput-based billing. SQL INSERT works for smaller fleets (<10K meters).',
    },
    'SCADA / OT': {
        'icon': 'sensors',
        'sources': ['OSIsoft PI, Aveva, ABB', 'Sub-second telemetry', 'Voltage, frequency, power factor'],
        'recommended': 'streaming_hp',
        'alt': 'dual_write',
        'rationale': 'SCADA feeds require sustained high throughput with low latency. HP SDK handles burst traffic well. Dual Write adds Postgres for operational dashboards needing <20ms reads.',
    },
    'Kafka / Event Streaming': {
        'icon': 'swap_horiz',
        'sources': ['Confluent, MSK, Redpanda', 'Event-driven architecture', 'Multi-consumer topics'],
        'recommended': 'streaming_hp',
        'alt': 'streaming_insert',
        'rationale': 'Openflow natively supports Kafka connectors (zero-code). For custom transforms or consumer logic, HP SDK gives full control. SQL INSERT for low-volume topics.',
        'openflow_note': True,
    },
    'Cloud Events': {
        'icon': 'cloud_queue',
        'sources': ['Azure EventHub, GCP Pub/Sub', 'IoT Hub, Event Grid', 'Serverless triggers'],
        'recommended': 'streaming_hp',
        'alt': 'streaming_insert',
        'rationale': 'Openflow supports EventHub (ConsumeAzureEventHub) and PubSub (ConsumeGCPubSub) natively. HP SDK for custom parsing or when Openflow is unavailable.',
        'openflow_note': True,
    },
    'Batch / File Drops': {
        'icon': 'folder_open',
        'sources': ['S3, Azure Blob, GCS', 'CSV, Parquet, JSON', 'Scheduled ETL outputs'],
        'recommended': 'stage_landing',
        'alt': 'snowflake_task',
        'rationale': 'Stage Landing with auto-ingest Snowpipe is ideal for file-based data. Snowflake Task for periodic SQL-based generation or transformation.',
    },
    'Database CDC': {
        'icon': 'storage',
        'sources': ['MySQL, PostgreSQL, Oracle', 'Change Data Capture', 'Debezium, AWS DMS'],
        'recommended': 'streaming_hp',
        'alt': 'stage_landing',
        'rationale': 'Openflow natively supports MySQL, PostgreSQL, SQL Server, and Oracle CDC. HP SDK for custom CDC pipelines. Stage Landing for DMS file outputs.',
        'openflow_note': True,
    },
}

# DATA_FORMATS, UTILITY_PROFILES, SNOWPIPE_SDK_LIMITS — imported from data_generators

FLUX_LOGO_BASE64 = "iVBORw0KGgoAAAANSUhEUgAAAFAAAABQCAYAAACOEfKtAAA8wUlEQVR42u28d1hUV9c+fO9zpgFD7x0pAgMoCAICOqhYKIptsMXee4mJMUZHNEVjNDEaEzWJGjvYS4wVsIsCogKiFGnSe51yzv7+ABPT8/5+7/N8z/t+374urrmYOXNmn7XXXuXe617Af94gXa/MhnCnKUMN4QcA9Jf3///xV4NSJQOA7Jsk25a3IZheWxZY/c4QJ3dKQZQA85823/+0CRGWXc8DELp52M9MvVvJd7MxNgvt7epOCKhXguL/s1pIkpRyQYJCwf7VNQoF2AQFWABkZR+bTXVf9afnZsrS5bYmdkolmAQF2L/aykolmCSlXKBU/udp6n+fJMmfvU9+eaWUAJBkftSfPzKj50IAeJqgEP2yzf9z7CHzb3IIklsfDpl5fkXYUEp/JwDSKRQq+qSfjR+lVAxCqI1EYi4SS4iRoUgEgHjHJao3RTu7jXTWsyAE9I17/3y/OT0NnXK/jpmzY7Sn179L0P9SASYp5SwA5HwzerlubdUeC8Jf/Pot3xmEgL7ezpQqCQAcnhV4JC7SPv2GMvz+0iEe1oKOjha1Wg2tluMA0H1zgj4fEWya847C68lHcQHetGvLdgqSwgQwmBjT60pDRvE3Yb3tL+1aqTD8d3jvf6kAw8PDAQBaiK15sSEaGho4QK0DAFmyKgKAMMx6HoCRl4flsFvXS+FgadozYqCPZTFQr+G0lAU0ANDL2zYyJ6uVNRBJLIJkxqEEoOGQM13bnuobgmEIDJpVHRqBvoGp1MiMIQQU9H+wvVMCDKUgYRZi5xPzA28//3QI7SOCx+vPAKBLE4Xr+ttvq94aQo++5X4z2M5Ox9/fX5ixcQh/Zp7fuxQg83pYbKz8YihNnt+zYpyrod+bYQ3ttJk4McP9XsGWvnRVf4dZ+EVD/zNHgkLBUprAJiT8pWfttHGdDoJNix9W+u3YXpcBMF3xHuSAoEsAoY9WBdP3g60ud33P58nGSPpZtMvGrv+t0lbKub2jvL7vMg8CAFAALAgQYaE3MPeTCO3HoVYfAQD9m3lRgNAEBdt1H/Jv38JxiYkcIXFcXFwi9zd2htJvZgkZhnDF1W23/WV2gwBYMux6Xg4Itsz2J4RlqaUAGp6jsDbWLQZgtszH/JKHpyXv6WKzKEjEDieEVFBO3WFkIG548/fmK+UEFJg1JiiIMbBgj92u2EypklkXl0j/xrtREpfI9Y9P0XbK898kQKrs1JwvxvUeWXRw+smf3hs4lgD09ft/NNYd3k15npILyU+SDHU4Os1ROgI8xQ2GaAN2p2nA8w7WAsYDPEcNpYIXUoDMnhJivWzFWWprZqT70RJ5AM/zhGeIkGV4IXnjgcPXJXMAiKkexj/Lf/UwE2gA1iEe4P9M8wBgWDc9y+dfjdub8enIL/wBs673yb9UgEqAYdav5911YNOnp93JK6dSB9mb6R19t59zJImP5xWdQfAfeBPwAKini+VtFccRlZazpQB0eOr3zWiPb89PlxUN9DDeLxCLSN6rlj4tQHVjc0urn68NKxFwePqiypEQois1kAiaOzSmv7p3YhwDgELD1xiJBV3aGUf+VPEoBQDB0nF9zpQ9LpiKpqYlH6+LWtalBOy/VIDxAOX5tUy3dtR2cFyyvZOhlDLgi/Mq9ZVKJVOV+McrGA45QwC4uFmOauJYeris/YNRTgZjzy0KTA/yNJuRX9WefD2n7rMbj6vqxkZ7DQkUs/3X7bn1zogYDzwrqbq29PTT+MPT/PcLRBJyLKkwCQTYmZ1CASA5q4oAQBvFKUNjfTkBhAxznPsTbaKdzkXJl5bV69q4mKFRpUG7mn8IAIle2fTfkOx3Tiymu9Rs/0z/nfnbRtDtCp/hnY5FJvqjiVNKGQC4FR919+CM3loJMOH4dN/Ce+8ENzgBk15f58pidNI7ofyPi0PUlsCizM3DtR8Pk21Y5Ge1u/LLGDqrh+XuNzx3566QQwAAb7kaTSz8PJau6Wc/rPOa3+0GogQYmiQXAGDT44dkZmwYnPbBYJcR+D/cvv93GVlX6rVvgu/Bgi9GN3cHRrwhMEKpkqEJClaOzgeMMBIsyY4fyC/pZXbFiSWfZK8Np8v9TBMBgCbJBQoZRAAw1AhTyr8ZQ0db6t/M3DKCfy/M+dtT84Lbj87wfwZAr8tjv/mwDCEEhkC3u6vlTXsUnoXo8uwKBVhKlQzDkF+lkIfmDUh4+cUYagx4AcA/iCT++Rb+E4/aKRBKSdfq0379qIDSBHbF4UdbaxvapRtnBp6c6W87JmZ2jC4hhBISz5O4RC6ZKnkA0BWzTXoChqhUTNLoXta8RMgiNbc+Q6EAG9A/hSAbnBJgdClu1jS0ciIBrAkrQmNDx2gBiFCgK84mhLQiOZz9jdekPM+TRqBQV0dY09auEb5+rsREcITE8zxPQSmwfLCX/ZYYt3cifK0Unx9OPVoPvFAqZKK4uES+M2alhGEI6B/Hj3+vpa8D0tevf5fjKpVgQIApDtLR95YHF2Ws6ksfKgd05GyNfZ61OTLtuzivrwGInyo7gYBz84J/uLEspH73cLf2c7P96/UBd0op8ff3F1JKGUIIoowkjnnbhrW95WSUnr1jgmqBzIZeW9yXe7xt+CZFQgJLaZJASSmD16lgl/bsnhDwYc7GGBooFfSllBJCCCa5GE26u6b//cefDc9/tiX21X1lBL27qi/dG+e+HYDoD7T5T1Ge30YhfzomhDkY/1rzQHpYQu+npYF7szcPevBBsO1khiE/26E3QE690ebiwWsCLd/dPMDh4vcju+VVbh1Kd4zwXggADEMgE2JK+vty2r4nhq4KtT4KACzz6/nPBswSFw+kE7tbbn65Z3zpEn+7vJ1je9PL64aufHPXEACz/f2FhBBEmEnlLzZG0w/kjjcAMCzLAIDljbeDVTfeDqjeEu18enEvixP9jQRrANgBAMsyUACsAmBpgoIN0xP2uDA3MDl9XfiTVf3s4/CrXBsAwAKOkj/VPH9AeGVVWEL+tyNrzi3ts1cGiJ5vGyoGwBye7h+d+8kQujPajWYoB6oBOIIQ7JrtLwRAEhRgCfP7hUyc1fvTl5/F1IbpYAyllDEEus3zMjlUvyOCvuWuf7TzKkNnHc+wve5jF6Z6jZmdbuMR0ifI1jnKU0cam/P1OO7ovLD1voaGCjOJtJ/tkIlHPaYuy7SNnJQCc9nUrpXTvbA4ND15eVgZAKsurXLdGivLubcyWOsGzJ7byzLm/TCbm9+NdSvYP87z2Wxvy0UAxAAg6BQ21ofYnb+7pA89OtqdPv4kIhUAKVTKJZRS8skIH/+nm6Kzn6wfUHBgsv/gTk3sWkiFAmxiIrj5vc19107xz9i/7yHiRsuwdt+ToQdy6y8RhsHyAAvZ4NDuN4x0BKauTka4kVb69N1dqUNfAGUMQ8DznVteATAyOUh4uByDPrqp1Wp5k7urB1Xn5Ne3TD/60EYgYFu1Wk56cLpvtqWlqfXwT5JHyWInbvafMcddAx4SkRA6qnbuwt4DT3NPn1iW+5X8HFWp5nlsbNQdPGfsZs/IwfrFJUVwtLND1f1HOLrxq0mrPCud5ozssWH+lisbLpR1rAWA4U7GUw+tGbC3rqoMbRoGvFqNsto2NLXzzw30RAbO1gZWKU+aSr86lb06rUNz+rMY90WjQuw+bNZQqLUMMp5XfDs3MWsWz3ea2mPT/G4btXWEGOqz4C1NL4Z8lBJFk+QC0j9Fy2Znd4YmH69qa3WwMLFxcjR1am5s0Q6LcB89wMNc79y9kpqUspaspsyig3V1Dak3M0oTQnyto6ZGur3f18W0NvFhWTZDiIYCJBvgU4rA708p4o+OHsMez8lpEzS0tk6M9Y5impsL7rxsfMSyjLqytKFwYpTnBH1Gb4zftKUWuTW12hPKeCYnKQnW/r3g5OtnnXXjTuXKSBOPyw9eFT8yC5oXs3CW5dmtO5ru7vk2o/BFvjZ68luGLlXPo+P8xOHXH5fmbLtTOpVlGA3PU5+v5wZtLy2pEh+8lF9943FF7rGklx8ok6veS8ioWXcgtfIrC9r+GBJJDxXPzFgb674o3Nc86taTV9sPnkr7Iju/4ZAyuWgzpTB8t49T+PY5vQ+YSJiQ8ibNK+jrv0zLKX/38ov6UjgVISXlD1LAIbq61mLAZcMAp+O5Hw2iqav70g/6d/v8Nx5bb1uUy4WSz4bSYzOCUwEYKjuNK/m18e1yHktCjz/9NKbdTciMZ7q2zJpo73O1B5bQrTcedlgMn0mNbT23G5l02yibvJxfll7EmQREHqv8fkTJ6tgex+SbDlfOvZjGM0YeSg+PXsGibgF8zDen+bTLP/Ap7w3mbHREwxmWAIDRB0O638/bGkv9pYgFoA9A+BoKZxnys4Mc7mw696FSTq8u6Z080cWw15v472Bz/ZAjk33zy7+IognTehQOtzGMAGCMrpDsTz3ra4SkK2gSBQDDD0/2ulC0ZSC9uTr0xWSZ1RxrYPAIJ+M4AH2mOul/WLs9hsaHOw9kCPk5qAUAKBQsQwAdHRM7E2LwadamWLp9on8DAEdCAFsd/aANM8a3b09/obFRzOMFVna9BTqWgT1nvEuXZ5ZSaWBsQtHWiOLVw3odj9h5rmpqYgoVWHnNJ4RA5BacN+rgJXr41EF+ir8s+eeMx8nyUP6WUXRhv561YE37s0yn85LLIehydkxCgoKVAI6nZ/hpb73T5ykA+EulZj30YAHAau9bPsdeboqglxYFahb4mH/WJTgQQsAQ8juI7E2JUkLiaacgKQigfgicnfBD1tloy4KFkwc7b/9gWo9vJpa7tBvqigRxJQ3q6fszoqflVy11szf4lKc0YF0y5bIJYROhpEiM53iRbaR139C9rm4yy8Oltepp4e6GY56qPrlcqf6cE4tDzlYKRbP1JJSAJ1oVOsC1qTmeU/GUF7fzbEVxg/6rp/UtNjyv1QOh4HlNEyiFWsNlm0hFLo86pLjSqtfL2S9opW5Ns+uMKI8Jz5lu2lJfX6MhsvbrqSnJp+pz7salpECbApAEhUwYN/a4epCJIDY8yJl9f2tS/nQf6+8Xj/AcqxGyFYy2Q1fdobE6cjV/z6orhZ8BeP6r5ACg8fG/BinYP0JbSEoKe3hW4IoPpwbs8tYXuX+eWrb++OPq2zZU222Av53L+/HXMGOcr/hlTnlaVlGzcMEYn35VRXWCkQvevZ5NCFVCTm7aNX4QNnHc7piZ0/VNrUxAZQFssaEzZ2Bq4l1SWjWrWa2StwuFome1Dcyr7JyX6ucPlOBgZtgjYIGNfwCTe/XyDWMV/SIhvVzHIbxfX6GOhHl68vh+qJtyGYtujrxIGFFdmFNfnF8gFAokUd28vXo5DIvjStzDBQJrc2LX0wcuPXp4ltW0DGgprjlJiKojMauaA+D2w9L+W1rbtczbx58KV4/1G/TwRoGotanN2K6bqSBk/Y3Blwoats8MdDD4fLJf0pwBLuPVrxpLH9e1FyqVYH5r936ljgqAJfHx/CB90aBgN/NNt09mePXsJp0XKoQXgKunU1+l5ZY08cuW9WVFhmJu/rhen+ZUtRb8cCHn+sczA1f/MM3/RogJtYlHPGPb3fWdPmPHMj8eOnxz67yFh/dvWNf2w/WbbHnYQMZx9GiiMjATt7S2lVY/fwa+vWkHAA3ExlY8T1lWLAQoZ7sx8fTd5pbWVqFIyHIsAQSsDgHA1zadqntZqKkoLK5uk+i3qR3d+O6z5+K+Wsx+++kW9Xcr3nm1c8W7rSqplPcaMiQMEvhQSpl3BnZblrp+0FNNB+c586NL760b63PLz0XKBMhd4NnLiZ65np+hAnIA6MQGm28sf/bKV1XeIB/c2+ZLAHTdOuXv7N6vjGIiwFNKSbgRufvsVeMNt6BufUTiDuH21aFpx34qyThzr1jvva/vXPRyNu5eWt323bRo78WnPo1dcPzK8+qFnySpl0zq3Xfu+KF5LaeyY2BvX9rU0u7x9PqN26Q6f5VApGv3ZO/eaYWPMxfoWVgZ6lqaSTT1dSaaitImHY3qemuXFaEgELAMCKhDgkLBxiU/9gIDoiUUYAVSAEBtVSnfWN+o4drbzG1t3cUSEW4lJr54fuvO6vbc1HSalFRERk3fmv4wc5Grt0zLSC3t10Y6L4jpbbv1xx8zueziRu67j6I/Y9Ua6baEjHm2dqZpOfl1NrvSyl8ONtdbsHhSj8n+nqYOmRCD6Oo28R1lKzqBTfy1ADvtICEAGlK+uDXoyIKBTt9+99RvUE+TbxaO6xk8qLctTj4ozayo1Vy2MhVMOXIl98y5a8/t508OjokMd8eBI/eanlYxHz4uq851LCs2MjAzIB4DB4x/criisqyh8gjbVrWh5VLB5y1ic0tjdydXsZ7UQqztKBKK2MauDSGkDAPKMOApb5WYmAhYyAy0HAeW54HmppcAIJOZatraGjYzRGvGajSn68uKzr18fPcFgBYGABkyKlLiGzCm1wA5X5GWLuDb2p6vO/Xy9pP0/BHjBvsMmDbdSfv9D7cNrz0sudS3b/f6klcq76YO3mz7SPePBgfZez171dD08bcP57Na0WNDO+uy+DM5LwGAxMfzfyfA15gZIYSox3917TmA59eutaYkP2uLmKHw/WHaUM+JPGUJw/MwM9PzLKpoxneHU0ucXK3uLJgaGKDhNKNnFrk+ifno+qzssAt7J72z2PGah/vnd0+f/6TpxfNDEOFdpiw7v/5xdT4AePQeaKqmREsAUJFQyLMMQAgYgfBhIsCB19ZrOQ4iEEBX1wwqIFtHh0ZIBbuePetQl5bean+dEnK8bg/exf0Tj5joqNhJb4GrasDp01c+DRUXV69dEqF0tjYKzi1pyJr57nGHcD8b/b0fxwxpaVQNaVNRUKoFg47c5MySMXOO5CYDqAUApBWDKsGQ+D9GuAV/jitQolSChEPODNxwoyKsl3Wvh09fvVBezRnraABBXQtcDfVF/RdM6OWiXNhHnlfW6rbw09uHInoYzI4a1ONC2YG4C4qPdh/+IftlxIDJCi9ZSB/x/as3ZqQmHA3TlmICAdJBgGcPrtXC31+ohJKJZ/axhGGg1WjBa/kOAAQcVfFaDaUMQ6Cja7e2Hkx8ezu5mpbWDIBnCAFPqQ6nazPJKjpmW8T06RI7CzPcO3pGe3Pv3j07hoj0I6ZNyK6urGE3/XD3K0sDkf2GBYFeHCEl+088SDx/s/SJhMFTMUF7kgbZACjDEBwdTdlEADIZ6J8J71cVUW9AVaTLuTCd57YEAITHZ/i0n53vm9eFwP3q+zMcRZ6Jc/okzAj1OQ6YTA1wdd1+eHl0S9mBBQ37F0Rvk9r6bfaatIxOOXuTm3LpIfVauE5L3EIuQ2Q7Rql8424GdkOdZq6gk9MKqE5gZDIBAAvPRPnOozTmxDXKOHiv+U3JgwUkNuNMIic+ifzhDF2QXsAN+ux7auA/pCzEx3N71tbRDx998RadFxV4FTBZbC4yHP11nN+9U3N6TgVg8Dts7xeQhPwBjNUJyHbJ6jXsR/7oJjztjKV5bi2TGJdNPkhM7HNGGXb9+pPK5MyiF9G7YuR0XXwKf94f7MOHVEtIqBReBj+EvTXa1c7Fwbu+oga5F6/WejTmNs2O6GaWW9Hx5ardt+4LPd2Odh8QodtnVCzHa7RsyuFjyL95IwOtjbvN7Mwf1JRVLrPuP3hi1Afv4+iMmSWoeta7VaN/cNCGDRF6Rgb09Lz5Weam+m+1tKlF7W3qiVI3t3HB48daugUFozQri79z7DhTm5n+PMBa/+RHMXaT7+bWme7J5tWmQaFi155eorb2jrqH1+8+qPnx9rsMk/V49GhPERKzOVkXvhjf+Up/TiwSFAwz7jiH1/Lg6R9uYcGecT22+csswq7cLN618srz7wEYUwqOkPhqALwvC1OWp0Kq4qp3p0ETEVMtige0cFYQhhDKOgd8PmztihE21mZIPXsW3Xx8MGXjB6YZt9KM5x1NaGqreBmr52J1TSTWKLIunvsyLzXVpVfkYETOmcZVxQ7zSz5w8Ouqp0+g49ED/iOHQ9PcSLVaai+Vmnm3tfLi2tIyuAT68d1iR3gX3kjJEBjqkt6zJqBP1BDUviyi5zZ9Rkoz0hgB4Q4Zmul8V9yqnjr1RAlE3kGqkZ+OlxpJxUi/cYsSPUOTsauWDzmPNq+iH5/2TEjIqu9C1n8rGYYQwpO4RA6AIQCWUjR7i0QuXy/vvUPDEf3DKcXjvk0tLiTLepsPXaTwuXjx6ovSqPDuds9eNakJqIjjOTS3ql+cv1v67sHnjad3Rztd7R/kEL54R+rki1Udh5VyuSA+OZkDIcRm2JSnwz780P308uV8zdPMpzA0dbCXuZuEjBkJc2dnPE6+g3tnz6H9VWmGVF+QRMGqWtv4Yfo2tt69hkfDPbwfV1hUwprY2PDONhbMN+/Fozknm1JVUzUj0jHU6BiKB8yfgdDowTQt5Q6RGhuD0XDa1MOHBYWpqbyQaNN1JezTlrqmfK1QPNOhV4BjSNxoauloSx5fSUbGT5fRXl0NDUfowOVvc6SlUXB53aqBaK28DoWCRWIi92YsfIIhHM9TvX0Tum81N5TMFAgFDE/5NjOpSDc/v6Ha0EjXSM/c6EC/j5JnCFp5bXZFg7rAt4+7Y25V+4vahuYfJbpi0tSqNXC11gtbPcnrlN6x7EXLL7y8cdjGSL51UdChjq03afyNG0cowPg6Ghp06EhMBBRMW2MrMVCr5gcNCcj66fhPY0uzs9e4hvazDxsxDD59Q7S3Ll7zy7xw3o+rKPncwsZoYnNl8YCUnTvjc27c1ncJl9OC2/cYQeQgWDk5gieU9Bw62KLkaRZeFb2CQ3dXnNu1n3CtrZRvV9Hn1y5zmobaLGMzg/eb6pqYhibynk1g8NS+kybCppsjnty6S37ctoNrKi25KZUILxmKRGHVzero0tJXfIC7C6SGhu7Ne/ulTL1QJZyqlJNwLwuadrWACfg2XQOeWlxb1PuKg51+j5yXNfu0Gq6pramDLStt8rK1Nu+lqy9qbWxWXfnZBsZYSxxGDpAFLz+UfrkRaHhTly8v6nGgV3ebt2ZsuV1+/2Vz6/dzejnr6EmYiVvvxL4CzgKAvkf/q9P37uqfk5pKb+3eVdSWdXM0gEc2vQeavqqsmccamS2ThfQxCYodjnZKuHsJiWz+7VvtjLY9WU8qdmzX8p4UQnAqNekeOxyD5sxCa10dX1taxogNDHmhiQlpfFVOfvxwE0hLI2W4dioUkAJCiKa9gxcZ2dk6B4+NIza9/DWlGRnC1FOn6xvyn2eJtJqN2pKsCzyllgIn37smAYFOoxct1bxIOEmvfqUcC3BnfmvTdICgS4sD9zrYGZsod2eM2J9Xde/NzzcPch5gYGtcPWdf2pPXsAt50wa8rjnRL28h/rseagkh4gOTel6LDHUO2XXyMeqr2rB6qg9Sn9e1pRc3n7WzNy0rbqbO+j4hUUYjp4nvl6tw/9zF2sKrlyNrH914QACYOcqsqlu144WW1h/4RMaYBA6L4hobW9iUI0dRmX4XOhIh1Yj0iFfkEISNHIYXT3LovSMJpD43p1VkbKrn0CcIvaKjwanUuP79fqhK8qiW04LV0yeBI0YjIKI/zc/NI8n796Eh4+FxVJUtZkhbeZfNZ+yHjM4JilN0798/iDqUZJPMfd+X6vINORJtS6u+kJYLQTtUKrVBU0uH1MfVdKy5kR5mf37b93ZDW+ZTpUxUDXP+dRUEIYS+Pt4lBJ3YmBJgvBQKokhM5N8sm3jDHrjsGO6+MyLMsVtddZNxa6tGx9BMX8DzEDMSMaiOFJSIVCWcCX9d10/ULgtik/Z+V6/Oejg9LM36XCISOQJA6ObnqVHzu3Ws7cJ8oqP5XlGR9P7ps0z6sQQiMjXBot07cfP0eaQeOAgBr7pmYSRaUFffNowTSDYKLGyYaZs/wsMLl0nqvn3Q95Jh3Hvv0PaWDj754BG2NPVOqUDT9hlflr2t01sSGMLAWBvae2vf2XOm+ji78XYZl3h/QQ0I1VK+vU7ItDeCaFrBadTg1GqIhUwbT5m89XtS375Up7qqlMsF8Skp2l/hBQqwMtkvqAz5R2fADKG0cznFXSClpAuo5PUBiQ5g3CaSNtk72PRqFBi+06xn6q1nb8eIVQ1aTVX5pyCiH141iF8i7ycVwxDouPZY0NpBtveeu5B6hoUwN/bvg9jAEAMnT+LOrd/AlD96eEVTkD709Wob+fVd2Njc8cXIDzcQEIbJvHgJzsFBkPXy5XfMW0r4qtIMMwtpXHXanXxAydgFXxI31NXp6dq57jWwcYzpaGzRtlZUsYbtTSVMS8Oegpc5B7siEA5AGwBtlyzau/54pRJM/N8F0H+HsL6Os4+NpmzcccKBUhUA1ZsfNnf9Qd2CnLzaEgd36RSuqqy5QdMOc1tzQ07INkoor7Iza2BKRTIRn52tNjAzfaxu0pC6qhqqr6+DkGFDWnWk+pxQQPTUqnYQAdSEEAqlkkF8PE8pfcYKhFCp1NTa0x1BwyJ5Qwtz2lBdD1arYaSGOk+q0+7kw3WoGHnZWlEb0ZeI9AOIvnFMa3kd11JUyAhYUDVDRC1q5gWAmi7htf/RA7MMQXw8/UdlHoJ/ciYaFw8uSEj9507y6aUClXA8hKBEAMrzai0k9R0CW0ZqYKDWivikRhNfI2cPHZWqWdRa8OJq5b0bnwPQQC4XkJwcrb6dzKS8uGITzKyod79gWllWgeNvv9eia2Wpnv7VdgNGKqVaMDHSnmGzRRcvHlX5BDuqVeppWipirW1t+Je37uHqp5u11uHh9K33V4kcQkNo3qWz/SE1DyX5P92mAArahjZaiSsLSUNDoo5z99HWwf68oLYa1pX5Td1Fbcs17QZLGXWdWk+oLRGztKi1WU2NDSQqiYh05OY3FHz1sPoSw5CWrsMy+rcH5AkJClZhLiOJO7NpXGIi/7qCW6mQCeMTs9WDbHSnbJ7Te5+eDot2noAIGHCvaydZMSirD4GBBSrsA5Dl0JfPLqthLmzbllh64/4MOMk7SPpuDShAAV/GPfhDr6ioaN9hwzWajnb2xv4DTP3z52AtrBC3dhVqyitx/dvv0ZL/HCxoMafSWBILC7FsaCSiJ0/Exd3f4/mVS2ANTRAQEwX/qCHci/RM9u6hI1xt1uOb0NStJJWlqa+fWs+777mgqdNihowaorVuKRP4Ft8DW5YNqq6CWNsChudhbaWDwoIaUMrAwtwY3/9YkP7euex+lNL2cEKYZIBb1yWrcKWcCQdA4lM4APR3Xvi3Y4SNqPuGuQFPXpW3lq78OvM9HtBpB55VA5USALoCiR1r4/a9fOkCF4N+/WjGuUuC1DPnbrQ+uq4gQFVXmO8BG1m0LHLomsBx4w2Funra1LMXBNmXfgJpaaQSqS7UjBAwtSR9J02Aa1BvZF66Sp9euUacevggMDaGV2u0zMXd36M56xGY9qZ2gHS0aaixqYcM4eMVsJN508ybd0jqwYPVbU+efgiWu8A0l+Tz9j7OQpHeeTu37p4B40bwPu5O2psfbml7fu/Obuv2otLWDj7s49m94vR0JOrlW++QfjJjfLgoVHgho+LL8bsfLvm7HcoCwMklcsUnU/xWuOvr0AtPKyo9oK9noSswnB5iM+b9yT1OPs2pqhv+XVZoBSH3Kgky64CyDpDGFkIa681ldZY+vu/4z59rcCz+Y3XmNxsHaSoK1zGEtFJIZbB2ec9x6MjdI99fGeMZPkCUdiWJXNz2BVP+8P51qQA3wTKubRpeJDGzIG11tbBwdYFYTx88YYj/qJFaUytrUpD9nLEwN0X2tatQqzrA8QSq1uYOqY4gp62qIuPp9RSDVy9e6Lv19ufCxoyW6tg7RlbWN03VaJnu+iL+Tk9Xtx2lBXnRj2/etzDqEcTrm9vrPbiXmlJUX/GJysIyrTityGHs4O7eNpZ67R8lFZ0vf/aKzop2HTbY18a6uKAuz14qIh0tan0PAwOrxPf7rV4Q2T28jejee1xQqSXrB7v4Dw+xefgg9RXkoVaobVYBlIKCgZ2pFOlZ1TnT9j+d0cCQu2M8qAjZ+DntkVEljSfxerJxi3Ijli6y3jl3jkpSXzS+pam9GvWN+g7DR84LmTBpWHdvGdKup+DmiVNoKiosleoJ4gUMnjfUti6XODjFBo8bx3vL+5G6hnpiYGqKA2s+QmtBHky6dUNzXS00VdWIWrMabj080djQAMpT3DqSgPwrl2qhUX1hYCxFa5t2PsQ61u59ghA+aqRWbGwsuH3+J6SdPaPmqiqOSg10TVvUiA6YMl/jam3DHl27Yq6y5tnedZRyhBA62lqycvOSPhsv3H+VsOhU7rq5LvofLZnoM1LLsuhQa8DzgLGRCE+f1MLKRAw9a4MNPVclryXTPc38pkQ5321pahVrWEFp+tNXx8UEla1tnLCssr1uf2n7XhDSpqSU+V3ZLKUEhDBmQVE73tr66ey05BSS/tNFYmJjg97hfeEeEoLnDx5pUo4l5tXk53ZIxMxdkYi501TXPltgZtnPLyoSXkMGc411deTR5auMrbs7lfUNIw9/ukqfHj9K2Ja6dE4ocTOS9dCPmDMbOhIhvXniJG9kakJ6DB7CVecXCe8cO4nqJ5mcmNXeF4rIi/YO7SBWR9/GM6wvFzVpPDS8hr15+hzy0tLBGBpi2uoP+LtHTjA3v9zQj2hbblL4C5OUUto/PkX7jr/Fjrlj3BfsOP5syudp1T+ECPFJL0d9Z4mEUQnFopdu3Yx8rKXCEWamuqih7NuRW+9vJQAwy0U/JHZgN/9Nux+fvgmU/MrLMARr+T8QXpcJoEpQ009hbRI5ozBq7iyhREcMaxMDlOe9VB/f9UOlbdXTLAcLg6xjl1JWaC1dF8DAeK0sYrBFaNwYLafhBMnnLqDg6hUQVRsoB7jEjMCQudPp3tlziaDuVWxzB7bKV650cfTyoD/Mm0+gagd4AqmjE0LHjEK3oCDNs9R0wcOEk6QlL6dYLKI/2FsaEntWrShtM3LzHjocQVH9CMdSaCi0z1IzBUnff3+yn/raeHgpuMTERF4BMAlUSQmJt7r1XlBpZm7tzfkn88IJSyh+A2Ftj3UdTTW8aPGPBccA8AIKEJLffGdP/uM7AJAkhyD5zbLeFMq9Fp4SYMKV8p9P8gZuuKHtPEpGmSQr/crF+HX9xCbWha1VVWon1cuKdf0dgr2Guvc8kFTwUKtjN8MhVL4jbMJ4mFhbae5euCRMO3e+DfW1F6QGogSBRKRuaWv/tLH8lXt7azsIBc+2NRaB6tcKGNalrqqGR3tHu1RHuIkyzPP2kgLfS5s+7Wvp6xsWPDoWYz9eo3l8Jdnh8dnTCwrzXny3apyHysvJkuy+eor/+sTZVlN35wZ1U5ldW2VlYWXW3WWJgJp5dgKUUpI2J4ABvPg+BoxcKhGDo+Q5IaDbhrqIrfXztFlVnR54XTLlCCEnXtce/BwpKhRgk5RywZ/wcYkcELzOkX8z9PqKMeTkksBDDz8d1jA6yPtQD13xjMPLo7JKDs6lp1YOyXO2cdzjEDHuxvi9x+nSB8808g3bqV5wNIVDz590XXv4/SoVsvP62Oqt5XTOo5e8blhshZWVkyMcfG8O+uY4jT2ZQuHaO1Mp/2UeDCFgbGWriFvvCqcJ8+mEU1fU829m0tD3NqvNPftVzxoUwuV+PZXe3Dienz+wxyYI7ePOrBpe+3xHVNKa4W7Rr0vdXo9+BmRvySf96IogqzUKgFXKZKLf82PAvlmBQf6KSKNIkFEBu57nutQ4xNzAZeVEvwlajlpU1raLJGLWv19PM/+8giocvPii1NfXsVER6eNVUtHctP/8k6Ony63ch8ycFtp9SF9BZmYWkhNPoTb7SYNQQJdxL5/s43ke7iEh+g0teny/x0YdiQZ3I2yGj/lJsXI5/+3cRW1s+bP+TRrdvYNXr/bWMTXFmRXLdtGiR/OITCEEsoDsbDUAmPvJXWvqmz4VGhqP9JL3Q9/YGF7QqmFSD56hNXcv5H4Sa1Ll2920n0DCHt60N7VNZiGYOaSfCx4VtdVkv6jLNDUQNUmEDJdTXB8q0GjIvrulQeUMKWYBXF3TTxAen8KRPwn1BH/GpSCJiVyXeIUhAry/enGwhb0Rq2ioV5m3UAY97PTQThl8vzeVa9aCLpnZxwZqje13R+98s/5yfjyxD9g678NVch0LU37vR5+h9Elmu4DglIe/14pnpw6VA4C5q5dLazvjZqpV3zqORA7thlVUo6KEAgwjlPJUpycYgSHLsADlgba2TAagdgYlrIAz7K3u3d+q5lVFRnVGyguqVI4xOXVla+6PF2cU38+Q9ooajEHLZ/OntlH7kTu+mzvSusMtup/D129PDmB+ul2k3fjlHdI3xNlsqK/5QFWbGs0dHPxCbdECppSIxMu/TcnPrgJO9Y9Pqf4vpXLKriO8nRODhjs76C3JLayShnlbB4o4DrsSMpH4tDatAyjQB/p7Ggqkiyb0kvTqYYV9Rx/Rworm2m6h/q6D5CZWjY5BjoYujnTb0hVUW168I7p3wLr00wf4Z69KBzOeYZaW9jajGIb1ULU21leoa0fRzooAhtHy4AkAngcIMQUDCSNgwLAMwHMtAMCWl1OBp98qDcRDbaztG5o8vDOEh36kgprKfZ7dbHbUNVfOuf3dvsWsvj7jHOar9+wnY5vAAY69S2rL81d/csVl1nh/NuKdIeTzb++2bTjyuF0NNDUDZc4i9J4+oJvd/FjPJa6O+mhTaxf693C6uObb7I+vFRQ08l2m708FqASY9fHgJzpaBAe5GBx/cjdXODXOG18ffsol3irWdAB0wSBnG3mgi2VFTR3b09VInP6sXrVwzWXRsMFudNxIH7MzD8rNrqSkF4bMjVBzIJTXqCu5svwfz7a2jjcPjZoyblxcgEhqiJsXr6K5vgFiK1vLlqZaSwA5oAIGIGABQnmuRaJuv9/C6lYB1JywDCASSgCgqKiowyMwwprwAnQ0txj59o/o7+zhirtnTrvn3r51WtzWUKjixeVqrcZe30CPdjR11LS+yr8f2afb7Ogga3xzMA1GRrq1i8f7SOOGuPAqLa8CRA5fJ6Sy3yQX4uSt4pYtK0KkFSU1PpqScp/ZQyz7XP26YCAo1XYxw+kf1sZkA4QSgsK6BpOWNir0CHTGlTtlmh+SiscViBBgry/aNCnay/ry6TS7MG9L430ns8iS79Jvvjc/uN3GTIdZuvHqkbcPpfcmQOPjO/dMaFMjM+2Dlbrdxk4+EL5k6fZFmzcHRIT20ValPqQNjx6qBa1NvI2rO0+kZpthaakHASsQChiIBAIKQhgRyzWC1zSAchAQBhCKnAAAtrIxYnMbZ2NLa745/xlfcD1FbaJWa95eu8p2RPyHC2wHD/vMZ9QI66DwEObZ7VQCPdr4YVLh9299fF3xY/KzxvglQVQl4MuHrbzU6GgmMa/MrfQQtLQ4vDW8Z8XzDqy80cL1Pngq56q5iSHMbUxQ8KJS2im0db9Le5nf1MZwlKfkTrP6xy/2347LKm3deSipaEw2cDzWwdR1zWz/4aYmInj0tEVDfQfSntfXLR7Yrb8uC52lX6bmna9QLWAYRjtGoWBb8vMmHNn02R2hljOa8fZy0+EhftwI1RMua+NKwaPzZ6tYTtVka2JGY0cMg66peQA4gQ04vkNHJCK8RkMFYoluJUTGYFldLUfB8jwI6dzdoMJ5Mj9//eA+vXmRkGVUVcWiC5s3C7td34OhUrU2auw4yGNiRBd37G55cvxYLGkqTZ3t7y8sZJjj61JK++85mqlaMa6ntz5LLG7dLYRbd3MYGBD07WFsuTGuh6caMDqcWXM78XbZpYSk/M+VVwsUALSExP+1DUxQKFgkAlRZRUh8SuKp/Q8SHYE+P703MNHZgIy5/qCYP3Y1P6G3j33w50cfCR/VqtI39bGT37pfJNUyzNcEtL4vzwsSO5HtrNIMOmXXuvIZNgJJyDtBJLTFUUju3Ulfa2jrdaC+sGSIk5isM9Q3MGcI4aVGpgOFpF0qEgqgUalZXsudndnT3vRkbv0LkRbeOgKBUMwI2tQAIBAYSsQidDcQs4ZcR7FQR3Clqq6Ov37uRNSkIfnWX+3OKygUSpOqcp8eJa2vrlKA2Z2WppkNCL9lSEZyZmXyzJH8UHmA7ZlNJ/Oqlo8TDqhuUHXUXSsymxTpMfXWmsEjfjifVfHJnZK3AfwoYBkkfdBXEO5lQUlc4j8m4Rh/Pc4v4emHEfT68r50rqfZSTEwCIAegOndgLt7xshqCj4O10bokY/BEGwb6ipWyuWC16f3bGfZLca7mhyu3jOKTvUxLQc6GUlRztZHr+9cQ9c/LFA5RE+mFp69j1jZe20LmvkOff/Bc2rSKyrp+oaxRbHhYXdHfXWibXbidWrQzX/1li1bdEQugXVjvjpO07Meaj8e378QELkBgJNI8MGNVRH0zOL+pQBkLAH84S98c8d1lfhafR3Tre7FZ5GtM3o7zwPgYwHM9hGzD0PEhH4e4UxffDiInl0Q2jTW02re3zKVXpetvhfmoLjzccyBXVMDtqV9HJUZ6mcdfjYp72Ts1pvR3+TUjFIBSQDs3+3rHHr0vdAg/+4GpqduVbE+ft28wVOfJT/lqeJTUrSExPOEEJbjqJ2nEJ8vjPMaf+ZqNn/tSe1xSqnWCMKJigGuY3U7GrS0sYUV6Eu1VTnPUitKcp/ZONhp2XYVJ2hr1XVxMpS6WErKynNeqO2tLXg9qZ7tqjWblqk5Kja3tOQfpTxkowKsnQa5W12wMTPr/lKtv33j4QeX+3gY2y4MdljDUWClwvnNFJQHISCEVOw8Xxh160HRwzVjuu9MXNBvt6OuYJBWw2mKVXTTiqsFivkbrqxXt7aXxE8L2HliSdjxPdN8vzyxtO/MLgSL/KrugxBC+3l6SuNHWpU2F1cZhgTbIrWgOTFyy51ZABoBkG2xXj+4uJj1UbW1CVytdRx/vFlYsvtqyaaejoaDP5ruP7xOpVLdTCs7V9+kau1uLzWyNJYE6eoKjWyt9CTaDtC3Nt7amKki74PnsSDA/tvVc/vOmLv5DqxnroNJeJj6xrnzL80tzTThA/t7nj9wirm///tjT9Z7yo/crj65NV1n9tJtHwoa2psqb1y5USQL8nftaWNncnDhgrq3wyDUFwv0F39x/bt8jXYWIUT/yxHuyUP6evgpVv84JbNdfSCcEDal89zj52gjnoAHhfEEB50tE6J7TDM2NUDpq4YcPQnbdvTy868O5tXtBcCsC+82fkRfm4MtJTXQNTZEu4FUERp//ThNULAkLpFjX7ORfjzownrbsxOFLGv2orSxZfhXDyIZhtTyxxSstrptUmyfbmufPSkxGhvjbTx37aXMndkNwQ2E3HjW0HEi71FJlbWuSD7Qz8rP29nEV8BQj1YNV/g8v4FEBLvpb/w2nZx71TqbYUiNFaX+W+YGbr2eXtKe8KB0zsvyprKONlUfA2dHU16gY3HjcGLtkxvJ+2lL3Zn5gxxGZT2vaDmb03y1OOORidjA0EHP0c62pbJK586+A5oXBUXDaorysqYOdI5sbGyRRs58ezvDMqq8nBrtyH4uw53sje38YmftKRMwPMfxZN06MOEAOzW5iM9eBzaXIW2PGzRnDj8s+3GIo6Svm6XEs66swSYy2tvA6lreoWRKSf9pSzMdhQL37tZmPgIBg3qeJh68XZwNmTmbklLE4026VqQxfK6sj141L8SlPyEEu/z9hYQAywJtgy/MD9HcWTmAHpjYs03GQsF0UqVEb7AgxQAinYDjJsApACGXlgSnn5/jX20PDO6yPaY7x/bMzPl0MHUXM9N/BnVZ02Ewdn4PNj2/gIH92L2z+/442t1yesFuRcWmWM/EL+MCDi2K8FstNnWLFxm7rABjOVaoZ9ujq6xL8OUo19TcjeEdQTr4btGioWKAYJ6fxerqPWPpLF+r3Z3n5b8nj3eyrGQiMASTHHUOpr8fTh+tHUCPzw7Yj84jDVGCQsEaAsZHFoe/v3d670V/VFf+l+clr8u4proZ9f1+gu8Kly6K6OsJdBXnsLsUbgsvLQ3af3JhaNlPKyLog/VRNFM5gA63k44EQ0AIwViZuaLi69F0ub/1UwBEqZQLuo4VfialDQIscr8eT98JdjqevW1E03shjrfurI+mSR8O/wgA2DeydyVVMoaA0zfjPavoqZH006FONQDMu7hy5ntGez9++dkwemttdGPKB4MLLywNuX1+ScD6WEux05tcOEWnUKzW9rFe8s1Y2XIAZn+GFRDyN5T+JKVc8Fvq/q8osIT80rKkkykp+m5ywN6KTQPpiYke9OSsXrfOzQ/9ctvQ7t8MlmACAOza5S8EoHdthTz3wvw+FQAcf8WUVChYmUwmglwpCNAXuT/+LFo739MiJ/vLuI53/Oxoyop+3M1PYj7w9/cXTpkil0AuFyhkMhEFiNxc952cTQPppnAbddYnEZrJ3c2SALBddY1GC1yNPtsR43n90rLQ04nTvCsfrfClmRv6FwZKRTKW+eVZft9L6fe9v/4EkfpnQ6EAmyT/BfJKUMhEhCEY52oxt3bPJDrdzaIYQP/fLUoXkXqQVDzj5cYYOsnFOBOE/I5SqwQYpVLJGACuz76IpXPdzJoyNo/glnnbqO+u7E+vKwevB/mFFf9wtr8QhGBjtNvN3C1DNU5Ax7V3wujhSb2aAJi8prz+ZojsgOmPt46iR+aElgCwp5QyXRpIlHK5IEn+f9cG5b/aE0vvxoq+zy8vCL0GoBsrYECpknm9Wq/ppIQh6A4MfbA2WvXliJ6P5YDgtzTX16zxjdFun7zaPYqLNJB05Hw5ll/Yw47uGubBPd4cUyMEfMGQ1z0GYQhMTf2gn3ZztGu7B4O8F5/H8uv62NcC0O/SwM6Ob1TJUKVcIBB01l8vD7JfXvPNcLrM13IV/t1c/9eE410zwqfc3zi8OnfzUNpPCk8Qgtn+EP5xCXECCwDJ66ISL68c1AGA6eLMkdfUUUKAxX1sBhbuGE3fDbSlzoAq/5sJ/NJQpwY3oOPJJ1H05JKwB0ZADACDfnrM8oNvyaqLtwymQXrM6YX+dvuq9oynw80ka/+kZ0Jn1WlnzwRcWBSQVLVvjPr4ssHXRvZwtujqBUH+1X1jCLN+Pe8KGPj3dv7yWVZJBiuVInqQjxsoJRHOij+sJUle9xUBQJ6X1qU62uiJ+0gEwTzPQ/G6FnsdpZSi2wB/p3MPHhW3fZ9admbqgG5CqZijIwa561tY6p/7ePet5H4+VgEjPM0TvxnpUf3dqrAtlsYS40370rfcb+Wn9g9x0S+uamk9W9OxkTAEcYl/XNfCRtzU+ltb60r09L2vXH2hsjYgA5SLgmcQAkqT/sVtTwCA59cyIqCjsqo5w72XS6+XpTUvm1rbMigFFImJfzjprhYl9MylZ/fE0CLUxXA2KCCTy4kCIIRhqASwtjPT08krazhfIxK9M31yKDdvyQmNRAtmV/yIgkuFTQ9rqlt4dzfTSg3DnDqTlPf2oO3pvl89qVkhAXzEnGZUQVH1U7kcPM/xf1YsQDntGiatvLyjsrz+qIu7lVQsFSD17ouazjOP5H/DFu5Scw/AdO/cPuMCu84V/rKzpLzTsy8LdNpZvnscP8JEtOuNliVMV2xgkfJuKPdg3YAZEsD+wrJwmr5tDJe1aTg33du6QSZmc3I3RdHvJv5Sqd9pDykDQC9xoTz/8soh6i7HQP6Jcuya7BupjOo+9P/1RmR/16OP0iQBACStG3783oaR1BwYTZhfehaAEBgAgckr+tALi4MX6urCOoxFXsWhGdr1fZ2pD0ADpcKOJx9H0c1RLjtpgoLdO8VRogSY100nVoS5Hm08NIuGi7CSML+h3v5Fg403u2f+W5uPUYAkKBSsspM1Qv8qtiSkv3akreEYKz0u8vKNJ3uqgRPHjo5mE/FLlQMPCO2tTVDV2NG3rQ3lDRSb62saCWuoc6FDyExoadGsMDaTQldf14LEJXJTowM18QC/Li6REkJwL6Po48qq8qoJ0V7zKU9t198g2r96PkIITVAo2E4vTf9zW+QBwLYpYZPuro3ks5Ryfkmo05zOmKtTQ7o0QXTu3cHXkpeGaDI+GtpyaGHfQBvANGdLLH38xYi3AOD6uqGn7iwP4W6ti+5YOcwn9s37M4QABDg5yeVJ8Zah9OCSiCfW1ta6/7StCf5TO1gq5ss66fXRfv6qpjZS38yR0EDHAQBoeLj8zUs1BvoiS4FILDA0NNBTaTizV0ATBx7FJQ22loCeUFe/v9jIgLGQMmJrQ7EAALKqqogSYHhKMdjWwNXBycE75VoO+vS0cZ8d5SMhhPzLlYv9l97dKRwpKSnUhPC1jo5mci1DHl+9l//uvaLG2n0pRUgBKBDPpKSAaoor77l7WBnfzyz+cu4PacdNAZdZw3ouvHw7X+9CXu1+Ul6VYWCk6/7gSUXi28cz91BA07+oiKZ08fomv/1+S6CjkVjWy8HuWX751wu+SblAlUqG9E/5n9wE9P9wsQiBrq6u1dPPR9KzS8OW/RdtvBj/2zqZK5VKhmEI18UAZf6slJgmKNhd/p0QfFtbm0ZHzFBOy5lTCiQoZSKWIa9Tvj8UaUKCgiWEqH5unvG/bJB/2DpZAADXN47dkBE/mKZ+Pk6dsHma7PVC/Hf9zv/EXvr/yBa9bumpJ5VE308vhbq1XehhIzUHAC+vbPLf9Tv/a8drsGJhsMvCOxuGqU4v7nfKGtD9o8Y+/wnj/wGjjGk+anuJLgAAAABJRU5ErkJggg=="


def get_login_token() -> str:
    with open('/snowflake/session/token', 'r') as f:
        return f.read()


def create_snowflake_session() -> Session:
    logger.info("Creating Snowflake session...")
    logger.info(f"Token exists: {os.path.isfile('/snowflake/session/token')}")
    logger.info(f"SNOWFLAKE_HOST: {os.getenv('SNOWFLAKE_HOST')}")
    logger.info(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    
    if os.path.isfile('/snowflake/session/token'):
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': 'oauth',
            'token': get_login_token(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'FLUX_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'FLUX_DB'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PRODUCTION'),
        }
        conn = snowflake.connector.connect(**creds)
        return Session.builder.configs({"connection": conn}).create()

    # LOCAL DEV FALLBACK: use env vars (injected via cortex source or .env)
    logger.info("SPCS token not found - attempting local dev session...")
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    if account and user and password:
        creds = {
            'account': account,
            'user': user,
            'password': password,
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'FLUX_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'FLUX_DB'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PRODUCTION'),
        }
        logger.info(f"Local dev session: account={account}, user={user}")
        return Session.builder.configs(creds).create()
    raise RuntimeError(
        "No SPCS token and no local credentials. "
        "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars, "
        "or use: cortex source <connection> --map account=SNOWFLAKE_ACCOUNT "
        "--map user=SNOWFLAKE_USER --map password=SNOWFLAKE_PASSWORD -- python3 fastapi_app.py"
    )


def get_valid_session() -> Optional[Session]:
    """
    Get a valid Snowflake session, refreshing the token if expired.
    SPCS automatically refreshes the token file at /snowflake/session/token,
    so we just need to reconnect when we detect an expired token.
    """
    global snowflake_session
    
    if snowflake_session is None:
        try:
            snowflake_session = create_snowflake_session()
        except Exception as e:
            logger.error(f"Failed to create Snowflake session: {e}")
            return None
    
    # Test the session with a simple query to detect token expiration
    try:
        snowflake_session.sql("SELECT 1").collect()
        return snowflake_session
    except Exception as e:
        error_str = str(e)
        # Check for token expiration errors (390114 is the token expired error code)
        if '390114' in error_str or ('token' in error_str.lower() and 'expired' in error_str.lower()):
            logger.warning("Snowflake token expired, refreshing session...")
            try:
                if snowflake_session:
                    try:
                        snowflake_session.close()
                    except Exception:
                        pass
                snowflake_session = create_snowflake_session()
                logger.info("Snowflake session refreshed successfully")
                return snowflake_session
            except Exception as refresh_error:
                logger.error(f"Failed to refresh Snowflake session: {refresh_error}")
                return None
        else:
            logger.error(f"Snowflake session error: {e}")
            raise


# All generator functions and their private constants (_LOAD_SHAPES, _ASSET_TYPES,
# _FAILURE_MODES, _SCADA_DEVICES, _SCADA_TAGS, _PROTOCOLS) are imported from
# data_generators module (Step 3A extraction).


def snowpipe_streaming_worker(job_id: str, config: dict):
    """
    Background worker for Snowpipe Streaming (supports both architectures).
    
    Classic (snowpipe_classic): SQL INSERT via Snowpark session.
      The native Classic SDK is Java-only; this is the Python equivalent.
    HP (snowpipe_hp): Uses snowpipe-streaming SDK with PIPE objects.
      Requires key-pair auth and pre-created PIPE object.
    """
    global active_streaming_jobs, snowflake_session
    
    mechanism = config.get('mechanism', 'snowpipe_classic')
    logger.info(f"Starting Snowpipe Streaming worker for job {job_id} (mechanism={mechanism})")
    
    meters = config.get('meters', 1000)
    rows_per_sec = config.get('rows_per_sec', 100)
    batch_size = config.get('batch_size', 100)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    production_source = config.get('production_source', 'SYNTHETIC')
    data_format = config.get('data_format', 'standard')
    target_table = config.get('target_table', f'{DB}.{SCHEMA_PRODUCTION}.AMI_STREAMING_DATA')
    
    # Initialize stats
    stats = {
        'total_rows': 0,
        'batches_sent': 0,
        'errors': 0,
        'consecutive_errors': 0,
        'last_error': '',
        'start_time': datetime.now(),
        'last_batch_time': None,
        'mechanism': mechanism,
        'batch_latencies': [],
        'peak_rps': 0,
    }
    
    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['stats'] = stats
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
    
    # Initialize HP SDK client if using HP architecture
    hp_client = None
    if mechanism == 'snowpipe_hp':
        from snowpipe_streaming_impl import is_running_in_spcs
        has_hp_creds = is_running_in_spcs() or os.environ.get('SNOWFLAKE_PRIVATE_KEY') or os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH') or _connection_params.get('auth_mode') == 'keypair'
        if not has_hp_creds:
            logger.info(f"Job {job_id}: Not in SPCS and no HP credentials configured — using SQL INSERT")
            hp_client = None
            mechanism = 'snowpipe_classic'
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['config']['mechanism'] = 'sql_insert (auto-local)'
                    active_streaming_jobs[job_id]['stats']['mechanism'] = 'sql_insert (auto-local)'
        else:
            try:
                from snowpipe_streaming_impl import HPStreamingClient, SnowpipeStreamingConfig
                # Extract credentials from existing Snowpark session if available
                session = get_valid_session()
                hp_kwargs = {
                    'architecture': 'hp',
                    'pipe_name': config.get('pipe_name', 'AMI_STREAMING_PIPE'),
                    'client_name': f'flux_hp_{job_id}',
                    'channel_name': f'flux_channel_{job_id}',
                    'private_key': _connection_params.get('private_key_pem', ''),
                }
                if session:
                    try:
                        hp_kwargs['account'] = session.get_current_account()
                        hp_kwargs['user'] = session.get_current_user()
                        hp_kwargs['role'] = session.get_current_role()
                        hp_kwargs['database'] = session.get_current_database()
                        hp_kwargs['schema'] = session.get_current_schema()
                    except Exception:
                        pass
                hp_config = SnowpipeStreamingConfig(**hp_kwargs)
                hp_client = HPStreamingClient(hp_config)
                if not hp_client.initialize():
                    logger.error(f"Job {job_id}: Failed to initialize HP SDK client, falling back to SQL INSERT")
                    hp_client = None
                    mechanism = 'snowpipe_classic'
                    with streaming_lock:
                        if job_id in active_streaming_jobs:
                            active_streaming_jobs[job_id]['config']['mechanism'] = 'sql_insert (fallback)'
                            active_streaming_jobs[job_id]['stats']['mechanism'] = 'sql_insert (fallback)'
                else:
                    logger.info(f"Job {job_id}: HP SDK client initialized successfully")
                    with streaming_lock:
                        if job_id in active_streaming_jobs:
                            active_streaming_jobs[job_id]['hp_client'] = hp_client
            except ImportError:
                logger.error(f"Job {job_id}: snowpipe-streaming not installed, falling back to SQL INSERT")
                hp_client = None
                mechanism = 'snowpipe_classic'
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['config']['mechanism'] = 'sql_insert (fallback)'
                        active_streaming_jobs[job_id]['stats']['mechanism'] = 'sql_insert (fallback)'
            except Exception as e:
                logger.error(f"Job {job_id}: HP init error: {e}, falling back to SQL INSERT")
                hp_client = None
                mechanism = 'snowpipe_classic'
    
    # Load meter fleet from production or generate synthetic
    meter_fleet = []
    try:
        session = get_valid_session()
        if session and production_source != 'SYNTHETIC':
            # Load real meter IDs from production table
            src_cfg = PRODUCTION_DATA_SOURCES.get(production_source)
            if src_cfg:
                result = session.sql(f"""
                    SELECT 
                        {src_cfg['meter_col']} as meter_id,
                        {src_cfg.get('transformer_col', 'NULL')} as transformer_id,
                        {src_cfg.get('circuit_col', 'NULL')} as circuit_id,
                        {src_cfg.get('substation_col', 'NULL')} as substation_id,
                        COALESCE({src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') as customer_segment,
                        {src_cfg.get('lat_col', 'NULL')} as latitude,
                        {src_cfg.get('lon_col', 'NULL')} as longitude
                    FROM {src_cfg['table']}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                """).collect()
                
                for row in result:
                    meter_fleet.append({
                        'meter_id': row['METER_ID'],
                        'transformer_id': row['TRANSFORMER_ID'],
                        'circuit_id': row['CIRCUIT_ID'],
                        'substation_id': row['SUBSTATION_ID'],
                        'customer_segment': row['CUSTOMER_SEGMENT'],
                        'latitude': row['LATITUDE'],
                        'longitude': row['LONGITUDE'],
                        'production_matched': True
                    })
                logger.info(f"Loaded {len(meter_fleet)} production meters for streaming")
    except Exception as e:
        logger.error(f"Failed to load production meters: {e}")
    
    # Fallback to synthetic if no production meters loaded
    if not meter_fleet:
        logger.info(f"Generating {meters} synthetic meters for streaming")
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-{service_area[:3]}-{i:06d}',
                'transformer_id': f'XFMR-{service_area[:3]}-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-{service_area[:3]}-{i // 100:04d}',
                'substation_id': f'SUB-{service_area[:3]}-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False
            })
    
    # Calculate timing
    batch_interval = batch_size / max(rows_per_sec, 1)  # seconds between batches
    batch_counter = 0
    
    # --- Grid topology state: correlated outages & voltage cascading ---
    grid_state = GridTopologyState()
    for m in meter_fleet:
        grid_state.register_meter(m)
    topo_summary = grid_state.summary()
    logger.info(
        f"Job {job_id}: Grid topology initialized — "
        f"{topo_summary['substations_total']} substations, "
        f"{topo_summary['circuits_total']} circuits, "
        f"{topo_summary['transformers_total']} transformers"
    )
    
    # Main streaming loop
    while True:
        # Check if job should stop
        with streaming_lock:
            if job_id not in active_streaming_jobs:
                logger.info(f"Job {job_id} removed, stopping worker")
                break
            if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                logger.info(f"Job {job_id} stopping")
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
                break
        
        try:
            # Advance grid topology (probabilistic outage/recovery transitions)
            grid_state.tick()
            
            # Generate batch of readings (topology-aware)
            batch = []
            for _ in range(min(batch_size, len(meter_fleet))):
                meter = random.choice(meter_fleet)
                if data_format == 'itron_grid_planning':
                    reading = generate_itron_grid_planning_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'symphony_iris':
                    reading = generate_symphony_iris_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'carto_spatial':
                    reading = generate_carto_spatial_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'siemens_edge':
                    reading = generate_siemens_edge_row(
                        meter, service_area, grid_state=grid_state)
                else:
                    reading = generate_ami_reading(
                        meter, service_area, emission_pattern,
                        grid_state=grid_state)
                batch.append(reading)
            
            if not batch:
                time.sleep(max(batch_interval, 0.1))
                continue
            
            batch_counter += 1
            
            if mechanism == 'snowpipe_hp' and hp_client:
                # HP Architecture: Use SDK append_rows
                offset_token = f"{job_id}_batch_{batch_counter}"
                batch_start = time.time()
                hp_client.write_rows(batch, offset_token=offset_token)
                hp_client.flush(timeout_seconds=10)
                batch_latency_ms = (time.time() - batch_start) * 1000
                
                # Update stats
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        s = active_streaming_jobs[job_id]['stats']
                        s['total_rows'] += len(batch)
                        s['batches_sent'] += 1
                        s['last_batch_time'] = datetime.now()
                        lats = s.get('batch_latencies', [])
                        lats.append(batch_latency_ms)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['batch_latencies'] = lats
                        elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                        rps = s['total_rows'] / elapsed
                        if rps > s.get('peak_rps', 0):
                            s['peak_rps'] = rps
                        s['grid_topology'] = grid_state.summary()
                        s['consecutive_errors'] = 0  # Reset on success
                
                logger.debug(f"Job {job_id}: HP SDK wrote {len(batch)} rows (offset: {offset_token})")
            else:
                # SQL INSERT Architecture: safe write via Snowpark DataFrame
                session = get_valid_session()
                if session:
                    columns = list(batch[0].keys())
                    data = [[row.get(c) for c in columns] for row in batch]
                    df = session.create_dataframe(data, schema=columns)
                    batch_start = time.time()
                    df.write.mode("append").save_as_table(target_table)
                    batch_latency_ms = (time.time() - batch_start) * 1000
                    
                    # Update stats
                    with streaming_lock:
                        if job_id in active_streaming_jobs:
                            s = active_streaming_jobs[job_id]['stats']
                            s['total_rows'] += len(batch)
                            s['batches_sent'] += 1
                            s['last_batch_time'] = datetime.now()
                            lats = s.get('batch_latencies', [])
                            lats.append(batch_latency_ms)
                            if len(lats) > 1000:
                                lats = lats[-1000:]
                            s['batch_latencies'] = lats
                            elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                            rps = s['total_rows'] / elapsed
                            if rps > s.get('peak_rps', 0):
                                s['peak_rps'] = rps
                            s['grid_topology'] = grid_state.summary()
                            s['consecutive_errors'] = 0  # Reset on success
                    
                    logger.debug(f"Job {job_id}: Classic SQL INSERT {len(batch)} rows")
            
            # Sleep for batch interval
            time.sleep(max(batch_interval, 0.1))
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Streaming error for job {job_id}: {error_msg}")
            consecutive_errors = 1
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    s = active_streaming_jobs[job_id]['stats']
                    s['errors'] += 1
                    s['last_error'] = error_msg[:200]
                    consecutive_errors = s.get('consecutive_errors', 0) + 1
                    s['consecutive_errors'] = consecutive_errors
                    # Stop after 10 consecutive errors — something is fundamentally wrong
                    if consecutive_errors >= 10:
                        active_streaming_jobs[job_id]['status'] = 'ERROR'
                        logger.error(
                            f"Job {job_id}: Stopped after {consecutive_errors} consecutive errors. "
                            f"Last error: {error_msg[:200]}"
                        )
                        break
            time.sleep(min(consecutive_errors, 5))  # Back off progressively up to 5s
    
    # Cleanup HP client if used
    if hp_client:
        try:
            hp_client.close()
            logger.info(f"Job {job_id}: HP SDK client closed")
        except Exception as e:
            logger.error(f"Job {job_id}: Error closing HP client: {e}")
    
    logger.info(f"Snowpipe Streaming worker for job {job_id} finished (mechanism={mechanism})")


# ============================================================================
# EVENTHUB → SNOWFLAKE STREAMING WORKER (Main App)
# ============================================================================

def eventhub_streaming_worker(job_id: str, config: dict):
    """
    Background worker: consume from Azure EventHub → stream to Snowflake.
    Uses the main app's active_streaming_jobs state pattern.
    Supports both HP SDK (pipe-based) and Classic SQL INSERT.
    """
    global active_streaming_jobs

    mechanism = config.get('mechanism', 'snowpipe_classic')
    eh_conn_str = config.get('eh_conn_str', '')
    eh_name = config.get('eh_name', '')
    eh_consumer_group = config.get('eh_consumer_group', '$Default')
    target_table = config.get('target_table', '')
    batch_size = config.get('batch_size', 100)
    pipe_name = config.get('pipe_name', 'AMI_STREAMING_PIPE')

    logger.info(f"[{job_id}] Starting EventHub worker: hub={eh_name}, mechanism={mechanism}")

    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
            active_streaming_jobs[job_id]['stats'] = {
                'total_rows': 0, 'batches_sent': 0, 'errors': 0,
                'start_time': datetime.now(), 'last_batch_time': None,
                'mechanism': mechanism, 'source': 'eventhub',
                'batch_latencies': [], 'peak_rps': 0,
            }

    consumer = None
    hp_client = None

    try:
        from azure.eventhub import EventHubConsumerClient

        # Initialize HP SDK client if using HP mechanism
        if mechanism == 'snowpipe_hp':
            from snowpipe_streaming_impl import is_running_in_spcs
            has_hp_creds = is_running_in_spcs() or os.environ.get('SNOWFLAKE_PRIVATE_KEY') or os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH') or _connection_params.get('auth_mode') == 'keypair'
            if not has_hp_creds:
                logger.info(f"[{job_id}] Not in SPCS — EventHub worker using SQL INSERT")
                hp_client = None
                mechanism = 'snowpipe_classic'
            else:
                try:
                    from snowpipe_streaming_impl import HPStreamingClient, SnowpipeStreamingConfig
                    eh_hp_kwargs = {
                        'architecture': 'hp',
                        'pipe_name': pipe_name,
                        'client_name': f'flux_eh_hp_{job_id}',
                        'channel_name': f'flux_eh_ch_{job_id}',
                        'private_key': _connection_params.get('private_key_pem', ''),
                    }
                    session = get_valid_session()
                    if session:
                        try:
                            eh_hp_kwargs['account'] = session.get_current_account()
                            eh_hp_kwargs['user'] = session.get_current_user()
                            eh_hp_kwargs['role'] = session.get_current_role()
                            eh_hp_kwargs['database'] = session.get_current_database()
                            eh_hp_kwargs['schema'] = session.get_current_schema()
                        except Exception:
                            pass
                    hp_config = SnowpipeStreamingConfig(**eh_hp_kwargs)
                    hp_client = HPStreamingClient(hp_config)
                    if not hp_client.initialize():
                        logger.error(f"[{job_id}] HP SDK init failed, falling back to SQL INSERT")
                        hp_client = None
                        mechanism = 'snowpipe_classic'
                    else:
                        with streaming_lock:
                            if job_id in active_streaming_jobs:
                                active_streaming_jobs[job_id]['hp_client'] = hp_client
                except Exception as e:
                    logger.error(f"[{job_id}] HP SDK import/init error: {e}, falling back to SQL INSERT")
                    hp_client = None
                    mechanism = 'snowpipe_classic'

        batch_buffer = []
        batch_counter = 0

        def _flush(rows):
            nonlocal batch_counter
            if not rows:
                return
            batch_counter += 1
            try:
                batch_start = time.time()
                if mechanism == 'snowpipe_hp' and hp_client:
                    offset_token = f"{job_id}_eh_batch_{batch_counter}"
                    hp_client.write_rows(rows, offset_token=offset_token)
                    hp_client.flush(timeout_seconds=10)
                else:
                    session = get_valid_session()
                    if session:
                        columns = list(rows[0].keys())
                        data = [[row.get(c) for c in columns] for row in rows]
                        df = session.create_dataframe(data, schema=columns)
                        df.write.mode("append").save_as_table(target_table)

                batch_latency_ms = (time.time() - batch_start) * 1000

                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        s = active_streaming_jobs[job_id]['stats']
                        s['total_rows'] += len(rows)
                        s['batches_sent'] += 1
                        s['last_batch_time'] = datetime.now()
                        lats = s.get('batch_latencies', [])
                        lats.append(batch_latency_ms)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['batch_latencies'] = lats
                        elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                        rps = s['total_rows'] / elapsed
                        if rps > s.get('peak_rps', 0):
                            s['peak_rps'] = rps
            except Exception as e:
                logger.error(f"[{job_id}] EventHub flush error: {e}")
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['errors'] += 1

        def on_event(partition_context, event):
            nonlocal batch_buffer
            # Check stop signal
            with streaming_lock:
                if job_id not in active_streaming_jobs:
                    raise StopIteration("Job removed")
                if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                    raise StopIteration("Stop requested")

            if event is None:
                return

            try:
                body = event.body_as_json()
            except Exception:
                body = {"raw_data": event.body_as_str()}

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            row = {
                "METER_ID": str(body.get("meter_id", body.get("device_id", body.get("id", "EH-UNKNOWN")))),
                "READING_TIMESTAMP": str(body.get("reading_ts", body.get("timestamp", ts))),
                "USAGE_KWH": float(body.get("reading_value", body.get("value", body.get("usage_kwh", 0)))),
                "VOLTAGE": float(body.get("voltage", 120.0)),
                "TEMPERATURE_C": float(body.get("temperature_c", body.get("temperature", 25.0))),
                "SERVICE_AREA": str(body.get("service_area", "UNKNOWN")),
                "CUSTOMER_SEGMENT": str(body.get("customer_segment", "RESIDENTIAL")),
                "TRANSFORMER_ID": str(body.get("transformer_id", "")),
                "SUBSTATION_ID": str(body.get("substation_id", "")),
                "IS_OUTAGE": bool(body.get("is_outage", False)),
                "DATA_QUALITY": str(body.get("quality", body.get("data_quality", "VALID"))),
                "INGESTION_TIMESTAMP": ts,
            }
            batch_buffer.append(row)
            if len(batch_buffer) >= batch_size:
                _flush(batch_buffer[:])
                batch_buffer.clear()

        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=eh_conn_str,
            consumer_group=eh_consumer_group,
            eventhub_name=eh_name,
        )
        logger.info(f"[{job_id}] Connected to EventHub: {eh_name}")

        try:
            consumer.receive(on_event=on_event, starting_position="-1")
        except StopIteration:
            logger.info(f"[{job_id}] EventHub stop requested, flushing remaining")

        if batch_buffer:
            _flush(batch_buffer[:])
            batch_buffer.clear()

    except ImportError:
        logger.error(f"[{job_id}] azure-eventhub not installed")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['stats']['errors'] += 1
    except Exception as e:
        logger.exception(f"[{job_id}] EventHub worker error: {e}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['stats']['errors'] += 1
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        if hp_client:
            try:
                hp_client.close()
            except Exception:
                pass
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
        logger.info(f"[{job_id}] EventHub worker finished")


# ============================================================================
# GENERATE DATA → AZURE EVENTHUB PRODUCER WORKER
# ============================================================================

def eventhub_producer_worker(job_id: str, config: dict):
    """
    Background worker: generate synthetic AMI data → produce to Azure EventHub.
    Reuses the same data generation logic as snowpipe_streaming_worker but sends
    events to EventHub via EventHubProducerClient instead of writing to Snowflake.
    """
    global active_streaming_jobs

    eh_conn_str = config.get('eh_conn_str', '')
    eh_name = config.get('eh_name', '')
    meters = config.get('meters', 1000)
    rows_per_sec = config.get('rows_per_sec', 100)
    batch_size = config.get('batch_size', 100)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    data_format = config.get('data_format', 'standard')

    logger.info(f"[{job_id}] Starting EventHub Producer worker: hub={eh_name}, "
                f"{meters} meters, {rows_per_sec} rows/sec, batch_size={batch_size}")


    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
            active_streaming_jobs[job_id]['stats'] = {
                'total_rows': 0, 'batches_sent': 0, 'errors': 0,
                'consecutive_errors': 0, 'last_error': '',
                'start_time': datetime.now(), 'last_batch_time': None,
                'mechanism': 'eventhub_producer', 'source': 'eventhub_produce',
                'batch_latencies': [], 'peak_rps': 0,
            }

    producer = None
    try:
        from azure.eventhub import EventHubProducerClient, EventData

        # Strip EntityPath from connection string if present — pass hub name separately
        conn_str_clean = eh_conn_str
        if 'EntityPath=' in conn_str_clean:
            parts = [p for p in conn_str_clean.split(';') if not p.startswith('EntityPath=')]
            conn_str_clean = ';'.join(parts)
            if not conn_str_clean.endswith(';'):
                conn_str_clean += ';'

        producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str_clean,
            eventhub_name=eh_name,
        )
        logger.info(f"[{job_id}] Connected to EventHub producer: {eh_name}")

        # Build meter fleet (synthetic)
        meter_fleet = []
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-{service_area[:3]}-{i:06d}',
                'transformer_id': f'XFMR-{service_area[:3]}-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-{service_area[:3]}-{i // 100:04d}',
                'substation_id': f'SUB-{service_area[:3]}-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False,
            })

        # Grid topology state
        grid_state = GridTopologyState()
        for m in meter_fleet:
            grid_state.register_meter(m)
        topo = grid_state.summary()
        logger.info(f"[{job_id}] Grid topology: {topo['substations_total']} substations, "
                    f"{topo['circuits_total']} circuits, {topo['transformers_total']} transformers")

        batch_interval = batch_size / max(rows_per_sec, 1)
        batch_counter = 0

        while True:
            with streaming_lock:
                if job_id not in active_streaming_jobs:
                    logger.info(f"[{job_id}] Job removed, stopping producer")
                    break
                if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                    logger.info(f"[{job_id}] Stop requested")
                    active_streaming_jobs[job_id]['status'] = 'STOPPED'
                    break

            try:
                grid_state.tick()

                # Generate batch of readings
                batch = []
                for _ in range(min(batch_size, len(meter_fleet))):
                    meter = random.choice(meter_fleet)
                    if data_format == 'itron_grid_planning':
                        reading = generate_itron_grid_planning_row(
                            meter, service_area, grid_state=grid_state)
                    elif data_format == 'symphony_iris':
                        reading = generate_symphony_iris_row(
                            meter, service_area, grid_state=grid_state)
                    elif data_format == 'carto_spatial':
                        reading = generate_carto_spatial_row(
                            meter, service_area, grid_state=grid_state)
                    elif data_format == 'siemens_edge':
                        reading = generate_siemens_edge_row(
                            meter, service_area, grid_state=grid_state)
                    else:
                        reading = generate_ami_reading(
                            meter, service_area, emission_pattern,
                            grid_state=grid_state)
                    batch.append(reading)

                if not batch:
                    time.sleep(max(batch_interval, 0.1))
                    continue

                batch_counter += 1

                # Send batch as EventHub events
                batch_start = time.time()
                event_data_batch = producer.create_batch()
                for row in batch:
                    event_json = json.dumps(row, default=str)
                    try:
                        event_data_batch.add(EventData(event_json))
                    except ValueError:
                        # Batch full — send what we have and start a new one
                        producer.send_batch(event_data_batch)
                        event_data_batch = producer.create_batch()
                        event_data_batch.add(EventData(event_json))
                producer.send_batch(event_data_batch)
                batch_latency_ms = (time.time() - batch_start) * 1000

                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        s = active_streaming_jobs[job_id]['stats']
                        s['total_rows'] += len(batch)
                        s['batches_sent'] += 1
                        s['last_batch_time'] = datetime.now()
                        lats = s.get('batch_latencies', [])
                        lats.append(batch_latency_ms)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['batch_latencies'] = lats
                        elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                        rps = s['total_rows'] / elapsed
                        if rps > s.get('peak_rps', 0):
                            s['peak_rps'] = rps
                        s['grid_topology'] = grid_state.summary()
                        s['consecutive_errors'] = 0

                if batch_counter % 10 == 0 or batch_counter == 1:
                    logger.info(f"[{job_id}] Produced {len(batch)} events to EventHub "
                                f"(batch {batch_counter}, {batch_latency_ms:.0f}ms)")

            except Exception as e:
                logger.error(f"[{job_id}] EventHub producer error: {e}")
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        s = active_streaming_jobs[job_id]['stats']
                        s['errors'] += 1
                        s['consecutive_errors'] = s.get('consecutive_errors', 0) + 1
                        s['last_error'] = str(e)
                        if s['consecutive_errors'] >= 10:
                            logger.error(f"[{job_id}] Too many consecutive errors, stopping")
                            active_streaming_jobs[job_id]['status'] = 'STOPPED'
                            break

            time.sleep(max(batch_interval, 0.01))

    except ImportError:
        logger.error(f"[{job_id}] azure-eventhub not installed. pip install azure-eventhub")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'ERROR'
                active_streaming_jobs[job_id]['stats']['last_error'] = 'azure-eventhub not installed'
    except Exception as e:
        logger.error(f"[{job_id}] EventHub producer fatal error: {e}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'ERROR'
                active_streaming_jobs[job_id]['stats']['last_error'] = str(e)
    finally:
        if producer:
            try:
                producer.close()
            except Exception:
                pass
        with streaming_lock:
            if job_id in active_streaming_jobs and active_streaming_jobs[job_id]['status'] == 'RUNNING':
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
        logger.info(f"[{job_id}] EventHub producer worker finished")


# ============================================================================
# PUBSUB → SNOWFLAKE STREAMING WORKER (Main App)
# ============================================================================

def pubsub_streaming_worker(job_id: str, config: dict):
    """
    Background worker: consume from Google PubSub → stream to Snowflake.
    Uses the main app's active_streaming_jobs state pattern.
    """
    global active_streaming_jobs

    mechanism = config.get('mechanism', 'snowpipe_classic')
    ps_project = config.get('ps_project', '')
    ps_subscription = config.get('ps_subscription', '')
    target_table = config.get('target_table', '')
    batch_size = config.get('batch_size', 100)
    pipe_name = config.get('pipe_name', 'AMI_STREAMING_PIPE')

    logger.info(f"[{job_id}] Starting PubSub worker: project={ps_project}, sub={ps_subscription}")

    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
            active_streaming_jobs[job_id]['stats'] = {
                'total_rows': 0, 'batches_sent': 0, 'errors': 0,
                'start_time': datetime.now(), 'last_batch_time': None,
                'mechanism': mechanism, 'source': 'pubsub',
                'batch_latencies': [], 'peak_rps': 0,
            }

    subscriber = None
    streaming_pull_future = None
    hp_client = None

    try:
        from google.cloud import pubsub_v1

        # Initialize HP SDK client if using HP mechanism
        if mechanism == 'snowpipe_hp':
            from snowpipe_streaming_impl import is_running_in_spcs
            has_hp_creds = is_running_in_spcs() or os.environ.get('SNOWFLAKE_PRIVATE_KEY') or os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH') or _connection_params.get('auth_mode') == 'keypair'
            if not has_hp_creds:
                logger.info(f"[{job_id}] Not in SPCS — PubSub worker using SQL INSERT")
                hp_client = None
                mechanism = 'snowpipe_classic'
            else:
                try:
                    from snowpipe_streaming_impl import HPStreamingClient, SnowpipeStreamingConfig
                    ps_hp_kwargs = {
                        'architecture': 'hp',
                        'pipe_name': pipe_name,
                        'client_name': f'flux_ps_hp_{job_id}',
                        'channel_name': f'flux_ps_ch_{job_id}',
                        'private_key': _connection_params.get('private_key_pem', ''),
                    }
                    session = get_valid_session()
                    if session:
                        try:
                            ps_hp_kwargs['account'] = session.get_current_account()
                            ps_hp_kwargs['user'] = session.get_current_user()
                            ps_hp_kwargs['role'] = session.get_current_role()
                            ps_hp_kwargs['database'] = session.get_current_database()
                            ps_hp_kwargs['schema'] = session.get_current_schema()
                        except Exception:
                            pass
                    hp_config = SnowpipeStreamingConfig(**ps_hp_kwargs)
                    hp_client = HPStreamingClient(hp_config)
                    if not hp_client.initialize():
                        logger.error(f"[{job_id}] HP SDK init failed, falling back to SQL INSERT")
                        hp_client = None
                        mechanism = 'snowpipe_classic'
                    else:
                        with streaming_lock:
                            if job_id in active_streaming_jobs:
                                active_streaming_jobs[job_id]['hp_client'] = hp_client
                except Exception as e:
                    logger.error(f"[{job_id}] HP SDK import/init error: {e}, falling back to SQL INSERT")
                    hp_client = None
                    mechanism = 'snowpipe_classic'

        batch_buffer = []
        buffer_lock = threading.Lock()
        batch_counter = 0

        def _flush(rows):
            nonlocal batch_counter
            if not rows:
                return
            batch_counter += 1
            try:
                batch_start = time.time()
                if mechanism == 'snowpipe_hp' and hp_client:
                    offset_token = f"{job_id}_ps_batch_{batch_counter}"
                    hp_client.write_rows(rows, offset_token=offset_token)
                    hp_client.flush(timeout_seconds=10)
                else:
                    session = get_valid_session()
                    if session:
                        columns = list(rows[0].keys())
                        data = [[row.get(c) for c in columns] for row in rows]
                        df = session.create_dataframe(data, schema=columns)
                        df.write.mode("append").save_as_table(target_table)

                batch_latency_ms = (time.time() - batch_start) * 1000

                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        s = active_streaming_jobs[job_id]['stats']
                        s['total_rows'] += len(rows)
                        s['batches_sent'] += 1
                        s['last_batch_time'] = datetime.now()
                        lats = s.get('batch_latencies', [])
                        lats.append(batch_latency_ms)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['batch_latencies'] = lats
                        elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                        rps = s['total_rows'] / elapsed
                        if rps > s.get('peak_rps', 0):
                            s['peak_rps'] = rps
            except Exception as e:
                logger.error(f"[{job_id}] PubSub flush error: {e}")
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['errors'] += 1

        def callback(message):
            nonlocal batch_buffer
            # Check stop signal
            with streaming_lock:
                if job_id not in active_streaming_jobs:
                    message.nack()
                    return
                if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                    message.nack()
                    return

            try:
                body = json.loads(message.data.decode("utf-8"))
            except Exception:
                body = {"raw_data": message.data.decode("utf-8", errors="replace")}

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            row = {
                "METER_ID": str(body.get("meter_id", body.get("device_id", body.get("id", "PS-UNKNOWN")))),
                "READING_TIMESTAMP": str(body.get("reading_ts", body.get("timestamp", ts))),
                "USAGE_KWH": float(body.get("reading_value", body.get("value", body.get("usage_kwh", 0)))),
                "VOLTAGE": float(body.get("voltage", 120.0)),
                "TEMPERATURE_C": float(body.get("temperature_c", body.get("temperature", 25.0))),
                "SERVICE_AREA": str(body.get("service_area", "UNKNOWN")),
                "CUSTOMER_SEGMENT": str(body.get("customer_segment", "RESIDENTIAL")),
                "TRANSFORMER_ID": str(body.get("transformer_id", "")),
                "SUBSTATION_ID": str(body.get("substation_id", "")),
                "IS_OUTAGE": bool(body.get("is_outage", False)),
                "DATA_QUALITY": str(body.get("quality", body.get("data_quality", "VALID"))),
                "INGESTION_TIMESTAMP": ts,
            }
            message.ack()

            with buffer_lock:
                batch_buffer.append(row)
                if len(batch_buffer) >= batch_size:
                    _flush(batch_buffer[:])
                    batch_buffer.clear()

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(ps_project, ps_subscription)
        logger.info(f"[{job_id}] Subscribing to: {subscription_path}")

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

        # Block until stop requested
        while True:
            with streaming_lock:
                if job_id not in active_streaming_jobs:
                    break
                if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                    break
            time.sleep(1)

        # Flush remaining
        with buffer_lock:
            if batch_buffer:
                _flush(batch_buffer[:])
                batch_buffer.clear()

    except ImportError:
        logger.error(f"[{job_id}] google-cloud-pubsub not installed")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['stats']['errors'] += 1
    except Exception as e:
        logger.exception(f"[{job_id}] PubSub worker error: {e}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['stats']['errors'] += 1
    finally:
        if streaming_pull_future:
            try:
                streaming_pull_future.cancel()
                streaming_pull_future.result(timeout=5)
            except Exception:
                pass
        if subscriber:
            try:
                subscriber.close()
            except Exception:
                pass
        if hp_client:
            try:
                hp_client.close()
            except Exception:
                pass
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
        logger.info(f"[{job_id}] PubSub worker finished")


# ============================================================================
# V1 vs V2 COMPARISON STREAMING WORKER (Main App)
# ============================================================================

def comparison_streaming_worker(job_id: str, config: dict):
    """
    Background worker that runs HP SDK (V2) and Classic SQL INSERT (V1)
    in parallel on the same generated data, tracking separate latency stats
    for side-by-side comparison.
    """
    global active_streaming_jobs, snowflake_session

    logger.info(f"[{job_id}] Starting V1 vs V2 comparison worker")

    meters = config.get('meters', 1000)
    rows_per_sec = config.get('rows_per_sec', 100)
    batch_size = config.get('batch_size', 100)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    production_source = config.get('production_source', 'SYNTHETIC')
    data_format = config.get('data_format', 'standard')
    target_table = config.get('target_table', '')
    pipe_name = config.get('pipe_name', 'AMI_STREAMING_PIPE')

    # Create a second table for SQL INSERT comparison
    sql_insert_table = target_table + '_SQL_COMPARE' if target_table else ''

    # Initialize comparison stats
    # NOTE: "HP SDK" = real Snowpipe Streaming V2 (pipe-based, Python SDK)
    #        "SQL INSERT" = direct INSERT INTO via Snowpark session (NOT the Java-only Classic V1 SDK)
    stats = {
        'total_rows': 0, 'batches_sent': 0, 'errors': 0,
        'start_time': datetime.now(), 'last_batch_time': None,
        'mechanism': 'comparison', 'batch_latencies': [], 'peak_rps': 0,
        'hp_sdk_latencies': [], 'sql_insert_latencies': [],
        'hp_sdk_rows': 0, 'sql_insert_rows': 0,
        'hp_sdk_errors': 0, 'sql_insert_errors': 0,
    }

    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['stats'] = stats
            active_streaming_jobs[job_id]['status'] = 'RUNNING'

    # Initialize HP SDK client for pipe-based streaming
    hp_client = None
    from snowpipe_streaming_impl import is_running_in_spcs
    has_hp_creds = is_running_in_spcs() or os.environ.get('SNOWFLAKE_PRIVATE_KEY') or os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH') or _connection_params.get('auth_mode') == 'keypair'
    if not has_hp_creds:
        logger.info(f"[{job_id}] Not in SPCS — comparison will only run SQL INSERT")
    else:
        try:
            from snowpipe_streaming_impl import HPStreamingClient, SnowpipeStreamingConfig
            cmp_hp_kwargs = {
                'architecture': 'hp',
                'pipe_name': pipe_name,
                'client_name': f'flux_cmp_hp_{job_id}',
                'channel_name': f'flux_cmp_ch_{job_id}',
                'private_key': _connection_params.get('private_key_pem', ''),
            }
            session = get_valid_session()
            if session:
                try:
                    cmp_hp_kwargs['account'] = session.get_current_account()
                    cmp_hp_kwargs['user'] = session.get_current_user()
                    cmp_hp_kwargs['role'] = session.get_current_role()
                    cmp_hp_kwargs['database'] = session.get_current_database()
                    cmp_hp_kwargs['schema'] = session.get_current_schema()
                except Exception:
                    pass
            hp_config = SnowpipeStreamingConfig(**cmp_hp_kwargs)
            hp_client = HPStreamingClient(hp_config)
            if not hp_client.initialize():
                logger.error(f"[{job_id}] HP SDK init failed — comparison will only run SQL INSERT")
                hp_client = None
            else:
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['hp_client'] = hp_client
        except Exception as e:
            logger.error(f"[{job_id}] HP SDK error: {e} — comparison will only run SQL INSERT")
            hp_client = None

    # Ensure SQL INSERT comparison table exists
    try:
        session = get_valid_session()
        if session and sql_insert_table:
            session.sql(f"CREATE TABLE IF NOT EXISTS {sql_insert_table} LIKE {target_table}").collect()
            logger.info(f"[{job_id}] SQL INSERT comparison table ready: {sql_insert_table}")
    except Exception as e:
        logger.warning(f"[{job_id}] Could not create SQL INSERT comparison table: {e}")

    # Generate synthetic meter fleet (same as snowpipe_streaming_worker)
    meter_fleet = []
    try:
        session = get_valid_session()
        if session and production_source != 'SYNTHETIC':
            src_cfg = PRODUCTION_DATA_SOURCES.get(production_source)
            if src_cfg:
                result = session.sql(f"""
                    SELECT {src_cfg['meter_col']} as meter_id
                    FROM {src_cfg['table']}
                    ORDER BY RANDOM() LIMIT {meters}
                """).collect()
                for row in result:
                    meter_fleet.append({
                        'meter_id': row['METER_ID'],
                        'transformer_id': f'XFMR-CMP-{len(meter_fleet):05d}',
                        'circuit_id': f'CIRCUIT-CMP-{len(meter_fleet) // 100:04d}',
                        'substation_id': f'SUB-CMP-{len(meter_fleet) // 1000:03d}',
                        'customer_segment': 'RESIDENTIAL',
                        'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                        'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                        'production_matched': True,
                    })
    except Exception:
        pass

    if not meter_fleet:
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-CMP-{i:06d}',
                'transformer_id': f'XFMR-CMP-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-CMP-{i // 100:04d}',
                'substation_id': f'SUB-CMP-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False,
            })

    batch_interval = batch_size / max(rows_per_sec, 1)
    batch_counter = 0

    # --- Grid topology state: correlated outages & voltage cascading ---
    grid_state = GridTopologyState()
    for m in meter_fleet:
        grid_state.register_meter(m)

    while True:
        with streaming_lock:
            if job_id not in active_streaming_jobs:
                break
            if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
                break

        try:
            # Advance grid topology (probabilistic outage/recovery transitions)
            grid_state.tick()

            batch = []
            for _ in range(min(batch_size, len(meter_fleet))):
                meter = random.choice(meter_fleet)
                if data_format == 'itron_grid_planning':
                    reading = generate_itron_grid_planning_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'symphony_iris':
                    reading = generate_symphony_iris_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'carto_spatial':
                    reading = generate_carto_spatial_row(
                        meter, service_area, grid_state=grid_state)
                elif data_format == 'siemens_edge':
                    reading = generate_siemens_edge_row(
                        meter, service_area, grid_state=grid_state)
                else:
                    reading = generate_ami_reading(meter, service_area, emission_pattern,
                                                   grid_state=grid_state)
                batch.append(reading)

            if not batch:
                time.sleep(max(batch_interval, 0.1))
                continue

            batch_counter += 1

            # HP SDK — pipe-based Snowpipe Streaming (the real V2 path)
            hp_latency = None
            if hp_client:
                try:
                    offset_token = f"{job_id}_cmp_hp_{batch_counter}"
                    t0 = time.time()
                    hp_client.write_rows(batch, offset_token=offset_token)
                    hp_client.flush(timeout_seconds=10)
                    hp_latency = (time.time() - t0) * 1000
                except Exception as e:
                    logger.error(f"[{job_id}] HP SDK batch error: {e}")
                    with streaming_lock:
                        if job_id in active_streaming_jobs:
                            active_streaming_jobs[job_id]['stats']['hp_sdk_errors'] += 1

            # SQL INSERT — direct INSERT INTO via Snowpark (Python equivalent, NOT the Java Classic SDK)
            sql_latency = None
            try:
                session = get_valid_session()
                if session and sql_insert_table:
                    columns = list(batch[0].keys())
                    data = [[row.get(c) for c in columns] for row in batch]
                    df = session.create_dataframe(data, schema=columns)
                    t0 = time.time()
                    df.write.mode("append").save_as_table(sql_insert_table)
                    sql_latency = (time.time() - t0) * 1000
            except Exception as e:
                logger.error(f"[{job_id}] SQL INSERT batch error: {e}")
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['sql_insert_errors'] += 1

            # Update stats
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    s = active_streaming_jobs[job_id]['stats']
                    s['total_rows'] += len(batch)
                    s['batches_sent'] += 1
                    s['last_batch_time'] = datetime.now()
                    if hp_latency is not None:
                        s['hp_sdk_rows'] += len(batch)
                        lats = s.get('hp_sdk_latencies', [])
                        lats.append(hp_latency)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['hp_sdk_latencies'] = lats
                    if sql_latency is not None:
                        s['sql_insert_rows'] += len(batch)
                        lats = s.get('sql_insert_latencies', [])
                        lats.append(sql_latency)
                        if len(lats) > 1000:
                            lats = lats[-1000:]
                        s['sql_insert_latencies'] = lats
                    # Combined latencies for overall perf
                    combined = hp_latency if hp_latency is not None else sql_latency
                    if combined is not None:
                        all_lats = s.get('batch_latencies', [])
                        all_lats.append(combined)
                        if len(all_lats) > 1000:
                            all_lats = all_lats[-1000:]
                        s['batch_latencies'] = all_lats
                    elapsed = (datetime.now() - s['start_time']).total_seconds() or 1
                    rps = s['total_rows'] / elapsed
                    if rps > s.get('peak_rps', 0):
                        s['peak_rps'] = rps
                    s['grid_topology'] = grid_state.summary()

            time.sleep(max(batch_interval, 0.1))

        except Exception as e:
            logger.error(f"[{job_id}] Comparison worker error: {e}")
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['stats']['errors'] += 1
            time.sleep(1)

    if hp_client:
        try:
            hp_client.close()
        except Exception:
            pass

    logger.info(f"[{job_id}] Comparison worker finished")


def raw_json_s3_streaming_worker(job_id: str, config: dict):
    """
    Background worker that simulates smart meters streaming JSON to S3.
    Writes JSON files to S3 bucket, which Snowpipe auto-ingests into Snowflake.
    
    This simulates the real-world flow:
    Smart Meters → Data Collector → S3 (JSON files) → Snowpipe → Snowflake
    """
    global active_streaming_jobs, snowflake_session
    
    if not BOTO3_AVAILABLE:
        logger.error(f"boto3 not available - cannot start S3 streaming for job {job_id}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
                active_streaming_jobs[job_id]['stats']['errors'] += 1
        return
    
    logger.info(f"Starting Raw JSON S3 Streaming worker for job {job_id}")
    
    # Get config
    meters = config.get('meters', 1000)
    rows_per_batch = config.get('rows_per_batch', 100)
    batch_interval_sec = config.get('batch_interval_sec', 10)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    production_source = config.get('production_source', 'SYNTHETIC')
    
    # S3 config
    s3_bucket = config.get('s3_bucket', os.getenv('S3_BUCKET', 'your-s3-bucket'))
    s3_prefix = config.get('s3_prefix', 'raw/ami/')
    aws_access_key = config.get('aws_access_key_id', os.getenv('AWS_ACCESS_KEY_ID', ''))
    aws_secret_key = config.get('aws_secret_access_key', os.getenv('AWS_SECRET_ACCESS_KEY', ''))
    aws_region = config.get('aws_region', 'us-west-2')
    aws_role_arn = config.get('aws_role_arn', os.getenv('AWS_ROLE_ARN', ''))
    
    # Initialize stats
    stats = {
        'total_rows': 0,
        'files_written': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'last_file_time': None
    }
    
    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['stats'] = stats
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
    
    # Initialize S3 client with role assumption
    try:
        if aws_access_key and aws_secret_key:
            # First create STS client with base credentials
            if aws_role_arn:
                # Assume role to get temporary credentials
                sts_client = boto3.client(
                    'sts',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
                assumed_role = sts_client.assume_role(
                    RoleArn=aws_role_arn,
                    RoleSessionName=f'flux-s3-streaming-{job_id[:8]}'
                )
                creds = assumed_role['Credentials']
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=creds['AccessKeyId'],
                    aws_secret_access_key=creds['SecretAccessKey'],
                    aws_session_token=creds['SessionToken'],
                    region_name=aws_region
                )
                logger.info(f"Assumed role {aws_role_arn} for S3 access")
            else:
                # Use base credentials directly
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
        else:
            # Try default credentials (IAM role, env vars, etc.)
            s3_client = boto3.client('s3', region_name=aws_region)
        
        # Test connection
        s3_client.head_bucket(Bucket=s3_bucket)
        logger.info(f"S3 client initialized for bucket: {s3_bucket}")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
                active_streaming_jobs[job_id]['stats']['errors'] += 1
        return
    
    # Load meter fleet
    meter_fleet = []
    try:
        session = get_valid_session()
        if session and production_source != 'SYNTHETIC':
            src_cfg = PRODUCTION_DATA_SOURCES.get(production_source)
            if src_cfg:
                result = session.sql(f"""
                    SELECT 
                        {src_cfg['meter_col']} as meter_id,
                        {src_cfg.get('transformer_col', 'NULL')} as transformer_id,
                        {src_cfg.get('circuit_col', 'NULL')} as circuit_id,
                        {src_cfg.get('substation_col', 'NULL')} as substation_id,
                        COALESCE({src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') as customer_segment,
                        {src_cfg.get('lat_col', 'NULL')} as latitude,
                        {src_cfg.get('lon_col', 'NULL')} as longitude
                    FROM {src_cfg['table']}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                """).collect()
                
                for row in result:
                    meter_fleet.append({
                        'meter_id': row['METER_ID'],
                        'transformer_id': row['TRANSFORMER_ID'],
                        'circuit_id': row['CIRCUIT_ID'],
                        'substation_id': row['SUBSTATION_ID'],
                        'customer_segment': row['CUSTOMER_SEGMENT'],
                        'latitude': float(row['LATITUDE']) if row['LATITUDE'] else None,
                        'longitude': float(row['LONGITUDE']) if row['LONGITUDE'] else None,
                        'production_matched': True
                    })
                logger.info(f"Loaded {len(meter_fleet)} production meters for S3 streaming")
    except Exception as e:
        logger.error(f"Failed to load production meters: {e}")
    
    # Fallback to synthetic
    if not meter_fleet:
        logger.info(f"Generating {meters} synthetic meters for S3 streaming")
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-{service_area[:3]}-{i:06d}',
                'transformer_id': f'XFMR-{service_area[:3]}-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-{service_area[:3]}-{i // 100:04d}',
                'substation_id': f'SUB-{service_area[:3]}-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False
            })
    
    # Main streaming loop - write JSON batches to S3
    while True:
        # Check if job should stop
        with streaming_lock:
            if job_id not in active_streaming_jobs:
                logger.info(f"Job {job_id} removed, stopping S3 worker")
                break
            if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                logger.info(f"Job {job_id} stopping")
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
                break
        
        try:
            # Generate batch of JSON records
            batch_timestamp = datetime.now()
            batch_id = f"BATCH_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}_{batch_timestamp.microsecond}"
            
            records = []
            for _ in range(rows_per_batch):
                meter = random.choice(meter_fleet)
                reading = generate_ami_reading(meter, service_area, emission_pattern)
                
                # Convert to JSON-serializable format
                json_record = {
                    'meter_id': reading['METER_ID'],
                    'transformer_id': reading['TRANSFORMER_ID'],
                    'circuit_id': reading['CIRCUIT_ID'],
                    'substation_id': reading['SUBSTATION_ID'],
                    'reading_timestamp': reading['READING_TIMESTAMP'].isoformat(),
                    'usage_kwh': reading['USAGE_KWH'],
                    'voltage': reading['VOLTAGE'],
                    'power_factor': reading['POWER_FACTOR'],
                    'temperature_c': reading['TEMPERATURE_C'],
                    'service_area': reading['SERVICE_AREA'],
                    'customer_segment': reading['CUSTOMER_SEGMENT'],
                    'latitude': reading['LATITUDE'],
                    'longitude': reading['LONGITUDE'],
                    'is_outage': reading['IS_OUTAGE'],
                    'data_quality': reading['DATA_QUALITY'],
                    'batch_id': batch_id,
                    'emission_timestamp': batch_timestamp.isoformat()
                }
                records.append(json_record)
            
            # Write JSON array to S3
            json_content = json.dumps(records, indent=None)
            s3_key = f"{s3_prefix}ami_stream_{batch_id}.json"
            
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            # Update stats
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['stats']['total_rows'] += len(records)
                    active_streaming_jobs[job_id]['stats']['files_written'] += 1
                    active_streaming_jobs[job_id]['stats']['last_file_time'] = datetime.now()
            
            logger.debug(f"Job {job_id}: Wrote {len(records)} records to s3://{s3_bucket}/{s3_key}")
            
            # Sleep between batches
            time.sleep(batch_interval_sec)
            
        except Exception as e:
            logger.error(f"S3 streaming error for job {job_id}: {e}")
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['stats']['errors'] += 1
            time.sleep(5)  # Back off on error
    
    logger.info(f"Raw JSON S3 Streaming worker for job {job_id} finished")


def internal_stage_streaming_worker(job_id: str, config: dict):
    """
    Background worker that streams raw AMI JSON files to Snowflake internal stages.
    
    UTILITY PERSPECTIVE:
    This simulates how raw AMI data from smart meters lands in a staging area
    before being processed through the medallion architecture (bronze → silver → gold).
    
    Real-world flow:
    Smart Meters → MDM/Head-End System → Internal Stage (JSON files) → Snowpipe/Task → Bronze Table
    
    This is critical for demonstrating:
    1. Raw data preservation for compliance/audit (utility regulation)
    2. Schema-on-read flexibility with VARIANT columns
    3. Snowpipe auto-ingest from internal stages
    4. Dynamic Tables consuming from staged data
    """
    global active_streaming_jobs, snowflake_session
    
    logger.info(f"Starting Internal Stage Streaming worker for job {job_id}")
    
    # Get config
    meters = config.get('meters', 1000)
    rows_per_batch = config.get('rows_per_batch', 100)
    batch_interval_sec = config.get('batch_interval_sec', 10)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    production_source = config.get('production_source', 'SYNTHETIC')
    stage_name = config.get('stage_name', f'{DB}.{SCHEMA_PRODUCTION}.STG_AMI_RAW_JSON')
    file_format = config.get('stage_file_format', 'json')
    
    # Initialize stats
    stats = {
        'total_rows': 0,
        'files_written': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'last_file_time': None,
        'stage_name': stage_name
    }
    
    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['stats'] = stats
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
    
    # Load meter fleet
    meter_fleet = []
    try:
        session = get_valid_session()
        if session and production_source != 'SYNTHETIC':
            src_cfg = PRODUCTION_DATA_SOURCES.get(production_source)
            if src_cfg:
                result = session.sql(f"""
                    SELECT 
                        {src_cfg['meter_col']} as meter_id,
                        {src_cfg.get('transformer_col', 'NULL')} as transformer_id,
                        {src_cfg.get('circuit_col', 'NULL')} as circuit_id,
                        {src_cfg.get('substation_col', 'NULL')} as substation_id,
                        COALESCE({src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') as customer_segment,
                        {src_cfg.get('lat_col', 'NULL')} as latitude,
                        {src_cfg.get('lon_col', 'NULL')} as longitude
                    FROM {src_cfg['table']}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                """).collect()
                
                for row in result:
                    meter_fleet.append({
                        'meter_id': row['METER_ID'],
                        'transformer_id': row['TRANSFORMER_ID'],
                        'circuit_id': row['CIRCUIT_ID'],
                        'substation_id': row['SUBSTATION_ID'],
                        'customer_segment': row['CUSTOMER_SEGMENT'],
                        'latitude': float(row['LATITUDE']) if row['LATITUDE'] else None,
                        'longitude': float(row['LONGITUDE']) if row['LONGITUDE'] else None,
                        'production_matched': True
                    })
                logger.info(f"Loaded {len(meter_fleet)} production meters for stage streaming")
    except Exception as e:
        logger.error(f"Failed to load production meters for stage streaming: {e}")
    
    # Fallback to synthetic
    if not meter_fleet:
        logger.info(f"Generating {meters} synthetic meters for stage streaming")
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-{service_area[:3]}-{i:06d}',
                'transformer_id': f'XFMR-{service_area[:3]}-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-{service_area[:3]}-{i // 100:04d}',
                'substation_id': f'SUB-{service_area[:3]}-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False
            })
    
    # Main streaming loop - write JSON batches to internal stage
    while True:
        # Check if job should stop
        with streaming_lock:
            if job_id not in active_streaming_jobs:
                logger.info(f"Job {job_id} removed, stopping stage streaming worker")
                break
            if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                logger.info(f"Job {job_id} stopping stage streaming")
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
                break
        
        try:
            session = get_valid_session()
            if not session:
                logger.error(f"No valid Snowflake session for stage streaming job {job_id}")
                time.sleep(5)
                continue
            
            # Generate batch of JSON records
            batch_timestamp = datetime.now()
            batch_id = f"BATCH_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}_{batch_timestamp.microsecond}"
            
            records = []
            for _ in range(rows_per_batch):
                meter = random.choice(meter_fleet)
                reading = generate_ami_reading(meter, service_area, emission_pattern)
                
                # Build raw JSON record ( This mirrors real AMI JSON from meters)
                json_record = {
                    'header': {
                        'source_system': 'AMI_HEAD_END',
                        'message_type': 'METER_READING',
                        'batch_id': batch_id,
                        'emission_timestamp': batch_timestamp.isoformat(),
                        'version': '2.0'
                    },
                    'meter': {
                        'meter_id': reading['METER_ID'],
                        'transformer_id': reading['TRANSFORMER_ID'],
                        'circuit_id': reading['CIRCUIT_ID'],
                        'substation_id': reading['SUBSTATION_ID'],
                        'customer_segment': reading['CUSTOMER_SEGMENT'],
                        'service_area': reading['SERVICE_AREA'],
                        'geo': {
                            'latitude': reading['LATITUDE'],
                            'longitude': reading['LONGITUDE']
                        }
                    },
                    'reading': {
                        'timestamp': reading['READING_TIMESTAMP'].isoformat(),
                        'usage_kwh': reading['USAGE_KWH'],
                        'voltage': reading['VOLTAGE'],
                        'power_factor': reading['POWER_FACTOR'],
                        'temperature_c': reading['TEMPERATURE_C']
                    },
                    'quality': {
                        'data_quality': reading['DATA_QUALITY'],
                        'is_outage': reading['IS_OUTAGE'],
                        'production_matched': reading['PRODUCTION_MATCHED']
                    },
                    'metadata': {
                        'emission_pattern': reading['EMISSION_PATTERN'],
                        'ingestion_timestamp': datetime.now().isoformat()
                    }
                }
                records.append(json_record)
            
            # Write JSON to temp file and PUT to stage
            # Using NDJSON (newline-delimited JSON) for easier processing
            import tempfile
            
            file_name = f"ami_stream_{batch_id}.json"
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                for record in records:
                    f.write(json.dumps(record) + '\n')
                temp_file_path = f.name
            
            try:
                # PUT file to internal stage
                put_result = session.sql(f"""
                    PUT 'file://{temp_file_path}' @{stage_name}/{file_name}
                    AUTO_COMPRESS = TRUE
                    OVERWRITE = TRUE
                """).collect()
                
                logger.debug(f"PUT result for job {job_id}: {put_result}")
                
                # Update stats
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['total_rows'] += len(records)
                        active_streaming_jobs[job_id]['stats']['files_written'] += 1
                        active_streaming_jobs[job_id]['stats']['last_file_time'] = datetime.now()
                
                logger.debug(f"Job {job_id}: Wrote {len(records)} records to @{stage_name}/{file_name}")
            finally:
                # Clean up temp file
                import os
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
            
            # Sleep between batches
            time.sleep(batch_interval_sec)
            
        except Exception as e:
            logger.error(f"Internal stage streaming error for job {job_id}: {e}")
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['stats']['errors'] += 1
            time.sleep(5)  # Back off on error
    
    logger.info(f"Internal Stage Streaming worker for job {job_id} finished")


def external_stage_streaming_worker(job_id: str, config: dict):
    """
    Background worker that streams raw AMI JSON files to Snowflake external stages (S3/Azure/GCS).
    
    UTILITY PERSPECTIVE:
    This simulates how raw AMI data from smart meters lands in customer-managed cloud storage
    (e.g., customer S3 bucket) before being ingested by Snowflake via external stage + Snowpipe.
    
    Real-world flow:
    Smart Meters → Customer Data Collector → S3/Azure → External Stage → Snowpipe → Bronze Table
    
    This differs from internal stage by:
    1. Customer maintains control of raw data in their cloud storage
    2. Enables multi-cloud / hybrid scenarios
    3. Allows data sharing with non-Snowflake systems
    
    IMPORTANT: External stages don't support PUT/GET commands from Snowflake.
    We must use cloud SDKs (boto3 for S3) to write directly to the bucket.
    """
    global active_streaming_jobs, snowflake_session
    
    logger.info(f"Starting External Stage Streaming worker for job {job_id}")
    
    # Get config
    meters = config.get('meters', 1000)
    rows_per_batch = config.get('rows_per_batch', 100)
    batch_interval_sec = config.get('batch_interval_sec', 10)
    service_area = config.get('service_area', 'TEXAS_GULF_COAST')
    emission_pattern = config.get('emission_pattern', 'STAGGERED_REALISTIC')
    production_source = config.get('production_source', 'SYNTHETIC')
    stage_name = config.get('stage_name')
    
    if not stage_name:
        logger.error(f"No stage_name provided for external stage streaming job {job_id}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
        return
    
    # Check boto3 availability for external stages
    if not BOTO3_AVAILABLE:
        logger.error(f"boto3 not available - cannot stream to external S3 stage for job {job_id}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
                active_streaming_jobs[job_id]['stats']['errors'] += 1
        return
    
    # Initialize stats
    stats = {
        'total_rows': 0,
        'files_written': 0,
        'batches_sent': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'last_batch_time': None,
        'last_file_time': None,
        'stage_name': stage_name
    }
    
    with streaming_lock:
        if job_id in active_streaming_jobs:
            active_streaming_jobs[job_id]['stats'] = stats
            active_streaming_jobs[job_id]['status'] = 'RUNNING'
    
    # PATTERN: Discover pipes that reference this stage for auto-refresh
    # Without S3 event notifications, Snowpipe won't detect new files
    # We trigger ALTER PIPE REFRESH periodically to ensure data flows through
    associated_pipes = []
    try:
        session = get_valid_session()
        if session:
            for schema_path in [f"{DB}.PRODUCTION", f"{DB}.DEV"]:
                try:
                    result = session.sql(f"SHOW PIPES IN SCHEMA {schema_path}").collect()
                    for row in result:
                        row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                        pipe_def = row_dict.get('definition', '') or ''
                        pipe_name = row_dict.get('name', '')
                        # Check if pipe references this stage
                        if stage_name.upper() in pipe_def.upper() or f"@{stage_name}".upper() in pipe_def.upper():
                            full_pipe_name = f"{schema_path}.{pipe_name}"
                            associated_pipes.append(full_pipe_name)
                            logger.info(f"Job {job_id}: Found associated pipe {full_pipe_name} for stage {stage_name}")
                except Exception as e:
                    logger.debug(f"Could not check pipes in {schema_path}: {e}")
    except Exception as e:
        logger.warning(f"Job {job_id}: Could not discover associated pipes: {e}")
    
    #  Track refresh state
    files_since_last_refresh = 0
    REFRESH_EVERY_N_FILES = 5  # Refresh pipes every N files written
    
    # Get S3 bucket/prefix from external stage metadata
    s3_bucket = None
    s3_prefix = ''
    
    try:
        session = get_valid_session()
        if session:
            # Query stage metadata to get the S3 URL
            result = session.sql(f"DESC STAGE {stage_name}").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                prop_name = row_dict.get('property', row_dict.get('PROPERTY', ''))
                prop_val = row_dict.get('property_value', row_dict.get('PROPERTY_VALUE', ''))
                
                if prop_name.upper() == 'URL':
                    # Parse s3://bucket/prefix
                    # Handle JSON array format: ["s3://bucket/prefix/"]
                    url_value = prop_val
                    if url_value.startswith('[') and url_value.endswith(']'):
                        try:
                            import json
                            urls = json.loads(url_value)
                            if urls and len(urls) > 0:
                                url_value = urls[0]
                        except Exception:
                            # Try simple string extraction
                            url_value = url_value.strip('[]"\'')
                    
                    if url_value.startswith('s3://'):
                        parts = url_value[5:].rstrip('/').split('/', 1)
                        s3_bucket = parts[0]
                        s3_prefix = parts[1] + '/' if len(parts) > 1 else ''
                        logger.info(f"External stage {stage_name} maps to s3://{s3_bucket}/{s3_prefix}")
    except Exception as e:
        logger.error(f"Failed to get external stage metadata: {e}")
    
    if not s3_bucket:
        logger.error(f"Could not determine S3 bucket from stage {stage_name} for job {job_id}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
                active_streaming_jobs[job_id]['stats']['errors'] += 1
        return
    
    # Initialize S3 client
    # Try multiple credential sources: env vars, IAM role, default chain
    s3_client = None
    aws_region = 'us-west-2'  # Default region
    
    try:
        # Try to get credentials from environment or IAM role
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '')
        aws_role_arn = os.getenv('AWS_ROLE_ARN', '')
        
        if aws_access_key and aws_secret_key:
            if aws_role_arn:
                # Assume role for temporary credentials
                sts_client = boto3.client(
                    'sts',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
                assumed_role = sts_client.assume_role(
                    RoleArn=aws_role_arn,
                    RoleSessionName=f'flux-ext-stage-{job_id[:8]}'
                )
                creds = assumed_role['Credentials']
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=creds['AccessKeyId'],
                    aws_secret_access_key=creds['SecretAccessKey'],
                    aws_session_token=creds['SessionToken'],
                    region_name=aws_region
                )
                logger.info(f"Assumed role {aws_role_arn} for external stage S3 access")
            else:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
                logger.info("Using AWS env credentials for external stage S3 access")
        else:
            # Try default credential chain (IAM role, instance profile, etc.)
            s3_client = boto3.client('s3', region_name=aws_region)
            logger.info("Using default AWS credential chain for external stage S3 access")
        
        # Test connection
        s3_client.head_bucket(Bucket=s3_bucket)
        logger.info(f"S3 client verified access to bucket: {s3_bucket}")
        
    except Exception as e:
        logger.error(f"Failed to initialize S3 client for external stage: {e}")
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'FAILED'
                active_streaming_jobs[job_id]['stats']['errors'] += 1
        return
    
    # Load meter fleet
    meter_fleet = []
    try:
        session = get_valid_session()
        if session and production_source != 'SYNTHETIC':
            src_cfg = PRODUCTION_DATA_SOURCES.get(production_source)
            if src_cfg:
                result = session.sql(f"""
                    SELECT 
                        {src_cfg['meter_col']} as meter_id,
                        {src_cfg.get('transformer_col', 'NULL')} as transformer_id,
                        {src_cfg.get('circuit_col', 'NULL')} as circuit_id,
                        {src_cfg.get('substation_col', 'NULL')} as substation_id,
                        COALESCE({src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') as customer_segment,
                        {src_cfg.get('lat_col', 'NULL')} as latitude,
                        {src_cfg.get('lon_col', 'NULL')} as longitude
                    FROM {src_cfg['table']}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                """).collect()
                
                for row in result:
                    meter_fleet.append({
                        'meter_id': row['METER_ID'],
                        'transformer_id': row['TRANSFORMER_ID'],
                        'circuit_id': row['CIRCUIT_ID'],
                        'substation_id': row['SUBSTATION_ID'],
                        'customer_segment': row['CUSTOMER_SEGMENT'],
                        'latitude': float(row['LATITUDE']) if row['LATITUDE'] else None,
                        'longitude': float(row['LONGITUDE']) if row['LONGITUDE'] else None,
                        'production_matched': True
                    })
                logger.info(f"Loaded {len(meter_fleet)} production meters for external stage streaming")
    except Exception as e:
        logger.error(f"Failed to load production meters for external stage streaming: {e}")
    
    # Fallback to synthetic
    if not meter_fleet:
        logger.info(f"Generating {meters} synthetic meters for external stage streaming")
        for i in range(meters):
            segment = 'INDUSTRIAL' if i % 10 == 1 else ('COMMERCIAL' if i % 10 == 0 else 'RESIDENTIAL')
            meter_fleet.append({
                'meter_id': f'MTR-{service_area[:3]}-{i:06d}',
                'transformer_id': f'XFMR-{service_area[:3]}-{i // 10:05d}',
                'circuit_id': f'CIRCUIT-{service_area[:3]}-{i // 100:04d}',
                'substation_id': f'SUB-{service_area[:3]}-{i // 1000:03d}',
                'customer_segment': segment,
                'latitude': 29.7604 + random.uniform(-0.5, 0.5),
                'longitude': -95.3698 + random.uniform(-0.5, 0.5),
                'production_matched': False
            })
    
    # Main streaming loop - write JSON directly to S3 using boto3
    while True:
        # Check if job should stop
        with streaming_lock:
            if job_id not in active_streaming_jobs:
                logger.info(f"Job {job_id} removed, stopping external stage streaming worker")
                break
            if active_streaming_jobs[job_id]['status'] == 'STOPPING':
                logger.info(f"Job {job_id} stopping external stage streaming")
                active_streaming_jobs[job_id]['status'] = 'STOPPED'
                break
        
        try:
            # Generate batch of JSON records
            batch_timestamp = datetime.now()
            batch_id = f"BATCH_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}_{batch_timestamp.microsecond}"
            
            records = []
            for _ in range(rows_per_batch):
                meter = random.choice(meter_fleet)
                reading = generate_ami_reading(meter, service_area, emission_pattern)
                
                # Build raw JSON record (same nested structure as internal stage)
                json_record = {
                    'header': {
                        'source_system': 'AMI_HEAD_END',
                        'message_type': 'METER_READING',
                        'batch_id': batch_id,
                        'emission_timestamp': batch_timestamp.isoformat(),
                        'version': '2.0'
                    },
                    'meter': {
                        'meter_id': reading['METER_ID'],
                        'transformer_id': reading['TRANSFORMER_ID'],
                        'circuit_id': reading['CIRCUIT_ID'],
                        'substation_id': reading['SUBSTATION_ID'],
                        'customer_segment': reading['CUSTOMER_SEGMENT'],
                        'service_area': reading['SERVICE_AREA'],
                        'geo': {
                            'latitude': reading['LATITUDE'],
                            'longitude': reading['LONGITUDE']
                        }
                    },
                    'reading': {
                        'timestamp': reading['READING_TIMESTAMP'].isoformat(),
                        'usage_kwh': reading['USAGE_KWH'],
                        'voltage': reading['VOLTAGE'],
                        'power_factor': reading['POWER_FACTOR'],
                        'temperature_c': reading['TEMPERATURE_C']
                    },
                    'quality': {
                        'data_quality': reading['DATA_QUALITY'],
                        'is_outage': reading['IS_OUTAGE'],
                        'production_matched': reading['PRODUCTION_MATCHED']
                    },
                    'metadata': {
                        'emission_pattern': reading['EMISSION_PATTERN'],
                        'ingestion_timestamp': datetime.now().isoformat()
                    }
                }
                records.append(json_record)
            
            # Write JSON directly to S3 using boto3
            file_name = f"ami_stream_{batch_id}.json"
            s3_key = f"{s3_prefix}{file_name}" if s3_prefix else file_name
            
            # Convert records to NDJSON (newline-delimited JSON)
            json_content = '\n'.join(json.dumps(record) for record in records)
            
            try:
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=json_content.encode('utf-8'),
                    ContentType='application/json'
                )
                
                # Update stats
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['total_rows'] += len(records)
                        active_streaming_jobs[job_id]['stats']['files_written'] += 1
                        active_streaming_jobs[job_id]['stats']['batches_sent'] += 1
                        active_streaming_jobs[job_id]['stats']['last_file_time'] = datetime.now()
                        active_streaming_jobs[job_id]['stats']['last_batch_time'] = datetime.now()
                
                logger.info(f"Job {job_id}: Wrote {len(records)} records to s3://{s3_bucket}/{s3_key}")
                
                # PATTERN: Trigger pipe refresh after every N files
                # This ensures data flows through Snowpipe without relying on S3 event notifications
                files_since_last_refresh += 1
                if associated_pipes and files_since_last_refresh >= REFRESH_EVERY_N_FILES:
                    try:
                        refresh_session = get_valid_session()
                        if refresh_session:
                            for pipe_name in associated_pipes:
                                try:
                                    refresh_session.sql(f"ALTER PIPE {pipe_name} REFRESH").collect()
                                    logger.info(f"Job {job_id}: Triggered refresh for pipe {pipe_name}")
                                except Exception as pipe_err:
                                    logger.debug(f"Job {job_id}: Pipe refresh failed for {pipe_name}: {pipe_err}")
                            files_since_last_refresh = 0  # Reset counter
                    except Exception as refresh_err:
                        logger.debug(f"Job {job_id}: Could not refresh pipes: {refresh_err}")
                
            except Exception as s3_err:
                logger.error(f"S3 put_object failed for job {job_id}: {s3_err}")
                with streaming_lock:
                    if job_id in active_streaming_jobs:
                        active_streaming_jobs[job_id]['stats']['errors'] += 1
            
            # Sleep between batches
            time.sleep(batch_interval_sec)
            
        except Exception as e:
            logger.error(f"External stage streaming error for job {job_id}: {e}")
            with streaming_lock:
                if job_id in active_streaming_jobs:
                    active_streaming_jobs[job_id]['stats']['errors'] += 1
            time.sleep(5)  # Back off on error
    
    logger.info(f"External Stage Streaming worker for job {job_id} finished")


def preload_dependencies_background():
    """
    PATTERN: Background preloading of dependencies on app startup.
    Caches tables, pipes, stages to improve UX when user navigates to pipeline steps.
    Runs in a background thread to not block app startup.
    """
    global dependency_cache
    
    try:
        session = get_valid_session()
        if not session:
            logger.warning("preload_dependencies: No valid session, skipping preload")
            return
        
        logger.info("preload_dependencies: Starting background preload of dependencies...")
        
        # Preload pipes from multiple schemas
        try:
            pipes = []
            seen_pipes = set()
            schemas_to_check = [f"{DB}.PRODUCTION", f"{DB}.DEV"]
            
            for schema_path in schemas_to_check:
                try:
                    result = session.sql(f"SHOW PIPES IN SCHEMA {schema_path}").collect()
                    for row in result:
                        row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                        pipe_info = {
                            'name': row_dict.get('name', ''),
                            'database': row_dict.get('database_name', ''),
                            'schema': row_dict.get('schema_name', ''),
                            'definition': row_dict.get('definition', ''),
                            'owner': row_dict.get('owner', ''),
                            'notification_channel': row_dict.get('notification_channel', ''),
                            'comment': row_dict.get('comment', ''),
                        }
                        full_name = f"{pipe_info['database']}.{pipe_info['schema']}.{pipe_info['name']}"
                        if full_name not in seen_pipes:
                            seen_pipes.add(full_name)
                            pipe_info['full_name'] = full_name
                            # Determine if it's an external stage pipe
                            definition = pipe_info.get('definition', '').upper()
                            pipe_info['is_external'] = any(x in definition for x in ['S3://', 'AZURE://', 'GCS://'])
                            pipe_info['auto_ingest'] = 'AUTO_INGEST' in definition
                            pipes.append(pipe_info)
                except Exception as e:
                    logger.warning(f"preload_dependencies: Could not load pipes from {schema_path}: {e}")
            
            # Sort by schema then name for consistent ordering
            pipes.sort(key=lambda x: (x['schema'], x['name']))
            
            with dependency_cache['lock']:
                dependency_cache['pipes'] = pipes
            logger.info(f"preload_dependencies: Cached {len(pipes)} pipes from {len(schemas_to_check)} schemas")
        except Exception as e:
            logger.warning(f"preload_dependencies: Failed to preload pipes: {e}")
        
        # Preload stages
        try:
            result = session.sql("SHOW STAGES IN ACCOUNT").collect()
            stages = {'internal': [], 'external': []}
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                stage_type = row_dict.get('type', '').upper()
                stage_info = {
                    'name': row_dict.get('name', ''),
                    'database': row_dict.get('database_name', ''),
                    'schema': row_dict.get('schema_name', ''),
                    'type': stage_type,
                    'url': row_dict.get('url', ''),
                    'owner': row_dict.get('owner', ''),
                    'comment': row_dict.get('comment', ''),
                }
                stage_info['full_name'] = f"{stage_info['database']}.{stage_info['schema']}.{stage_info['name']}"
                
                # Determine cloud provider for external stages
                url = stage_info['url'] or ''
                if stage_type == 'EXTERNAL':
                    if 's3://' in url.lower():
                        stage_info['cloud_provider'] = 'AWS S3'
                    elif 'azure://' in url.lower() or 'blob.core.windows.net' in url.lower():
                        stage_info['cloud_provider'] = 'Azure Blob'
                    elif 'gcs://' in url.lower() or 'storage.googleapis.com' in url.lower():
                        stage_info['cloud_provider'] = 'Google Cloud Storage'
                    else:
                        stage_info['cloud_provider'] = 'External'
                    stages['external'].append(stage_info)
                else:
                    stage_info['cloud_provider'] = 'Snowflake Internal'
                    stages['internal'].append(stage_info)
            
            # Sort by full_name for consistent ordering
            stages['internal'].sort(key=lambda x: x['full_name'])
            stages['external'].sort(key=lambda x: x['full_name'])
            
            with dependency_cache['lock']:
                dependency_cache['stages'] = stages
            logger.info(f"preload_dependencies: Cached {len(stages['internal'])} internal, {len(stages['external'])} external stages")
        except Exception as e:
            logger.warning(f"preload_dependencies: Failed to preload stages: {e}")
        
        # Preload bronze tables
        try:
            result = session.sql(f"""
                SELECT table_catalog, table_schema, table_name, row_count, bytes
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
                WHERE DELETED IS NULL 
                AND table_schema IN ('PRODUCTION', 'DEV')
                AND table_catalog = '{DB}'
                AND (UPPER(table_name) LIKE '%BRONZE%' OR UPPER(table_name) LIKE '%RAW%' OR UPPER(table_name) LIKE '%AMI%')
                ORDER BY table_schema, table_name
            """).collect()
            
            tables = []
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                db = row_dict.get('TABLE_CATALOG', '')
                schema = row_dict.get('TABLE_SCHEMA', '')
                name = row_dict.get('TABLE_NAME', '')
                tables.append({
                    'database': db,
                    'schema': schema,
                    'name': name,
                    'full_name': f"{db}.{schema}.{name}",
                    'row_count': row_dict.get('ROW_COUNT', 0),
                    'bytes': row_dict.get('BYTES', 0),
                    'has_variant': True,  # These are known bronze tables
                })
            
            with dependency_cache['lock']:
                dependency_cache['tables'] = tables
            logger.info(f"preload_dependencies: Cached {len(tables)} bronze/raw tables")
        except Exception as e:
            logger.warning(f"preload_dependencies: Failed to preload tables: {e}")
        
        # Mark cache as refreshed
        with dependency_cache['lock']:
            dependency_cache['last_refresh'] = datetime.now()
        
        logger.info("preload_dependencies: Background preload complete!")
        
    except Exception as e:
        logger.error(f"preload_dependencies: Background preload failed: {e}")


@asynccontextmanager
def _post_connect_setup():
    """Run stale-job reconciliation and dependency preloading after a session is established."""
    global snowflake_session
    try:
        if snowflake_session:
            logger.info("Reconciling stale streaming job states...")
            snowflake_session.sql(f"""
                UPDATE {DB}.{SCHEMA_PRODUCTION}.STREAMING_JOBS 
                SET STATUS = 'STALE', 
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE STATUS = 'RUNNING'
            """).collect()
            logger.info("Stale job reconciliation complete - marked orphaned RUNNING jobs as STALE")
    except Exception as e:
        logger.warning(f"Could not reconcile stale jobs: {e}")
    try:
        preload_thread = threading.Thread(target=preload_dependencies_background, daemon=True)
        preload_thread.start()
        logger.info("Started background dependency preloading thread")
    except Exception as e:
        logger.warning(f"Could not start background preload: {e}")


async def lifespan(app: FastAPI):
    global snowflake_session
    logger.info("Starting FLUX Data Forge...")
    try:
        snowflake_session = create_snowflake_session()
        logger.info("Snowflake connected!")
        _post_connect_setup()
    except Exception as e:
        logger.warning(f"No auto-connection available — waiting for user to connect via UI. ({e})")
        snowflake_session = None
    
    yield
    logger.info("Shutting down...")
    if snowflake_session:
        snowflake_session.close()


app = FastAPI(title="FLUX Data Forge", version="5.0", lifespan=lifespan)


@app.get("/api/cache/status")
async def get_cache_status():
    """
     Check the status of the dependency cache for debugging.
    Returns what has been preloaded and when.
    """
    with dependency_cache['lock']:
        return {
            "pipes_cached": dependency_cache['pipes'] is not None,
            "pipes_count": len(dependency_cache['pipes']) if dependency_cache['pipes'] else 0,
            "stages_cached": dependency_cache['stages'] is not None,
            "stages_count": {
                "internal": len(dependency_cache['stages']['internal']) if dependency_cache['stages'] else 0,
                "external": len(dependency_cache['stages']['external']) if dependency_cache['stages'] else 0,
            },
            "tables_cached": dependency_cache['tables'] is not None,
            "tables_count": len(dependency_cache['tables']) if dependency_cache['tables'] else 0,
            "last_refresh": str(dependency_cache['last_refresh']) if dependency_cache['last_refresh'] else None,
        }


@app.get("/logo.png")
async def get_logo():
    image_data = base64.b64decode(FLUX_LOGO_BASE64)
    return Response(content=image_data, media_type="image/png")


def _py_syntax_color(code: str) -> str:
    """Apply manual Python syntax coloring via regex (no third-party libs)."""
    import re
    import html as _html
    escaped = _html.escape(code)
    # Comments (must come first so they aren't overridden)
    escaped = re.sub(r'(#[^\n]*)', r'<span style="color:#64748b;font-style:italic;">\1</span>', escaped)
    # Triple-quote docstrings
    escaped = re.sub(r'(&quot;&quot;&quot;)(.*?)(&quot;&quot;&quot;)', r'<span style="color:#86efac;">\1\2\3</span>', escaped, flags=re.DOTALL)
    # Strings (single and double quoted)
    escaped = re.sub(r'(&#x27;[^&#]*?&#x27;)', r'<span style="color:#86efac;">\1</span>', escaped)
    escaped = re.sub(r'(&quot;[^&]*?&quot;)', r'<span style="color:#86efac;">\1</span>', escaped)
    # f-string prefix
    escaped = re.sub(r'\bf(&#x27;|&quot;)', r'<span style="color:#86efac;">f</span><span style="color:#86efac;">\1', escaped)
    # Keywords
    kw = r'\b(from|import|as|def|class|return|if|else|elif|for|in|while|with|try|except|finally|raise|yield|async|await|True|False|None|and|or|not|is|lambda|pass|break|continue|global|nonlocal|assert|del)\b'
    escaped = re.sub(kw, r'<span style="color:#c084fc;">\1</span>', escaped)
    # Decorators
    escaped = re.sub(r'(@\w+)', r'<span style="color:#fbbf24;">\1</span>', escaped)
    # Numbers
    escaped = re.sub(r'\b(\d+\.?\d*)\b', r'<span style="color:#fbbf24;">\1</span>', escaped)
    # Function calls
    escaped = re.sub(r'(\w+)\(', r'<span style="color:#7dd3fc;">\1</span>(', escaped)
    return escaped


def _sql_syntax_color(code: str) -> str:
    """Apply manual SQL syntax coloring."""
    import re
    import html as _html
    escaped = _html.escape(code)
    # Comments
    escaped = re.sub(r'(--[^\n]*)', r'<span style="color:#64748b;font-style:italic;">\1</span>', escaped)
    # SQL keywords
    kw = r'\b(CREATE|OR|REPLACE|PIPE|AS|COPY|INTO|FROM|SELECT|TABLE|DATA_SOURCE|TYPE|STREAMING|VARCHAR|FLOAT|BOOLEAN|TIMESTAMP_NTZ|COALESCE|CURRENT_TIMESTAMP|CLUSTER_AT_INGEST_TIME|TRUE|FALSE|GRANT|ON|TO|ROLE|OPERATE|INSERT|EVOLVE|SCHEMA|MATCH_BY_COLUMN_NAME|CASE_INSENSITIVE)\b'
    escaped = re.sub(kw, r'<span style="color:#c084fc;">\1</span>', escaped, flags=re.IGNORECASE)
    # Strings
    escaped = re.sub(r"(&#x27;[^&]*?&#x27;)", r'<span style="color:#86efac;">\1</span>', escaped)
    # Column references ($1:NAME)
    escaped = re.sub(r'(\$\d+:\w+)', r'<span style="color:#7dd3fc;">\1</span>', escaped)
    # Type casts (::TYPE)
    escaped = re.sub(r'(::)(\w+(?:\(\d+\))?)', r'<span style="color:#fbbf24;">\1\2</span>', escaped)
    return escaped


def _build_code_showcase_panel(mechanism: str) -> str:
    """Build the HP SDK code showcase panel with mini-wizard. Returns empty string if not HP mechanism."""
    if mechanism != 'snowpipe_hp':
        return ''

    # Placeholder tokens — replaced AFTER syntax coloring with <span> elements
    P = {
        'account': '__WIZ_ACCOUNT__',
        'user': '__WIZ_USER__',
        'role': '__WIZ_ROLE__',
        'database': '__WIZ_DATABASE__',
        'schema': '__WIZ_SCHEMA__',
        'pipe': '__WIZ_PIPE__',
    }

    # --- Tab 1: Prerequisites ---
    prereq_code = f"""# Install the HP Snowpipe Streaming SDK
pip install snowpipe-streaming

# Generate an RSA key pair for JWT auth
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Register the public key with your Snowflake user
# ALTER USER {P['user']} SET RSA_PUBLIC_KEY='<paste contents of rsa_key.pub>';"""

    # --- Tab 2: Create PIPE DDL ---
    pipe_ddl = f"""-- Create the target table
CREATE TABLE IF NOT EXISTS {P['database']}.{P['schema']}.SENSOR_READINGS (
    DEVICE_ID       VARCHAR(50),
    READING_TIME    TIMESTAMP_NTZ,
    VALUE           FLOAT,
    QUALITY         VARCHAR(20)  DEFAULT 'VALID',
    INGESTED_AT     TIMESTAMP_NTZ
);

-- Create a streaming PIPE with server-side transforms
CREATE OR REPLACE PIPE {P['database']}.{P['schema']}.{P['pipe']}
AS
COPY INTO {P['database']}.{P['schema']}.SENSOR_READINGS (
    DEVICE_ID, READING_TIME, VALUE, QUALITY, INGESTED_AT
)
FROM (
    SELECT
        $1:DEVICE_ID::VARCHAR(50),
        $1:READING_TIME::TIMESTAMP_NTZ,
        $1:VALUE::FLOAT,
        COALESCE($1:QUALITY::VARCHAR(20), 'VALID'),
        CURRENT_TIMESTAMP()
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
)
CLUSTER_AT_INGEST_TIME = TRUE;

-- Grant permissions
GRANT OPERATE ON PIPE {P['database']}.{P['schema']}.{P['pipe']} TO ROLE {P['role']};"""

    # --- Tab 3: Stream Data (the core SDK usage) ---
    stream_code = f"""from snowflake.ingest.streaming import StreamingIngestClient

# 1. Load your private key
with open('rsa_key.p8', 'r') as f:
    private_key = f.read()

# 2. Connect to Snowflake
client = StreamingIngestClient(
    client_name='my_app',
    db_name='{P['database']}',
    schema_name='{P['schema']}',
    pipe_name='{P['pipe']}',
    properties={{
        'url': 'https://{P['account']}.snowflakecomputing.com',
        'account': '{P['account']}',
        'user': '{P['user']}',
        'private_key': private_key,
        'role': '{P['role']}',
    }},
)

# 3. Open a channel
channel, status = client.open_channel(channel_name='ch1')

# 4. Stream rows
channel.append_rows(
    rows=[
        {{"DEVICE_ID": "S-001", "READING_TIME": "2026-01-17T12:00:00", "VALUE": 23.5}},
        {{"DEVICE_ID": "S-002", "READING_TIME": "2026-01-17T12:00:00", "VALUE": 18.1}},
    ],
    start_offset_token="batch_1",
    end_offset_token="batch_1",
)

# 5. Flush to Snowflake
channel.initiate_flush()
channel.wait_for_flush(timeout_seconds=30)

# 6. Check status
statuses = client.get_channel_statuses(["ch1"])
print(f"Rows landed: {{statuses['ch1'].rows_inserted_count}}")

# 7. Clean up
channel.close()
client.close()"""

    # --- Tab 4: Full Mini-App ---
    full_app_code = f"""#!/usr/bin/env python3
\"\"\"Minimal HP streaming app - generates and streams sensor data.\"\"\"
import time, random, datetime
from snowflake.ingest.streaming import StreamingIngestClient

with open('rsa_key.p8') as f:
    key = f.read()

client = StreamingIngestClient(
    client_name='mini_demo', db_name='{P['database']}',
    schema_name='{P['schema']}', pipe_name='{P['pipe']}',
    properties={{
        'url': 'https://{P['account']}.snowflakecomputing.com',
        'account': '{P['account']}', 'user': '{P['user']}',
        'private_key': key, 'role': '{P['role']}',
    }},
)
channel, _ = client.open_channel(channel_name='demo_ch')

try:
    batch_num = 0
    while True:
        rows = [
            {{
                "DEVICE_ID": f"S-{{i:04d}}",
                "READING_TIME": datetime.datetime.now().isoformat(),
                "VALUE": round(random.uniform(10, 40), 2),
            }}
            for i in range(100)  # 100 rows per batch
        ]
        token = f"batch_{{batch_num}}"
        channel.append_rows(rows=rows, start_offset_token=token, end_offset_token=token)
        channel.initiate_flush()
        channel.wait_for_flush(timeout_seconds=30)
        batch_num += 1
        print(f"Batch {{batch_num}}: sent {{len(rows)}} rows")
        time.sleep(1)  # 1 batch/sec
except KeyboardInterrupt:
    print("Shutting down...")
finally:
    channel.close()
    client.close()"""

    # --- Tab 5: EventHub → HP SDK Bridge ---
    eventhub_code = f"""#!/usr/bin/env python3
\"\"\"Azure EventHub -> Snowflake HP Streaming bridge.

Consumes events from Azure EventHub and streams them into
Snowflake via Snowpipe Streaming HP SDK (V2).

Requires:
    pip install snowpipe-streaming azure-eventhub cryptography
\"\"\"
import json, time, threading
from datetime import datetime, timezone
from azure.eventhub import EventHubConsumerClient
from snowflake.ingest.streaming import StreamingIngestClient

# ── Snowflake config (uses wizard values) ─────────────
ACCOUNT  = "{P['account']}"
USER     = "{P['user']}"
ROLE     = "{P['role']}"
DATABASE = "{P['database']}"
SCHEMA   = "{P['schema']}"
PIPE     = "{P['pipe']}"
KEY_PATH = "rsa_key.p8"  # PKCS8 PEM

# ── EventHub config ───────────────────────────────────
EVENTHUB_CONN_STR = "<Endpoint=sb://...>"
EVENTHUB_NAME     = "<hub-name>"
CONSUMER_GROUP    = "$Default"
BATCH_SIZE = 500

# ── Setup HP SDK client & channel ─────────────────────
with open(KEY_PATH) as f:
    key = f.read()

client = StreamingIngestClient(
    client_name="eventhub_bridge",
    db_name=DATABASE, schema_name=SCHEMA, pipe_name=PIPE,
    properties={{
        "url": f"https://{{ACCOUNT}}.snowflakecomputing.com",
        "account": ACCOUNT, "user": USER,
        "private_key": key, "role": ROLE,
    }},
)
channel, _ = client.open_channel(channel_name="eh_ch_0")

# ── Buffer & flush logic ─────────────────────────────
buffer = []
lock = threading.Lock()
batch_num = 0

def flush():
    global batch_num
    with lock:
        if not buffer:
            return
        rows = list(buffer)
        buffer.clear()
    token = f"batch_{{batch_num}}"
    channel.append_rows(rows=rows,
        start_offset_token=token, end_offset_token=token)
    channel.initiate_flush()
    channel.wait_for_flush(timeout_seconds=30)
    batch_num += 1
    print(f"Flushed batch {{batch_num}}: {{len(rows)}} rows")

# ── EventHub consumer callback ────────────────────────
def on_event(partition_context, event):
    try:
        body = json.loads(event.body_as_str())
    except Exception:
        body = {{"RAW_DATA": event.body_as_str()}}
    body["_PARTITION"] = partition_context.partition_id
    body["_ENQUEUED_AT"] = str(event.enqueued_time)
    body["_INGESTED_AT"] = datetime.now(timezone.utc).isoformat()
    with lock:
        buffer.append(body)
        if len(buffer) >= BATCH_SIZE:
            flush()

# ── Main loop ─────────────────────────────────────────
consumer = EventHubConsumerClient.from_connection_string(
    EVENTHUB_CONN_STR, CONSUMER_GROUP,
    eventhub_name=EVENTHUB_NAME,
)
print("Listening for EventHub events... (Ctrl+C to stop)")
try:
    with consumer:
        consumer.receive(on_event=on_event, starting_position="-1")
except KeyboardInterrupt:
    flush()  # drain remaining
    channel.close()
    client.close()
    print("Shutdown complete.")"""

    # Doc links for each tab annotation
    doc_links = {
        'setup': '<a class="code-doc-link" href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started" target="_blank">Getting Started Tutorial &nearr;</a>',
        'ddl': '<a class="code-doc-link" href="https://docs.snowflake.com/en/sql-reference/sql/create-pipe" target="_blank">CREATE PIPE Reference &nearr;</a>',
        'stream': '<a class="code-doc-link" href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-python-sdk-reference" target="_blank">Python SDK Reference &nearr;</a>',
        'app': '<a class="code-doc-link" href="https://pypi.org/project/snowpipe-streaming/" target="_blank">PyPI: snowpipe-streaming &nearr;</a>',
        'eventhub': '<a class="code-doc-link" href="https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send" target="_blank">Azure EventHub SDK &nearr;</a>',
    }

    tabs = [
        ("setup", "1. Prerequisites", prereq_code, "Install SDK + generate RSA key pair for authentication.", "bash", 3),
        ("ddl", "2. Create PIPE", pipe_ddl, "Define your target table and streaming PIPE with server-side transforms.", "sql", None),
        ("stream", "3. Stream Data", stream_code, "Connect, open a channel, stream rows, flush, done. That's the whole SDK.", "python", 7),
        ("app", "4. Full App", full_app_code, "A complete streaming app in ~35 lines. Copy, edit credentials, run.", "python", 35),
        ("eventhub", "5. EventHub Bridge", eventhub_code, "Stream from Azure EventHub into Snowflake via HP SDK. Production-ready bridge pattern.", "python", 40),
    ]

    def _inject_param_spans(html: str) -> str:
        """Replace placeholder tokens with <span class='code-param'> elements after syntax coloring."""
        for field, token in P.items():
            html = html.replace(token, f'<span class="code-param" data-field="{field}">{token}</span>')
        return html

    tab_buttons = ""
    tab_contents = ""
    for i, (tab_id, label, code, annotation, lang, line_count) in enumerate(tabs):
        active = " active" if i == 0 else ""
        line_badge = f' <span class="code-line-count">{line_count} lines of real code</span>' if line_count else ""
        tab_buttons += f'<button class="code-tab-btn{active}" data-codetab="{tab_id}">{label}{line_badge}</button>\n'

        colored = _sql_syntax_color(code.strip()) if lang == "sql" else _py_syntax_color(code.strip())
        colored = _inject_param_spans(colored)
        link_html = doc_links.get(tab_id, '')
        tab_contents += f'''
        <div class="code-tab-content{active}" data-codetab="{tab_id}">
            <div class="code-tab-annotation">{get_material_icon('info', '14px', '#64748b')} {annotation} {link_html}</div>
            <div style="position: relative;">
                <button class="code-copy-btn" onclick="copyCodeTab(this, '{tab_id}')">Copy</button>
                <pre><code>{colored}</code></pre>
            </div>
        </div>'''

    # Default values for the wizard fields — these match the placeholder tokens
    defaults = {
        'account': 'MY_ACCOUNT',
        'user': 'MY_USER',
        'role': 'SYSADMIN',
        'database': 'MY_DB',
        'schema': 'MY_SCHEMA',
        'pipe': 'SENSOR_PIPE',
    }

    return f'''
            <div class="panel code-showcase" style="margin-top: 24px;">
                <div class="panel-title code-showcase-toggle" onclick="toggleCodeShowcase()">
                    {get_material_icon('integration_instructions', '20px', '#f59e0b')} Build Your Own HP Streaming App
                    <span style="margin-left: auto; font-size: 0.75rem; font-weight: 400;">
                        <a class="code-doc-link" href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview" target="_blank" onclick="event.stopPropagation();">HP Architecture Docs &nearr;</a>
                    </span>
                    <span class="material-symbols-outlined chevron" id="code-showcase-chevron">expand_more</span>
                </div>
                <div id="code-showcase-body" style="display: none;">
                    <div style="padding: 12px 16px 4px; color: #94a3b8; font-size: 0.85rem;">
                        Fill in your Snowflake details below &mdash; all code snippets update automatically.
                    </div>
                    <div class="code-wizard" id="codeWizard">
                        <label>Account<input type="text" id="wiz_account" value="{defaults['account']}" placeholder="xy12345"></label>
                        <label>User<input type="text" id="wiz_user" value="{defaults['user']}" placeholder="MY_USER"></label>
                        <label>Role<input type="text" id="wiz_role" value="{defaults['role']}" placeholder="SYSADMIN"></label>
                        <label>Database<input type="text" id="wiz_database" value="{defaults['database']}" placeholder="MY_DB"></label>
                        <label>Schema<input type="text" id="wiz_schema" value="{defaults['schema']}" placeholder="MY_SCHEMA"></label>
                        <label>Pipe Name<input type="text" id="wiz_pipe" value="{defaults['pipe']}" placeholder="SENSOR_PIPE"></label>
                    </div>
                    <div style="padding: 16px;">
                        <div class="code-tabs">
                            {tab_buttons}
                        </div>
                        {tab_contents}
                    </div>
                </div>
            </div>
            <script>
            function toggleCodeShowcase() {{
                const body = document.getElementById('code-showcase-body');
                const chevron = document.getElementById('code-showcase-chevron');
                if (body.style.display === 'none') {{
                    body.style.display = 'block';
                    chevron.classList.add('open');
                }} else {{
                    body.style.display = 'none';
                    chevron.classList.remove('open');
                }}
            }}
            document.querySelectorAll('.code-tab-btn').forEach(btn => {{
                btn.addEventListener('click', function(e) {{
                    e.stopPropagation();
                    const tabId = this.dataset.codetab;
                    document.querySelectorAll('.code-tab-btn').forEach(b => b.classList.remove('active'));
                    document.querySelectorAll('.code-tab-content').forEach(c => c.classList.remove('active'));
                    this.classList.add('active');
                    document.querySelector('.code-tab-content[data-codetab="' + tabId + '"]').classList.add('active');
                }});
            }});
            function copyCodeTab(btn, tabId) {{
                const pre = btn.parentElement.querySelector('pre');
                const text = pre.textContent || pre.innerText;
                navigator.clipboard.writeText(text).then(() => {{
                    btn.textContent = 'Copied!';
                    btn.style.background = '#22c55e';
                    btn.style.color = '#fff';
                    setTimeout(() => {{
                        btn.textContent = 'Copy';
                        btn.style.background = '';
                        btn.style.color = '';
                    }}, 1500);
                }});
            }}
            // --- Mini-wizard: update all code-param spans when inputs change ---
            const wizFields = ['account', 'user', 'role', 'database', 'schema', 'pipe'];
            function updateCodeParams() {{
                wizFields.forEach(field => {{
                    const input = document.getElementById('wiz_' + field);
                    if (!input) return;
                    const val = input.value.trim() || input.placeholder;
                    document.querySelectorAll('.code-param[data-field="' + field + '"]').forEach(span => {{
                        span.textContent = val;
                    }});
                }});
            }}
            // Wire up all wizard inputs
            document.querySelectorAll('#codeWizard input').forEach(input => {{
                input.addEventListener('input', updateCodeParams);
            }});
            // Initialize on load
            updateCodeParams();
            </script>
    '''


def get_material_icon(name: str, size: str = "24px", color: str = "#e2e8f0") -> str:
    return f'<span class="material-symbols-outlined" style="font-size:{size};color:{color};vertical-align:middle;">{name}</span>'


def get_openflow_icon(size: str = "24px", color: str = "#4AC1E0") -> str:
    """Return an inline SVG of the Openflow logo (hub-and-spoke flow design).
    Pixel-perfect potrace vectorization of OpenflowLogoBlueTransparent.png."""
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 276 262" width="{size}" height="{size}" style="vertical-align:middle;display:inline-block;" preserveAspectRatio="xMidYMid meet"><g transform="translate(0,262) scale(0.1,-0.1)" fill="{color}" stroke="none"><path d="M571 2448 c-84 -15 -159 -78 -190 -161 -6 -16 -11 -52 -11 -81 0 -102 73 -189 187 -222 68 -20 146 -8 205 31 21 14 46 25 55 25 10 0 82 -34 161 -74 l143 -74 75 51 c64 44 73 53 59 63 -16 12 -236 128 -330 174 -48 24 -50 27 -57 74 -18 127 -158 219 -297 194z m115 -114 c45 -22 74 -70 74 -126 0 -130 -192 -178 -260 -65 -42 68 -17 156 53 188 52 23 89 24 133 3z M2086 2448 c-93 -13 -179 -99 -192 -189 l-6 -46 -137 -62 c-480 -215 -804 -474 -900 -718 l-19 -48 -111 -3 c-71 -2 -111 1 -111 8 0 5 -17 29 -38 52 -158 175 -443 74 -443 -158 0 -155 180 -274 334 -220 51 19 103 60 133 107 l24 39 100 0 c94 0 100 -1 100 -20 0 -41 43 -146 85 -210 24 -36 75 -96 114 -133 l71 -69 59 33 c111 61 110 57 40 114 -133 109 -209 254 -194 370 26 215 322 468 805 689 138 64 156 70 165 56 3 -6 30 -22 58 -37 166 -85 368 33 367 214 -1 82 -56 170 -132 209 -42 22 -114 31 -172 22z m123 -116 c62 -33 89 -119 57 -181 -46 -87 -195 -93 -246 -9 -28 45 -27 117 2 153 42 54 125 70 187 37z m-1754 -926 c56 -37 80 -118 51 -175 -25 -46 -71 -71 -135 -71 -49 0 -57 3 -92 39 -38 38 -45 60 -40 120 8 86 137 138 216 87z M1508 1729 c-38 -22 -68 -43 -68 -46 0 -3 38 -45 84 -94 61 -64 95 -110 123 -166 36 -72 38 -83 38 -167 0 -81 -3 -98 -29 -149 -91 -179 -349 -362 -751 -533 -97 -41 -101 -42 -120 -24 -65 59 -179 76 -263 40 -56 -24 -116 -83 -138 -136 -19 -46 -17 -131 5 -179 23 -52 85 -107 144 -130 55 -21 155 -17 203 8 66 34 120 107 132 179 l7 40 120 52 c343 147 613 324 736 482 76 98 111 169 133 275 l6 27 134 1 135 2 15 -30 c22 -42 91 -97 144 -115 101 -35 206 -6 275 74 47 55 60 97 55 174 -3 51 -10 70 -38 109 -67 94 -213 135 -315 88 -43 -19 -111 -84 -121 -113 -5 -16 -20 -18 -155 -18 -129 0 -149 2 -149 16 0 21 -65 150 -98 194 -30 41 -163 180 -171 180 -3 0 -36 -18 -73 -41z m932 -315 c51 -20 80 -66 80 -126 0 -78 -58 -128 -148 -128 -141 0 -184 187 -57 250 48 24 72 25 125 4z m-1717 -953 c24 -24 31 -40 35 -84 4 -49 2 -57 -28 -90 -86 -98 -236 -54 -248 72 -3 37 0 58 12 76 51 77 165 90 229 26z M1549 597 l-77 -53 105 -47 c57 -26 145 -64 196 -84 51 -20 97 -40 103 -45 6 -4 14 -26 17 -47 13 -100 131 -191 246 -191 101 0 188 57 230 150 99 219 -175 424 -379 284 l-51 -35 -72 27 c-40 15 -105 42 -146 61 -40 18 -79 33 -85 33 -6 0 -45 -24 -87 -53z m669 -114 c69 -43 82 -152 24 -204 -41 -37 -96 -52 -142 -39 -74 21 -105 70 -101 157 1 12 15 38 31 57 47 53 130 66 188 29z"/></g></svg>'''


def format_number(n):
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def get_base_styles():
    return """
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
    <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" rel="stylesheet" />
    <style>
        :root {
            --space-xs: 4px;
            --space-sm: 10px;
            --space-md: 16px;
            --space-lg: 20px;
            --space-xl: 28px;
            --space-2xl: 36px;
            --space-3xl: 48px;
            --color-bg-primary: #0f172a;
            --color-bg-secondary: #1e293b;
            --color-bg-tertiary: #334155;
            --color-border: #475569;
            --color-border-subtle: #334155;
            --color-text-primary: #f1f5f9;
            --color-text-secondary: #cbd5e1;
            --color-text-muted: #94a3b8;
            --color-accent: #38bdf8;
            --color-accent-secondary: #6366f1;
            --color-success: #22c55e;
            --color-warning: #f59e0b;
            --color-error: #ef4444;
            --radius-sm: 4px;
            --radius-md: 8px;
            --radius-lg: 12px;
            /* Neumorphic shadow variables - Snowflake dark theme */
            --shadow-neu-outset: 6px 6px 12px rgba(0, 0, 0, 0.4), -4px -4px 10px rgba(56, 189, 248, 0.03);
            --shadow-neu-inset: inset 3px 3px 6px rgba(0, 0, 0, 0.3), inset -2px -2px 5px rgba(56, 189, 248, 0.02);
            --shadow-neu-subtle: 4px 4px 8px rgba(0, 0, 0, 0.3), -2px -2px 6px rgba(56, 189, 248, 0.02);
            --shadow-neu-pressed: inset 2px 2px 4px rgba(0, 0, 0, 0.3), inset -1px -1px 3px rgba(56, 189, 248, 0.02);
        }
        
        * { 
            box-sizing: border-box; 
            margin: 0; 
            padding: 0;
        }
        
        /* Prevent layout shift - stable widths */
        .template-grid, .fleet-grid, .mechanism-grid, .dest-grid, .mode-toggle, .metrics-row {
            contain: layout style;
        }
        
        /* Custom scrollbar styling */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: rgba(51, 65, 85, 0.3); border-radius: 3px; }
        ::-webkit-scrollbar-thumb { background: rgba(100, 116, 139, 0.5); border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: rgba(100, 116, 139, 0.8); }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #0c1222 50%, #0f172a 100%);
            background-attachment: fixed;
            color: var(--color-text-primary);
            min-height: 100vh;
            font-size: 14px;
            line-height: 1.5;
            -webkit-font-smoothing: antialiased;
        }
        
        .container { 
            max-width: 1400px; 
            width: 100%;
            margin: 0 auto; 
            padding: var(--space-xl) var(--space-lg);
            overflow-x: hidden;
        }
        
        .material-symbols-outlined { 
            font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 20;
            font-size: 20px;
            vertical-align: middle;
        }
        .icon-sm { font-size: 16px; font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 16; }
        
        .header {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: var(--space-lg);
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-lg);
            padding: var(--space-xl) var(--space-lg);
            margin-bottom: var(--space-lg);
            box-shadow: var(--shadow-neu-outset);
        }
        .header-logo { width: 56px; height: 56px; }
        .header h1 {
            font-size: 2rem;
            font-weight: 600;
            color: var(--color-text-primary);
            line-height: 1.2;
            letter-spacing: -0.02em;
        }
        .header .subtitle {
            color: var(--color-text-muted);
            font-size: 1rem;
            margin-top: var(--space-xs);
            font-weight: 400;
        }
        
        .status-bar {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-md) var(--space-lg);
            margin-bottom: var(--space-lg);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: var(--space-lg);
            font-size: 0.9375rem;
            box-shadow: var(--shadow-neu-subtle);
        }
        .status-item { 
            display: flex; 
            align-items: center; 
            gap: var(--space-sm); 
            font-size: 0.9375rem; 
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--color-success);
            box-shadow: 0 0 8px rgba(34, 197, 94, 0.5);
        }
        .status-dot.active { animation: pulse 2s infinite; }
        .status-dot.disconnected {
            background: #f87171;
            box-shadow: 0 0 8px rgba(248, 113, 113, 0.5);
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; box-shadow: 0 0 8px rgba(34, 197, 94, 0.5); }
            50% { opacity: 0.5; box-shadow: 0 0 12px rgba(34, 197, 94, 0.8); }
        }
        .status-label { color: var(--color-text-muted); }
        .status-value { color: var(--color-text-primary); font-weight: 500; user-select: text; -webkit-user-select: text; cursor: text; }
        
        .tabs {
            display: flex;
            gap: var(--space-sm);
            background: var(--color-bg-secondary);
            padding: var(--space-sm);
            border-radius: var(--radius-md);
            margin-bottom: var(--space-xl);
            border: 1px solid var(--color-border-subtle);
            box-shadow: var(--shadow-neu-inset);
        }
        .tab {
            flex: 1;
            padding: var(--space-md) var(--space-xl);
            text-align: center;
            border-radius: var(--radius-sm);
            cursor: pointer;
            transition: all 0.2s ease;
            text-decoration: none;
            color: var(--color-text-muted);
            font-weight: 500;
            font-size: 1rem;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: var(--space-sm);
        }
        .tab:hover { 
            background: rgba(56, 189, 248, 0.08); 
            color: var(--color-text-primary); 
        }
        .tab.active {
            background: var(--color-accent);
            color: var(--color-bg-primary);
            font-weight: 600;
            box-shadow: var(--shadow-neu-subtle);
        }
        
        .section-header {
            color: var(--color-text-primary);
            font-size: 1.0625rem;
            font-weight: 600;
            margin: var(--space-xl) 0 var(--space-md);
            display: flex;
            align-items: center;
            gap: var(--space-sm);
        }
        .section-num {
            background: var(--color-accent);
            color: var(--color-bg-primary);
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8125rem;
            font-weight: 600;
            box-shadow: 0 2px 8px rgba(56, 189, 248, 0.3);
        }
        
        /* Guided flow: step locking */
        .step-section {
            position: relative;
            transition: opacity 0.3s ease, filter 0.3s ease;
        }
        .step-locked {
            opacity: 0.3;
            pointer-events: none;
            filter: grayscale(0.5);
        }
        .step-locked::after {
            content: attr(data-lock-msg);
            position: absolute;
            top: 36px;
            left: 50%;
            transform: translateX(-50%);
            font-size: 0.8rem;
            color: #94a3b8;
            background: rgba(15,23,42,0.9);
            padding: 6px 16px;
            border-radius: 20px;
            border: 1px solid rgba(148,163,184,0.2);
            z-index: 10;
            white-space: nowrap;
        }
        .step-locked .section-num {
            background: #475569;
            box-shadow: none;
        }
        @keyframes stepUnlock {
            0% { transform: scale(1); }
            50% { transform: scale(1.005); }
            100% { transform: scale(1); }
        }
        .step-unlocking {
            animation: stepUnlock 0.3s ease;
        }
        
        /* Stable grid layouts - prevent resizing */
        .template-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        @media (max-width: 900px) {
            .template-grid { grid-template-columns: repeat(2, 1fr); }
        }
        .template-btn {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-md) var(--space-md);
            cursor: pointer;
            transition: all 0.2s ease;
            text-align: center;
            text-decoration: none;
            min-width: 0;
            box-shadow: var(--shadow-neu-subtle);
        }
        .template-btn:hover { 
            border-color: var(--color-accent); 
            background: rgba(56, 189, 248, 0.04);
            transform: translateY(-2px);
            box-shadow: 8px 8px 16px rgba(0, 0, 0, 0.4), -4px -4px 12px rgba(56, 189, 248, 0.05);
        }
        .template-btn.active {
            border-color: var(--color-accent);
            background: rgba(56, 189, 248, 0.1);
            box-shadow: var(--shadow-neu-pressed), 0 0 0 2px rgba(56, 189, 248, 0.2);
        }
        .template-btn .icon { font-size: 1.5rem; margin-bottom: var(--space-sm); }
        .template-btn .name { 
            color: var(--color-text-primary); 
            font-weight: 600; 
            font-size: 0.9375rem; 
        }
        .template-btn .rows { 
            color: var(--color-text-muted); 
            font-size: 0.8125rem; 
            margin-top: 4px; 
        }
        
        .template-desc {
            color: var(--color-text-secondary);
            font-size: 0.9375rem;
            padding: var(--space-md) var(--space-lg);
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            margin-bottom: var(--space-md);
            box-shadow: var(--shadow-neu-inset);
        }
        
        .mode-toggle {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        .mode-btn {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-md);
            cursor: pointer;
            transition: all 0.2s ease;
            text-align: center;
            text-decoration: none;
            box-shadow: var(--shadow-neu-subtle);
        }
        .mode-btn:hover { 
            border-color: var(--color-accent);
            transform: translateY(-2px);
            box-shadow: 8px 8px 16px rgba(0, 0, 0, 0.4), -4px -4px 12px rgba(56, 189, 248, 0.05);
        }
        .mode-btn.active {
            border-color: var(--color-accent);
            background: rgba(56, 189, 248, 0.1);
            box-shadow: var(--shadow-neu-pressed), 0 0 0 2px rgba(56, 189, 248, 0.2);
        }
        .mode-btn .mode-icon { font-size: 1.5rem; margin-bottom: var(--space-sm); }
        .mode-btn .mode-name { 
            color: var(--color-text-primary); 
            font-weight: 600; 
            font-size: 1rem;
        }
        .mode-btn .mode-desc { 
            color: var(--color-text-muted); 
            font-size: 0.875rem; 
            margin-top: var(--space-xs); 
        }
        
        .info-box {
            border-radius: var(--radius-md);
            padding: var(--space-md) var(--space-lg);
            margin-bottom: var(--space-md);
            box-shadow: var(--shadow-neu-inset);
        }
        .info-box.green {
            background: rgba(34, 197, 94, 0.08);
            border: 1px solid rgba(34, 197, 94, 0.3);
        }
        .info-box.blue {
            background: rgba(56, 189, 248, 0.08);
            border: 1px solid rgba(56, 189, 248, 0.3);
        }
        .info-box.purple {
            background: rgba(168, 85, 247, 0.08);
            border: 1px solid rgba(168, 85, 247, 0.3);
        }
        .info-box .title {
            font-weight: 600;
            font-size: 0.9375rem;
            margin-bottom: var(--space-sm);
            display: flex;
            align-items: center;
            gap: var(--space-sm);
        }
        .info-box .desc {
            color: var(--color-text-secondary);
            font-size: 0.9375rem;
            line-height: 1.6;
        }
        
        /* Stable grid layouts - prevent resizing on selection */
        .fleet-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        .mechanism-grid, .dest-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        @media (max-width: 900px) {
            .fleet-grid { grid-template-columns: repeat(2, 1fr); }
            .mechanism-grid, .dest-grid { grid-template-columns: 1fr; }
        }
        
        /* Stable metrics row layout */
        .metrics-row {
            display: grid;
            grid-template-columns: repeat(6, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        @media (max-width: 1100px) {
            .metrics-row { grid-template-columns: repeat(3, 1fr); }
        }
        @media (max-width: 700px) {
            .metrics-row { grid-template-columns: repeat(2, 1fr); }
        }
        .metric-card {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-lg);
            text-align: center;
            min-width: 0;
            box-shadow: var(--shadow-neu-subtle);
        }
        .metric-value {
            color: var(--color-accent);
            font-size: 1.75rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            text-shadow: 0 0 20px rgba(56, 189, 248, 0.3);
        }
        .metric-label {
            color: var(--color-text-muted);
            font-size: 0.8125rem;
            margin-top: 4px;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }
        
        .mechanism-card, .dest-card {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-md) var(--space-lg);
            cursor: pointer;
            transition: all 0.2s ease;
            text-decoration: none;
            min-width: 0;
            box-shadow: var(--shadow-neu-subtle);
        }
        .mechanism-card:hover, .dest-card:hover { 
            border-color: var(--color-accent); 
            background: rgba(56, 189, 248, 0.04);
            transform: translateY(-2px);
            box-shadow: 8px 8px 16px rgba(0, 0, 0, 0.4), -4px -4px 12px rgba(56, 189, 248, 0.05);
        }
        .mechanism-card.active, .dest-card.active {
            border-color: var(--color-accent);
            background: rgba(56, 189, 248, 0.1);
            box-shadow: var(--shadow-neu-pressed), 0 0 0 2px rgba(56, 189, 248, 0.2);
        }
        .card-header { 
            display: flex; 
            align-items: center; 
            gap: var(--space-sm); 
            margin-bottom: var(--space-sm); 
        }
        .card-name { 
            color: var(--color-text-primary); 
            font-weight: 600; 
            font-size: 0.9375rem;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .card-latency {
            padding: 3px var(--space-sm);
            border-radius: 8px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }
        .card-desc { 
            color: var(--color-text-muted); 
            font-size: 0.875rem; 
            line-height: 1.5; 
        }
        
        .form-row {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: var(--space-md);
            margin-bottom: var(--space-lg);
        }
        @media (max-width: 900px) {
            .form-row { grid-template-columns: repeat(2, 1fr); }
        }
        @media (max-width: 600px) {
            .form-row { grid-template-columns: 1fr; }
        }
        .form-group { display: flex; flex-direction: column; min-width: 0; }
        .form-label {
            color: var(--color-text-secondary);
            font-size: 0.875rem;
            font-weight: 500;
            margin-bottom: var(--space-sm);
        }
        
        input, select {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-sm);
            padding: var(--space-md);
            color: var(--color-text-primary);
            font-size: 0.9375rem;
            font-family: inherit;
            transition: all 0.2s ease;
            min-width: 0;
            box-shadow: var(--shadow-neu-inset);
        }
        input:hover, select:hover {
            border-color: var(--color-border);
        }
        input:focus, select:focus {
            outline: none;
            border-color: var(--color-accent);
            box-shadow: var(--shadow-neu-inset), 0 0 0 3px rgba(56, 189, 248, 0.15);
        }
        input::placeholder {
            color: var(--color-text-muted);
        }
        input:disabled, select:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            background: var(--color-bg-tertiary);
        }
        
        /* Loading spinner */
        .spinner {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid rgba(56, 189, 248, 0.2);
            border-top-color: #38bdf8;
            border-radius: 50%;
            animation: spin 0.6s linear infinite;
        }
        .spinner.white {
            border-color: rgba(255, 255, 255, 0.2);
            border-top-color: white;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        input[type="range"] {
            -webkit-appearance: none;
            height: 6px;
            background: var(--color-bg-tertiary);
            border-radius: 3px;
            padding: 0;
            border: none;
            box-shadow: var(--shadow-neu-inset);
        }
        input[type="range"]::-webkit-slider-thumb {
            -webkit-appearance: none;
            width: 18px;
            height: 18px;
            background: var(--color-accent);
            border-radius: 50%;
            cursor: pointer;
            border: 2px solid var(--color-bg-primary);
            box-shadow: 0 2px 6px rgba(56, 189, 248, 0.4);
        }
        
        .btn-primary {
            background: linear-gradient(135deg, var(--color-accent) 0%, #0ea5e9 100%);
            color: var(--color-bg-primary);
            border: none;
            padding: var(--space-md) var(--space-xl);
            border-radius: var(--radius-md);
            font-size: 0.9375rem;
            font-weight: 600;
            font-family: inherit;
            cursor: pointer;
            width: 100%;
            margin-top: var(--space-lg);
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: var(--space-sm);
            box-shadow: 0 4px 12px rgba(56, 189, 248, 0.3), var(--shadow-neu-subtle);
        }
        .btn-primary:hover { 
            background: linear-gradient(135deg, #0ea5e9 0%, var(--color-accent) 100%);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(56, 189, 248, 0.4), 8px 8px 16px rgba(0, 0, 0, 0.3);
        }
        .btn-primary:active {
            transform: translateY(0);
            box-shadow: 0 2px 8px rgba(56, 189, 248, 0.3), var(--shadow-neu-pressed);
        }
        
        .btn-secondary {
            background: var(--color-bg-tertiary);
            color: var(--color-text-primary);
            border: 1px solid var(--color-border);
            padding: var(--space-sm) var(--space-lg);
            border-radius: var(--radius-sm);
            font-size: 0.875rem;
            font-weight: 500;
            font-family: inherit;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: var(--shadow-neu-subtle);
        }
        .btn-secondary:hover { 
            background: var(--color-border);
            transform: translateY(-1px);
        }
        .btn-secondary:active {
            box-shadow: var(--shadow-neu-pressed);
            transform: translateY(0);
        }
        
        /* Pipeline step action buttons */
        .btn-step {
            color: white;
            border: none;
            border-radius: 4px;
            padding: 6px 10px;
            font-size: 0.7rem;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 4px;
        }
        .btn-step:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-step:active { transform: translateY(0); }
        .btn-step.cyan { background: #0ea5e9; }
        .btn-step.green { background: #22c55e; }
        .btn-step.blue { background: #38bdf8; }
        .btn-step.purple { background: #a855f7; }
        
        .main-content {
            display: grid;
            grid-template-columns: 1fr 380px;
            gap: var(--space-2xl);
            align-items: start;
        }
        
        /* Full-width layout for complex flows like Stage Landing */
        .main-content.full-width {
            grid-template-columns: 1fr;
        }
        
        .panel {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-neu-outset);
            padding: var(--space-2xl);
        }
        
        /* Accordion styles for pipeline steps */
        .pipeline-accordion {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: var(--space-lg);
            margin-top: var(--space-lg);
        }
        .accordion-step {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            overflow: hidden;
            transition: all 0.2s ease;
            box-shadow: var(--shadow-neu-subtle);
        }
        .accordion-step.active {
            border-color: var(--color-accent);
            box-shadow: var(--shadow-neu-pressed), 0 0 0 2px rgba(56, 189, 248, 0.15);
        }
        .accordion-step-header {
            padding: var(--space-md) var(--space-lg);
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: var(--space-sm);
            background: var(--color-bg-tertiary);
            border-bottom: 1px solid var(--color-border-subtle);
            transition: all 0.2s ease;
        }
        .accordion-step-header:hover {
            background: var(--color-border-subtle);
        }
        .accordion-step-number {
            width: 28px;
            height: 28px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.875rem;
            font-weight: 600;
            flex-shrink: 0;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
        }
        .accordion-step-title {
            font-size: 0.9375rem;
            font-weight: 500;
            color: var(--color-text-primary);
        }
        .accordion-step-body {
            padding: var(--space-xl);
        }
        .accordion-step-desc {
            color: var(--color-text-muted);
            font-size: 0.875rem;
            margin-bottom: var(--space-lg);
        }
        
        @media (max-width: 900px) {
            .pipeline-accordion {
                grid-template-columns: 1fr;
            }
        }
        
        .panel.sidebar {
            position: sticky;
            top: var(--space-xl);
            align-self: start;
            max-height: calc(100vh - var(--space-xl) - var(--space-xl));
            contain: layout;
        }
        .panel-title {
            color: var(--color-text-primary);
            font-size: 1.0625rem;
            font-weight: 600;
            margin-bottom: var(--space-xl);
            padding-bottom: var(--space-md);
            border-bottom: 1px solid var(--color-border-subtle);
            display: flex;
            align-items: center;
            gap: var(--space-sm);
        }
        
        .divider {
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--color-border-subtle), transparent);
            margin: var(--space-xl) 0;
        }
        
        .sdk-limits-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: var(--space-md);
            box-shadow: var(--shadow-neu-inset);
            border-radius: var(--radius-md);
            overflow: hidden;
        }
        .sdk-limits-table th, .sdk-limits-table td {
            padding: var(--space-md) var(--space-lg);
            text-align: left;
            border-bottom: 1px solid var(--color-border-subtle);
        }
        .sdk-limits-table th {
            color: var(--color-text-muted);
            font-weight: 500;
            font-size: 0.8125rem;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }
        .sdk-limits-table td {
            color: var(--color-text-primary);
            font-size: 0.9375rem;
        }
        .sdk-limits-table .value {
            color: var(--color-success);
            font-weight: 600;
        }
        
        /* Code Showcase Panel */
        .code-showcase { margin-top: 24px; }
        .code-showcase .panel-body { padding: 0; }
        .code-showcase-toggle {
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: space-between;
            user-select: none;
        }
        .code-showcase-toggle .chevron {
            transition: transform 0.2s ease;
            font-size: 20px;
            color: #64748b;
        }
        .code-showcase-toggle .chevron.open { transform: rotate(180deg); }
        .code-tabs {
            display: flex;
            gap: 0;
            border-bottom: 1px solid #334155;
            background: #0f172a;
            border-radius: 8px 8px 0 0;
            overflow-x: auto;
        }
        .code-tab-btn {
            padding: 10px 18px;
            background: transparent;
            border: none;
            color: #64748b;
            font-size: 0.8rem;
            font-weight: 500;
            cursor: pointer;
            white-space: nowrap;
            border-bottom: 2px solid transparent;
            transition: all 0.15s ease;
        }
        .code-tab-btn:hover { color: #e2e8f0; background: #1e293b; }
        .code-tab-btn.active {
            color: #38bdf8;
            border-bottom-color: #38bdf8;
            background: #1e293b;
        }
        .code-tab-content {
            display: none;
            position: relative;
        }
        .code-tab-content.active { display: block; }
        .code-tab-content pre {
            background: #0f172a;
            margin: 0;
            padding: 16px 20px;
            border-radius: 0 0 8px 8px;
            font-size: 0.78rem;
            overflow-x: auto;
            color: #e2e8f0;
            border: 1px solid #334155;
            border-top: none;
            line-height: 1.6;
            font-family: 'SF Mono', 'Fira Code', 'Monaco', monospace;
        }
        .code-tab-annotation {
            padding: 10px 16px;
            font-size: 0.8rem;
            color: #94a3b8;
            background: #1e293b;
            border-left: 1px solid #334155;
            border-right: 1px solid #334155;
        }
        .code-copy-btn {
            position: absolute;
            top: 8px;
            right: 12px;
            background: #334155;
            border: none;
            color: #94a3b8;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.7rem;
            cursor: pointer;
            z-index: 2;
            transition: all 0.15s ease;
        }
        .code-copy-btn:hover { background: #475569; color: #e2e8f0; }
        .code-line-count {
            display: inline-block;
            margin-left: 8px;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.7rem;
            font-weight: 600;
            background: #1e293b;
            color: #64748b;
        }
        .code-wizard {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 8px;
            padding: 12px 16px;
            background: #0f172a;
            border-bottom: 1px solid #1e293b;
        }
        .code-wizard label {
            display: flex;
            flex-direction: column;
            gap: 3px;
            font-size: 0.7rem;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .code-wizard input {
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 4px;
            color: #e2e8f0;
            padding: 5px 8px;
            font-size: 0.8rem;
            font-family: 'SF Mono', 'Fira Code', monospace;
            outline: none;
            transition: border-color 0.15s ease;
        }
        .code-wizard input:focus { border-color: #f59e0b; }
        .code-wizard input::placeholder { color: #475569; }
        .code-param {
            color: #f59e0b;
            background: rgba(245, 158, 11, 0.1);
            padding: 0 2px;
            border-radius: 2px;
        }
        .code-doc-link {
            color: #f59e0b;
            text-decoration: none;
            font-weight: 500;
            transition: opacity 0.15s;
        }
        .code-doc-link:hover { text-decoration: underline; opacity: 0.85; }
        
        .volume-slider-container {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-xl);
            margin-bottom: var(--space-xl);
        }
        .slider-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: var(--space-lg);
        }
        .slider-value {
            color: var(--color-success);
            font-size: 2rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }
        .slider-unit {
            color: var(--color-text-muted);
            font-size: 0.9375rem;
        }
        
        .monitor-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: var(--space-xl);
        }
        .monitor-card {
            background: var(--color-bg-secondary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-lg);
            padding: var(--space-2xl);
        }
        .monitor-card h3 {
            color: var(--color-text-primary);
            font-size: 1.0625rem;
            font-weight: 600;
            margin-bottom: var(--space-lg);
            display: flex;
            align-items: center;
            gap: var(--space-sm);
        }
        
        .expander {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            margin-bottom: var(--space-lg);
            overflow: hidden;
        }
        .expander-header {
            padding: var(--space-md) var(--space-lg);
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: var(--space-sm);
            color: var(--color-text-primary);
            font-weight: 500;
            font-size: 0.9375rem;
            transition: background 0.2s ease;
        }
        .expander-header:hover { background: rgba(56, 189, 248, 0.04); }
        .expander-content {
            padding: var(--space-xl);
            border-top: 1px solid var(--color-border-subtle);
            display: none;
        }
        .expander.open .expander-content { display: block; }
        
        .checkbox-row {
            display: flex;
            align-items: center;
            gap: var(--space-sm);
            padding: var(--space-sm) 0;
        }
        .checkbox-row input[type="checkbox"] {
            width: 18px;
            height: 18px;
            accent-color: var(--color-accent);
            cursor: pointer;
        }
        .checkbox-label {
            color: var(--color-text-primary);
            font-size: 0.9375rem;
        }
        .checkbox-desc {
            color: var(--color-text-muted);
            font-size: 0.875rem;
        }
        
        .target-section {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-xl);
            margin-bottom: var(--space-xl);
        }
        .target-section-title {
            color: var(--color-text-secondary);
            font-size: 0.9375rem;
            font-weight: 500;
            margin-bottom: var(--space-md);
            display: flex;
            align-items: center;
            gap: var(--space-sm);
        }
        .target-section select,
        .target-section input[type="text"] {
            width: 100%;
            margin-bottom: var(--space-sm);
            font-size: 0.9375rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .target-section .checkbox-label {
            font-size: 0.875rem;
            color: var(--color-text-muted);
        }
        
        .success-panel {
            text-align: center;
            padding: var(--space-3xl);
        }
        .success-icon {
            color: var(--color-success);
            margin-bottom: var(--space-xl);
        }
        .success-title {
            color: var(--color-success);
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: var(--space-md);
        }
        .success-details {
            background: var(--color-bg-primary);
            border: 1px solid var(--color-border-subtle);
            border-radius: var(--radius-md);
            padding: var(--space-xl);
            margin: var(--space-xl) auto;
            max-width: 480px;
            text-align: left;
        }
        .success-details p {
            color: var(--color-text-secondary);
            font-size: 0.9375rem;
            margin-bottom: var(--space-sm);
        }
        .success-details strong {
            color: var(--color-text-primary);
        }
        
        @media (max-width: 1024px) {
            .main-content { grid-template-columns: 1fr; }
            .panel.sidebar { position: static; }
        }
        @media (max-width: 600px) {
            .tabs { flex-direction: column; }
            .container { padding: var(--space-sm); }
            .header { padding: var(--space-sm); flex-direction: column; text-align: center; }
            .header h1 { font-size: 1.25rem; }
        }
    </style>
    """


def get_header_html():
    return f"""
    <div class="header">
        <img src="/logo.png" class="header-logo" alt="FLUX Logo">
        <div>
            <h1>FLUX Data Forge</h1>
            <div class="subtitle">Synthetic Data for Energy & Utilities</div>
        </div>
    </div>
    """


def get_status_bar_html():
    connected = snowflake_session is not None
    if connected:
        try:
            row = snowflake_session.sql(
                "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA(), "
                "CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_USER()"
            ).collect()[0]
            acct, db, schema = str(row[0]), str(row[1]), str(row[2])
            role, wh, user = str(row[3]), str(row[4]), str(row[5])
        except Exception:
            acct, db, schema = "N/A", "N/A", "N/A"
            role, wh, user = "N/A", "N/A", "N/A"
        dot_class = "status-dot active"
        label = "Connected"
        auth_mode = _connection_params.get('auth_mode', 'password')
        if auth_mode == 'keypair':
            auth_badge = ('<span style="display:inline-flex;align-items:center;gap:4px;'
                          'padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600;'
                          'background:rgba(34,197,94,0.12);color:#4ade80;border:1px solid rgba(34,197,94,0.25);">'
                          '&#x1f511; Key-Pair</span>')
        else:
            auth_badge = ('<span style="display:inline-flex;align-items:center;gap:4px;'
                          'padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600;'
                          'background:rgba(245,158,11,0.12);color:#fbbf24;border:1px solid rgba(245,158,11,0.25);">'
                          '&#x1f512; Password</span>')
    else:
        acct, db, schema = "---", "---", "---"
        role, wh, user = "---", "---", "---"
        dot_class = "status-dot disconnected"
        label = "Not Connected"
        auth_badge = ""

    disconnect_btn = (
        '<button onclick="fluxDisconnect()" style="margin-left:auto;flex-shrink:0;padding:3px 10px;'
        'border:1px solid rgba(248,113,113,0.4);border-radius:6px;background:transparent;'
        'color:#f87171;font-size:0.75rem;cursor:pointer;transition:all 0.15s;" '
        'onmouseover="this.style.background=&quot;rgba(248,113,113,0.1)&quot;" '
        'onmouseout="this.style.background=&quot;transparent&quot;">Disconnect</button>'
        '<script>function fluxDisconnect(){if(!confirm("Disconnect from Snowflake?"))return;'
        'fetch("/api/disconnect",{method:"POST"}).then(()=>window.location.reload());}</script>'
    ) if connected else ''

    return f"""
    <div class="status-bar" id="flux-status-bar" style="padding:8px 14px;">
        <div class="status-item">
            <div class="{dot_class}" id="sb-dot"></div>
            <span class="status-value" id="sb-label">{label}</span>
            <span id="sb-auth">{auth_badge}</span>
            <span id="sb-password-warning" style="display:{'inline-flex' if connected and _connection_params.get('auth_mode', 'password') != 'keypair' else 'none'};align-items:center;gap:4px;padding:2px 8px;border-radius:4px;font-size:0.7rem;font-weight:500;background:rgba(245,158,11,0.08);color:#f59e0b;border:1px solid rgba(245,158,11,0.18);">&#x26a0; Streaming limited to SQL INSERT &mdash; switch to Key-Pair for SDK</span>
        </div>
        <div class="status-item">
            <span class="status-label">Account:</span>
            <span class="status-value" id="sb-account">{acct}</span>
        </div>
        <div class="status-item">
            <span class="status-label">User:</span>
            <span class="status-value" id="sb-user">{user}</span>
        </div>
        <div class="status-item">
            <span class="status-label">Role:</span>
            <span class="status-value" id="sb-role">{role}</span>
        </div>
        <div class="status-item">
            <span class="status-label">Warehouse:</span>
            <span class="status-value" id="sb-wh">{wh}</span>
        </div>
        <div class="status-item">
            <span class="status-label">Target:</span>
            <span class="status-value" id="sb-target">{db}.{schema}</span>
        </div>
        {disconnect_btn}
    </div>
    <script>
    (function() {{
        let _sbTimer = null;
        function refreshStatusBar() {{
            fetch('/api/connection-status').then(r => r.json()).then(d => {{
                const dot = document.getElementById('sb-dot');
                const lbl = document.getElementById('sb-label');
                const auth = document.getElementById('sb-auth');
                if (!dot) return;
                if (d.connected) {{
                    dot.className = 'status-dot active';
                    lbl.textContent = 'Connected';
                    document.getElementById('sb-account').textContent = d.account || '---';
                    document.getElementById('sb-user').textContent = d.user || '---';
                    document.getElementById('sb-role').textContent = d.role || '---';
                    document.getElementById('sb-wh').textContent = d.warehouse || '---';
                    document.getElementById('sb-target').textContent =
                        (d.database || '---') + '.' + (d.schema || '---');
                    if (d.auth_mode === 'keypair') {{
                        auth.innerHTML = '<span style="display:inline-flex;align-items:center;gap:4px;'
                            + 'padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600;'
                            + 'background:rgba(34,197,94,0.12);color:#4ade80;border:1px solid rgba(34,197,94,0.25);">'
                            + '&#x1f511; Key-Pair</span>';
                    }} else {{
                        auth.innerHTML = '<span style="display:inline-flex;align-items:center;gap:4px;'
                            + 'padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600;'
                            + 'background:rgba(245,158,11,0.12);color:#fbbf24;border:1px solid rgba(245,158,11,0.25);">'
                            + '&#x1f512; Password</span>';
                    }}
                    const pwWarn = document.getElementById('sb-password-warning');
                    if (pwWarn) pwWarn.style.display = d.auth_mode === 'keypair' ? 'none' : 'inline-flex';
                }} else {{
                    dot.className = 'status-dot disconnected';
                    lbl.textContent = 'Not Connected';
                    auth.innerHTML = '';
                    ['sb-account','sb-user','sb-role','sb-wh','sb-target'].forEach(
                        id => {{ const el = document.getElementById(id); if(el) el.textContent = '---'; }}
                    );
                    const pwWarn = document.getElementById('sb-password-warning');
                    if (pwWarn) pwWarn.style.display = 'none';
                }}
            }}).catch(() => {{}});
        }}
        // Poll every 30s
        if (!window._sbPolling) {{
            window._sbPolling = true;
            _sbTimer = setInterval(refreshStatusBar, 30000);
        }}
    }})();
    </script>
    {_get_connection_modal_html() if not connected else ""}
    """


def _get_connection_modal_html():
    """Connection overlay shown when app starts without a Snowflake session."""
    return """
    <style>
      @keyframes connFadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
      @keyframes connSpin { to { transform:rotate(360deg); } }
      #conn-overlay input:focus, #conn-overlay select:focus, #conn-overlay textarea:focus {
        border-color: var(--color-accent) !important;
        box-shadow: 0 0 0 2px rgba(56,189,248,0.15) !important;
        outline: none;
      }
      #conn-overlay input, #conn-overlay select, #conn-overlay textarea {
        transition: border-color 0.15s, box-shadow 0.15s;
      }
      .conn-spinner {
        display:inline-block; width:14px; height:14px; border:2px solid transparent;
        border-top-color:currentColor; border-radius:50%; animation:connSpin 0.6s linear infinite;
        vertical-align:middle; margin-right:6px;
      }
      .conn-section-label {
        font-size:0.7rem; font-weight:600; text-transform:uppercase; letter-spacing:0.06em;
        color:var(--color-text-muted); margin-bottom:10px; padding-top:4px;
        border-top:1px solid var(--color-border-subtle);
      }
      .conn-section-label:first-of-type { border-top:none; padding-top:0; }
    </style>

    <div id="conn-overlay" style="
        position:fixed; inset:0; z-index:9999;
        background:rgba(15,23,42,0.92); backdrop-filter:blur(6px);
        display:flex; align-items:center; justify-content:center;
    ">
      <div style="
        background:var(--color-bg-secondary); border:1px solid var(--color-border-subtle);
        border-radius:var(--radius-lg); width:500px; max-width:92vw;
        box-shadow:0 24px 48px rgba(0,0,0,0.5); max-height:90vh; overflow-y:auto;
        overflow-x:hidden; position:relative;
      ">
        <!-- Accent gradient stripe -->
        <div style="height:3px;background:linear-gradient(90deg,#38bdf8,#6366f1,#38bdf8);border-radius:var(--radius-lg) var(--radius-lg) 0 0;"></div>

        <div style="padding:28px 32px 32px;">

        <!-- Header -->
        <div style="display:flex;align-items:center;gap:14px;margin-bottom:22px;">
          <div style="width:40px;height:40px;border-radius:var(--radius-md);background:rgba(41,181,232,0.1);display:flex;align-items:center;justify-content:center;flex-shrink:0;">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <!-- Snowflake geometric mark: 6-arm star with center dot -->
              <circle cx="12" cy="12" r="1.8" fill="#29B5E8"/>
              <!-- Vertical arm -->
              <rect x="11" y="2" width="2" height="8" rx="1" fill="#29B5E8"/>
              <rect x="11" y="14" width="2" height="8" rx="1" fill="#29B5E8"/>
              <!-- Top-right / bottom-left arm -->
              <rect x="11" y="2" width="2" height="8" rx="1" fill="#29B5E8" transform="rotate(60 12 12)"/>
              <rect x="11" y="14" width="2" height="8" rx="1" fill="#29B5E8" transform="rotate(60 12 12)"/>
              <!-- Top-left / bottom-right arm -->
              <rect x="11" y="2" width="2" height="8" rx="1" fill="#29B5E8" transform="rotate(-60 12 12)"/>
              <rect x="11" y="14" width="2" height="8" rx="1" fill="#29B5E8" transform="rotate(-60 12 12)"/>
              <!-- Branch tips -->
              <circle cx="12" cy="3" r="1.2" fill="#29B5E8"/>
              <circle cx="12" cy="21" r="1.2" fill="#29B5E8"/>
              <circle cx="4.2" cy="7.5" r="1.2" fill="#29B5E8"/>
              <circle cx="19.8" cy="16.5" r="1.2" fill="#29B5E8"/>
              <circle cx="4.2" cy="16.5" r="1.2" fill="#29B5E8"/>
              <circle cx="19.8" cy="7.5" r="1.2" fill="#29B5E8"/>
            </svg>
          </div>
          <div>
            <div style="font-size:1.15rem;font-weight:600;color:var(--color-text-primary);">Connect to Snowflake</div>
            <div style="font-size:0.8rem;color:var(--color-text-muted);">Enter credentials or select a saved connection</div>
          </div>
        </div>

        <!-- Saved connections dropdown -->
        <div id="conn-saved-section" style="margin-bottom:18px;">
          <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Saved Connection
            <select id="conn-saved" onchange="loadSavedConnection(this.value)" style="
              width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
              border:1px solid var(--color-border);border-radius:var(--radius-sm);
              color:var(--color-text-primary);font-size:0.88rem;box-sizing:border-box;
            ">
              <option value="">-- Manual entry --</option>
            </select>
          </label>
        </div>

        <form id="conn-form" onsubmit="submitConnect(event)" style="display:flex;flex-direction:column;gap:16px;">
          <input type="hidden" name="auth_mode" id="conn-auth-mode" value="password">
          <input type="hidden" name="connection_name" id="conn-name-hidden" value="">

          <!-- Credentials section -->
          <div class="conn-section-label">Credentials</div>

          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Account
              <input name="account" id="conn-account" required placeholder="e.g. GZB42423" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.88rem;box-sizing:border-box;
              ">
            </label>
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">User
              <input name="user" id="conn-user" required placeholder="e.g. MY_USER" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.88rem;box-sizing:border-box;
              ">
            </label>
          </div>

          <!-- Auth toggle (inside credentials section) -->
          <div>
            <div style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;margin-bottom:6px;">Authentication</div>
            <div style="display:flex;gap:0;border:1px solid var(--color-border);border-radius:var(--radius-sm);overflow:hidden;">
              <button type="button" id="auth-pw-btn" onclick="setAuthMode('password')" style="
                flex:1;padding:7px;border:none;font-size:0.78rem;font-weight:600;cursor:pointer;
                background:#38bdf8;color:#0f172a;transition:all 0.15s;
              ">Password</button>
              <button type="button" id="auth-kp-btn" onclick="setAuthMode('keypair')" style="
                flex:1;padding:7px;border:none;font-size:0.78rem;font-weight:600;cursor:pointer;
                background:var(--color-bg-tertiary);color:var(--color-text-muted);transition:all 0.15s;
              ">Key-Pair</button>
            </div>
            <div style="margin-top:6px;padding:6px 10px;background:rgba(56,189,248,0.06);
              border:1px solid rgba(56,189,248,0.15);border-radius:var(--radius-sm);
              font-size:0.7rem;color:#7dd3fc;display:flex;align-items:flex-start;gap:6px;line-height:1.45;">
              <span style="flex-shrink:0;margin-top:1px;">&#9432;</span>
              <span>High-Performance Snowpipe Streaming requires <strong>Key-Pair</strong> authentication.
              Password auth will fall back to standard SQL INSERT for streaming — not the native Snowpipe Streaming SDK.</span>
            </div>
          </div>

          <!-- Password field -->
          <div id="pw-section">
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Password
              <input name="password" id="conn-password" type="password" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.88rem;box-sizing:border-box;
              ">
            </label>
            <div id="pw-saved-hint" style="display:none;margin-top:5px;font-size:0.72rem;color:var(--color-text-muted);
              display:none;align-items:center;gap:4px;">
              <svg width="12" height="12" viewBox="0 0 16 16" fill="none" style="flex-shrink:0;">
                <rect x="3" y="7" width="10" height="7" rx="1.5" stroke="#94a3b8" stroke-width="1.3" fill="none"/>
                <path d="M5.5 7V5a2.5 2.5 0 015 0v2" stroke="#94a3b8" stroke-width="1.3" fill="none"/>
              </svg>
              <span>Password will be loaded from saved connection file</span>
            </div>
          </div>

          <!-- Key-pair field (hidden by default) -->
          <div id="kp-section" style="display:none;">
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Private Key (PEM)
              <textarea name="private_key" id="conn-private-key" rows="4" placeholder="-----BEGIN PRIVATE KEY-----&#10;...&#10;-----END PRIVATE KEY-----" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.78rem;font-family:'SF Mono',Monaco,Menlo,monospace;
                box-sizing:border-box;resize:vertical;line-height:1.5;
              "></textarea>
            </label>
            <div style="margin-top:6px;">
              <label style="font-size:0.72rem;color:var(--color-text-muted);font-weight:500;display:flex;align-items:center;gap:6px;">
                Or upload <code style="font-size:0.72rem;">.p8</code> / <code style="font-size:0.72rem;">.pem</code> file
                <input type="file" id="conn-key-file" accept=".p8,.pem" style="font-size:0.75rem;color:var(--color-text-muted);max-width:220px;"
                  onchange="handleKeyFileUpload(this)">
              </label>
            </div>
          </div>

          <!-- Session Context section -->
          <div class="conn-section-label" style="margin-top:2px;">Session Context</div>

          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Role
              <input name="role" id="conn-role" list="dl-roles" value="" placeholder="User default" autocomplete="off" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.82rem;box-sizing:border-box;
              ">
              <datalist id="dl-roles"></datalist>
            </label>
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Warehouse
              <input name="warehouse" id="conn-warehouse" list="dl-warehouses" value="" placeholder="User default" autocomplete="off" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.82rem;box-sizing:border-box;
              ">
              <datalist id="dl-warehouses"></datalist>
            </label>
            <label style="font-size:0.78rem;color:var(--color-text-muted);font-weight:500;">Database
              <input name="database" id="conn-database" list="dl-databases" value="FLUX_DB" placeholder="FLUX_DB" autocomplete="off" style="
                width:100%;margin-top:4px;padding:10px 12px;background:var(--color-bg-primary);
                border:1px solid var(--color-border);border-radius:var(--radius-sm);
                color:var(--color-text-primary);font-size:0.82rem;box-sizing:border-box;
              ">
              <datalist id="dl-databases"></datalist>
            </label>
          </div>

          <!-- Feedback area -->
          <div id="conn-error" style="display:none;color:#f87171;font-size:0.8rem;padding:10px 12px;
            background:rgba(248,113,113,0.06);border:1px solid rgba(248,113,113,0.15);
            border-radius:var(--radius-sm);animation:connFadeIn 0.2s ease-out;"></div>
          <div id="conn-success" style="display:none;animation:connFadeIn 0.2s ease-out;"></div>

          <!-- Buttons -->
          <div style="display:flex;gap:10px;margin-top:4px;">
            <button type="button" id="test-btn" onclick="testConnection()" style="
              padding:10px 20px;background:transparent;color:var(--color-text-secondary);font-weight:500;
              font-size:0.85rem;border:1px solid var(--color-border);border-radius:var(--radius-sm);cursor:pointer;
              transition:all 0.15s;flex-shrink:0;
            " onmouseover="this.style.borderColor='var(--color-accent)';this.style.color='var(--color-accent)'"
              onmouseout="this.style.borderColor='var(--color-border)';this.style.color='var(--color-text-secondary)'">
              Test
            </button>
            <button type="submit" id="conn-btn" style="
              flex:1;padding:11px 20px;background:var(--color-accent);color:#0f172a;font-weight:600;
              font-size:0.95rem;border:none;border-radius:var(--radius-sm);cursor:pointer;
              transition:all 0.15s;letter-spacing:0.01em;
            " onmouseover="this.style.background='#7dd3fc'" onmouseout="this.style.background='var(--color-accent)'">
              Connect
            </button>
          </div>
        </form>

        </div>
      </div>
    </div>

    <script>
    // ── Datalist helper for session-context dropdowns ─────────────
    function populateDatalist(dlId, items) {
        const dl = document.getElementById(dlId);
        if (!dl) return;
        dl.innerHTML = '';
        items.forEach(v => { const o = document.createElement('option'); o.value = v; dl.appendChild(o); });
    }
    function fetchSessionOptions(formOrName) {
        let body;
        if (typeof formOrName === 'string') {
            // Saved connection name — send as form data with just the name
            body = new FormData();
            body.append('connection_name', formOrName);
        } else {
            body = new FormData(formOrName);
        }
        fetch('/api/session-options', { method: 'POST', body })
            .then(r => r.json())
            .then(d => {
                if (d.status !== 'ok') return;
                populateDatalist('dl-roles', d.roles || []);
                populateDatalist('dl-warehouses', d.warehouses || []);
                populateDatalist('dl-databases', d.databases || []);
            })
            .catch(() => {});
    }

    // ── Auth mode toggle ───────────────────────────────────────────
    function setAuthMode(mode) {
        document.getElementById('conn-auth-mode').value = mode;
        const pwBtn = document.getElementById('auth-pw-btn');
        const kpBtn = document.getElementById('auth-kp-btn');
        const pwSec = document.getElementById('pw-section');
        const kpSec = document.getElementById('kp-section');
        if (mode === 'keypair') {
            kpBtn.style.background = '#38bdf8'; kpBtn.style.color = '#0f172a';
            pwBtn.style.background = 'var(--color-bg-tertiary)'; pwBtn.style.color = 'var(--color-text-muted)';
            kpSec.style.display = ''; pwSec.style.display = 'none';
        } else {
            pwBtn.style.background = '#38bdf8'; pwBtn.style.color = '#0f172a';
            kpBtn.style.background = 'var(--color-bg-tertiary)'; kpBtn.style.color = 'var(--color-text-muted)';
            pwSec.style.display = ''; kpSec.style.display = 'none';
        }
    }

    // ── Key file upload handler ──────────────────────────────────────
    function handleKeyFileUpload(input) {
        const file = input.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = e => {
            document.getElementById('conn-private-key').value = e.target.result;
        };
        reader.readAsText(file);
    }

    // ── Load saved connections into dropdown ────────────────────────
    let _savedConns = [];
    (async function loadConnections() {
        try {
            const resp = await fetch('/api/connections');
            const data = await resp.json();
            _savedConns = data.connections || [];
            const sel = document.getElementById('conn-saved');
            if (!_savedConns.length) {
                document.getElementById('conn-saved-section').style.display = 'none';
                return;
            }
            _savedConns.forEach(c => {
                const opt = document.createElement('option');
                opt.value = c.name;
                opt.textContent = c.name + '  \u2022  ' + c.account + ' / ' + c.user;
                sel.appendChild(opt);
            });
        } catch(e) {
            document.getElementById('conn-saved-section').style.display = 'none';
        }
    })();

    function loadSavedConnection(name) {
        document.getElementById('conn-name-hidden').value = name;
        const hintEl = document.getElementById('pw-saved-hint');
        const errEl = document.getElementById('conn-error');
        const okEl = document.getElementById('conn-success');
        errEl.style.display = 'none';
        okEl.style.display = 'none';
        if (!name) {
            // Reset all fields for manual entry
            document.getElementById('conn-account').value = '';
            document.getElementById('conn-user').value = '';
            document.getElementById('conn-role').value = '';
            document.getElementById('conn-warehouse').value = '';
            document.getElementById('conn-database').value = 'FLUX_DB';
            document.getElementById('conn-password').value = '';
            document.getElementById('conn-password').placeholder = '';
            hintEl.style.display = 'none';
            return;
        }
        const c = _savedConns.find(x => x.name === name);
        if (!c) return;
        document.getElementById('conn-account').value = c.account || '';
        document.getElementById('conn-user').value = c.user || '';
        document.getElementById('conn-role').value = c.role || '';
        document.getElementById('conn-warehouse').value = c.warehouse || '';
        document.getElementById('conn-database').value = c.database || 'FLUX_DB';
        document.getElementById('conn-password').value = '';
        document.getElementById('conn-password').placeholder = '\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022';
        hintEl.style.display = 'flex';

        // Auto-resolve empty session context fields from Snowflake
        const roleEl = document.getElementById('conn-role');
        const whEl = document.getElementById('conn-warehouse');
        const dbEl = document.getElementById('conn-database');
        if (!c.role || !c.warehouse || !c.database) {
            // Show resolving state on empty fields
            if (!c.role) roleEl.placeholder = 'Resolving\u2026';
            if (!c.warehouse) whEl.placeholder = 'Resolving\u2026';
            fetch('/api/resolve-defaults/' + encodeURIComponent(name))
                .then(r => r.json())
                .then(d => {
                    if (d.status === 'ok') {
                        if (!roleEl.value && d.role) roleEl.value = d.role;
                        if (!whEl.value && d.warehouse) whEl.value = d.warehouse;
                        if (!dbEl.value && d.database) dbEl.value = d.database;
                    }
                    roleEl.placeholder = 'User default';
                    whEl.placeholder = 'User default';
                })
                .catch(() => {
                    roleEl.placeholder = 'User default';
                    whEl.placeholder = 'User default';
                });
        }

        // Populate datalists with available roles/warehouses/databases
        fetchSessionOptions(name);
    }

    // ── Test Connection ─────────────────────────────────────────────
    async function testConnection() {
        const btn = document.getElementById('test-btn');
        const errEl = document.getElementById('conn-error');
        const okEl = document.getElementById('conn-success');
        errEl.style.display = 'none';
        okEl.style.display = 'none';
        btn.innerHTML = '<span class="conn-spinner"></span>Testing\u2026';
        btn.disabled = true;
        btn.style.opacity = '0.7';
        try {
            const form = document.getElementById('conn-form');
            const resp = await fetch('/api/test-connection', { method: 'POST', body: new FormData(form) });
            const data = await resp.json();
            if (data.status === 'ok') {
                const d = data.details || {};
                // Backfill empty fields with what Snowflake actually resolved
                const roleEl = document.getElementById('conn-role');
                const whEl = document.getElementById('conn-warehouse');
                const dbEl = document.getElementById('conn-database');
                if (!roleEl.value && d.role) roleEl.value = d.role;
                if (!whEl.value && d.warehouse) whEl.value = d.warehouse;
                if (!dbEl.value && d.database) dbEl.value = d.database;
                // Show resolved session context
                const ctx = [d.role, d.warehouse, d.database].filter(Boolean).join(' / ');
                const isKP = d.auth_mode === 'keypair';
                const authBadge = isKP
                    ? '<span style="display:inline-block;padding:1px 7px;border-radius:4px;font-size:0.68rem;font-weight:600;background:rgba(74,222,128,0.12);color:#4ade80;">Key-Pair &#x2713; HP Ready</span>'
                    : '<span style="display:inline-block;padding:1px 7px;border-radius:4px;font-size:0.68rem;font-weight:600;background:rgba(245,158,11,0.12);color:#fbbf24;">Password &#x2014; SQL INSERT only</span>';
                okEl.innerHTML = '<div style="padding:10px 12px;background:rgba(74,222,128,0.06);border:1px solid rgba(74,222,128,0.15);border-radius:var(--radius-sm);">'
                    + '<div style="display:flex;align-items:center;gap:6px;color:#4ade80;font-size:0.82rem;font-weight:600;">'
                    + '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="7" stroke="#4ade80" stroke-width="1.5"/><path d="M5 8l2 2 4-4" stroke="#4ade80" stroke-width="1.5" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>'
                    + ' ' + data.message + '</div>'
                    + '<div style="margin-top:6px;display:flex;align-items:center;gap:8px;font-size:0.75rem;color:var(--color-text-muted);letter-spacing:0.01em;">'
                    + d.user + ' @ ' + d.account + (ctx ? ' &nbsp;&middot;&nbsp; ' + ctx : '')
                    + ' &nbsp;' + authBadge
                    + '</div></div>';
                okEl.style.display = 'block';
                // Populate datalists so user can adjust role/warehouse/database before connecting
                fetchSessionOptions(document.getElementById('conn-form'));
            } else {
                errEl.textContent = data.message || 'Test failed';
                errEl.style.display = 'block';
            }
        } catch (err) {
            errEl.textContent = 'Network error: ' + err.message;
            errEl.style.display = 'block';
        } finally {
            btn.textContent = 'Test';
            btn.disabled = false;
            btn.style.opacity = '1';
        }
    }

    // ── Submit ──────────────────────────────────────────────────────
    async function submitConnect(e) {
        e.preventDefault();
        const btn = document.getElementById('conn-btn');
        const errEl = document.getElementById('conn-error');
        const okEl = document.getElementById('conn-success');
        errEl.style.display = 'none';
        okEl.style.display = 'none';
        btn.innerHTML = '<span class="conn-spinner"></span>Connecting\u2026';
        btn.disabled = true;
        btn.style.opacity = '0.7';
        try {
            const form = document.getElementById('conn-form');
            const resp = await fetch('/api/connect', { method: 'POST', body: new FormData(form) });
            const data = await resp.json();
            if (data.status === 'ok') {
                document.getElementById('conn-overlay').style.display = 'none';
                window.location.reload();
            } else {
                errEl.textContent = data.message || 'Connection failed';
                errEl.style.display = 'block';
            }
        } catch (err) {
            errEl.textContent = 'Network error: ' + err.message;
            errEl.style.display = 'block';
        } finally {
            btn.textContent = 'Connect';
            btn.disabled = false;
            btn.style.opacity = '1';
        }
    }
    </script>
    """


def get_tabs_html(active_tab: str):
    tabs = [
        ('generate', 'settings', 'Generate'),
        ('monitor', 'monitoring', 'Monitor'),
        ('validate', 'check_circle', 'Validate'),
        ('history', 'history', 'History'),
        ('openflow', None, 'Openflow'),
    ]
    html = '<div class="tabs">'
    for tab_id, icon, label in tabs:
        active = 'active' if tab_id == active_tab else ''
        if tab_id == 'openflow':
            icon_html = get_openflow_icon("20px", "#e2e8f0" if active_tab != 'openflow' else "#4AC1E0")
        else:
            icon_html = get_material_icon(icon, "20px")
        html += f'<a href="/{tab_id}" class="tab {active}">{icon_html} {label}</a>'
    html += '</div>'
    return html


@app.get("/", response_class=HTMLResponse)
async def home():
    return RedirectResponse(url="/generate")


@app.get("/health")
async def health():
    return {"status": "healthy", "connected": snowflake_session is not None}


# ── Connection Management (local dev) ──────────────────────────────────────

# Store user-provided connection params for reconnection
_connection_params: Dict[str, str] = {}


@app.get("/api/connection-status")
async def connection_status():
    """Return current connection state for frontend gate."""
    if snowflake_session is not None:
        try:
            row = snowflake_session.sql(
                "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_DATABASE(), "
                "CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_SCHEMA()"
            ).collect()[0]
            return {
                "connected": True,
                "account": str(row[0]),
                "user": str(row[1]),
                "database": str(row[2]),
                "role": str(row[3]),
                "warehouse": str(row[4]),
                "schema": str(row[5]),
                "mode": "spcs" if os.path.isfile('/snowflake/session/token') else "local",
                "auth_mode": _connection_params.get('auth_mode', 'password'),
            }
        except Exception:
            pass
    return {"connected": False, "mode": "disconnected"}


@app.get("/api/connections")
async def api_connections():
    """Read saved connections from ~/.snowflake/connections.toml for the dropdown."""
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # Python < 3.11
    toml_path = os.path.expanduser("~/.snowflake/connections.toml")
    if not os.path.isfile(toml_path):
        # Try legacy path
        toml_path = os.path.expanduser("~/.snowflake/config.toml")
    if not os.path.isfile(toml_path):
        return JSONResponse({"connections": []})
    try:
        with open(toml_path, "rb") as f:
            cfg = tomllib.load(f)
        conns = []
        connections_section = cfg.get("connections", cfg)
        for name, params in connections_section.items():
            if not isinstance(params, dict):
                continue
            conns.append({
                "name": name,
                "account": params.get("account", ""),
                "user": params.get("user", ""),
                "role": params.get("role", ""),
                "warehouse": params.get("warehouse", ""),
                "database": params.get("database", ""),
                "authenticator": params.get("authenticator", ""),
            })
        return JSONResponse({"connections": conns})
    except Exception as e:
        logger.warning(f"Failed to read connections.toml: {e}")
        return JSONResponse({"connections": []})


def _resolve_connection_params(account, user, password, private_key, auth_mode, role, warehouse, database, connection_name):
    """Resolve and validate connection params, merging saved connection if specified.
    Returns (creds_dict, error_message). error_message is None on success.
    
    Empty role/warehouse are omitted from creds so Snowflake uses the user's defaults.
    Database defaults to FLUX_DB since the app requires it.
    """
    if connection_name:
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib
        toml_path = os.path.expanduser("~/.snowflake/connections.toml")
        if not os.path.isfile(toml_path):
            toml_path = os.path.expanduser("~/.snowflake/config.toml")
        if os.path.isfile(toml_path):
            with open(toml_path, "rb") as f:
                cfg = tomllib.load(f)
            conn_cfg = cfg.get("connections", cfg).get(connection_name, {})
            # Fill in any blanks from the saved connection
            if not account:
                account = conn_cfg.get("account", "")
            if not user:
                user = conn_cfg.get("user", "")
            if not password and not private_key:
                password = conn_cfg.get("password", "")
            if not role:
                role = conn_cfg.get("role", "")
            if not warehouse:
                warehouse = conn_cfg.get("warehouse", "")
            if not database:
                database = conn_cfg.get("database", "")

    if not account or not user:
        return None, "Account and user are required"
    if auth_mode == "password" and not password:
        return None, "Password is required"
    if auth_mode == "keypair" and not private_key:
        return None, "Private key PEM is required"

    # Build creds — omit empty role/warehouse so Snowflake uses user defaults
    if auth_mode == "keypair":
        from cryptography.hazmat.primitives import serialization
        pem_bytes = private_key.strip().encode("utf-8")
        p_key = serialization.load_pem_private_key(pem_bytes, password=None)
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        creds = {'account': account, 'user': user, 'private_key': pkb}
    else:
        creds = {'account': account, 'user': user, 'password': password}

    if role:
        creds['role'] = role
    if warehouse:
        creds['warehouse'] = warehouse
    # Database defaults to FLUX_DB (required by the app), schema always PRODUCTION
    creds['database'] = database or 'FLUX_DB'
    creds['schema'] = 'PRODUCTION'

    return creds, None


def _activate_warehouse(session, preferred_wh: str = "") -> str | None:
    """Ensure a warehouse is active on *session*.

    Tries *preferred_wh* first. If it doesn't exist / no access, falls back to
    the first warehouse the user can see.  Returns the active warehouse name,
    or None if nothing could be activated.
    """
    if preferred_wh:
        try:
            session.sql(f"USE WAREHOUSE {preferred_wh}").collect()
            return preferred_wh
        except Exception:
            logger.warning(f"Warehouse '{preferred_wh}' unavailable, trying fallback…")

    # Fallback: pick the first warehouse the user has access to
    try:
        wh_rows = session.sql("SHOW WAREHOUSES").collect()
        if wh_rows:
            fallback = str(wh_rows[0][0])
            session.sql(f"USE WAREHOUSE {fallback}").collect()
            logger.info(f"Activated fallback warehouse: {fallback}")
            return fallback
    except Exception:
        pass
    return None


@app.post("/api/test-connection")
async def api_test_connection(
    account: str = Form(""),
    user: str = Form(""),
    password: str = Form(""),
    private_key: str = Form(""),
    auth_mode: str = Form("password"),
    role: str = Form(""),
    warehouse: str = Form(""),
    database: str = Form(""),
    connection_name: str = Form(""),
):
    """Test Snowflake connection without replacing the active session."""
    try:
        creds, err = _resolve_connection_params(
            account, user, password, private_key, auth_mode, role, warehouse, database, connection_name)
        if err:
            return JSONResponse({"status": "error", "message": err})
        test_session = Session.builder.configs(creds).create()
        _activate_warehouse(test_session, creds.get('warehouse', ''))
        row = test_session.sql(
            "SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
        ).collect()[0]
        test_session.close()
        return JSONResponse({
            "status": "ok",
            "message": f"Connection successful (Snowflake {row[0]})",
            "details": {
                "account": str(row[1]), "user": str(row[2]), "role": str(row[3]),
                "warehouse": str(row[4]) if row[4] else "", "database": str(row[5]) if row[5] else "",
                "auth_mode": auth_mode,
            },
        })
    except Exception as e:
        logger.warning(f"Test connection failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.post("/api/test-eventhub")
async def api_test_eventhub(
    conn_str: str = Form(""),
    hub_name: str = Form(""),
    mode: str = Form("consume"),
):
    """Test connectivity to Azure EventHub (consume or produce mode)."""
    import base64 as _b64
    if conn_str.startswith('b64:'):
        try:
            conn_str = _b64.b64decode(conn_str[4:]).decode('utf-8')
        except Exception:
            return JSONResponse({"status": "error", "message": "Invalid base64-encoded connection string"})
    if not conn_str or not hub_name:
        return JSONResponse({"status": "error", "message": "Connection string and EventHub name are required"})
    try:
        if mode == 'produce':
            from azure.eventhub import EventHubProducerClient
            conn_str_clean = conn_str
            if 'EntityPath=' in conn_str_clean:
                parts = [p for p in conn_str_clean.split(';') if not p.startswith('EntityPath=')]
                conn_str_clean = ';'.join(parts)
                if not conn_str_clean.endswith(';'):
                    conn_str_clean += ';'
            producer = EventHubProducerClient.from_connection_string(
                conn_str=conn_str_clean, eventhub_name=hub_name)
            props = producer.get_eventhub_properties()
            producer.close()
            return JSONResponse({
                "status": "ok",
                "message": f"Connected to EventHub '{props['name']}' ({len(props.get('partition_ids', []))} partitions)"
            })
        else:
            from azure.eventhub import EventHubConsumerClient
            consumer = EventHubConsumerClient.from_connection_string(
                conn_str=conn_str, consumer_group="$Default", eventhub_name=hub_name)
            props = consumer.get_eventhub_properties()
            consumer.close()
            return JSONResponse({
                "status": "ok",
                "message": f"Connected to EventHub '{props['name']}' ({len(props.get('partition_ids', []))} partitions)"
            })
    except ImportError:
        return JSONResponse({"status": "error", "message": "azure-eventhub package not installed. Run: pip install azure-eventhub"})
    except Exception as e:
        logger.warning(f"EventHub test connection failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.post("/api/test-pubsub")
async def api_test_pubsub(
    project_id: str = Form(""),
    subscription_name: str = Form(""),
):
    """Test connectivity to Google Cloud Pub/Sub subscription."""
    if not project_id or not subscription_name:
        return JSONResponse({"status": "error", "message": "Project ID and Subscription Name are required"})
    try:
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        sub = subscriber.get_subscription(request={"subscription": subscription_path})
        subscriber.close()
        return JSONResponse({
            "status": "ok",
            "message": f"Subscription '{subscription_name}' found (topic: {sub.topic.split('/')[-1]})"
        })
    except ImportError:
        return JSONResponse({"status": "error", "message": "google-cloud-pubsub package not installed. Run: pip install google-cloud-pubsub"})
    except Exception as e:
        logger.warning(f"Pub/Sub test connection failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/resolve-defaults/{connection_name}")
async def api_resolve_defaults(connection_name: str):
    """Quick probe: connect using saved TOML creds and return the resolved role/warehouse/database."""
    try:
        creds, err = _resolve_connection_params("", "", "", "", "password", "", "", "", connection_name)
        if err:
            return JSONResponse({"status": "error"})
        probe = Session.builder.configs(creds).create()
        _activate_warehouse(probe, creds.get('warehouse', ''))
        row = probe.sql(
            "SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
        ).collect()[0]
        probe.close()
        return JSONResponse({
            "status": "ok",
            "role": str(row[0]) if row[0] else "",
            "warehouse": str(row[1]) if row[1] else "",
            "database": str(row[2]) if row[2] else "",
        })
    except Exception:
        return JSONResponse({"status": "error"})


@app.post("/api/session-options")
async def api_session_options(
    account: str = Form(""),
    user: str = Form(""),
    password: str = Form(""),
    private_key: str = Form(""),
    auth_mode: str = Form("password"),
    role: str = Form(""),
    warehouse: str = Form(""),
    database: str = Form(""),
    connection_name: str = Form(""),
):
    """Probe Snowflake for available roles, warehouses, and databases.

    Creates an ephemeral session, queries metadata, and closes immediately.
    Only returns object names the authenticated user can see — no privilege escalation.
    """
    try:
        creds, err = _resolve_connection_params(
            account, user, password, private_key, auth_mode, role, warehouse, database, connection_name)
        if err:
            return JSONResponse({"status": "error"})
        probe = Session.builder.configs(creds).create()
        _activate_warehouse(probe, creds.get('warehouse', ''))

        cur_row = probe.sql(
            "SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
        ).collect()[0]

        roles = sorted(
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in probe.sql("SHOW ROLES").collect()
        )
        warehouses = sorted(
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in probe.sql("SHOW WAREHOUSES").collect()
        )
        databases = sorted(
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in probe.sql("SHOW DATABASES").collect()
        )
        probe.close()

        return JSONResponse({
            "status": "ok",
            "roles": roles,
            "warehouses": warehouses,
            "databases": databases,
            "current_role": str(cur_row[0]) if cur_row[0] else "",
            "current_warehouse": str(cur_row[1]) if cur_row[1] else "",
            "current_database": str(cur_row[2]) if cur_row[2] else "",
        })
    except Exception as e:
        logger.warning(f"session-options probe failed: {e}")
        return JSONResponse({"status": "error"})


@app.post("/api/connect")
async def api_connect(
    account: str = Form(""),
    user: str = Form(""),
    password: str = Form(""),
    private_key: str = Form(""),
    auth_mode: str = Form("password"),
    role: str = Form(""),
    warehouse: str = Form(""),
    database: str = Form(""),
    connection_name: str = Form(""),
):
    """Connect to Snowflake from the UI (local dev mode). Supports password and key-pair auth."""
    global snowflake_session
    try:
        creds, err = _resolve_connection_params(
            account, user, password, private_key, auth_mode, role, warehouse, database, connection_name)
        if err:
            return JSONResponse({"status": "error", "message": err})

        new_session = Session.builder.configs(creds).create()
        new_session.sql("SELECT CURRENT_VERSION()").collect()

        # Explicitly activate warehouse — Snowpark sets the name but doesn't
        # always activate a suspended warehouse, causing "No active warehouse" errors.
        wh = creds.get('warehouse', '')
        active_wh = _activate_warehouse(new_session, wh)

        if not active_wh:
            new_session.close()
            return JSONResponse({
                "status": "error",
                "message": "Connected to Snowflake but no warehouse is available. "
                           + (f"The configured warehouse '{wh}' does not exist or you lack access. " if wh else "")
                           + "Please specify a valid warehouse."
            })

        if snowflake_session:
            try:
                snowflake_session.close()
            except Exception:
                pass
        snowflake_session = new_session

        _connection_params.update({
            'account': creds.get('account', ''), 'user': creds.get('user', ''),
            'role': creds.get('role', ''), 'warehouse': creds.get('warehouse', ''),
            'database': creds.get('database', ''), 'auth_mode': auth_mode,
            'private_key_pem': private_key if auth_mode == 'keypair' else '',
        })
        _post_connect_setup()

        logger.info(f"Connected via UI: account={creds['account']}, user={creds['user']}, auth={auth_mode}")
        return JSONResponse({"status": "ok", "message": f"Connected to {creds['account']}"})
    except Exception as e:
        logger.exception("UI connection failed")
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.post("/api/disconnect")
async def api_disconnect():
    """Close the active Snowflake session so the user can reconnect via the modal."""
    global snowflake_session
    if snowflake_session:
        try:
            snowflake_session.close()
        except Exception:
            pass
        snowflake_session = None
        logger.info("User disconnected from Snowflake via UI")
    return JSONResponse({"status": "ok"})


@app.get("/generate", response_class=HTMLResponse)
async def generate_page(
    template: str = "SE Demo",
    mode: str = "batch",
    fleet: str = "Demo (1K)",
    data_flow: str = "streaming_insert",
    service_area: str = "TEXAS_GULF_COAST",
    rows_per_sec: int = 1000,
    batch_size_mb: int = 10,
    max_client_lag: int = 1
):
    tmpl = USE_CASE_TEMPLATES.get(template, USE_CASE_TEMPLATES['SE Demo'])
    fleet_cfg = FLEET_PRESETS.get(fleet, FLEET_PRESETS['Demo (1K)'])
    area_cfg = UTILITY_PROFILES.get(service_area, UTILITY_PROFILES['TEXAS_GULF_COAST'])
    
    # Get the selected data flow config (or default)
    flow_cfg = DATA_FLOWS.get(data_flow, DATA_FLOWS['streaming_insert'])
    mechanism = flow_cfg['mechanism']  # Extract for backward compatibility
    dest = flow_cfg['dest']
    
    template_btns = ""
    for name, cfg in USE_CASE_TEMPLATES.items():
        active = "active" if name == template else ""
        short_name = name.replace(' Demo', '').replace(' POC', '').replace(' Training', '')
        icon = cfg.get('icon', 'data_object')
        template_btns += f'''
        <div class="template-btn {active}" data-param="template" data-value="{name}">
            <div class="icon">{get_material_icon(icon, "28px", "#38bdf8")}</div>
            <div class="name">{short_name}</div>
            <div class="rows">~{cfg['estimated_rows']} rows</div>
        </div>
        '''
    
    mode_btns = f'''
    <div class="mode-btn {'active' if mode == 'batch' else ''}" data-param="mode" data-value="batch">
        <div class="mode-icon">{get_material_icon('inventory_2', '32px', '#38bdf8')}</div>
        <div class="mode-name">Batch</div>
        <div class="mode-desc">Historical Data</div>
    </div>
    <div class="mode-btn {'active' if mode == 'streaming' else ''}" data-param="mode" data-value="streaming">
        <div class="mode-icon">{get_material_icon('stream', '32px', '#22c55e')}</div>
        <div class="mode-name">Streaming</div>
        <div class="mode-desc">Real-time Simulation</div>
    </div>
    '''
    
    fleet_btns = ""
    for name, cfg in FLEET_PRESETS.items():
        active = "active" if name == fleet else ""
        short_name = name.split(' (')[0]
        fleet_btns += f'''
        <div class="template-btn {active}" data-param="fleet" data-value="{name}">
            <div class="name">{short_name}</div>
            <div class="rows">{cfg['desc']}</div>
        </div>
        '''
    
    # Build unified data flow cards (replaces separate mechanism + destination)
    data_flow_cards = ""
    for flow_id, cfg in DATA_FLOWS.items():
        active = "active" if flow_id == data_flow else ""
        icon = cfg.get('icon', 'settings')
        data_flow_cards += f'''
        <div class="mechanism-card {active}" data-param="data_flow" data-value="{flow_id}">
            <div class="card-header">
                {get_material_icon(icon, '28px', cfg['color'])}
                <span class="card-name">{cfg['name']}</span>
                <span class="card-latency" style="background: {cfg['color']}20; color: {cfg['color']}">{cfg['latency']}</span>
            </div>
            <div class="card-desc">{cfg['desc']}</div>
            {'<div style="margin-top: 6px; font-size: 0.8rem; color: #94a3b8;">' + get_material_icon('speed', '14px', '#94a3b8') + ' ' + cfg['throughput'] + ' &nbsp;|&nbsp; ' + cfg['billing'] + '</div>' if cfg.get('throughput') else ''}
            {'<span style="display: inline-block; margin-top: 8px; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; background: ' + cfg['color'] + '20; color: ' + cfg['color'] + ';">' + cfg['badge'] + '</span>' if cfg.get('badge') else ''}
        </div>
        '''
    
    # Build streaming architecture advisor cards
    advisor_cards = ""
    for src_name, adv in STREAMING_ADVISOR.items():
        rec_flow = DATA_FLOWS.get(adv['recommended'], {})
        alt_flow = DATA_FLOWS.get(adv['alt'], {})
        openflow_badge = '<span style="display:inline-block;margin-top:4px;padding:1px 6px;border-radius:3px;font-size:0.62rem;font-weight:600;background:rgba(245,158,11,0.12);color:#f59e0b;">Openflow</span>' if adv.get('openflow_note') else ''
        sources_html = ''.join(f'<div style="font-size:0.72rem;color:#94a3b8;">&bull; {s}</div>' for s in adv['sources'])
        advisor_cards += f'''
        <div style="padding:10px;background:#1e293b;border:1px solid #334155;border-radius:8px;cursor:pointer;transition:all 0.15s;"
             onmouseenter="this.style.borderColor='rgba(99,102,241,0.5)'"
             onmouseleave="this.style.borderColor='#334155'"
             onclick="showAdvisor('{src_name}', '{adv['recommended']}', '{adv.get('alt','')}', this)">
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">
                {get_material_icon(adv['icon'], '18px', '#a5b4fc')}
                <span style="font-size:0.82rem;font-weight:600;color:#e2e8f0;">{src_name}</span>
            </div>
            {sources_html}
            <div style="margin-top:6px;font-size:0.68rem;">
                <span style="color:{rec_flow.get('color','#38bdf8')};">&rarr; {rec_flow.get('name','')}</span>
            </div>
            {openflow_badge}
        </div>
        '''

    service_area_options = ""
    for area_id, area in UTILITY_PROFILES.items():
        selected = "selected" if area_id == service_area else ""
        service_area_options += f'<option value="{area_id}" {selected}>{area["name"]}</option>'
    
    if mode == "streaming":
        meters = fleet_cfg['meters']
        readings_per_min = meters // 15
        events_per_min = max(1, int(readings_per_min * 0.02))
        rows_per_hour = readings_per_min * 60
        rows_per_hour_fmt = format_number(rows_per_hour)
        
        est_row_size = SNOWPIPE_SDK_LIMITS['row_size_estimate_bytes']
        max_rows_per_batch = (batch_size_mb * 1024 * 1024) // est_row_size
        batches_per_second = max(1, rows_per_sec // max_rows_per_batch) if max_rows_per_batch > 0 else 1
        throughput_mb_s = (rows_per_sec * est_row_size) / (1024 * 1024)
        
        sdk_limits_html = ""
        if mechanism in ['snowpipe_classic', 'snowpipe_hp']:
            if mechanism == 'snowpipe_hp':
                sdk_info_title = f"{get_material_icon('rocket_launch', '20px', '#f59e0b')} HP Snowpipe Streaming SDK"
                sdk_info_color = '#f59e0b'
                sdk_info_desc = (
                    'High-Performance SDK streams via PIPE objects with server-side validation. '
                    'Up to <b>10 GB/s per table</b>. Requires key-pair auth and pre-created PIPE. '
                    'Throughput-based billing.'
                )
                sdk_max_throughput_label = '10 GB/s (HP SDK)'
            else:
                sdk_info_title = f"{get_material_icon('bolt', '20px', '#22c55e')} SQL INSERT via Snowpark"
                sdk_info_color = '#22c55e'
                sdk_info_desc = (
                    'SQL INSERT via Snowpark session — throughput depends on warehouse size '
                    '(XL &asymp; 25 MB/s). Rows are buffered client-side and inserted in batches. '
                    'No PIPE object or key-pair auth required. Warehouse credits billing.'
                )
                sdk_max_throughput_label = 'Warehouse-dependent'
            sdk_limits_html = f'''
            <div class="section-header">
                <span class="section-num">6</span>
                Snowpipe SDK Volume Configuration
            </div>
            <div class="info-box blue">
                <div class="title" style="color: {sdk_info_color};">{sdk_info_title}</div>
                <div class="desc">
                    {sdk_info_desc}
                    Optimal batch size is <b>10-16 MB</b>.
                </div>
            </div>
            
            <div class="volume-slider-container">
                <div class="slider-header">
                    <span class="form-label">Target Rows per Second</span>
                    <span><span class="slider-value">{rows_per_sec:,}</span> <span class="slider-unit">rows/sec</span></span>
                </div>
                <input type="range" id="rows_per_sec" min="100" max="100000" step="100" value="{rows_per_sec}" 
                       style="width: 100%;"
                       oninput="recalcSDK();"
                       onchange="recalcSDK(); syncSDKParams();">
                <div style="display: flex; justify-content: space-between; color: #64748b; font-size: 0.8rem; margin-top: 4px;">
                    <span>100</span>
                    <span>50K</span>
                    <span>100K</span>
                </div>
            </div>
            
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Batch Size (MB)</label>
                    <select id="batch_size_mb" onchange="recalcSDK(); syncSDKParams();">
                        <option value="5" {'selected' if batch_size_mb == 5 else ''}>5 MB</option>
                        <option value="10" {'selected' if batch_size_mb == 10 else ''}>10 MB (Recommended)</option>
                        <option value="16" {'selected' if batch_size_mb == 16 else ''}>16 MB (Max)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Batch Size (Rows)</label>
                    <select id="batch_size_rows" onchange="recalcSDK(); syncSDKParams();">
                        <option value="0" selected>Auto</option>
                        <option value="10">10 rows</option>
                        <option value="50">50 rows</option>
                        <option value="100">100 rows</option>
                        <option value="250">250 rows</option>
                        <option value="500">500 rows</option>
                        <option value="1000">1,000 rows</option>
                        <option value="5000">5,000 rows</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">MAX_CLIENT_LAG (seconds)</label>
                    <select id="max_client_lag" onchange="recalcSDK(); syncSDKParams();">
                        <option value="1" {'selected' if max_client_lag == 1 else ''}>1 sec (Default)</option>
                        <option value="5" {'selected' if max_client_lag == 5 else ''}>5 sec</option>
                        <option value="30" {'selected' if max_client_lag == 30 else ''}>30 sec (Iceberg)</option>
                        <option value="60" {'selected' if max_client_lag == 60 else ''}>60 sec</option>
                        <option value="300" {'selected' if max_client_lag == 300 else ''}>5 min</option>
                        <option value="600" {'selected' if max_client_lag == 600 else ''}>10 min (Max)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Est. Throughput</label>
                    <div id="est_throughput" style="background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 12px; color: #22c55e; font-weight: 600;">
                        {throughput_mb_s:.2f} MB/s
                    </div>
                </div>
            </div>
            
            <table class="sdk-limits-table">
                <tr>
                    <th>Parameter</th>
                    <th>Your Config</th>
                    <th>SDK Limit</th>
                </tr>
                <tr>
                    <td>Rows per batch</td>
                    <td class="value" id="sdk_rows_per_batch">{min(rows_per_sec, max_rows_per_batch):,} (auto)</td>
                    <td id="sdk_rows_limit">{max_rows_per_batch:,} (at {batch_size_mb}MB)</td>
                </tr>
                <tr>
                    <td>Batches per second</td>
                    <td class="value" id="sdk_batches_per_sec">{batches_per_second}</td>
                    <td>Depends on latency</td>
                </tr>
                <tr>
                    <td>Flush interval</td>
                    <td class="value" id="sdk_flush_interval">{max_client_lag} sec</td>
                    <td>1-600 sec</td>
                </tr>
                <tr>
                    <td>Max throughput</td>
                    <td class="value" id="sdk_max_throughput">{throughput_mb_s:.2f} MB/s</td>
                    <td>{sdk_max_throughput_label}</td>
                </tr>
            </table>
            
            <script>
            const ROW_SIZE_BYTES = {SNOWPIPE_SDK_LIMITS['row_size_estimate_bytes']};
            
            function recalcSDK() {{
                const rps = parseInt(document.getElementById('rows_per_sec').value) || 1000;
                const batchMB = parseInt(document.getElementById('batch_size_mb').value) || 10;
                const lag = parseInt(document.getElementById('max_client_lag').value) || 1;
                const batchRowsSel = document.getElementById('batch_size_rows');
                const batchRowsVal = batchRowsSel ? parseInt(batchRowsSel.value) || 0 : 0;
                
                const maxRowsPerBatch = Math.floor((batchMB * 1024 * 1024) / ROW_SIZE_BYTES);
                // Effective rows per batch: explicit selection (capped by SDK limit) or auto
                const effectiveRowsPerBatch = batchRowsVal > 0
                    ? Math.min(batchRowsVal, maxRowsPerBatch)
                    : Math.min(rps, maxRowsPerBatch);
                const batchesPerSec = effectiveRowsPerBatch > 0 ? Math.max(1, Math.ceil(rps / effectiveRowsPerBatch)) : 1;
                const throughputMBs = (rps * ROW_SIZE_BYTES) / (1024 * 1024);
                
                // Update slider display value
                document.querySelector('.slider-value').textContent = rps.toLocaleString();
                
                // Update throughput box
                document.getElementById('est_throughput').textContent = throughputMBs.toFixed(2) + ' MB/s';
                
                // Update table
                const batchLabel = batchRowsVal > 0
                    ? effectiveRowsPerBatch.toLocaleString() + ' (explicit)'
                    : effectiveRowsPerBatch.toLocaleString() + ' (auto)';
                document.getElementById('sdk_rows_per_batch').textContent = batchLabel;
                document.getElementById('sdk_rows_limit').textContent = maxRowsPerBatch.toLocaleString() + ' (at ' + batchMB + 'MB)';
                document.getElementById('sdk_batches_per_sec').textContent = batchesPerSec.toLocaleString();
                document.getElementById('sdk_flush_interval').textContent = lag + ' sec';
                document.getElementById('sdk_max_throughput').textContent = throughputMBs.toFixed(2) + ' MB/s';
            }}
            
            function syncSDKParams() {{
                const params = new URLSearchParams(window.location.search);
                const rowsInput = document.getElementById('rows_per_sec');
                const batchSelect = document.getElementById('batch_size_mb');
                const lagSelect = document.getElementById('max_client_lag');
                const batchRowsSelect = document.getElementById('batch_size_rows');
                if (rowsInput) params.set('rows_per_sec', rowsInput.value);
                if (batchSelect) params.set('batch_size_mb', batchSelect.value);
                if (lagSelect) params.set('max_client_lag', lagSelect.value);
                if (batchRowsSelect) {{
                    params.set('batch_size_rows', batchRowsSelect.value);
                    const hidden = document.getElementById('form_batch_size_rows');
                    if (hidden) hidden.value = batchRowsSelect.value;
                }}
                window.history.replaceState(null, '', window.location.pathname + '?' + params.toString());
            }}
            </script>
            '''
        
        # Build advisor rationale JSON for client-side display
        import json as _json
        advisor_js_data = {}
        for src_name, adv in STREAMING_ADVISOR.items():
            rec_flow = DATA_FLOWS.get(adv['recommended'], {})
            alt_flow = DATA_FLOWS.get(adv['alt'], {})
            advisor_js_data[src_name] = {
                'rationale': adv['rationale'],
                'recommended': adv['recommended'],
                'recommended_name': rec_flow.get('name', ''),
                'recommended_color': rec_flow.get('color', '#38bdf8'),
                'alt': adv.get('alt', ''),
                'alt_name': alt_flow.get('name', ''),
                'alt_color': alt_flow.get('color', '#94a3b8'),
            }
        
        advisor_script = f'''
        <script>
        const ADVISOR_DATA = {_json.dumps(advisor_js_data)};
        function showAdvisor(srcName, recFlow, altFlow, el) {{
            const info = ADVISOR_DATA[srcName];
            if (!info) return;
            const detail = document.getElementById('advisor_detail');
            const rationale = document.getElementById('advisor_rationale');
            const actions = document.getElementById('advisor_actions');
            detail.style.display = '';
            rationale.innerHTML = '<div style="font-weight:600;color:#a5b4fc;margin-bottom:6px;">' + srcName + ' &mdash; Recommendation</div>' + info.rationale;
            let btns = '<button style="padding:6px 14px;border-radius:6px;border:1px solid ' + info.recommended_color + ';background:' + info.recommended_color + '18;color:' + info.recommended_color + ';font-size:0.78rem;font-weight:600;cursor:pointer;" onclick="selectDataFlow(\\'' + recFlow + '\\')">&rarr; Use ' + info.recommended_name + '</button>';
            if (altFlow) {{
                btns += '<button style="padding:6px 14px;border-radius:6px;border:1px solid #475569;background:#1e293b;color:#94a3b8;font-size:0.78rem;font-weight:500;cursor:pointer;" onclick="selectDataFlow(\\'' + altFlow + '\\')">' + info.alt_name + ' (alternative)</button>';
            }}
            actions.innerHTML = btns;
        }}
        function selectDataFlow(flowId) {{
            const params = new URLSearchParams(window.location.search);
            params.set('data_flow', flowId);
            window.location.search = params.toString();
        }}
        </script>
        '''

        hp_auth_warning = ''
        _is_spcs = os.path.isfile('/snowflake/session/token')
        if not _is_spcs and _connection_params.get('auth_mode') != 'keypair' and mode == 'streaming':
            _warn_icon = get_material_icon('warning', '18px', '#f59e0b')
            hp_auth_warning = (
                '<div style="margin-bottom:14px;padding:10px 14px;background:rgba(245,158,11,0.07);'
                'border:1px solid rgba(245,158,11,0.25);border-radius:8px;display:flex;align-items:flex-start;'
                'gap:10px;font-size:0.82rem;color:#fbbf24;line-height:1.5;">'
                + _warn_icon +
                '<div><strong>Password auth detected.</strong> High-Performance Snowpipe Streaming requires '
                'Key-Pair authentication. HP Streaming options will fall back to standard SQL INSERT — '
                'not the native Snowpipe Streaming SDK. '
                '<a href="javascript:void(0)" onclick="document.getElementById(\'conn-overlay\').style.display=\'flex\'" '
                'style="color:#38bdf8;text-decoration:underline;cursor:pointer;">Reconnect with Key-Pair</a> '
                'to enable HP mode.</div></div>'
            )

        pipe_icon = get_material_icon('verified', '14px', '#22c55e')
        hp_pipe_section_html = ''
        if mechanism == 'snowpipe_hp' and dest != 'stage':
            hp_pipe_section_html = (
                '<div class="section-header"><span class="section-num">6</span>Streaming PIPE</div>'
                '<p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">'
                'HP Streaming requires a PIPE object with <code style="color: #f59e0b; background: rgba(245,158,11,0.1); padding: 2px 6px; border-radius: 4px;">DATA_SOURCE(TYPE =&gt; \'STREAMING\')</code>. '
                'Verify or create the pipe below.</p>'
                '<div style="display: grid; grid-template-columns: 1fr auto; gap: 12px; align-items: end; margin-bottom: 8px;">'
                '<div class="form-group" style="margin-bottom:0;">'
                '<label class="form-label">PIPE Name</label>'
                '<select id="hp_pipe_name" onchange="document.getElementById(\'form_pipe_name\').value = this.value; syncStreamFields();" style="width:100%;">'
                '<option value="">Loading pipes...</option>'
                '</select>'
                '<input type="text" id="hp_pipe_name_new" placeholder="Enter new PIPE name" '
                'oninput="document.getElementById(\'form_pipe_name\').value = this.value;" '
                'style="display:none; margin-top: 6px;">'
                '<label class="checkbox-row" style="margin-top: var(--space-sm); cursor: pointer;">'
                '<input type="checkbox" id="hp_pipe_create_new" onchange="togglePipeNewInput()">'
                '<span class="checkbox-label">Create new PIPE</span>'
                '</label>'
                '</div>'
                '<div style="display:flex;align-items:center;gap:8px;">'
                '<button type="button" class="btn-secondary" onclick="checkOrCreateHpPipe()" '
                'style="font-size: 0.85rem; padding: 8px 16px; white-space: nowrap;" id="hp_pipe_btn">'
                + pipe_icon + ' Check / Create PIPE</button>'
                '<span id="hp_pipe_status" style="font-size: 0.75rem; font-weight: 500;"></span>'
                '</div>'
                '</div>'
                '<span id="hp_pipe_msg" style="font-size: 0.82rem; color: #94a3b8; display:block; margin-bottom: 16px; overflow-wrap: break-word; word-break: break-word;"></span>'
                '<div class="divider"></div>'
            )

        config_section = f'''
        <style>
            .advisor-details[open] > .advisor-summary {{ border-radius: 10px 10px 0 0; }}
        </style>
        <div class="section-header">
            <span class="section-num">3</span>
            Data Flow
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Choose the complete data pipeline. Each option defines HOW data flows and WHERE it lands.
        </p>
        
        <!-- Streaming Architecture Advisor - BEFORE data flow cards to guide selection -->
        <details class="advisor-details" style="margin-bottom: 20px;">
            <summary class="advisor-summary" style="cursor: pointer; display: flex; align-items: center; gap: 8px; padding: 12px 16px; background: rgba(99,102,241,0.08); border: 1px solid rgba(99,102,241,0.25); border-radius: 10px; font-size: 0.88rem; font-weight: 600; color: #a5b4fc; list-style: none;">
                {get_material_icon('assistant_navigation', '18px', '#a5b4fc')} Not sure which path? Let me help you choose
                <span style="margin-left: auto; font-size: 0.72rem; color: #94a3b8; font-weight: 400;">Click to expand</span>
            </summary>
            <div style="padding: 16px; background: #0f172a; border: 1px solid rgba(99,102,241,0.15); border-top: none; border-radius: 0 0 10px 10px;">
                <p style="color: #94a3b8; font-size: 0.82rem; margin-bottom: 14px;">
                    Select your source system type below. The advisor recommends the optimal data flow and explains why.
                    Sources supported by <b style="color: #f59e0b;">Snowflake Openflow</b> (native NiFi-based connectors) are flagged.
                </p>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
                    {advisor_cards}
                </div>
                <div id="advisor_detail" style="display: none; margin-top: 14px; padding: 14px; background: rgba(99,102,241,0.04); border: 1px solid rgba(99,102,241,0.15); border-radius: 8px;">
                    <div id="advisor_rationale" style="color: #cbd5e1; font-size: 0.82rem; line-height: 1.6;"></div>
                    <div style="margin-top: 10px; display: flex; gap: 8px;" id="advisor_actions"></div>
                </div>
            </div>
        </details>
        
        {hp_auth_warning}
        <div class="mechanism-grid">
            {data_flow_cards}
        </div>
        
        <div class="section-header" style="margin-top: 24px;">
            <span class="section-num">4</span>
            Configure Fleet Size & Data Source
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Set your fleet size and choose whether to use production meter IDs from your infrastructure tables 
            or generate synthetic IDs.
        </p>
        
        <div class="fleet-grid">
            {fleet_btns}
        </div>
        
        <!-- Fleet Size Input -->
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin-bottom: 24px;">
            <div class="form-group">
                <label class="form-label">Fleet Size</label>
                <input type="number" id="custom_fleet_size" value="{meters}" min="100" max="600000" 
                       onchange="updateFleetMetrics(this.value); deselectFleetPresets();"
                       oninput="deselectFleetPresets();"
                       style="font-size: 1.125rem; font-weight: 600;">
            </div>
            <div class="form-group">
                <label class="form-label">Production Data Source</label>
                <select id="production_source" onchange="fetchProductionMeters()">
                    <option value="METER_INFRASTRUCTURE" selected>Meter Infrastructure (596K real meters)</option>
                    <option value="AMI_METADATA_SEARCH">AMI Metadata Search (596K)</option>
                    <option value="SYNTHETIC">Synthetic (No production data)</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Emission Pattern</label>
                <select id="emission_pattern" onchange="updateFleetMetrics(document.getElementById('custom_fleet_size').value)">
                    <option value="STAGGERED_REALISTIC" selected>Staggered (Realistic)</option>
                    <option value="UNIFORM">Uniform (All meters)</option>
                    <option value="PARTIAL_REPORTING">Partial (98% reporting)</option>
                    <option value="DEGRADED_NETWORK">Degraded (85% - storm sim)</option>
                </select>
            </div>
        </div>
        
        <!-- Production Matching Status -->
        <div id="production_match_status" class="info-box green" style="display: none; margin-bottom: 24px;">
            <div class="title" style="color: #10b981;">{get_material_icon('check_circle', '20px', '#10b981')} Production Matching Active</div>
            <div class="desc" id="production_match_desc">Loading production meters...</div>
        </div>
        
        <!-- Calculated Metrics (auto-update based on fleet size) -->
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 16px;" id="fleet_metrics">
            <div class="metric-card">
                <div class="metric-value" id="metric_fleet_size">{meters:,}</div>
                <div class="metric-label">Fleet Size</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_readings_per_min">{readings_per_min:,}</div>
                <div class="metric-label">Readings/min</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_events_per_min">~{events_per_min:,}</div>
                <div class="metric-label">Events/min</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_rows_per_hour">{rows_per_hour_fmt}</div>
                <div class="metric-label">Rows/hour</div>
            </div>
        </div>
        
        <!-- Extended Metrics Row -->
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px;">
            <div class="metric-card">
                <div class="metric-value" id="metric_voltage_events">~{int(meters * 0.02 / 15):,}</div>
                <div class="metric-label">Voltage Events/min</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_outages">~{max(1, int(meters * 0.005 / 15)):,}</div>
                <div class="metric-label">Outage Signals/min</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_mb_per_hour">{round(meters * 4 * 500 / (1024*1024), 1)}</div>
                <div class="metric-label">MB/hour</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="metric_gb_per_day">{round(meters * 4 * 500 * 24 / (1024*1024*1024), 2)}</div>
                <div class="metric-label">GB/day</div>
            </div>
        </div>
        
        <div class="section-header">
            <span class="section-num">5</span>
            Snowflake Target
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Choose the destination database, schema, and table for streaming data.
        </p>
        <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 16px;">
            <div class="form-group">
                <label class="form-label">Database</label>
                <select id="stream_sf_database" onchange="loadStreamSchemas(this.value)" style="width:100%;">
                    <option value="">Loading databases...</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Schema</label>
                <select id="stream_sf_schema" onchange="loadStreamTables()" style="width:100%;">
                    <option value="">Select database first</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Table</label>
                <select id="stream_table" style="width:100%;" onchange="syncStreamFields()">
                    <option value="">Select schema first</option>
                </select>
                <input type="text" id="stream_new_table" placeholder="Enter new table name" style="display: none; margin-top: 6px;" oninput="syncStreamFields()">
                <label class="checkbox-row" style="margin-top: var(--space-sm); cursor: pointer;">
                    <input type="checkbox" id="stream_create_new" onchange="toggleStreamNewTable()">
                    <span class="checkbox-label">Create new table</span>
                </label>
            </div>
        </div>
        
        <div class="divider"></div>
        
        {hp_pipe_section_html}
        
        <div class="section-header">
            <span class="section-num">{'7' if mechanism == 'snowpipe_hp' and dest != 'stage' else '6'}</span>
            Streaming Input
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Choose how data enters the pipeline &mdash; generate it locally or consume from a cloud message bus.
        </p>
        <div style="display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px;">
            <button type="button" class="btn-secondary streaming-src-btn active" data-src="test" data-color="#a78bfa"
                    onclick="setStreamSource('test')"
                    style="font-size: 0.85rem; padding: 10px 18px; border-left: 3px solid #a78bfa; display: flex; flex-direction: column; align-items: flex-start; gap: 2px; min-width: 140px;">
                <span style="display:flex;align-items:center;gap:6px;">{get_material_icon('science', '16px', '#a78bfa')} Generate Locally</span>
                <span style="font-size: 0.7rem; color: #94a3b8; font-weight: 400;">Synthetic AMI data</span>
            </button>
            <button type="button" class="btn-secondary streaming-src-btn" data-src="eventhub" data-color="#38bdf8"
                    onclick="setStreamSource('eventhub')"
                    style="font-size: 0.85rem; padding: 10px 18px; border-left: 3px solid #38bdf8; display: flex; flex-direction: column; align-items: flex-start; gap: 2px; min-width: 140px;">
                <span style="display:flex;align-items:center;gap:6px;">{get_material_icon('cloud_download', '16px', '#38bdf8')} Consume EventHub</span>
                <span style="font-size: 0.7rem; color: #94a3b8; font-weight: 400;">Azure &rarr; Snowflake</span>
            </button>
            <button type="button" class="btn-secondary streaming-src-btn" data-src="eventhub_produce" data-color="#f59e0b"
                    onclick="setStreamSource('eventhub_produce')"
                    style="font-size: 0.85rem; padding: 10px 18px; border-left: 3px solid #f59e0b; display: flex; flex-direction: column; align-items: flex-start; gap: 2px; min-width: 140px;">
                <span style="display:flex;align-items:center;gap:6px;">{get_material_icon('cloud_upload', '16px', '#f59e0b')} Publish to EventHub</span>
                <span style="font-size: 0.7rem; color: #94a3b8; font-weight: 400;">Generate &rarr; Azure</span>
            </button>
            <button type="button" class="btn-secondary streaming-src-btn" data-src="pubsub" data-color="#4ade80"
                    onclick="setStreamSource('pubsub')"
                    style="font-size: 0.85rem; padding: 10px 18px; border-left: 3px solid #4ade80; display: flex; flex-direction: column; align-items: flex-start; gap: 2px; min-width: 140px;">
                <span style="display:flex;align-items:center;gap:6px;">{get_material_icon('cloud_download', '16px', '#4ade80')} Consume Pub/Sub</span>
                <span style="font-size: 0.7rem; color: #94a3b8; font-weight: 400;">Google Cloud &rarr; Snowflake</span>
            </button>
        </div>
        <!-- EventHub Producer fields -->
        <div id="eventhub_produce_fields" style="display:none; margin-bottom: 12px;">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div class="form-group">
                    <label class="form-label">Connection String</label>
                    <input type="text" id="eh_produce_conn_str" placeholder="Endpoint=sb://..." oninput="syncStreamFields()">
                </div>
                <div class="form-group">
                    <label class="form-label">EventHub Name</label>
                    <input type="text" id="eh_produce_name" placeholder="e.g. centerpoint" oninput="syncStreamFields()">
                </div>
            </div>
            <div style="margin-top: 8px; display: flex; align-items: center; gap: 10px;">
                <button type="button" onclick="testEventHubProduce()" id="btn_test_eh_produce"
                    style="padding: 6px 16px; border-radius: 6px; border: 1px solid #f59e0b; background: rgba(245,158,11,0.1); color: #f59e0b; font-size: 0.85rem; cursor: pointer;">
                    Test Connection
                </button>
                <span id="eh_produce_test_status" style="font-size: 0.85rem;"></span>
            </div>
        </div>
        <!-- EventHub fields -->
        <div id="eventhub_fields" style="display:none; margin-bottom: 12px;">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div class="form-group">
                    <label class="form-label">Connection String</label>
                    <input type="text" id="eh_conn_str" placeholder="Endpoint=sb://..." oninput="syncStreamFields()">
                </div>
                <div class="form-group">
                    <label class="form-label">EventHub Name</label>
                    <input type="text" id="eh_name" placeholder="EventHub Name" oninput="syncStreamFields()">
                </div>
            </div>
            <div class="form-group" style="max-width: 50%;">
                <label class="form-label">Consumer Group</label>
                <input type="text" id="eh_consumer_group" value="$Default" placeholder="Consumer Group" oninput="syncStreamFields()">
            </div>
            <div style="margin-top: 8px; display: flex; align-items: center; gap: 10px;">
                <button type="button" onclick="testEventHubConsume()" id="btn_test_eh_consume"
                    style="padding: 6px 16px; border-radius: 6px; border: 1px solid #38bdf8; background: rgba(56,189,248,0.1); color: #38bdf8; font-size: 0.85rem; cursor: pointer;">
                    Test Connection
                </button>
                <span id="eh_consume_test_status" style="font-size: 0.85rem;"></span>
            </div>
        </div>
        <!-- PubSub fields -->
        <div id="pubsub_fields" style="display:none; margin-bottom: 12px;">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div class="form-group">
                    <label class="form-label">GCP Project ID</label>
                    <input type="text" id="ps_project" placeholder="GCP Project ID" oninput="syncStreamFields()">
                </div>
                <div class="form-group">
                    <label class="form-label">Subscription Name</label>
                    <input type="text" id="ps_subscription" placeholder="Subscription Name" oninput="syncStreamFields()">
                </div>
            </div>
            <div style="margin-top: 8px; display: flex; align-items: center; gap: 10px;">
                <button type="button" onclick="testPubSub()" id="btn_test_pubsub"
                    style="padding: 6px 16px; border-radius: 6px; border: 1px solid #4ade80; background: rgba(74,222,128,0.1); color: #4ade80; font-size: 0.85rem; cursor: pointer;">
                    Test Connection
                </button>
                <span id="pubsub_test_status" style="font-size: 0.85rem;"></span>
            </div>
        </div>
        
        
        <script>
        // Toggle PIPE name between dropdown and manual text input
        function togglePipeNewInput() {{
            const checkbox = document.getElementById('hp_pipe_create_new');
            const selectEl = document.getElementById('hp_pipe_name');
            const newInput = document.getElementById('hp_pipe_name_new');
            const formPipe = document.getElementById('form_pipe_name');
            if (checkbox && checkbox.checked) {{
                if (selectEl) selectEl.style.display = 'none';
                if (newInput) {{ newInput.style.display = 'block'; newInput.value = ''; }}
                if (formPipe) formPipe.value = '';
            }} else {{
                if (selectEl) selectEl.style.display = 'block';
                if (newInput) newInput.style.display = 'none';
                if (formPipe) formPipe.value = selectEl ? selectEl.value : '';
            }}
        }}

        // Fetch pipes for the currently selected database/schema and populate the PIPE dropdown
        async function loadStreamPipes() {{
            const dbEl = document.getElementById('stream_sf_database');
            const schemaEl = document.getElementById('stream_sf_schema');
            const selectEl = document.getElementById('hp_pipe_name');
            if (!dbEl || !schemaEl || !selectEl) return;
            const db = dbEl.value;
            const schema = schemaEl.value;
            if (!db || !schema) {{
                selectEl.innerHTML = '<option value="">Select target first</option>';
                return;
            }}
            selectEl.innerHTML = '<option value="">Loading pipes...</option>';
            try {{
                const resp = await fetch(`/api/pipes/${{encodeURIComponent(db)}}/${{encodeURIComponent(schema)}}`);
                const data = await resp.json();
                const pipes = data.pipes || [];
                selectEl.innerHTML = '';
                if (pipes.length === 0) {{
                    selectEl.innerHTML = '<option value="">No pipes found — create one below</option>';
                }} else {{
                    selectEl.innerHTML = '<option value="">-- Select PIPE --</option>';
                    pipes.forEach(name => {{
                        const opt = document.createElement('option');
                        opt.value = name;
                        opt.textContent = name;
                        selectEl.appendChild(opt);
                    }});
                }}
                // Auto-select if there's only one pipe
                if (pipes.length === 1) {{
                    selectEl.value = pipes[0];
                    if (document.getElementById('form_pipe_name')) {{
                        document.getElementById('form_pipe_name').value = pipes[0];
                    }}
                }}
            }} catch (e) {{
                console.error('Failed to load pipes:', e);
                selectEl.innerHTML = '<option value="">Error loading pipes</option>';
            }}
        }}

        // ── Test Connection helpers ──
        async function testEventHubProduce() {{
            const connStr = document.getElementById('eh_produce_conn_str')?.value || '';
            const hubName = document.getElementById('eh_produce_name')?.value || '';
            const statusEl = document.getElementById('eh_produce_test_status');
            const btn = document.getElementById('btn_test_eh_produce');
            if (!connStr || !hubName) {{ statusEl.innerHTML = '<span style="color:#f87171;">Fill in both fields first</span>'; return; }}
            btn.disabled = true; btn.textContent = 'Testing...';
            statusEl.innerHTML = '<span style="color:#94a3b8;">Connecting...</span>';
            try {{
                const body = new FormData();
                body.append('conn_str', 'b64:' + btoa(connStr));
                body.append('hub_name', hubName);
                body.append('mode', 'produce');
                const resp = await fetch('/api/test-eventhub', {{ method: 'POST', body }});
                const data = await resp.json();
                if (data.status === 'ok') {{
                    statusEl.innerHTML = '<span style="color:#4ade80;">&#10003; ' + data.message + '</span>';
                }} else {{
                    statusEl.innerHTML = '<span style="color:#f87171;">&#10007; ' + data.message + '</span>';
                }}
            }} catch (e) {{
                statusEl.innerHTML = '<span style="color:#f87171;">&#10007; Network error</span>';
            }}
            btn.disabled = false; btn.textContent = 'Test Connection';
        }}

        async function testEventHubConsume() {{
            const connStr = document.getElementById('eh_conn_str')?.value || '';
            const hubName = document.getElementById('eh_name')?.value || '';
            const statusEl = document.getElementById('eh_consume_test_status');
            const btn = document.getElementById('btn_test_eh_consume');
            if (!connStr || !hubName) {{ statusEl.innerHTML = '<span style="color:#f87171;">Fill in both fields first</span>'; return; }}
            btn.disabled = true; btn.textContent = 'Testing...';
            statusEl.innerHTML = '<span style="color:#94a3b8;">Connecting...</span>';
            try {{
                const body = new FormData();
                body.append('conn_str', 'b64:' + btoa(connStr));
                body.append('hub_name', hubName);
                body.append('mode', 'consume');
                const resp = await fetch('/api/test-eventhub', {{ method: 'POST', body }});
                const data = await resp.json();
                if (data.status === 'ok') {{
                    statusEl.innerHTML = '<span style="color:#4ade80;">&#10003; ' + data.message + '</span>';
                }} else {{
                    statusEl.innerHTML = '<span style="color:#f87171;">&#10007; ' + data.message + '</span>';
                }}
            }} catch (e) {{
                statusEl.innerHTML = '<span style="color:#f87171;">&#10007; Network error</span>';
            }}
            btn.disabled = false; btn.textContent = 'Test Connection';
        }}

        async function testPubSub() {{
            const projectId = document.getElementById('ps_project')?.value || '';
            const subName = document.getElementById('ps_subscription')?.value || '';
            const statusEl = document.getElementById('pubsub_test_status');
            const btn = document.getElementById('btn_test_pubsub');
            if (!projectId || !subName) {{ statusEl.innerHTML = '<span style="color:#f87171;">Fill in both fields first</span>'; return; }}
            btn.disabled = true; btn.textContent = 'Testing...';
            statusEl.innerHTML = '<span style="color:#94a3b8;">Connecting...</span>';
            try {{
                const body = new FormData();
                body.append('project_id', projectId);
                body.append('subscription_name', subName);
                const resp = await fetch('/api/test-pubsub', {{ method: 'POST', body }});
                const data = await resp.json();
                if (data.status === 'ok') {{
                    statusEl.innerHTML = '<span style="color:#4ade80;">&#10003; ' + data.message + '</span>';
                }} else {{
                    statusEl.innerHTML = '<span style="color:#f87171;">&#10007; ' + data.message + '</span>';
                }}
            }} catch (e) {{
                statusEl.innerHTML = '<span style="color:#f87171;">&#10007; Network error</span>';
            }}
            btn.disabled = false; btn.textContent = 'Test Connection';
        }}

        // Sync main-column inputs to sidebar form hidden fields
        function syncStreamFields() {{
            const f = id => document.getElementById(id);
            // Table
            const createNew = f('stream_create_new')?.checked;
            const tbl = createNew ? (f('stream_new_table')?.value || '') : (f('stream_table')?.value || '');
            if (f('form_table')) f('form_table').value = tbl;
            if (f('form_new_table')) f('form_new_table').value = createNew ? (f('stream_new_table')?.value || '') : '';
            // Source fields
            const srcMode = f('form_stream_source')?.value || 'test';
            if (srcMode === 'eventhub_produce') {{
                // Producer mode: copy from producer inputs into the shared hidden fields
                // Base64-encode the connection string to avoid semicolons being parsed as form delimiters
                const rawConn = f('eh_produce_conn_str')?.value || '';
                if (f('form_eh_conn_str')) f('form_eh_conn_str').value = rawConn ? 'b64:' + btoa(rawConn) : '';
                if (f('form_eh_name')) f('form_eh_name').value = f('eh_produce_name')?.value || '';
                if (f('form_eh_consumer_group')) f('form_eh_consumer_group').value = '';
            }} else {{
                // Consumer mode: also base64-encode the connection string
                const rawConn = f('eh_conn_str')?.value || '';
                if (f('form_eh_conn_str')) f('form_eh_conn_str').value = rawConn ? 'b64:' + btoa(rawConn) : '';
                if (f('form_eh_name')) f('form_eh_name').value = f('eh_name')?.value || '';
                if (f('form_eh_consumer_group')) f('form_eh_consumer_group').value = f('eh_consumer_group')?.value || '$Default';
            }}
            if (f('form_ps_project')) f('form_ps_project').value = f('ps_project')?.value || '';
            if (f('form_ps_subscription')) f('form_ps_subscription').value = f('ps_subscription')?.value || '';
            // Update sidebar display
            const targetDisplay = f('sidebar_target_display');
            if (targetDisplay) {{
                const tableName = tbl ? tbl.split('.').pop() : 'Not selected';
                targetDisplay.textContent = tableName;
                targetDisplay.style.color = tbl ? '#38bdf8' : '#64748b';
            }}
            const sourceDisplay = f('sidebar_source_display');
            if (sourceDisplay) {{
                const srcVal = f('form_stream_source')?.value || 'test';
                const labels = {{ test: 'Generate Locally', eventhub: 'Consume EventHub', eventhub_produce: 'Publish to EventHub', pubsub: 'Consume Pub/Sub' }};
                sourceDisplay.textContent = labels[srcVal] || srcVal;
            }}
        }}
        function setStreamSource(src) {{
            document.getElementById('form_stream_source').value = src;
            document.querySelectorAll('.streaming-src-btn').forEach(b => {{
                const catColor = b.dataset.color || '#38bdf8';
                b.classList.toggle('active', b.dataset.src === src);
                if (b.dataset.src === src) {{
                    // Convert hex to rgba for background tint
                    const r = parseInt(catColor.slice(1,3),16);
                    const g = parseInt(catColor.slice(3,5),16);
                    const bl = parseInt(catColor.slice(5,7),16);
                    b.style.background = `rgba(${{r}},${{g}},${{bl}},0.12)`;
                    b.style.color = catColor;
                    b.style.borderColor = catColor;
                    b.style.borderLeftColor = catColor;
                }} else {{
                    b.style.background = '';
                    b.style.color = '';
                    b.style.borderColor = '';
                    b.style.borderLeftColor = catColor;
                }}
            }});
            document.getElementById('eventhub_fields').style.display = src === 'eventhub' ? '' : 'none';
            document.getElementById('eventhub_produce_fields').style.display = src === 'eventhub_produce' ? '' : 'none';
            document.getElementById('pubsub_fields').style.display = src === 'pubsub' ? '' : 'none';
            // Hide test-data-only steps (Behavior/Format + Preview) for external consumer sources
            // but show them for test data and eventhub_produce (which generates data)
            const showGenSteps = (src === 'test' || src === 'eventhub_produce');
            const s8 = document.getElementById('step-8-section');
            const s9 = document.getElementById('step-9-section');
            if (s8) s8.style.display = showGenSteps ? '' : 'none';
            if (s9) s9.style.display = showGenSteps ? '' : 'none';
            syncStreamFields();
            if (typeof updateStepAvailability === 'function') updateStepAvailability();
        }}
        </script>
        
        <div class="divider"></div>
        
        <div class="section-header">
            <span class="section-num">{'8' if mechanism == 'snowpipe_hp' and dest != 'stage' else '7'}</span>
            Streaming Behavior & Data Format
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Configure emission patterns, data format, and preview records before streaming.
        </p>
        
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin-bottom: 20px;">
            <div class="form-group">
                <label class="form-label">Reading Interval</label>
                <select id="reading_interval" onchange="syncFormFields()">
                    <option value="5">5 minutes</option>
                    <option value="15" selected>15 minutes (Standard AMI)</option>
                    <option value="30">30 minutes</option>
                    <option value="60">60 minutes</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Customer Segment Filter</label>
                <select id="segment_filter" onchange="syncFormFields(); fetchProductionMeters();">
                    <option value="">All Segments</option>
                    <option value="RESIDENTIAL">Residential Only</option>
                    <option value="COMMERCIAL">Commercial Only</option>
                    <option value="INDUSTRIAL">Industrial Only</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Data Format</label>
                {'<select id="data_format" disabled style="background: rgba(14,165,233,0.1); border-color: #0ea5e9;"><option value="raw_ami" selected>Raw JSON (VARIANT)</option></select><div style="font-size: 0.8125rem; color: #0ea5e9; margin-top: 6px;">Stage Landing uses raw JSON format</div>' if dest == 'stage' else '<select id="data_format" onchange="syncFormFields()"><option value="standard" selected>Standard AMI (Structured)</option><option value="raw_ami">Raw AMI (with VARIANT)</option><option value="minimal">Minimal (Essential only)</option><optgroup label="Energy Solutions Marketplace"><option value="itron_grid_planning">Itron Grid Planning (8,760-hr)</option><option value="symphony_iris">SymphonyAI IRIS Foundry</option><option value="carto_spatial">CARTO Spatial Analytics</option><option value="siemens_edge">Siemens Industrial Edge</option></optgroup></select>'}
            </div>
        </div>
        
        <div class="info-box blue" id="emission_info" style="margin-bottom: 32px;">
            <div class="title" style="color: #38bdf8;">{get_material_icon('info', '20px', '#38bdf8')} Staggered Emission Pattern</div>
            <div class="desc">
                Meters report across the full 15-minute interval (~900 seconds spread). 
                This mimics real AMI behavior where readings arrive continuously as each meter's interval elapses.
                <b>Report rate:</b> 100% of meters report each interval.
            </div>
        </div>
        
        <!-- Preview Section -->
        <div class="section-header">
            <span class="section-num">{'9' if mechanism == 'snowpipe_hp' and dest != 'stage' else '8'}</span>
            Preview Records
        </div>
        <p style="color: #94a3b8; font-size: 0.9375rem; margin-bottom: 16px;">
            Verify your configuration - generate sample records to confirm production matching.
        </p>
        
        <button type="button" class="btn-secondary" onclick="previewRecords()" 
                style="margin-bottom: 20px; display: inline-flex; align-items: center; gap: 8px; font-size: 0.9375rem; padding: 12px 20px;">
            {get_material_icon('preview', '20px')} Preview Sample Records
        </button>
        
        <div id="preview_results" style="display: none;">
            <div class="info-box purple" id="preview_status">
                <div class="title" style="color: #a855f7;">Loading preview...</div>
            </div>
            <div id="preview_table_container" style="margin-top: 16px; width: 100%; overflow-x: auto; overflow-y: visible; position: relative;"></div>
        </div>
        
        <script>
        // Preview records function with dynamic loading stages
        async function previewRecords() {{
            const resultsDiv = document.getElementById('preview_results');
            const statusDiv = document.getElementById('preview_status');
            const tableContainer = document.getElementById('preview_table_container');
            
            const source = document.getElementById('production_source').value;
            const fleetSize = document.getElementById('custom_fleet_size').value;
            const pattern = document.getElementById('emission_pattern').value;
            const segment = document.getElementById('segment_filter').value;
            const dataFormat = document.getElementById('data_format').value;
            
            resultsDiv.style.display = 'block';
            tableContainer.innerHTML = '';
            
            // Insight: Dynamic loading stages make wait times feel productive
            // Show users what's happening in the data pipeline - builds confidence
            const loadingStages = [
                {{ icon: 'cloud_download', msg: `Connecting to production source...`, detail: source }},
                {{ icon: 'search', msg: `Sampling meter fleet...`, detail: `${{Number(fleetSize).toLocaleString()}} meters${{segment ? ' (' + segment + ')' : ''}}` }},
                {{ icon: 'bolt', msg: `Generating AMI readings...`, detail: pattern.replace(/_/g, ' ').toLowerCase() }},
                {{ icon: 'transform', msg: `Applying data transformations...`, detail: dataFormat.toUpperCase() + ' format' }},
                {{ icon: 'verified', msg: `Validating output schema...`, detail: '13 columns' }}
            ];
            
            let currentStage = 0;
            let dotCount = 0;
            
            // Render loading state with animated progress
            function renderLoadingState() {{
                const stage = loadingStages[currentStage];
                const dots = '.'.repeat(dotCount % 4);
                const progress = Math.round(((currentStage + 1) / loadingStages.length) * 100);
                
                // Build stage indicators
                let stagesHtml = '<div style="display: flex; gap: 6px; margin-top: 12px; flex-wrap: wrap;">';
                loadingStages.forEach((s, i) => {{
                    const isComplete = i < currentStage;
                    const isCurrent = i === currentStage;
                    const color = isComplete ? '#22c55e' : (isCurrent ? '#38bdf8' : '#475569');
                    const bg = isComplete ? 'rgba(34,197,94,0.15)' : (isCurrent ? 'rgba(56,189,248,0.2)' : 'rgba(71,85,105,0.1)');
                    const icon = isComplete ? 'check_circle' : s.icon;
                    stagesHtml += `<div style="display: flex; align-items: center; gap: 4px; padding: 4px 8px; background: ${{bg}}; border-radius: 6px; border: 1px solid ${{color}}30;">
                        <span class="material-symbols-outlined" style="font-size: 14px; color: ${{color}};">${{icon}}</span>
                        <span style="font-size: 0.7rem; color: ${{color}}; white-space: nowrap;">${{i + 1}}</span>
                    </div>`;
                }});
                stagesHtml += '</div>';
                
                statusDiv.className = 'info-box blue';
                statusDiv.innerHTML = `
                    <div class="title" style="color: #38bdf8; display: flex; align-items: center; gap: 10px;">
                        <span class="material-symbols-outlined" style="font-size: 20px; animation: pulse 1s ease-in-out infinite;">${{stage.icon}}</span>
                        ${{stage.msg}}${{dots}}
                    </div>
                    <div class="desc" style="margin-top: 8px;">
                        <div style="display: flex; align-items: center; gap: 12px;">
                            <span style="color: #7dd3fc; font-weight: 500;">${{stage.detail}}</span>
                            <span style="color: #64748b;">Step ${{currentStage + 1}} of ${{loadingStages.length}}</span>
                        </div>
                        <div style="margin-top: 10px; background: #1e293b; border-radius: 4px; height: 6px; overflow: hidden;">
                            <div style="background: linear-gradient(90deg, #0ea5e9, #38bdf8); height: 100%; width: ${{progress}}%; transition: width 0.3s ease; border-radius: 4px;"></div>
                        </div>
                        ${{stagesHtml}}
                    </div>
                `;
            }}
            
            // Add CSS animation if not present
            if (!document.getElementById('preview-loading-styles')) {{
                const styleEl = document.createElement('style');
                styleEl.id = 'preview-loading-styles';
                styleEl.textContent = `@keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}`;
                document.head.appendChild(styleEl);
            }}
            
            renderLoadingState();
            
            // Animate through stages (every 600ms advance stage, every 200ms update dots)
            const stageInterval = setInterval(() => {{
                if (currentStage < loadingStages.length - 1) {{
                    currentStage++;
                    renderLoadingState();
                }}
            }}, 600);
            
            const dotInterval = setInterval(() => {{
                dotCount++;
                renderLoadingState();
            }}, 200);
            
            try {{
                const url = `/api/streaming/preview?production_source=${{source}}&fleet_size=${{fleetSize}}&emission_pattern=${{pattern}}&data_format=${{dataFormat}}${{segment ? '&segment_filter=' + segment : ''}}`;
                const resp = await fetch(url);
                
                // Clear loading animations
                clearInterval(stageInterval);
                clearInterval(dotInterval);
                
                // Check if response is OK and content-type is JSON
                const contentType = resp.headers.get('content-type');
                if (!resp.ok || !contentType || !contentType.includes('application/json')) {{
                    const errorText = await resp.text();
                    throw new Error(errorText.substring(0, 200) || `Server error: ${{resp.status}}`);
                }}
                
                const data = await resp.json();
                
                if (data.status === 'success' && data.records.length > 0) {{
                    statusDiv.className = 'info-box green';
                    const matchStatus = data.production_matched ? 
                        '<span style="color: #22c55e;">Production Matched</span>' : 
                        '<span style="color: #f59e0b;">Synthetic IDs</span>';
                    statusDiv.innerHTML = `
                        <div class="title" style="color: #22c55e;">{get_material_icon('check_circle', '20px', '#22c55e')} Preview Generated</div>
                        <div class="desc">
                            ${{matchStatus}} - ${{data.sample_size}} sample records from <code>${{data.production_source}}</code><br>
                            <b>Data format:</b> ${{data.data_format}} | <b>Pattern:</b> ${{data.emission_pattern}}<br>
                            <small>${{data.notes.join(' | ')}}</small>
                        </div>
                    `;
                    
                    // Build HTML table with horizontal scroll wrapper
                    // Using display:block + overflow-x:scroll for reliable horizontal scrolling
                    const cols = data.schema.filter(c => c !== 'RAW_PAYLOAD');
                    let tableHtml = '<div style="display: block; overflow-x: scroll; overflow-y: visible; max-width: 100%; border-radius: 8px; border: 1px solid #334155; -webkit-overflow-scrolling: touch;">';
                    tableHtml += '<table style="width: auto; min-width: max-content; border-collapse: collapse; font-size: 0.85rem; table-layout: auto;">';
                    tableHtml += '<thead><tr style="background: rgba(56,189,248,0.1);">';
                    cols.forEach(col => {{
                        tableHtml += `<th style="padding: 10px 14px; text-align: left; border-bottom: 1px solid #334155; color: #94a3b8; white-space: nowrap;">${{col}}</th>`;
                    }});
                    tableHtml += '</tr></thead><tbody>';
                    
                    data.records.forEach((row, idx) => {{
                        const bgColor = idx % 2 === 0 ? 'rgba(15,23,42,0.5)' : 'rgba(30,41,59,0.5)';
                        tableHtml += `<tr style="background: ${{bgColor}};">`;
                        cols.forEach(col => {{
                            let val = row[col];
                            if (val === null || val === undefined) val = '-';
                            if (typeof val === 'number') val = val.toLocaleString();
                            if (typeof val === 'boolean') val = val ? 'true' : 'false';
                            // Highlight production-matched meter IDs
                            const style = col === 'METER_ID' && data.production_matched ? 
                                'color: #22c55e; font-weight: 600; white-space: nowrap;' : 'color: #e2e8f0; white-space: nowrap;';
                            tableHtml += `<td style="padding: 10px 14px; border-bottom: 1px solid #1e293b; ${{style}}">${{val}}</td>`;
                        }});
                        tableHtml += '</tr>';
                    }});
                    tableHtml += '</tbody></table></div>';
                    
                    // Build sample JSON file preview (what the raw file will look like)
                    if (data.records.length > 0) {{
                        // Create a sample record in the format that will be written to stage/file
                        const sampleRecord = data.records[0];
                        const jsonFilePreview = {{
                            meter_id: sampleRecord.METER_ID,
                            transformer_id: sampleRecord.TRANSFORMER_ID,
                            circuit_id: sampleRecord.CIRCUIT_ID,
                            substation_id: sampleRecord.SUBSTATION_ID,
                            reading_timestamp: sampleRecord.READING_TIMESTAMP,
                            usage_kwh: sampleRecord.USAGE_KWH,
                            voltage: sampleRecord.VOLTAGE,
                            power_factor: sampleRecord.POWER_FACTOR,
                            customer_segment: sampleRecord.CUSTOMER_SEGMENT,
                            latitude: sampleRecord.LATITUDE,
                            longitude: sampleRecord.LONGITUDE,
                            data_quality: sampleRecord.DATA_QUALITY,
                            emission_pattern: sampleRecord.EMISSION_PATTERN
                        }};
                        
                        tableHtml += `
                            <div style="margin-top: 24px;">
                                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 12px;">
                                    <span style="color: #a855f7; font-size: 1.1rem;">{{</span>
                                    <span style="color: #e2e8f0; font-size: 0.9rem; font-weight: 600;">Sample JSON Record</span>
                                    <span style="font-size: 0.75rem; color: #64748b; background: rgba(168,85,247,0.15); padding: 2px 8px; border-radius: 4px;">Raw file format</span>
                                </div>
                                <pre style="background: #0f172a; padding: 16px 20px; border-radius: 8px; font-size: 0.8rem; overflow-x: auto; color: #e2e8f0; border: 1px solid #334155; line-height: 1.6; font-family: 'SF Mono', 'Fira Code', 'Monaco', monospace;"><code>${{JSON.stringify(jsonFilePreview, null, 2).replace(/"([^"]+)":/g, '<span style="color: #7dd3fc;">"$1"</span>:').replace(/: "([^"]+)"/g, ': <span style="color: #86efac;">"$1"</span>').replace(/: (\\d+\\.?\\d*)/g, ': <span style="color: #fbbf24;">$1</span>').replace(/: (null)/g, ': <span style="color: #94a3b8;">$1</span>')}}</code></pre>
                                <div style="margin-top: 8px; font-size: 0.75rem; color: #64748b;">
                                    Each line in the stage file will contain one JSON object like the above.
                                </div>
                            </div>
                        `;
                    }}
                    
                    tableContainer.innerHTML = tableHtml;
                }} else {{
                    statusDiv.className = 'info-box orange';
                    statusDiv.innerHTML = `
                        <div class="title" style="color: #f59e0b;">{get_material_icon('warning', '20px', '#f59e0b')} Preview Failed</div>
                        <div class="desc">${{data.error || 'No records generated. Check your configuration.'}}</div>
                    `;
                }}
            }} catch (e) {{
                // Clear loading animations on error
                clearInterval(stageInterval);
                clearInterval(dotInterval);
                statusDiv.className = 'info-box red';
                statusDiv.innerHTML = `
                    <div class="title" style="color: #ef4444;">{get_material_icon('error', '20px', '#ef4444')} Error</div>
                    <div class="desc">${{e.message}}</div>
                `;
            }}
        }}
        
        // Deselect fleet preset buttons when user types custom value
        function deselectFleetPresets() {{
            document.querySelectorAll('[data-param="fleet"]').forEach(btn => {{
                btn.classList.remove('active');
            }});
        }}
        
        // Fleet size and metrics calculation
        async function updateFleetMetrics(fleetSize) {{
            const pattern = document.getElementById('emission_pattern').value;
            try {{
                const resp = await fetch(`/api/streaming/calculate-metrics?fleet_size=${{fleetSize}}&emission_pattern=${{pattern}}`);
                const data = await resp.json();
                
                // Update metric cards
                document.getElementById('metric_fleet_size').textContent = Number(fleetSize).toLocaleString();
                document.getElementById('metric_readings_per_min').textContent = Math.round(data.metrics.readings_per_min).toLocaleString();
                document.getElementById('metric_events_per_min').textContent = '~' + Math.round(data.metrics.total_events_per_min).toLocaleString();
                document.getElementById('metric_rows_per_hour').textContent = data.metrics.rows_per_hour_formatted;
                document.getElementById('metric_mb_per_hour').textContent = data.metrics.mb_per_hour;
                document.getElementById('metric_gb_per_day').textContent = data.metrics.gb_per_day;
                
                // Update extended metrics
                if (data.events_per_interval) {{
                    document.getElementById('metric_voltage_events').textContent = '~' + Math.round(data.events_per_interval.voltage_anomalies / 15).toLocaleString();
                    document.getElementById('metric_outages').textContent = '~' + Math.max(1, Math.round(data.events_per_interval.outages / 15)).toLocaleString();
                }}
                
                // Update emission info box
                const infoBox = document.getElementById('emission_info');
                const patternName = document.getElementById('emission_pattern').options[document.getElementById('emission_pattern').selectedIndex].text;
                infoBox.innerHTML = `
                    <div class="title" style="color: #38bdf8;">{get_material_icon('info', '20px', '#38bdf8')} ${{patternName}}</div>
                    <div class="desc">
                        ${{data.pattern_description}}<br>
                        <b>Report rate:</b> ${{data.metrics.report_percentage}}% of meters report each interval.
                        <b>Stagger:</b> ${{data.stagger_seconds}} seconds spread.
                    </div>
                `;
            }} catch (e) {{
                console.error('Failed to update metrics:', e);
            }}
        }}
        
        // Fetch production meters when source changes
        async function fetchProductionMeters() {{
            const source = document.getElementById('production_source').value;
            const fleetSize = document.getElementById('custom_fleet_size').value;
            const segment = document.getElementById('segment_filter')?.value || '';
            const statusDiv = document.getElementById('production_match_status');
            const descDiv = document.getElementById('production_match_desc');
            
            if (source === 'SYNTHETIC') {{
                statusDiv.style.display = 'none';
                return;
            }}
            
            statusDiv.style.display = 'block';
            statusDiv.className = 'info-box blue';
            descDiv.textContent = 'Fetching production meters...';
            
            try {{
                const url = `/api/production/meters?source=${{source}}&sample_size=${{fleetSize}}${{segment ? '&segment=' + segment : ''}}`;
                const resp = await fetch(url);
                const data = await resp.json();
                
                if (data.status === 'fetched' || data.status === 'cached') {{
                    statusDiv.className = 'info-box green';
                    descDiv.innerHTML = `<b>${{data.count.toLocaleString()}} real meters</b> loaded from <code>${{data.table || source}}</code>. ` +
                        `Streamed data will match actual grid assets in your infrastructure tables.`;
                }} else if (data.status === 'generated') {{
                    statusDiv.className = 'info-box purple';
                    descDiv.innerHTML = `<b>${{data.count.toLocaleString()}} synthetic meters</b> generated. ` +
                        `Meter IDs are synthetic (MTR-SYN-XXXXXX) - not matched to production data.`;
                }} else {{
                    statusDiv.className = 'info-box orange';
                    descDiv.textContent = `Error: ${{data.error || 'Unknown error'}}`;
                }}
            }} catch (e) {{
                statusDiv.className = 'info-box orange';
                descDiv.textContent = `Failed to fetch meters: ${{e.message}}`;
            }}
        }}
        
        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {{
            fetchProductionMeters();
            updateFleetMetrics(document.getElementById('custom_fleet_size').value);
        }});
        </script>
        
        {sdk_limits_html}
        {advisor_script}
        
        <!-- Guided Flow: runtime section wrapping + step availability -->
        <script>
        (function() {{
            // ── Runtime section wrapping ──
            // Maps section header text (prefix) → step ID
            const stepMap = [
                ['Data Flow', 'step-3-section'],
                ['Configure Fleet', 'step-4-section'],
                ['Snowflake Target', 'step-5-section'],
                ['Streaming PIPE', 'step-6-section'],
                ['Streaming Input', 'step-7-section'],
                ['Streaming Behavior', 'step-8-section'],
                ['Preview Records', 'step-9-section'],
            ];
            
            function wrapSections() {{
                const headers = document.querySelectorAll('.section-header');
                headers.forEach(hdr => {{
                    const text = hdr.textContent.trim();
                    const match = stepMap.find(([prefix]) => text.includes(prefix));
                    if (!match) return;
                    const stepId = match[1];
                    // Don't double-wrap
                    if (document.getElementById(stepId)) return;
                    
                    // Collect this header + all siblings until the next section-header or divider
                    const wrapper = document.createElement('div');
                    wrapper.id = stepId;
                    wrapper.className = 'step-section';
                    
                    hdr.parentNode.insertBefore(wrapper, hdr);
                    
                    let node = wrapper.nextSibling;
                    wrapper.appendChild(hdr);
                    while (node) {{
                        const next = node.nextSibling;
                        // Stop at divider or next section-header
                        if (node.nodeType === 1) {{
                            if (node.classList && node.classList.contains('divider')) break;
                            if (node.classList && node.classList.contains('section-header')) break;
                            // Also stop if we hit another step-section (already wrapped)
                            if (node.classList && node.classList.contains('step-section')) break;
                        }}
                        wrapper.appendChild(node);
                        node = next;
                    }}
                }});
            }}
            
            // ── Step lock/unlock helpers ──
            function lockStep(stepId, msg) {{
                const el = document.getElementById(stepId);
                if (!el) return;
                el.classList.add('step-locked');
                el.classList.remove('step-unlocking');
                el.setAttribute('data-lock-msg', msg || 'Complete previous steps first');
            }}
            function unlockStep(stepId) {{
                const el = document.getElementById(stepId);
                if (!el) return;
                if (!el.classList.contains('step-locked')) return;
                el.classList.remove('step-locked');
                el.classList.add('step-unlocking');
                el.removeAttribute('data-lock-msg');
                setTimeout(() => el.classList.remove('step-unlocking'), 400);
            }}
            
            // ── Main availability logic ──
            window.updateStepAvailability = function() {{
                const f = id => document.getElementById(id);
                
                // Step 3: Data Flow — always unlocked
                unlockStep('step-3-section');
                
                // Step 4: Fleet — needs a data flow card selected
                const hasFlow = !!document.querySelector('.mechanism-card.selected, .flow-card.selected, .data-flow-card.selected, input[name="data_flow"]:checked');
                // Also accept if the page loaded with a pre-selected flow (the card might use 'active' class)
                const hasFlowAlt = !!document.querySelector('.mechanism-card.active');
                if (hasFlow || hasFlowAlt) {{
                    unlockStep('step-4-section');
                }} else {{
                    lockStep('step-4-section', 'Select a data flow first');
                    // Lock everything downstream
                    lockStep('step-5-section', 'Select a data flow first');
                    lockStep('step-6-section', 'Select a data flow first');
                    lockStep('step-7-section', 'Select a data flow first');
                    lockStep('step-8-section', 'Select a data flow first');
                    lockStep('step-9-section', 'Select a data flow first');
                    setSubmitState(false);
                    return;
                }}
                
                // Step 5: Target — needs fleet size > 0
                const fleetEl = f('custom_fleet_size');
                const hasFleet = fleetEl && parseInt(fleetEl.value) > 0;
                if (hasFleet) {{
                    unlockStep('step-5-section');
                }} else {{
                    lockStep('step-5-section', 'Set fleet size first');
                    lockStep('step-6-section', 'Set fleet size first');
                    lockStep('step-7-section', 'Set fleet size first');
                    lockStep('step-8-section', 'Set fleet size first');
                    lockStep('step-9-section', 'Set fleet size first');
                    setSubmitState(false);
                    return;
                }}
                
                // Step 6 (HP PIPE) + Step 7 (Data Source): need table selected
                const createNew = f('stream_create_new')?.checked;
                const hasTable = createNew
                    ? !!(f('stream_new_table')?.value.trim())
                    : !!(f('stream_table')?.value);
                
                if (hasTable) {{
                    unlockStep('step-6-section');
                    unlockStep('step-7-section');
                }} else {{
                    lockStep('step-6-section', 'Select a target table first');
                    lockStep('step-7-section', 'Select a target table first');
                    lockStep('step-8-section', 'Select a target table first');
                    lockStep('step-9-section', 'Select a target table first');
                    setSubmitState(false);
                    return;
                }}
                
                // Steps 8 & 9: only for test data source (visibility handled by setStreamSource)
                const src = f('form_stream_source')?.value || 'test';
                if (src === 'test') {{
                    unlockStep('step-8-section');
                    unlockStep('step-9-section');
                }}
                
                // Enable submit when all required steps are satisfied
                setSubmitState(true);
            }};
            
            // ── Submit button state ──
            function setSubmitState(enabled) {{
                const form = document.getElementById('streaming_form');
                if (!form) return;
                const btn = form.querySelector('button[type="submit"]');
                if (!btn) return;
                if (enabled) {{
                    btn.disabled = false;
                    btn.style.opacity = '';
                    btn.style.cursor = '';
                    btn.style.pointerEvents = '';
                }} else {{
                    btn.disabled = true;
                    btn.style.opacity = '0.4';
                    btn.style.cursor = 'not-allowed';
                    btn.style.pointerEvents = 'none';
                }}
            }}
            
            // ── Wire up event listeners ──
            function wireStepGuard() {{
                const listeners = [
                    ['custom_fleet_size', 'input'],
                    ['stream_sf_database', 'change'],
                    ['stream_sf_schema', 'change'],
                    ['stream_table', 'change'],
                    ['stream_create_new', 'change'],
                    ['stream_new_table', 'input'],
                    ['hp_pipe_name', 'input'],
                    ['reading_interval', 'change'],
                    ['segment_filter', 'change'],
                    ['data_format', 'change'],
                ];
                listeners.forEach(([id, evt]) => {{
                    const el = document.getElementById(id);
                    if (el) el.addEventListener(evt, window.updateStepAvailability);
                }});
                // Also listen for data flow card clicks
                document.querySelectorAll('.mechanism-card').forEach(card => {{
                    card.addEventListener('click', () => {{
                        setTimeout(window.updateStepAvailability, 50);
                    }});
                }});
            }}
            
            // ── Init on DOM ready ──
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', () => {{
                    wrapSections();
                    wireStepGuard();
                    window.updateStepAvailability();
                    if (typeof setStreamSource === 'function') setStreamSource('test');
                }});
            }} else {{
                wrapSections();
                wireStepGuard();
                window.updateStepAvailability();
                if (typeof setStreamSource === 'function') setStreamSource('test');
            }}
        }})();
        </script>
        '''
        
        streaming_info = f'''
        <div class="info-box green">
            <div class="title" style="color: #10b981;">{get_material_icon('stream', '20px', '#10b981')} Real-Time AMI Streaming Simulation</div>
            <div class="desc">
                Simulates how utility AMI data arrives in production: meters report on 15-minute intervals, 
                but readings arrive continuously as each meter's interval elapses. Choose your streaming mechanism 
                and target destination to configure the ingestion pipeline.
            </div>
        </div>
        '''
    else:
        meters = tmpl['meters']
        days = tmpl['days']
        interval = tmpl['interval_minutes']
        start_date = (date.today() - timedelta(days=days)).isoformat()
        
        config_section = f'''
        <div class="section-header">
            <span class="section-num">3</span>
            Configure Volume
        </div>
        <form action="/api/generate" method="post">
            <input type="hidden" name="template" value="{template}">
            <input type="hidden" name="fleet" value="{fleet}">
            <input type="hidden" name="mode" value="batch">
            <input type="hidden" name="data_flow" value="{data_flow}">
            <input type="hidden" name="service_area" value="{service_area}">
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Number of Meters</label>
                    <input type="number" name="meters" value="{meters}" min="10" max="100000">
                </div>
                <div class="form-group">
                    <label class="form-label">Days of Data</label>
                    <input type="number" name="days" value="{days}" min="1" max="730">
                </div>
                <div class="form-group">
                    <label class="form-label">Reading Interval</label>
                    <select name="interval">
                        <option value="5" {'selected' if interval == 5 else ''}>5 min</option>
                        <option value="15" {'selected' if interval == 15 else ''}>15 min</option>
                        <option value="30" {'selected' if interval == 30 else ''}>30 min</option>
                        <option value="60" {'selected' if interval == 60 else ''}>60 min</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Start Date</label>
                    <input type="date" name="start_date" value="{start_date}">
                </div>
            </div>
            
            <div class="section-header">
                <span class="section-num">4</span>
                Service Area & Climate
            </div>
            <div class="form-row" style="grid-template-columns: 1fr 1fr;">
                <div class="form-group">
                    <label class="form-label">Service Area / Climate Profile</label>
                    <select name="service_area">
                        {service_area_options}
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Meter ID Prefix</label>
                    <input type="text" name="meter_prefix" value="MTR" maxlength="10">
                </div>
            </div>
            <div class="info-box blue" style="margin-bottom: 16px;">
                <div class="title" style="color: #38bdf8;">{get_material_icon('location_on', '20px', '#38bdf8')} {area_cfg['name']}</div>
                <div class="desc">{area_cfg['description']}</div>
            </div>
            
            <div class="section-header">
                <span class="section-num">5</span>
                Target Snowflake Table
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Database</label>
                    <select name="sf_database" id="sf_database" onchange="loadSchemas(this.value)">
                        <option value="">Loading...</option>
                    </select>
                    <input type="text" id="new_database" name="new_database" placeholder="Or enter new database name" style="margin-top: 8px; display: none;">
                    <label style="font-size: 0.8rem; color: #64748b; margin-top: 4px; cursor: pointer;">
                        <input type="checkbox" id="create_new_db" onchange="toggleNewInput('database')"> Create new
                    </label>
                </div>
                <div class="form-group">
                    <label class="form-label">Schema</label>
                    <select name="sf_schema" id="sf_schema" onchange="loadTables(document.getElementById('sf_database').value, this.value)">
                        <option value="">Select database first</option>
                    </select>
                    <input type="text" id="new_schema" name="new_schema" placeholder="Or enter new schema name" style="margin-top: 8px; display: none;">
                    <label style="font-size: 0.8rem; color: #64748b; margin-top: 4px; cursor: pointer;">
                        <input type="checkbox" id="create_new_schema" onchange="toggleNewInput('schema')"> Create new
                    </label>
                </div>
                <div class="form-group">
                    <label class="form-label">Table</label>
                    <select name="sf_table" id="sf_table">
                        <option value="">Select schema first</option>
                    </select>
                    <input type="text" id="new_table" name="new_table" placeholder="Or enter new table name" value="AMI_INTERVAL_READINGS" style="margin-top: 8px; display: none;">
                    <label style="font-size: 0.8rem; color: #64748b; margin-top: 4px; cursor: pointer;">
                        <input type="checkbox" id="create_new_table" onchange="toggleNewInput('table')" checked> Create new
                    </label>
                </div>
            </div>
            <input type="hidden" name="table" id="full_table_path" value="{DB}.{SCHEMA_PRODUCTION}.AMI_INTERVAL_READINGS">
            
            <div class="form-row" style="grid-template-columns: 1fr 1fr;">
                <div class="form-group">
                    <label class="form-label">Include RAW_PAYLOAD</label>
                    <select name="include_variant" id="include_variant">
                        <option value="false">No</option>
                        <option value="true">Yes (VARIANT column)</option>
                    </select>
                </div>
                <div class="form-group">
                    <button type="button" class="btn-secondary" onclick="createTable()" style="margin-top: 22px;">
                        {get_material_icon('add_circle', '18px')} Create Table Now
                    </button>
                </div>
            </div>
            <div id="table_status" style="margin-bottom: 16px;"></div>
            
            <div class="section-header">
                <span class="section-num">6</span>
                Additional Data Types
            </div>
            <p style="color: #94a3b8; font-size: 0.9rem; margin-bottom: 12px;">
                Generate supporting datasets for Digital Twin, Asset Management, and Outage Restoration demos.
            </p>
            <div class="form-row" style="grid-template-columns: 1fr 1fr;">
                <div class="checkbox-row">
                    <input type="checkbox" name="gen_asset360" id="gen_asset360">
                    <label for="gen_asset360" class="checkbox-label">Asset 360 (GIS coordinates, transformers)</label>
                </div>
                <div class="checkbox-row">
                    <input type="checkbox" name="gen_work_orders" id="gen_work_orders">
                    <label for="gen_work_orders" class="checkbox-label">O&M Work Orders (SAP style)</label>
                </div>
            </div>
            <div class="form-row" style="grid-template-columns: 1fr 1fr;">
                <div class="checkbox-row">
                    <input type="checkbox" name="gen_power_quality" id="gen_power_quality">
                    <label for="gen_power_quality" class="checkbox-label">Power Quality Events</label>
                </div>
                <div class="checkbox-row">
                    <input type="checkbox" name="gen_erm" id="gen_erm">
                    <label for="gen_erm" class="checkbox-label">ERM Outage History</label>
                </div>
            </div>
            
            <button type="submit" class="btn-primary">{get_material_icon('rocket_launch', '20px')} Generate Batch Data</button>
        </form>
        
        <script>
        // Load databases on page load
        document.addEventListener('DOMContentLoaded', function() {{
            loadDatabases();
        }});
        
        async function loadDatabases() {{
            try {{
                const resp = await fetch('/api/databases');
                const data = await resp.json();
                const select = document.getElementById('sf_database');
                // Exit early if element doesn't exist on this page
                if (!select) return;
                
                select.innerHTML = '<option value="">-- Select Database --</option>';
                data.databases.forEach(db => {{
                    const opt = document.createElement('option');
                    opt.value = db;
                    opt.textContent = db;
                    if (db === '{DB}') opt.selected = true;
                    select.appendChild(opt);
                }});
                // Load schemas for default selection
                if (select.value) loadSchemas(select.value);
            }} catch (e) {{
                console.error('Failed to load databases:', e);
            }}
        }}
        
        async function loadSchemas(database) {{
            if (!database) return;
            try {{
                const resp = await fetch(`/api/schemas/${{database}}`);
                const data = await resp.json();
                const select = document.getElementById('sf_schema');
                // Exit early if element doesn't exist on this page
                if (!select) return;
                
                select.innerHTML = '<option value="">-- Select Schema --</option>';
                data.schemas.forEach(schema => {{
                    const opt = document.createElement('option');
                    opt.value = schema;
                    opt.textContent = schema;
                    if (schema === 'PRODUCTION' || schema === 'PUBLIC') opt.selected = true;
                    select.appendChild(opt);
                }});
                // Load tables for default selection
                if (select.value) loadTables(database, select.value);
                updateTablePath();
            }} catch (e) {{
                console.error('Failed to load schemas:', e);
            }}
        }}
        
        async function loadTables(database, schema) {{
            if (!database || !schema) return;
            try {{
                const resp = await fetch(`/api/tables/${{database}}/${{schema}}`);
                const data = await resp.json();
                const select = document.getElementById('sf_table');
                select.innerHTML = '<option value="">-- Select Table or Create New --</option>';
                data.tables.forEach(table => {{
                    const opt = document.createElement('option');
                    opt.value = table;
                    opt.textContent = table;
                    if (table === 'AMI_INTERVAL_READINGS') opt.selected = true;
                    select.appendChild(opt);
                }});
                updateTablePath();
            }} catch (e) {{
                console.error('Failed to load tables:', e);
            }}
        }}
        
        function toggleNewInput(type) {{
            const checkbox = document.getElementById('create_new_' + type);
            const newInput = document.getElementById('new_' + type);
            const select = document.getElementById('sf_' + type);
            
            if (checkbox.checked) {{
                newInput.style.display = 'block';
                select.disabled = true;
            }} else {{
                newInput.style.display = 'none';
                select.disabled = false;
            }}
            updateTablePath();
        }}
        
        function updateTablePath() {{
            let db = document.getElementById('create_new_db').checked 
                ? document.getElementById('new_database').value 
                : document.getElementById('sf_database').value;
            let schema = document.getElementById('create_new_schema').checked 
                ? document.getElementById('new_schema').value 
                : document.getElementById('sf_schema').value;
            let table = document.getElementById('create_new_table').checked 
                ? document.getElementById('new_table').value 
                : document.getElementById('sf_table').value;
            
            db = db || '{DB}';
            schema = schema || 'PRODUCTION';
            table = table || 'AMI_INTERVAL_READINGS';
            
            document.getElementById('full_table_path').value = `${{db}}.${{schema}}.${{table}}`;
        }}
        
        // Update path when inputs change
        ['new_database', 'new_schema', 'new_table', 'sf_database', 'sf_schema', 'sf_table'].forEach(id => {{
            const el = document.getElementById(id);
            if (el) el.addEventListener('change', updateTablePath);
            if (el) el.addEventListener('input', updateTablePath);
        }});
        
        async function createTable() {{
            let db = document.getElementById('create_new_db').checked 
                ? document.getElementById('new_database').value 
                : document.getElementById('sf_database').value;
            let schema = document.getElementById('create_new_schema').checked 
                ? document.getElementById('new_schema').value 
                : document.getElementById('sf_schema').value;
            let table = document.getElementById('create_new_table').checked 
                ? document.getElementById('new_table').value 
                : document.getElementById('sf_table').value;
            let includeVariant = document.getElementById('include_variant').value === 'true';
            
            if (!db || !schema || !table) {{
                document.getElementById('table_status').innerHTML = 
                    '<div style="color: #ef4444; padding: 10px; background: rgba(239,68,68,0.1); border-radius: 6px;">Please specify database, schema, and table name.</div>';
                return;
            }}
            
            document.getElementById('table_status').innerHTML = 
                '<div style="color: #38bdf8; padding: 10px;">Creating table...</div>';
            
            try {{
                const formData = new FormData();
                formData.append('database', db);
                formData.append('schema', schema);
                formData.append('table', table);
                formData.append('include_variant', includeVariant);
                
                const resp = await fetch('/api/create-table', {{
                    method: 'POST',
                    body: formData
                }});
                const data = await resp.json();
                
                if (data.status === 'success') {{
                    document.getElementById('table_status').innerHTML = 
                        `<div style="color: #22c55e; padding: 10px; background: rgba(34,197,94,0.1); border-radius: 6px;">✓ Table created: ${{data.table}}</div>`;
                    // Refresh the tables dropdown
                    loadTables(db, schema);
                }} else {{
                    document.getElementById('table_status').innerHTML = 
                        `<div style="color: #ef4444; padding: 10px; background: rgba(239,68,68,0.1); border-radius: 6px;">Error: ${{data.message}}</div>`;
                }}
            }} catch (e) {{
                document.getElementById('table_status').innerHTML = 
                    `<div style="color: #ef4444; padding: 10px; background: rgba(239,68,68,0.1); border-radius: 6px;">Error: ${{e.message}}</div>`;
            }}
        }}
        
        // Initialize new table checkbox as checked
        document.addEventListener('DOMContentLoaded', function() {{
            toggleNewInput('table');
        }});
        </script>
        '''
        streaming_info = ""
    
    # Use unified data flow config for display
    flow_display = DATA_FLOWS.get(data_flow, DATA_FLOWS['streaming_insert'])
    
    preview_content = f'''
    <div class="panel-title">{get_material_icon('play_circle', '20px')} Launch Streaming</div>
    
    <!-- Compact status summary -->
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 12px; padding: 10px; background: rgba(15,23,42,0.5); border-radius: 8px;">
        <div>
            <div style="color: #64748b; font-size: 0.7rem;">Data Flow</div>
            <div style="color: {flow_display['color']}; font-size: 0.85rem; font-weight: 600;">{flow_display['name']}</div>
        </div>
        <div>
            <div style="color: #64748b; font-size: 0.7rem;">Fleet</div>
            <div style="color: #e2e8f0; font-size: 0.85rem; font-weight: 600;" id="preview_fleet_size">{fleet_cfg['meters']:,}</div>
        </div>
    </div>
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 12px; padding: 10px; background: rgba(15,23,42,0.5); border-radius: 8px;">
        <div>
            <div style="color: #64748b; font-size: 0.7rem;">Target</div>
            <div style="color: #38bdf8; font-size: 0.82rem; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" id="sidebar_target_display">Not selected</div>
        </div>
        <div>
            <div style="color: #64748b; font-size: 0.7rem;">Source</div>
            <div style="color: #a78bfa; font-size: 0.82rem; font-weight: 600;" id="sidebar_source_display">Test Data</div>
        </div>
    </div>
    <p style="color: #64748b; font-size: 0.78rem; margin-bottom: 12px; line-height: 1.4;">
        Configure all steps in the main panel, then click Start Streaming to begin ingestion.
    </p>
    '''
    
    if mode == "streaming":
        preview_content += f'''
        <form action="/api/stream" method="post" id="streaming_form" onsubmit="syncStreamFields()">
            <input type="hidden" name="template" value="{template}">
            <input type="hidden" name="fleet" value="{fleet}">
            <input type="hidden" name="mode" value="streaming">
            <input type="hidden" name="data_flow" value="{data_flow}">
            <input type="hidden" name="meters" id="form_meters" value="{fleet_cfg['meters']}">
            <input type="hidden" name="interval" id="form_interval" value="15">
            <input type="hidden" name="service_area" value="{service_area}">
            <input type="hidden" name="rows_per_sec" value="{rows_per_sec}">
            <input type="hidden" name="batch_size_mb" value="{batch_size_mb}">
            <input type="hidden" name="max_client_lag" value="{max_client_lag}"">
            <input type="hidden" name="batch_size_rows" id="form_batch_size_rows" value="0">
            <!-- New production matching fields -->
            <input type="hidden" name="production_source" id="form_production_source" value="METER_INFRASTRUCTURE">
            <input type="hidden" name="emission_pattern" id="form_emission_pattern" value="STAGGERED_REALISTIC">
            <input type="hidden" name="segment_filter" id="form_segment_filter" value="">
            <input type="hidden" name="data_format" id="form_data_format" value="{'raw_ami' if dest == 'stage' else 'standard'}">
            <input type="hidden" name="pipe_name" id="form_pipe_name" value="AMI_STREAMING_PIPE">
            <!-- Mirror fields for main-column inputs (synced via JS) -->
            <input type="hidden" name="table" id="form_table" value="">
            <input type="hidden" name="new_table" id="form_new_table" value="">
            <input type="hidden" name="stream_source" id="form_stream_source" value="test">
            <input type="hidden" name="eh_conn_str" id="form_eh_conn_str" value="">
            <input type="hidden" name="eh_name" id="form_eh_name" value="">
            <input type="hidden" name="eh_consumer_group" id="form_eh_consumer_group" value="$Default">
            <input type="hidden" name="ps_project" id="form_ps_project" value="">
            <input type="hidden" name="ps_subscription" id="form_ps_subscription" value="">
            '''
        
        # HP Streaming PIPE check script (used by main-column pipe section)
        if mechanism == 'snowpipe_hp' and dest != 'stage':
            preview_content += f'''
            <script>
            async function checkOrCreateHpPipe() {{
                const createNewPipe = document.getElementById('hp_pipe_create_new')?.checked;
                const pipeName = createNewPipe
                    ? (document.getElementById('hp_pipe_name_new')?.value || '').trim()
                    : (document.getElementById('hp_pipe_name')?.value || '').trim();
                const statusEl = document.getElementById('hp_pipe_status');
                const msgEl = document.getElementById('hp_pipe_msg');
                const btn = document.getElementById('hp_pipe_btn');
                
                if (!pipeName) {{
                    msgEl.textContent = 'Enter a pipe name';
                    msgEl.style.color = '#ef4444';
                    return;
                }}
                
                // Get target database/schema/table from the form
                const dbEl = document.getElementById('stream_sf_database');
                const schemaEl = document.getElementById('stream_sf_schema');
                const tableEl = document.getElementById('stream_table');
                const newTableEl = document.getElementById('stream_new_table');
                const createNewChecked = document.getElementById('stream_create_new')?.checked;
                const database = dbEl ? dbEl.value : '{DB}';
                const schema = schemaEl ? schemaEl.value : '{SCHEMA_PRODUCTION}';
                const tableName = (createNewChecked && newTableEl && newTableEl.value.trim())
                    ? newTableEl.value.trim()
                    : (tableEl ? tableEl.value : 'AMI_STREAMING_READINGS');
                
                btn.disabled = true;
                msgEl.textContent = 'Checking...';
                msgEl.style.color = '#94a3b8';
                statusEl.innerHTML = '';
                
                try {{
                    // Try to deploy (creates table + pipe if not exists)
                    const formData = new FormData();
                    formData.append('architecture', 'hp');
                    formData.append('database', database);
                    formData.append('schema', schema);
                    formData.append('table_name', tableName);
                    formData.append('pipe_name', pipeName);
                    
                    const resp = await fetch('/api/streaming/deploy-ddl', {{method: 'POST', body: formData}});
                    const data = await resp.json();
                    
                    if (data.status === 'success') {{
                        statusEl.innerHTML = '<span style="color: #22c55e;">&#x2713; Ready</span>';
                        msgEl.textContent = 'PIPE exists and is configured for streaming';
                        msgEl.style.color = '#22c55e';
                        // Update hidden form field
                        document.getElementById('form_pipe_name').value = pipeName;
                    }} else if (data.status === 'partial') {{
                        // Check if all errors are benign "already exists" type
                        const errs = data.errors || [];
                        const allBenign = errs.length > 0 && errs.every(e => {{
                            const msg = (e.error || '').toLowerCase();
                            return msg.includes('already exists') || msg.includes('already granted')
                                || msg.includes('object already exists');
                        }});
                        if (allBenign) {{
                            statusEl.innerHTML = '<span style="color: #22c55e;">&#x2713; Ready</span>';
                            msgEl.textContent = 'PIPE and table already exist — ready for streaming';
                            msgEl.style.color = '#22c55e';
                        }} else {{
                            statusEl.innerHTML = '<span style="color: #f59e0b;">&#x26A0; Partial</span>';
                            // Show first real error detail — strip Snowflake query ID noise
                            const firstErr = errs.find(e => {{
                                const msg = (e.error || '').toLowerCase();
                                return !msg.includes('already exists') && !msg.includes('already granted');
                            }});
                            let detail = firstErr ? firstErr.error : ('DDL partially applied with ' + errs.length + ' issue(s)');
                            // Strip Snowflake error prefix like "(1304): 01c2bf96-...:002003 (02000): "
                            detail = detail.replace(/^\(\d+\):\s*[\w-]+:\d+\s*\(\d+\):\s*/i, '');
                            // Also strip embedded query IDs
                            detail = detail.replace(/[\da-f]{{8}}-[\da-f]{{4}}-[\da-f]{{4}}-[\da-f]{{4}}-[\da-f]{{12}}:\d+/gi, '').trim();
                            if (detail.length > 150) detail = detail.substring(0, 150) + '\u2026';
                            msgEl.textContent = detail;
                            msgEl.style.color = '#f59e0b';
                        }}
                        document.getElementById('form_pipe_name').value = pipeName;
                    }} else {{
                        statusEl.innerHTML = '<span style="color: #ef4444;">&#x2717; Error</span>';
                        let errMsg = data.message || 'Failed to create PIPE';
                        errMsg = errMsg.replace(/^\(\d+\):\s*[\w-]+:\d+\s*\(\d+\):\s*/i, '');
                        errMsg = errMsg.replace(/[\da-f]{{8}}-[\da-f]{{4}}-[\da-f]{{4}}-[\da-f]{{4}}-[\da-f]{{12}}:\d+/gi, '').trim();
                        if (errMsg.length > 150) errMsg = errMsg.substring(0, 150) + '\u2026';
                        msgEl.textContent = errMsg;
                        msgEl.style.color = '#ef4444';
                    }}
                }} catch (err) {{
                    statusEl.innerHTML = '<span style="color: #ef4444;">&#x2717; Error</span>';
                    msgEl.textContent = err.message;
                    msgEl.style.color = '#ef4444';
                }} finally {{
                    btn.disabled = false;
                }}
            }}
            
            // Auto-generate pipe name from table name
            const streamTableEl = document.getElementById('stream_table');
            if (streamTableEl) {{
                streamTableEl.addEventListener('change', function() {{
                    const tbl = this.value;
                    if (tbl) {{
                        const pipeName = tbl.split('.').pop() + '_PIPE';
                        document.getElementById('hp_pipe_name').value = pipeName;
                        document.getElementById('form_pipe_name').value = pipeName;
                        // Reset status
                        document.getElementById('hp_pipe_status').innerHTML = '';
                        document.getElementById('hp_pipe_msg').textContent = '';
                    }}
                }});
            }}
            </script>
            '''
        
        # Add Stage target section if stage destination is selected
        if dest == 'stage':
            stage_color = '#0ea5e9'
            stage_icon = 'folder_open'
            stage_title = 'Stage Destination'
            json_selected = 'selected'
            parquet_selected = ''
            
            # Add visual pipeline flow diagram for Stage Landing
            preview_content += f'''
            <!-- ========== STAGE LANDING PIPELINE FLOW DIAGRAM ========== -->
            <div style="margin-bottom: 16px; padding: 16px; background: linear-gradient(135deg, rgba(14,165,233,0.1) 0%, rgba(99,102,241,0.1) 100%); border: 1px solid rgba(14,165,233,0.3); border-radius: 12px;">
                <div style="text-align: center; margin-bottom: 12px;">
                    <span style="color: #0ea5e9; font-weight: 600; font-size: 0.9rem;">
                        {get_material_icon('account_tree', '18px', '#0ea5e9')} Stage Landing Data Pipeline
                    </span>
                </div>
                
                <!-- Pipeline Flow Visualization -->
                <div style="display: flex; align-items: center; justify-content: center; gap: 6px; flex-wrap: wrap; padding: 12px 0;">
                    <!-- Step 1: Smart Meters -->
                    <div style="text-align: center; padding: 6px 10px; background: rgba(34,197,94,0.15); border: 1px solid rgba(34,197,94,0.3); border-radius: 8px; min-width: 70px;">
                        <div style="font-size: 1.1rem;">📱</div>
                        <div style="color: #22c55e; font-size: 0.65rem; font-weight: 600;">Meters</div>
                    </div>
                    
                    <div style="color: #64748b; font-size: 0.9rem;">→</div>
                    
                    <!-- Step 2: External Stage -->
                    <div style="text-align: center; padding: 6px 10px; background: rgba(14,165,233,0.15); border: 1px solid rgba(14,165,233,0.3); border-radius: 8px; min-width: 70px;">
                        <div style="font-size: 1.1rem;">☁️</div>
                        <div style="color: #0ea5e9; font-size: 0.65rem; font-weight: 600;">Stage</div>
                    </div>
                    
                    <div style="color: #64748b; font-size: 0.9rem;">→</div>
                    
                    <!-- Step 3: Snowpipe -->
                    <div style="text-align: center; padding: 6px 10px; background: rgba(168,85,247,0.15); border: 1px solid rgba(168,85,247,0.3); border-radius: 8px; min-width: 70px;">
                        <div style="font-size: 1.1rem;">⚡</div>
                        <div style="color: #a855f7; font-size: 0.65rem; font-weight: 600;">Snowpipe</div>
                    </div>
                    
                    <div style="color: #64748b; font-size: 0.9rem;">→</div>
                    
                    <!-- Step 4: Target Table -->
                    <div style="text-align: center; padding: 6px 10px; background: rgba(56,189,248,0.15); border: 1px solid rgba(56,189,248,0.3); border-radius: 8px; min-width: 70px;">
                        <div style="font-size: 1.1rem;">❄️</div>
                        <div style="color: #38bdf8; font-size: 0.65rem; font-weight: 600;">Bronze</div>
                    </div>
                </div>
                
                <div style="text-align: center; color: #94a3b8; font-size: 0.7rem; margin-top: 6px; padding-top: 6px; border-top: 1px dashed rgba(148,163,184,0.3);">
                    💡 <em>Configure each step below</em>
                </div>
            </div>
            '''
            
            # NEW: Use horizontal 4-column accordion layout for Stage Landing Pipeline
            # Advanced Mode: Production-grade UX with conditional visibility and smart defaults
            preview_content += f'''
            <!-- ========== STAGE LANDING PIPELINE - ENTERPRISE 4-STEP LAYOUT ========== -->
            <div class="pipeline-accordion">
                <!-- STEP 1: Stage (Primary - always visible first) -->
                <div class="accordion-step active" id="step1-panel">
                    <div class="accordion-step-header" onclick="highlightStep(1)" style="background: rgba(14,165,233,0.1);">
                        <span class="accordion-step-number" style="background: #0ea5e9; color: white;">1</span>
                        <span class="accordion-step-title">{get_material_icon('folder_open', '16px', '#0ea5e9')} Stage</span>
                        <span id="stage_type_badge" style="margin-left: auto; font-size: 0.65rem; padding: 2px 6px; border-radius: 4px; background: rgba(34,197,94,0.2); color: #22c55e; display: none;">Internal</span>
                    </div>
                    <div class="accordion-step-body">
                        <p class="accordion-step-desc">Landing zone for raw data files.</p>
                        <select name="stage_name" id="stage_name" onchange="onStageChange();" style="width: 100%; font-size: 0.8rem;">
                            <option value="">Loading stages...</option>
                        </select>
                        <div id="new_stage_container" style="display: none; margin-top: 8px; padding: 10px; background: rgba(14,165,233,0.05); border-radius: 6px;">
                            <input type="text" id="new_stage_name" name="new_stage_name" placeholder="Stage name (e.g., AMI_RAW_STAGE)" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;">
                            <input type="text" id="new_stage_url" name="new_stage_url" placeholder="External URL (leave empty for internal stage)" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;" oninput="onStageUrlChange();">
                            <div id="stage_url_hint" style="font-size: 0.7rem; color: #64748b; margin-bottom: 6px;">
                                💡 Leave empty for Snowflake-managed internal stage
                            </div>
                            <button type="button" class="btn-step cyan" onclick="createStageNow()">
                                {get_material_icon('add', '14px')} Create Stage
                            </button>
                        </div>
                        <div id="stage_status" style="margin-top: 6px; font-size: 0.75rem;"></div>
                        <div style="margin-top: 8px;">
                            <label style="color: #94a3b8; font-size: 0.7rem; display: block; margin-bottom: 4px;">File Format</label>
                            <select name="stage_file_format" id="stage_file_format" onchange="updatePipePreview()" style="width: 100%; font-size: 0.8rem;">
                                <option value="json" {json_selected}>JSON</option>
                                <option value="parquet" {parquet_selected}>Parquet</option>
                                <option value="csv">CSV</option>
                            </select>
                        </div>
                    </div>
                </div>
                
                <!-- STEP 2: Storage Integration (Conditional - only for external stages) -->
                <div class="accordion-step" id="step2-panel" style="display: none;">
                    <div class="accordion-step-header" onclick="highlightStep(2)" style="background: rgba(34,197,94,0.1);">
                        <span class="accordion-step-number" style="background: #22c55e; color: white;">2</span>
                        <span class="accordion-step-title">{get_material_icon('cloud_sync', '16px', '#22c55e')} Integration</span>
                        <span style="margin-left: auto; font-size: 0.6rem; padding: 2px 4px; border-radius: 3px; background: rgba(239,68,68,0.2); color: #ef4444;">Required</span>
                    </div>
                    <div class="accordion-step-body">
                        <p class="accordion-step-desc">Cloud storage credentials for external stage access.</p>
                        <select name="storage_integration" id="storage_integration" onchange="toggleNewIntegrationInput()" style="width: 100%; font-size: 0.8rem;">
                            <option value="">Loading...</option>
                        </select>
                        <div id="new_integration_container" style="display: none; margin-top: 8px; padding: 10px; background: rgba(34,197,94,0.05); border-radius: 6px;">
                            <input type="text" id="new_integration_name" name="new_integration_name" placeholder="Integration name" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;">
                            <select id="new_integration_type" name="new_integration_type" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;">
                                <option value="S3">AWS S3</option>
                                <option value="AZURE">Azure Blob</option>
                                <option value="GCS">Google Cloud Storage</option>
                            </select>
                            <input type="text" id="storage_allowed_locations" name="storage_allowed_locations" placeholder="s3://bucket/path/" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;">
                            <button type="button" onclick="generateIntegrationSQL()" style="background: #22c55e; color: white; border: none; border-radius: 4px; padding: 6px 10px; font-size: 0.7rem; cursor: pointer; width: 100%;">
                                {get_material_icon('code', '14px')} Generate SQL
                            </button>
                        </div>
                        <div id="integration_status" style="margin-top: 6px; font-size: 0.75rem;"></div>
                    </div>
                </div>
                
                <!-- STEP 3: Target Table (Always visible - key for medallion architecture) -->
                <div class="accordion-step" id="step3-panel">
                    <div class="accordion-step-header" onclick="highlightStep(3)" style="background: rgba(56,189,248,0.1);">
                        <span class="accordion-step-number" style="background: #38bdf8; color: white;">2</span>
                        <span class="accordion-step-title">{get_material_icon('table_chart', '16px', '#38bdf8')} Bronze Table</span>
                    </div>
                    <div class="accordion-step-body">
                        <p class="accordion-step-desc">Raw data landing table (VARIANT column).</p>
                        <select name="target_table" id="target_table" onchange="onTargetTableChange();" style="width: 100%; font-size: 0.8rem;">
                            <option value="">Loading tables...</option>
                        </select>
                        <div id="new_table_container" style="display: none; margin-top: 8px; padding: 10px; background: rgba(56,189,248,0.05); border-radius: 6px;">
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 6px;">
                                <select id="new_table_database" onchange="loadNewTableSchemas()" style="font-size: 0.8rem;">
                                    <option value="">Database...</option>
                                </select>
                                <select id="new_table_schema" style="font-size: 0.8rem;">
                                    <option value="">Schema...</option>
                                </select>
                            </div>
                            <input type="text" id="new_table_name" name="new_table_name" placeholder="Table name (e.g., AMI_BRONZE_RAW)" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;" oninput="updateTablePreview();">
                            <div id="table_preview" style="font-size: 0.7rem; color: #64748b; margin-bottom: 6px; font-family: monospace;">
                                → Will create: <span id="table_full_path" style="color: #38bdf8;">...</span>
                            </div>
                            
                            <!--  Opt-in Snowpipe creation with external stage -->
                            <div id="auto_pipe_option" style="margin: 8px 0; padding: 8px; background: rgba(168,85,247,0.1); border-radius: 6px; border: 1px solid rgba(168,85,247,0.3);">
                                <label style="display: flex; align-items: flex-start; gap: 8px; cursor: pointer; font-size: 0.75rem; color: #e2e8f0;">
                                    <input type="checkbox" id="auto_create_pipe" name="auto_create_pipe" style="width: auto; margin-top: 2px;">
                                    <div>
                                        <span style="color: #a855f7; font-weight: 500;">{get_material_icon('bolt', '14px', '#a855f7')} Auto-create Snowpipe</span>
                                        <div style="color: #94a3b8; font-size: 0.7rem; margin-top: 2px;">
                                            Automatically create a pipe to load data from external stage into this table
                                        </div>
                                    </div>
                                </label>
                                <div id="pipe_source_stage_container" style="display: none; margin-top: 8px; padding-left: 20px;">
                                    <!-- Source Stage Selection -->
                                    <div style="margin-bottom: 8px;">
                                        <label style="color: #94a3b8; font-size: 0.7rem; display: block; margin-bottom: 4px;">
                                            {get_material_icon('folder', '12px', '#94a3b8')} Source Stage
                                        </label>
                                        <select id="pipe_source_stage" style="width: 100%; font-size: 0.75rem; padding: 4px;">
                                            <option value="">Select source stage...</option>
                                        </select>
                                    </div>
                                    
                                    <!-- File Pattern with clear explanation -->
                                    <div style="margin-bottom: 4px;">
                                        <label style="color: #94a3b8; font-size: 0.7rem; display: block; margin-bottom: 4px;">
                                            {get_material_icon('filter_alt', '12px', '#94a3b8')} File Pattern (which files to ingest)
                                        </label>
                                        <select id="pipe_file_pattern_preset" onchange="updateFilePatternFromPreset()" style="width: 100%; font-size: 0.75rem; padding: 4px; margin-bottom: 4px;">
                                            <option value=".*ami_stream.*\\.json">AMI Stream Files (ami_stream_*.json)</option>
                                            <option value=".*ami_data.*\\.json">AMI Data Files (ami_data_*.json)</option>
                                            <option value=".*\\.json">All JSON Files (*.json)</option>
                                            <option value=".*\\.parquet">All Parquet Files (*.parquet)</option>
                                            <option value="custom">Custom Pattern...</option>
                                        </select>
                                        <input type="text" id="pipe_file_pattern" value=".*ami_stream.*\\.json" 
                                            style="width: 100%; font-size: 0.75rem; padding: 4px; display: none;" 
                                            placeholder="Regex pattern, e.g., .*orders.*\\.json">
                                        <div style="color: #64748b; font-size: 0.65rem; margin-top: 4px;">
                                            {get_material_icon('info', '10px', '#64748b')} Only files matching this pattern will be loaded into the table
                                        </div>
                                    </div>
                                </div>
                            </div>
                            
                            <button type="button" class="btn-step blue" onclick="createBronzeTable()">
                                {get_material_icon('add', '14px')} Create Bronze Table
                            </button>
                        </div>
                        <div id="table_status" style="margin-top: 6px; font-size: 0.75rem;"></div>
                        
                        <!--  Pipe status display after table creation -->
                        <div id="pipe_detection_status" style="display: none; margin-top: 8px; padding: 10px; border-radius: 6px; font-size: 0.75rem;"></div>
                        <div id="table_schema_preview" style="display: none; margin-top: 8px; padding: 8px; background: rgba(15,23,42,0.5); border-radius: 6px; font-family: monospace; font-size: 0.65rem; color: #94a3b8;">
                            <div style="color: #64748b; margin-bottom: 4px;">Bronze table schema:</div>
                            <code style="color: #38bdf8;">RAW_DATA VARIANT</code> - stores raw JSON/Parquet<br/>
                            <code style="color: #38bdf8;">LOADED_AT TIMESTAMP</code> - ingestion timestamp<br/>
                            <code style="color: #38bdf8;">FILE_NAME VARCHAR</code> - source file tracking
                        </div>
                    </div>
                </div>
                
                <!-- STEP 4: Snowpipe (Final step - ties everything together) -->
                <div class="accordion-step" id="step4-panel">
                    <div class="accordion-step-header" onclick="highlightStep(4)" style="background: rgba(168,85,247,0.1);">
                        <span class="accordion-step-number" style="background: #a855f7; color: white;">3</span>
                        <span class="accordion-step-title">{get_material_icon('bolt', '16px', '#a855f7')} Snowpipe</span>
                    </div>
                    <div class="accordion-step-body">
                        <p class="accordion-step-desc">Auto-ingest from stage to bronze table.</p>
                        <select name="pipe_name" id="pipe_name" onchange="toggleNewPipeInput()" style="width: 100%; font-size: 0.8rem;">
                            <option value="">Loading pipes...</option>
                        </select>
                        <div id="new_pipe_container" style="display: none; margin-top: 8px; padding: 10px; background: rgba(168,85,247,0.05); border-radius: 6px;">
                            <input type="text" id="new_pipe_name" name="new_pipe_name" placeholder="Pipe name (e.g., AMI_INGEST_PIPE)" style="width: 100%; margin-bottom: 6px; font-size: 0.8rem;" oninput="updatePipePreview();">
                            <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 0.75rem; color: #94a3b8;">
                                <input type="checkbox" id="pipe_auto_ingest" name="pipe_auto_ingest" checked style="width: auto;" onchange="updatePipePreview();">
                                AUTO_INGEST (recommended for cloud event notifications)
                            </label>
                        </div>
                        <div id="pipe_status" style="margin-top: 6px; font-size: 0.75rem;"></div>
                        <div id="pipe_preview" style="display: none; margin-top: 8px; padding: 8px; background: rgba(15,23,42,0.5); border-radius: 6px; font-family: monospace; font-size: 0.65rem; color: #94a3b8; overflow-x: auto;">
                            <pre id="pipe_ddl_preview" style="margin: 0; white-space: pre-wrap;"></pre>
                            <button type="button" onclick="createPipeNow()" style="margin-top: 6px; background: #a855f7; color: white; border: none; border-radius: 4px; padding: 5px 10px; font-size: 0.7rem; cursor: pointer; width: 100%;">
                                {get_material_icon('bolt', '14px')} Create Snowpipe
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Pipeline Summary Card - Shows current selections -->
            <div id="pipeline_summary" style="margin-top: 12px; padding: 12px 14px; background: linear-gradient(135deg, rgba(14,165,233,0.08) 0%, rgba(168,85,247,0.08) 100%); border-radius: 8px; border: 1px solid rgba(148,163,184,0.2);">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                    {get_material_icon('check_circle', '16px', '#22c55e')}
                    <span style="color: #f8fafc; font-size: 0.8rem; font-weight: 500;">Pipeline Configuration</span>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 8px; font-size: 0.7rem;">
                    <div>
                        <span style="color: #64748b;">Stage:</span>
                        <span id="summary_stage" style="color: #0ea5e9; display: block; font-family: monospace; overflow: hidden; text-overflow: ellipsis;">Not selected</span>
                    </div>
                    <div>
                        <span style="color: #64748b;">Target:</span>
                        <span id="summary_target" style="color: #38bdf8; display: block; font-family: monospace; overflow: hidden; text-overflow: ellipsis;">Not selected</span>
                    </div>
                    <div>
                        <span style="color: #64748b;">Pipe:</span>
                        <span id="summary_pipe" style="color: #a855f7; display: block; font-family: monospace; overflow: hidden; text-overflow: ellipsis;">Not selected</span>
                    </div>
                    <div>
                        <span style="color: #64748b;">Format:</span>
                        <span id="summary_format" style="color: #f59e0b; display: block;">JSON</span>
                    </div>
                </div>
            </div>
            
            <!-- Medallion Architecture Note -->
            <div style="margin-top: 8px; padding: 8px 12px; background: rgba(34,197,94,0.05); border-radius: 6px; border-left: 3px solid #22c55e;">
                <span style="color: #22c55e; font-size: 0.75rem; font-weight: 500;">
                    {get_material_icon('lightbulb', '14px', '#22c55e')} Medallion Architecture
                </span>
                <span style="color: #64748b; font-size: 0.7rem; margin-left: 6px;">
                    Bronze (raw VARIANT) → Silver (cleansed) → Gold (analytics-ready)
                </span>
            </div>
            
            <script>
            // ========== ENTERPRISE PIPELINE CONFIGURATION ==========
            // Smart 4-step flow with conditional visibility and real-time summary
            
            let isExternalStage = false;  // Track stage type for conditional UI
            let selectedStageInfo = null; // Store selected stage metadata
            
            // ========== STEP NUMBER MANAGEMENT ==========
            function updateStepNumbers() {{
                // Renumber visible steps dynamically
                const visibleSteps = document.querySelectorAll('.accordion-step:not([style*="display: none"])');
                visibleSteps.forEach((step, idx) => {{
                    const numEl = step.querySelector('.accordion-step-number');
                    if (numEl) numEl.textContent = idx + 1;
                }});
            }}
            
            function highlightStep(stepNum) {{
                const panels = ['step1-panel', 'step2-panel', 'step3-panel', 'step4-panel'];
                panels.forEach((id, i) => {{
                    const el = document.getElementById(id);
                    if (el) el.classList.toggle('active', i === stepNum - 1);
                }});
            }}
            
            // ========== PIPELINE SUMMARY ==========
            function updatePipelineSummary() {{
                const stageSelect = document.getElementById('stage_name');
                const targetSelect = document.getElementById('target_table');
                const pipeSelect = document.getElementById('pipe_name');
                const formatSelect = document.getElementById('stage_file_format');
                
                // Update stage
                const summaryStage = document.getElementById('summary_stage');
                if (summaryStage && stageSelect) {{
                    const val = stageSelect.value;
                    summaryStage.textContent = (val && val !== '__create_new__') ? val.split('.').pop() : 'Not selected';
                }}
                
                // Update target
                const summaryTarget = document.getElementById('summary_target');
                if (summaryTarget && targetSelect) {{
                    const val = targetSelect.value;
                    summaryTarget.textContent = (val && val !== '__create_new__') ? val.split('.').pop() : 'Not selected';
                }}
                
                // Update pipe  
                const summaryPipe = document.getElementById('summary_pipe');
                if (summaryPipe && pipeSelect) {{
                    const val = pipeSelect.value;
                    if (val === '__create_new__') {{
                        const newName = document.getElementById('new_pipe_name');
                        summaryPipe.textContent = newName && newName.value ? newName.value : 'New pipe...';
                    }} else {{
                        summaryPipe.textContent = val ? val.split('.').pop() : 'Not selected';
                    }}
                }}
                
                // Update format
                const summaryFormat = document.getElementById('summary_format');
                if (summaryFormat && formatSelect) {{
                    summaryFormat.textContent = formatSelect.value.toUpperCase();
                }}
            }}
            
            // ========== STAGE CHANGE HANDLER (Main orchestrator) ==========
            function onStageChange() {{
                const select = document.getElementById('stage_name');
                const container = document.getElementById('new_stage_container');
                const integrationPanel = document.getElementById('step2-panel');
                const typeBadge = document.getElementById('stage_type_badge');
                
                if (!select) return;
                
                const value = select.value;
                const selectedOption = select.options[select.selectedIndex];
                
                // Toggle create new container
                if (container) {{
                    container.style.display = value === '__create_new__' ? 'block' : 'none';
                    if (value === '__create_new__') {{
                        const nameInput = document.getElementById('new_stage_name');
                        if (nameInput) nameInput.focus();
                    }}
                }}
                
                // Determine if selected stage is internal or external
                if (selectedOption && selectedOption.parentElement) {{
                    const groupLabel = selectedOption.parentElement.label || '';
                    isExternalStage = groupLabel.includes('External') || groupLabel.includes('S3') || groupLabel.includes('Azure') || groupLabel.includes('GCS');
                }} else {{
                    isExternalStage = false;
                }}
                
                // For "Create New" - check URL field to determine type
                if (value === '__create_new__') {{
                    const urlField = document.getElementById('new_stage_url');
                    isExternalStage = urlField && urlField.value && urlField.value.trim().length > 0;
                }}
                
                // Update type badge
                if (typeBadge) {{
                    if (value && value !== '__create_new__') {{
                        typeBadge.style.display = 'inline';
                        if (isExternalStage) {{
                            typeBadge.textContent = 'External';
                            typeBadge.style.background = 'rgba(239,68,68,0.2)';
                            typeBadge.style.color = '#ef4444';
                        }} else {{
                            typeBadge.textContent = 'Internal';
                            typeBadge.style.background = 'rgba(34,197,94,0.2)';
                            typeBadge.style.color = '#22c55e';
                        }}
                    }} else {{
                        typeBadge.style.display = 'none';
                    }}
                }}
                
                // Show/hide integration panel based on stage type
                if (integrationPanel) {{
                    integrationPanel.style.display = isExternalStage ? '' : 'none';
                }}
                
                // Update step numbers after visibility change
                updateStepNumbers();
                updatePipeStage();
                updatePipePreview();
                updatePipelineSummary();
            }}
            
            // Handle URL field changes when creating new stage
            function onStageUrlChange() {{
                const urlField = document.getElementById('new_stage_url');
                const hintEl = document.getElementById('stage_url_hint');
                const integrationPanel = document.getElementById('step2-panel');
                
                if (urlField && hintEl) {{
                    const hasUrl = urlField.value && urlField.value.trim().length > 0;
                    isExternalStage = hasUrl;
                    
                    if (hasUrl) {{
                        hintEl.innerHTML = '☁️ External stage - requires Storage Integration (Step 2)';
                        hintEl.style.color = '#f59e0b';
                    }} else {{
                        hintEl.innerHTML = '💡 Leave empty for Snowflake-managed internal stage';
                        hintEl.style.color = '#64748b';
                    }}
                }}
                
                // Show/hide integration panel
                if (integrationPanel) {{
                    integrationPanel.style.display = isExternalStage ? '' : 'none';
                }}
                
                updateStepNumbers();
            }}
            
            // ========== STORAGE INTEGRATION FUNCTIONS ==========
            function toggleNewIntegrationInput() {{
                const select = document.getElementById('storage_integration');
                const container = document.getElementById('new_integration_container');
                if (!select || !container) return;
                
                if (select.value === '__create_new__') {{
                    container.style.display = 'block';
                    const nameInput = document.getElementById('new_integration_name');
                    if (nameInput) nameInput.focus();
                }} else {{
                    container.style.display = 'none';
                }}
            }}
            
            async function generateIntegrationSQL() {{
                const name = document.getElementById('new_integration_name').value;
                const type = document.getElementById('new_integration_type').value;
                const locations = document.getElementById('storage_allowed_locations').value;
                
                if (!name || !locations) {{
                    document.getElementById('integration_status').innerHTML = '<span style="color: #ef4444;">Please fill in all fields.</span>';
                    return;
                }}
                
                document.getElementById('integration_status').innerHTML = '<span style="color: #f59e0b;">Generating SQL...</span>';
                
                try {{
                    const formData = new FormData();
                    formData.append('integration_name', name);
                    formData.append('integration_type', type);
                    formData.append('storage_allowed_locations', locations);
                    
                    const resp = await fetch('/api/storage-integrations/create', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (data.sql) {{
                        // Store SQL for copy button
                        window._integrationSQL = data.sql;
                        document.getElementById('integration_status').innerHTML = `
                            <div style="background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 10px; margin-top: 8px;">
                                <div style="color: #22c55e; font-size: 0.75rem; margin-bottom: 6px;">Run this SQL with ACCOUNTADMIN role:</div>
                                <pre style="color: #94a3b8; font-size: 0.7rem; white-space: pre-wrap; margin: 0;">${{data.sql}}</pre>
                                <button onclick="navigator.clipboard.writeText(window._integrationSQL); this.textContent='✓ Copied!'; this.style.background='#22c55e';" style="margin-top: 8px; background: #334155; color: white; border: none; border-radius: 4px; padding: 4px 8px; font-size: 0.7rem; cursor: pointer;">
                                    Copy SQL
                                </button>
                            </div>`;
                    }} else {{
                        document.getElementById('integration_status').innerHTML = `<span style="color: #ef4444;">Error: ${{data.detail || 'Failed to generate SQL'}}</span>`;
                    }}
                }} catch (err) {{
                    document.getElementById('integration_status').innerHTML = `<span style="color: #ef4444;">Error: ${{err.message}}</span>`;
                }}
            }}
            
            // ========== STAGE FUNCTIONS ==========
            // Note: toggleNewStageInput is now handled by onStageChange()
            function toggleNewStageInput() {{
                // Legacy compatibility - redirect to main handler
                onStageChange();
            }}
            
            async function createStageNow() {{
                const stageName = document.getElementById('new_stage_name').value;
                const stageUrl = document.getElementById('new_stage_url').value;
                const integrationSelect = document.getElementById('storage_integration');
                const integration = integrationSelect ? integrationSelect.value : '';
                const fileFormat = document.getElementById('stage_file_format').value || 'json';
                
                if (!stageName) {{
                    document.getElementById('stage_status').innerHTML = '<span style="color: #ef4444;">Please enter a stage name.</span>';
                    return;
                }}
                
                // If URL is provided, it's an external stage and needs integration
                if (stageUrl && (!integration || integration === '__not_required__' || integration === '__create_new__')) {{
                    document.getElementById('stage_status').innerHTML = '<span style="color: #ef4444;">External stages require a Storage Integration. Configure in Step 2.</span>';
                    highlightStep(2);
                    return;
                }}
                
                document.getElementById('stage_status').innerHTML = '<span style="color: #f59e0b;">Creating stage...</span>';
                
                try {{
                    const formData = new FormData();
                    formData.append('stage_name', stageName);
                    formData.append('stage_url', stageUrl || '');
                    formData.append('storage_integration', stageUrl ? integration : '');
                    formData.append('file_format', fileFormat.toUpperCase());
                    formData.append('force_replace', 'true');
                    
                    const resp = await fetch('/api/stages/create', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (data.status === 'created' || data.status === 'replaced') {{
                        document.getElementById('stage_status').innerHTML = `<span style="color: #22c55e;">✓ Stage <strong>${{data.stage_name}}</strong> ${{data.status}}!</span>`;
                        loadStages(data.stage_name);
                        // Auto-advance to next step
                        highlightStep(isExternalStage ? 2 : 3);
                    }} else if (data.status === 'exists') {{
                        document.getElementById('stage_status').innerHTML = `<span style="color: #f59e0b;">Stage exists. ${{data.message || 'Use force_replace to overwrite.'}}</span>`;
                    }} else {{
                        document.getElementById('stage_status').innerHTML = `<span style="color: #ef4444;">Error: ${{data.detail || 'Failed to create stage'}}</span>`;
                    }}
                }} catch (err) {{
                    document.getElementById('stage_status').innerHTML = `<span style="color: #ef4444;">Error: ${{err.message}}</span>`;
                }}
            }}
            
            // Legacy function - now handled by onStageChange()
            function updateIntegrationVisibility() {{
                // This is now handled by onStageChange() with better logic
                // Keeping for backward compatibility with other pages
                const stageSelect = document.getElementById('stage_name');
                if (!stageSelect) return;
                
                const badge = document.getElementById('integration_optional_badge');
                if (!badge) return; // Not on stage_landing page
                
                const selectedOption = stageSelect.options[stageSelect.selectedIndex];
                if (selectedOption && selectedOption.parentElement) {{
                    const groupLabel = selectedOption.parentElement.label || '';
                    const isInternal = groupLabel.includes('Internal');
                    
                    badge.style.display = 'inline';
                    if (isInternal) {{
                        badge.textContent = 'Not needed for internal stage';
                        badge.style.background = 'rgba(34,197,94,0.3)';
                        badge.style.color = '#22c55e';
                    }} else {{
                        badge.textContent = 'Required for external stage';
                        badge.style.background = 'rgba(239,68,68,0.2)';
                        badge.style.color = '#ef4444';
                    }}
                }}
            }}
            
            // ========== TARGET TABLE FUNCTIONS ==========
            function onTargetTableChange() {{
                const select = document.getElementById('target_table');
                const container = document.getElementById('new_table_container');
                const schemaPreview = document.getElementById('table_schema_preview');
                
                if (!select) return;
                
                if (select.value === '__create_new__') {{
                    if (container) container.style.display = 'block';
                    if (schemaPreview) schemaPreview.style.display = 'block';
                    loadDatabasesForNewTable();
                }} else {{
                    if (container) container.style.display = 'none';
                    if (schemaPreview) schemaPreview.style.display = select.value ? 'block' : 'none';
                }}
                
                updatePipePreview();
                updatePipelineSummary();
            }}
            
            async function loadDatabasesForNewTable() {{
                try {{
                    const resp = await fetch('/api/databases');
                    const data = await resp.json();
                    const select = document.getElementById('new_table_database');
                    if (!select) return;
                    
                    select.innerHTML = '<option value="">Select database...</option>';
                    data.databases.forEach(db => {{
                        const opt = document.createElement('option');
                        opt.value = db;
                        opt.textContent = db;
                    if (db === '{DB}') opt.selected = true;
                        select.appendChild(opt);
                    }});
                    
                    // Load schemas for default selection
                    if (select.value) loadNewTableSchemas();
                }} catch (e) {{
                    console.error('Failed to load databases for new table:', e);
                }}
            }}
            
            async function loadNewTableSchemas() {{
                const dbSelect = document.getElementById('new_table_database');
                const schemaSelect = document.getElementById('new_table_schema');
                if (!dbSelect || !schemaSelect || !dbSelect.value) return;
                
                try {{
                    const resp = await fetch(`/api/schemas/${{dbSelect.value}}`);
                    const data = await resp.json();
                    
                    schemaSelect.innerHTML = '<option value="">Select schema...</option>';
                    data.schemas.forEach(schema => {{
                        const opt = document.createElement('option');
                        opt.value = schema;
                        opt.textContent = schema;
                        if (schema === 'PRODUCTION') opt.selected = true;
                        schemaSelect.appendChild(opt);
                    }});
                    
                    updateTablePreview();
                }} catch (e) {{
                    console.error('Failed to load schemas:', e);
                }}
            }}
            
            function updateTablePreview() {{
                const db = document.getElementById('new_table_database')?.value || '';
                const schema = document.getElementById('new_table_schema')?.value || '';
                const name = document.getElementById('new_table_name')?.value || 'TABLE_NAME';
                
                const fullPath = document.getElementById('table_full_path');
                if (fullPath) {{
                    if (db && schema && name) {{
                        fullPath.textContent = `${{db}}.${{schema}}.${{name.toUpperCase()}}`;
                    }} else {{
                        fullPath.textContent = '...';
                    }}
                }}
            }}
            
            async function createBronzeTable() {{
                const db = document.getElementById('new_table_database')?.value;
                const schema = document.getElementById('new_table_schema')?.value;
                const tableName = document.getElementById('new_table_name')?.value;
                const statusEl = document.getElementById('table_status');
                const pipeStatusEl = document.getElementById('pipe_detection_status');
                
                //  Check if user opted in to auto-create pipe
                const autoCreatePipe = document.getElementById('auto_create_pipe')?.checked || false;
                const sourceStage = document.getElementById('pipe_source_stage')?.value || '';
                const filePattern = document.getElementById('pipe_file_pattern')?.value || '.*ami_stream.*\\.json';
                
                if (!db || !schema || !tableName) {{
                    if (statusEl) statusEl.innerHTML = '<span style="color: #ef4444;">Please fill in all fields.</span>';
                    return;
                }}
                
                //  Validate pipe creation requirements
                if (autoCreatePipe && !sourceStage) {{
                    if (statusEl) statusEl.innerHTML = '<span style="color: #ef4444;">Please select a source stage for the Snowpipe.</span>';
                    return;
                }}
                
                const fullName = `${{db}}.${{schema}}.${{tableName.toUpperCase()}}`;
                if (statusEl) statusEl.innerHTML = '<span style="color: #f59e0b;">Creating bronze table...</span>';
                
                try {{
                    const formData = new FormData();
                    formData.append('table_name', fullName);
                    formData.append('table_type', 'bronze_variant');
                    
                    //  Include pipe creation options if user opted in
                    if (autoCreatePipe) {{
                        formData.append('create_pipe', 'true');
                        formData.append('source_stage', sourceStage);
                        formData.append('file_pattern', filePattern);
                    }}
                    
                    const resp = await fetch('/api/tables/create-bronze', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (data.status === 'created' || data.status === 'exists') {{
                        if (statusEl) statusEl.innerHTML = `<span style="color: #22c55e;">✓ Table <strong>${{fullName}}</strong> ready!</span>`;
                        
                        //  Show pipe status with actionable options
                        if (pipeStatusEl) {{
                            if (data.pipe_status?.pipe_created_now) {{
                                // Pipe was just created
                                pipeStatusEl.style.display = 'block';
                                pipeStatusEl.style.background = 'rgba(34, 197, 94, 0.15)';
                                pipeStatusEl.style.border = '1px solid rgba(34, 197, 94, 0.3)';
                                pipeStatusEl.innerHTML = `
                                    <div style="color: #22c55e; font-weight: 600; margin-bottom: 6px;">
                                        ✓ Snowpipe Created Successfully
                                    </div>
                                    <div style="color: #94a3b8; font-size: 12px;">Pipe:</div>
                                    <div style="color: #a855f7; font-family: monospace; font-size: 11px; word-break: break-all; margin: 4px 0 8px 0;">${{data.pipe_status.pipe_name}}</div>
                                    <div style="color: #64748b; font-size: 12px;">Data will auto-ingest from S3 → Table</div>
                                `;
                                
                                //  Refresh Step 4 pipes dropdown and auto-select the new pipe
                                await loadPipes(data.pipe_status.pipe_name);
                            }} else if (data.pipe_status?.has_pipe) {{
                                // Existing pipe found
                                pipeStatusEl.style.display = 'block';
                                pipeStatusEl.style.background = 'rgba(34, 197, 94, 0.1)';
                                pipeStatusEl.style.border = '1px solid rgba(34, 197, 94, 0.2)';
                                pipeStatusEl.innerHTML = `
                                    <div style="color: #22c55e; margin-bottom: 4px;">✓ Pipe exists</div>
                                    <div style="color: #a855f7; font-family: monospace; font-size: 11px; word-break: break-all;">${{data.pipe_status.pipe_name}}</div>
                                `;
                            }} else if (data.requires_pipe && !autoCreatePipe) {{
                                // No pipe found - offer to create one
                                pipeStatusEl.style.display = 'block';
                                pipeStatusEl.style.background = 'rgba(251, 191, 36, 0.15)';
                                pipeStatusEl.style.border = '1px solid rgba(251, 191, 36, 0.3)';
                                
                                let stageOptions = '<option value="">Select stage...</option>';
                                if (data.available_stages) {{
                                    data.available_stages.forEach(s => {{
                                        const label = s.shared ? `${{s.full_name}} (shared)` : s.full_name;
                                        stageOptions += `<option value="${{s.full_name}}">${{label}}</option>`;
                                    }});
                                }}
                                
                                pipeStatusEl.innerHTML = `
                                    <div style="color: #fbbf24; font-weight: 600; margin-bottom: 6px;">
                                        ⚠ No Snowpipe Found
                                    </div>
                                    <div style="color: #94a3b8; margin-bottom: 8px;">
                                        Without a pipe, data won't auto-load from S3 into this table.
                                    </div>
                                    <div style="background: rgba(15,23,42,0.5); padding: 8px; border-radius: 4px;">
                                        <div style="color: #e2e8f0; margin-bottom: 6px; font-weight: 500;">Create Snowpipe Now?</div>
                                        <select id="create_pipe_stage_select" style="width: 100%; font-size: 0.75rem; margin-bottom: 6px;">
                                            ${{stageOptions}}
                                        </select>
                                        <input type="text" id="create_pipe_pattern" value=".*ami_stream.*\\.json" style="width: 100%; font-size: 0.75rem; margin-bottom: 6px;" placeholder="File pattern (regex)">
                                        <button type="button" onclick="createPipeForTable('${{db}}', '${{schema}}', '${{tableName.toUpperCase()}}')" 
                                            style="background: #a855f7; color: white; border: none; border-radius: 4px; padding: 6px 10px; font-size: 0.7rem; cursor: pointer; width: 100%;">
                                            Create Snowpipe
                                        </button>
                                    </div>
                                `;
                            }} else {{
                                pipeStatusEl.style.display = 'none';
                            }}
                        }}
                        
                        //  Immediately add the new table to dropdown (no API call delay)
                        addTableToDropdownAndSelect(fullName);
                        
                        //  If pipe was created, immediately add it to Step 4 dropdown
                        if (data.pipe_status?.pipe_created_now && data.pipe_status?.pipe_name) {{
                            addPipeToDropdownAndSelect(data.pipe_status.pipe_name);
                        }}
                        
                        // Auto-advance to pipe step
                        highlightStep(isExternalStage ? 4 : 3);
                        
                        // Update pipeline summary
                        updatePipelineSummary();
                    }} else {{
                        if (statusEl) statusEl.innerHTML = `<span style="color: #ef4444;">Error: ${{data.detail || 'Failed to create table'}}</span>`;
                    }}
                }} catch (err) {{
                    if (statusEl) statusEl.innerHTML = `<span style="color: #ef4444;">Error: ${{err.message}}</span>`;
                }}
            }}
            
            //  Immediately add a newly created table to dropdown without API call
            function addTableToDropdownAndSelect(tableName) {{
                const select = document.getElementById('target_table');
                if (!select) return;
                
                // Check if table already exists in dropdown
                for (let opt of select.options) {{
                    if (opt.value === tableName) {{
                        opt.selected = true;
                        onTargetTableChange();
                        return;
                    }}
                }}
                
                // Find or create the optgroup for bronze tables
                let bronzeGroup = null;
                for (let child of select.children) {{
                    if (child.tagName === 'OPTGROUP' && child.label.includes('Bronze')) {{
                        bronzeGroup = child;
                        break;
                    }}
                }}
                
                if (!bronzeGroup) {{
                    bronzeGroup = document.createElement('optgroup');
                    bronzeGroup.label = '❄️ Bronze Tables (VARIANT)';
                    select.appendChild(bronzeGroup);
                }}
                
                // Add the new table option
                const opt = document.createElement('option');
                opt.value = tableName;
                opt.textContent = tableName;
                bronzeGroup.appendChild(opt);
                
                // Select it
                select.value = tableName;
                onTargetTableChange();
            }}
            
            //  Immediately add a newly created pipe to dropdown without API call
            function addPipeToDropdownAndSelect(pipeName) {{
                const select = document.getElementById('pipe_name');
                if (!select) return;
                
                // Extract just the pipe name if fully qualified
                const shortName = pipeName.includes('.') ? pipeName.split('.').pop() : pipeName;
                
                // Check if pipe already exists in dropdown
                for (let opt of select.options) {{
                    if (opt.value === pipeName || opt.value === shortName) {{
                        opt.selected = true;
                        return;
                    }}
                }}
                
                // Find or create the optgroup for pipes
                let pipeGroup = null;
                for (let child of select.children) {{
                    if (child.tagName === 'OPTGROUP' && child.label.includes('Pipes')) {{
                        pipeGroup = child;
                        break;
                    }}
                }}
                
                if (!pipeGroup) {{
                    pipeGroup = document.createElement('optgroup');
                    pipeGroup.label = '⚡ Available Pipes';
                    select.appendChild(pipeGroup);
                }}
                
                // Add the new pipe option
                const opt = document.createElement('option');
                opt.value = shortName;
                opt.textContent = shortName;
                pipeGroup.appendChild(opt);
                
                // Select it
                select.value = shortName;
            }}
            
            //  Create pipe for an existing table (opt-in from warning)
            async function createPipeForTable(db, schema, tableName) {{
                const stageSelect = document.getElementById('create_pipe_stage_select');
                const patternInput = document.getElementById('create_pipe_pattern');
                const pipeStatusEl = document.getElementById('pipe_detection_status');
                
                const sourceStage = stageSelect?.value;
                const pattern = patternInput?.value || '.*ami_stream.*\\.json';
                
                if (!sourceStage) {{
                    alert('Please select a source stage');
                    return;
                }}
                
                pipeStatusEl.innerHTML = '<div style="color: #f59e0b;">Creating Snowpipe...</div>';
                
                try {{
                    const formData = new FormData();
                    formData.append('database', db);
                    formData.append('schema', schema);
                    formData.append('table_name', tableName);
                    formData.append('source_stage', sourceStage);
                    formData.append('file_pattern', pattern);
                    formData.append('auto_refresh', 'true');
                    
                    const resp = await fetch('/api/pipes/auto-create', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (data.status === 'created' || data.status === 'exists') {{
                        pipeStatusEl.style.background = 'rgba(34, 197, 94, 0.15)';
                        pipeStatusEl.style.border = '1px solid rgba(34, 197, 94, 0.3)';
                        pipeStatusEl.innerHTML = `
                            <div style="color: #22c55e; font-weight: 600; margin-bottom: 4px;">
                                ✓ Snowpipe Created!
                            </div>
                            <div style="color: #94a3b8;">
                                <div>Pipe: <span style="color: #a855f7; font-family: monospace;">${{data.pipe_name}}</span></div>
                                <div style="margin-top: 4px;">${{data.files_queued || 0}} files queued for ingestion</div>
                            </div>
                        `;
                        
                        //  Immediately add pipe to dropdown (no API delay)
                        addPipeToDropdownAndSelect(data.pipe_name);
                        
                        //  Update pipeline summary to show the new pipe
                        updatePipelineSummary();
                    }} else {{
                        pipeStatusEl.innerHTML = `<div style="color: #ef4444;">Error: ${{data.detail || 'Failed to create pipe'}}</div>`;
                    }}
                }} catch (err) {{
                    pipeStatusEl.innerHTML = `<div style="color: #ef4444;">Error: ${{err.message}}</div>`;
                }}
            }}
            
            //  Toggle pipe source stage dropdown based on checkbox
            document.addEventListener('DOMContentLoaded', function() {{
                const autoPipeCheckbox = document.getElementById('auto_create_pipe');
                const pipeStageContainer = document.getElementById('pipe_source_stage_container');
                
                if (autoPipeCheckbox && pipeStageContainer) {{
                    autoPipeCheckbox.addEventListener('change', async function() {{
                        if (this.checked) {{
                            pipeStageContainer.style.display = 'block';
                            // Load available stages
                            await loadPipeSourceStages();
                        }} else {{
                            pipeStageContainer.style.display = 'none';
                        }}
                    }});
                }}
            }});
            
            //  Handle file pattern preset selection
            function updateFilePatternFromPreset() {{
                const presetSelect = document.getElementById('pipe_file_pattern_preset');
                const patternInput = document.getElementById('pipe_file_pattern');
                
                if (!presetSelect || !patternInput) return;
                
                if (presetSelect.value === 'custom') {{
                    // Show the custom input field
                    patternInput.style.display = 'block';
                    patternInput.value = '';
                    patternInput.focus();
                }} else {{
                    // Hide custom input and use preset value
                    patternInput.style.display = 'none';
                    patternInput.value = presetSelect.value;
                }}
            }}
            
            async function loadPipeSourceStages() {{
                const select = document.getElementById('pipe_source_stage');
                if (!select) return;
                
                try {{
                    // Fetch external stages from PRODUCTION (shared) + current schema
                    const resp = await fetch('/api/stages/{DB}/PRODUCTION');
                    const data = await resp.json();
                    
                    select.innerHTML = '<option value="">Select source stage...</option>';
                    
                    if (data.stages?.external) {{
                        data.stages.external.forEach(stage => {{
                            const opt = document.createElement('option');
                            opt.value = stage.full_name;
                            opt.textContent = stage.full_name;
                            select.appendChild(opt);
                        }});
                    }}
                }} catch (err) {{
                    console.error('Failed to load stages:', err);
                }}
            }}
            
            async function loadTargetTables(selectValue = null) {{
                const select = document.getElementById('target_table');
                if (!select) return;
                
                try {{
                    // Get tables that have VARIANT column (bronze tables)
                    const resp = await fetch('/api/tables/bronze');
                    const data = await resp.json();
                    
                    select.innerHTML = '<option value="">-- Select Target Table --</option>';
                    
                    // Add "Create New" option
                    const createOpt = document.createElement('option');
                    createOpt.value = '__create_new__';
                    createOpt.textContent = '+ Create New Bronze Table...';
                    createOpt.style.fontStyle = 'italic';
                    select.appendChild(createOpt);
                    
                    if (data.tables && data.tables.length > 0) {{
                        const tableGroup = document.createElement('optgroup');
                        tableGroup.label = '❄️ Bronze Tables (VARIANT)';
                        data.tables.forEach(tbl => {{
                            const opt = document.createElement('option');
                            opt.value = tbl.full_name;
                            opt.textContent = tbl.full_name;
                            tableGroup.appendChild(opt);
                        }});
                        select.appendChild(tableGroup);
                        
                        // Auto-select if requested
                        if (selectValue) {{
                            for (let opt of select.options) {{
                                if (opt.value === selectValue) {{
                                    opt.selected = true;
                                    break;
                                }}
                            }}
                        }} else if (data.tables.length > 0) {{
                            // Auto-select first table
                            select.value = data.tables[0].full_name;
                        }}
                    }} else {{
                        // No tables - show message and auto-select "Create New"
                        const emptyOpt = document.createElement('option');
                        emptyOpt.value = '';
                        emptyOpt.textContent = '(No bronze tables found - create one)';
                        emptyOpt.disabled = true;
                        select.appendChild(emptyOpt);
                        select.value = '__create_new__';
                        onTargetTableChange();
                    }}
                    
                    updatePipelineSummary();
                }} catch (e) {{
                    console.error('Failed to load target tables:', e);
                    select.innerHTML = '<option value="__create_new__">+ Create New Bronze Table...</option>';
                    select.value = '__create_new__';
                    onTargetTableChange();
                }}
            }}
            
            // ========== PIPE FUNCTIONS ==========
            function toggleNewPipeInput() {{
                const select = document.getElementById('pipe_name');
                const container = document.getElementById('new_pipe_container');
                const preview = document.getElementById('pipe_preview');
                if (!select) return;
                
                if (select.value === '__create_new__') {{
                    if (container) container.style.display = 'block';
                    if (preview) preview.style.display = 'block';
                    const nameInput = document.getElementById('new_pipe_name');
                    if (nameInput) nameInput.focus();
                    updatePipePreview();
                }} else {{
                    if (container) container.style.display = 'none';
                    if (preview) preview.style.display = 'none';
                }}
                updatePipelineSummary();
            }}
            
            function updatePipeStage() {{
                updatePipePreview();
                updatePipelineSummary();
            }}
            
            function updatePipePreview() {{
                const stageSelect = document.getElementById('stage_name');
                const pipeNameInput = document.getElementById('new_pipe_name');
                const targetSelect = document.getElementById('target_table');
                const autoIngestCheckbox = document.getElementById('pipe_auto_ingest');
                const formatSelect = document.getElementById('stage_file_format');
                const previewEl = document.getElementById('pipe_ddl_preview');
                
                if (!previewEl) return;
                
                let stageName = stageSelect?.value || 'FLUX_DB.PRODUCTION.STG_AMI_RAW';
                const pipeName = pipeNameInput?.value || 'PIPE_AMI_RAW_INGEST';
                // Use the new target_table select, fall back to legacy pipe_target_table
                let targetTable = targetSelect?.value;
                if (!targetTable || targetTable === '__create_new__') {{
                    const legacyInput = document.getElementById('pipe_target_table');
                    targetTable = legacyInput?.value || 'FLUX_DB.PRODUCTION.AMI_BRONZE_RAW';
                }}
                const autoIngest = autoIngestCheckbox?.checked ?? true;
                const fileFormat = (formatSelect?.value || 'json').toUpperCase();
                
                // Skip preview if critical fields missing
                if (stageName === '__create_new__' || !targetTable) {{
                    previewEl.textContent = '-- Select stage and target table to preview pipe DDL --';
                    return;
                }}
                
                // Ensure stage reference has @ prefix
                stageName = stageName.replace(/^@/, '');
                
                // Generate format-appropriate SELECT clause
                let selectClause;
                if (fileFormat === 'JSON') {{
                    selectClause = `  SELECT 
    $1 AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @${{stageName}}`;
                }} else if (fileFormat === 'PARQUET') {{
                    selectClause = `  SELECT 
    OBJECT_CONSTRUCT(*) AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @${{stageName}}`;
                }} else {{
                    selectClause = `  SELECT 
    OBJECT_CONSTRUCT(
      'col1', $1, 'col2', $2, 'col3', $3, 'col4', $4, 'col5', $5,
      'col6', $6, 'col7', $7, 'col8', $8, 'col9', $9, 'col10', $10
    ) AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @${{stageName}}`;
                }}
                
                const ddl = `CREATE OR REPLACE PIPE ${{pipeName}}
${{autoIngest ? '  AUTO_INGEST = TRUE' : '  AUTO_INGEST = FALSE'}}
  COMMENT = 'Auto-ingests raw AMI data from stage to bronze table'
AS
COPY INTO ${{targetTable}} (RAW_DATA, FILE_NAME, LOAD_TS)
FROM (
${{selectClause}}
)
FILE_FORMAT = (TYPE = ${{fileFormat}});`;
                
                previewEl.textContent = ddl;
                updatePipelineSummary();
            }}
            
            // ========== CREATE PIPE NOW ==========
            async function createPipeNow() {{
                const pipeName = document.getElementById('new_pipe_name')?.value;
                const stageName = document.getElementById('stage_name')?.value;
                // Use new target_table select
                let targetTable = document.getElementById('target_table')?.value;
                if (!targetTable || targetTable === '__create_new__') {{
                    // Fall back to check if table name was entered in create form
                    const newTableDb = document.getElementById('new_table_database')?.value;
                    const newTableSchema = document.getElementById('new_table_schema')?.value;
                    const newTableName = document.getElementById('new_table_name')?.value;
                    if (newTableDb && newTableSchema && newTableName) {{
                        targetTable = `${{newTableDb}}.${{newTableSchema}}.${{newTableName.toUpperCase()}}`;
                    }}
                }}
                const fileFormat = document.getElementById('stage_file_format').value || 'json';
                const autoIngest = document.getElementById('pipe_auto_ingest').checked;
                
                if (!pipeName) {{
                    document.getElementById('pipe_status').innerHTML = '<span style="color: #ef4444;">Please enter a pipe name.</span>';
                    return;
                }}
                if (!stageName || stageName === '__create_new__') {{
                    document.getElementById('pipe_status').innerHTML = '<span style="color: #ef4444;">Please select a stage first.</span>';
                    return;
                }}
                if (!targetTable) {{
                    document.getElementById('pipe_status').innerHTML = '<span style="color: #ef4444;">Please enter a target table.</span>';
                    return;
                }}
                
                document.getElementById('pipe_status').innerHTML = '<span style="color: #f59e0b;">Creating pipe...</span>';
                
                try {{
                    const formData = new FormData();
                    formData.append('pipe_name', pipeName);
                    formData.append('stage_name', stageName.replace(/^@/, ''));
                    formData.append('target_table', targetTable);
                    formData.append('file_format', fileFormat.toUpperCase());
                    formData.append('auto_ingest', autoIngest);
                    formData.append('force_replace', 'true'); // Allow replace from UI
                    
                    const resp = await fetch('/api/pipes/create', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (data.status === 'created' || data.status === 'replaced') {{
                        let successHtml = `
                            <div style="color: #22c55e; font-weight: 600; margin-bottom: 6px;">
                                ✓ Snowpipe ${{data.was_replaced ? 'Replaced' : 'Created'}} Successfully
                            </div>
                            <div style="color: #94a3b8; font-size: 12px;">Pipe:</div>
                            <div style="color: #a855f7; font-family: monospace; font-size: 11px; word-break: break-all; margin: 4px 0 8px 0;">${{data.pipe_name}}</div>
                            <div style="color: #64748b; font-size: 11px;">Schema: ${{data.database}}.${{data.schema}}</div>
                        `;
                        if (data.notification_channel) {{
                            successHtml += `<div style="margin-top: 6px; padding: 8px; background: rgba(34,197,94,0.1); border-radius: 4px; font-size: 0.75rem;">
                                <strong>SQS Notification Channel:</strong><br/>
                                <code style="font-size: 0.7rem; word-break: break-all;">${{data.notification_channel}}</code>
                                <div style="color: #94a3b8; margin-top: 4px;">Configure your cloud event source to send notifications here for auto-ingest.</div>
                            </div>`;
                        }}
                        document.getElementById('pipe_status').innerHTML = successHtml;
                        
                        // Reload pipes dropdown
                        loadPipes();
                    }} else if (data.status === 'exists') {{
                        document.getElementById('pipe_status').innerHTML = `<span style="color: #f59e0b;">Pipe <strong>${{data.pipe_name}}</strong> already exists. ${{data.message}}</span>`;
                    }} else {{
                        document.getElementById('pipe_status').innerHTML = `<span style="color: #ef4444;">Error: ${{data.detail || 'Failed to create pipe'}}</span>`;
                    }}
                }} catch (err) {{
                    document.getElementById('pipe_status').innerHTML = `<span style="color: #ef4444;">Error: ${{err.message}}</span>`;
                }}
            }}
            
            // Add event listeners for pipe preview updates
            document.addEventListener('DOMContentLoaded', function() {{
                const pipeNameInput = document.getElementById('new_pipe_name');
                const autoIngestCheckbox = document.getElementById('pipe_auto_ingest');
                const formatSelect = document.getElementById('stage_file_format');
                const newTableName = document.getElementById('new_table_name');
                
                if (pipeNameInput) pipeNameInput.addEventListener('input', updatePipePreview);
                if (autoIngestCheckbox) autoIngestCheckbox.addEventListener('change', updatePipePreview);
                if (formatSelect) formatSelect.addEventListener('change', updatePipelineSummary);
                if (newTableName) newTableName.addEventListener('input', updateTablePreview);
                
                // Initial summary update
                setTimeout(updatePipelineSummary, 500);
            }});
            
            // ========== LOAD STORAGE INTEGRATIONS ==========
            (async function() {{
                try {{
                    const resp = await fetch('/api/storage-integrations');
                    const data = await resp.json();
                    const select = document.getElementById('storage_integration');
                    select.innerHTML = '<option value="">-- Select Integration --</option>';
                    
                    // Add "Create New" option
                    const createOpt = document.createElement('option');
                    createOpt.value = '__create_new__';
                    createOpt.textContent = '+ Create New Integration...';
                    createOpt.style.fontStyle = 'italic';
                    select.appendChild(createOpt);
                    
                    // Add "Not Required" option for internal stages
                    const notReqOpt = document.createElement('option');
                    notReqOpt.value = '__not_required__';
                    notReqOpt.textContent = '(Not required for internal stages)';
                    select.appendChild(notReqOpt);
                    
                    if (data.integrations && data.integrations.length > 0) {{
                        const intGroup = document.createElement('optgroup');
                        intGroup.label = '☁️ Available Integrations';
                        data.integrations.forEach(integ => {{
                            const opt = document.createElement('option');
                            opt.value = integ.name;
                            opt.textContent = integ.name + ' (' + integ.type + ')';
                            intGroup.appendChild(opt);
                        }});
                        select.appendChild(intGroup);
                        // Note: Don't auto-select here. updateIntegrationVisibility() will set the right value based on stage type.
                    }}
                }} catch (e) {{
                    console.error('Failed to load storage integrations:', e);
                    const select = document.getElementById('storage_integration');
                    select.innerHTML = '<option value="">Error loading integrations</option><option value="__create_new__">+ Create New...</option><option value="__not_required__">(Not required for internal stages)</option>';
                }}
            }})();
            
            // ========== LOAD STAGES ==========
            async function loadStages(selectValue = null, retryCount = 0) {{
                const maxRetries = 3;
                const select = document.getElementById('stage_name');
                if (!select) {{
                    console.error('stage_name element not found');
                    return;
                }}
                try {{
                    // Show loading state
                    if (retryCount === 0) {{
                        select.innerHTML = '<option value="">Loading stages...</option>';
                    }}
                    
                    const resp = await fetch('/api/stages');
                    if (!resp.ok) {{
                        throw new Error('API returned ' + resp.status);
                    }}
                    const data = await resp.json();
                    console.log('Stages API response:', data);
                    
                    // Check for error in response (backend returns 200 with error field on exception)
                    if (data.error) {{
                        console.warn('Stages API returned error:', data.error);
                        if (retryCount < maxRetries) {{
                            console.log(`Retrying stages load (attempt ${{retryCount + 2}}/${{maxRetries + 1}})...`);
                            setTimeout(() => loadStages(selectValue, retryCount + 1), 1000 * (retryCount + 1));
                            return;
                        }}
                        throw new Error(data.error);
                    }}
                    
                    // Validate response structure
                    if (!data || !data.stages) {{
                        throw new Error('Invalid response: missing stages');
                    }}
                    
                    select.innerHTML = '<option value="">-- Select Stage --</option>';
                    
                    // Add "Create New" option at top
                    const createOpt = document.createElement('option');
                    createOpt.value = '__create_new__';
                    createOpt.textContent = '+ Create New Stage...';
                    createOpt.style.fontStyle = 'italic';
                    select.appendChild(createOpt);
                    
                    // Populate internal stages
                    const internalStages = data.stages.internal || [];
                    const externalStages = data.stages.external || [];
                    
                    if (internalStages.length > 0) {{
                        const intGroup = document.createElement('optgroup');
                        intGroup.label = '📁 Internal Stages (Snowflake-managed)';
                        internalStages.forEach(s => {{
                            const opt = document.createElement('option');
                            opt.value = s.full_name;
                            opt.textContent = s.full_name;
                            intGroup.appendChild(opt);
                        }});
                        select.appendChild(intGroup);
                    }}
                    // Populate external stages
                    if (externalStages.length > 0) {{
                        const extGroup = document.createElement('optgroup');
                        extGroup.label = '☁️ External Stages (S3/Azure/GCS)';
                        externalStages.forEach(s => {{
                            const opt = document.createElement('option');
                            opt.value = s.full_name;
                            opt.textContent = s.full_name + ' (' + (s.cloud_provider || 'External') + ')';
                            extGroup.appendChild(opt);
                        }});
                        select.appendChild(extGroup);
                    }}
                    
                    // Show message if no stages found
                    if (internalStages.length === 0 && externalStages.length === 0) {{
                        const emptyOpt = document.createElement('option');
                        emptyOpt.value = '';
                        emptyOpt.textContent = '(No stages found - create one above)';
                        emptyOpt.disabled = true;
                        select.appendChild(emptyOpt);
                    }}
                    
                    // If a specific value was requested (e.g., after creating a stage), select it
                    if (selectValue) {{
                        for (let opt of select.options) {{
                            if (opt.value === selectValue || opt.value.endsWith('.' + selectValue)) {{
                                opt.selected = true;
                                onStageChange();
                                return;
                            }}
                        }}
                    }}
                    
                    // Auto-select first available stage (internal preferred for simplicity)
                    if (internalStages.length > 0) {{
                        for (let opt of select.options) {{
                            if (opt.value && opt.value !== '__create_new__') {{
                                opt.selected = true;
                                break;
                            }}
                        }}
                    }} else if (externalStages.length > 0) {{
                        for (let opt of select.options) {{
                            if (opt.value && opt.value !== '__create_new__') {{
                                opt.selected = true;
                                break;
                            }}
                        }}
                    }}
                    
                    // Apply stage change to update UI state
                    onStageChange();
                }} catch (e) {{
                    console.error('Failed to load stages:', e);
                    // Retry on network errors
                    if (retryCount < maxRetries && (e.name === 'TypeError' || e.message.includes('fetch'))) {{
                        console.log(`Network error, retrying stages load (attempt ${{retryCount + 2}}/${{maxRetries + 1}})...`);
                        setTimeout(() => loadStages(selectValue, retryCount + 1), 1000 * (retryCount + 1));
                        return;
                    }}
                    if (select) {{
                        select.innerHTML = `<option value="">Error: ${{e.message || 'Failed to load stages'}}</option><option value="__create_new__">+ Create New Stage...</option>`;
                    }}
                }}
            }}
            // Load stages on page init
            loadStages();
            // Load target tables for bronze table selection
            loadTargetTables();
            
            // ========== LOAD PIPES ==========
            async function loadPipes(selectValue = null) {{
                try {{
                    const resp = await fetch('/api/pipes');
                    const data = await resp.json();
                    const select = document.getElementById('pipe_name');
                    select.innerHTML = '<option value="">-- Select Pipe --</option>';
                    
                    // Add "Create New" option at top
                    const createOpt = document.createElement('option');
                    createOpt.value = '__create_new__';
                    createOpt.textContent = '+ Create New Snowpipe...';
                    createOpt.style.fontStyle = 'italic';
                    select.appendChild(createOpt);
                    
                    if (data.pipes && data.pipes.length > 0) {{
                        //  Group pipes by schema for better visibility
                        const pipesBySchema = {{}};
                        data.pipes.forEach(pipe => {{
                            const schemaKey = `${{pipe.database}}.${{pipe.schema}}`;
                            if (!pipesBySchema[schemaKey]) {{
                                pipesBySchema[schemaKey] = [];
                            }}
                            pipesBySchema[schemaKey].push(pipe);
                        }});
                        
                        // Create optgroup for each schema
                        Object.keys(pipesBySchema).sort().forEach(schemaKey => {{
                            const pipeGroup = document.createElement('optgroup');
                            pipeGroup.label = `⚡ ${{schemaKey}}`;
                            pipesBySchema[schemaKey].forEach(pipe => {{
                                const opt = document.createElement('option');
                                opt.value = pipe.full_name;  // Use full name for unique identification
                                opt.textContent = pipe.name;
                                opt.dataset.schema = schemaKey;
                                pipeGroup.appendChild(opt);
                            }});
                            select.appendChild(pipeGroup);
                        }});
                        
                        //  Auto-select the specified pipe (e.g., newly created one)
                        if (selectValue) {{
                            // Try to match by full name or just the pipe name
                            const pipeName = selectValue.includes('.') ? selectValue.split('.').pop() : selectValue;
                            for (let opt of select.options) {{
                                if (opt.value === selectValue || opt.value.endsWith('.' + pipeName) || 
                                    opt.value.toUpperCase() === selectValue.toUpperCase() ||
                                    opt.textContent.toUpperCase() === pipeName.toUpperCase()) {{
                                    opt.selected = true;
                                    // Update pipeline summary
                                    updatePipelineSummary();
                                    break;
                                }}
                            }}
                        }}
                    }} else {{
                        // No pipes - auto-select "Create New"
                        select.value = '__create_new__';
                        toggleNewPipeInput();
                    }}
                }} catch (e) {{
                    console.error('Failed to load pipes:', e);
                    const select = document.getElementById('pipe_name');
                    select.innerHTML = '<option value="__create_new__">+ Create New Snowpipe...</option>';
                    select.value = '__create_new__';
                    toggleNewPipeInput();
                }}
            }}
            // Load pipes on page load
            loadPipes();
            </script>
            '''
        
        if dest in ['postgres', 'dual']:
            preview_content += f'''
            <div class="target-section">
                <div class="target-section-title" style="color: var(--color-success);">
                    {get_material_icon('storage', '16px', '#22c55e')} Managed Postgres target
                </div>
                <select name="pg_database" id="pg_database">
                    <option value="">Loading Postgres instances...</option>
                </select>
                <input type="text" name="pg_table" placeholder="Table name" value="ami_streaming_readings">
            </div>
            <script>
            (async function() {{
                try {{
                    const resp = await fetch('/api/postgres/databases');
                    const data = await resp.json();
                    const select = document.getElementById('pg_database');
                    select.innerHTML = '<option value="">Select Postgres instance</option>';
                    data.postgres_databases.forEach(pg => {{
                        const opt = document.createElement('option');
                        opt.value = pg.integration;
                        opt.textContent = pg.integration + ' (' + pg.status + ')';
                        select.appendChild(opt);
                    }});
                }} catch (e) {{
                    console.error('Failed to load postgres:', e);
                }}
            }})();
            </script>
            '''
        
        preview_content += f'''
            <div style="position: sticky; bottom: 0; background: linear-gradient(to top, #1e293b 80%, transparent); padding-top: 16px; margin-top: 16px;">
                <button type="submit" class="btn-primary">{get_material_icon('stream', '20px')} Start Streaming</button>
            </div>
        </form>
        
        <script>
        (async function() {{
            // Load databases for streaming form
            try {{
                const resp = await fetch('/api/databases');
                const data = await resp.json();
                const select = document.getElementById('stream_sf_database');
                // Exit early if element doesn't exist on this page
                if (!select) return;
                
                select.innerHTML = '<option value="">-- Select Database --</option>';
                data.databases.forEach(db => {{
                    const opt = document.createElement('option');
                    opt.value = db;
                    opt.textContent = db;
                    if (db === '{DB}') opt.selected = true;
                    select.appendChild(opt);
                }});
                if (select.value) loadStreamSchemas(select.value);
            }} catch (e) {{
                console.error('Failed to load databases:', e);
            }}
        }})();
        
        async function loadStreamSchemas(database) {{
            if (!database) return;
            try {{
                const resp = await fetch(`/api/schemas/${{database}}`);
                const data = await resp.json();
                const select = document.getElementById('stream_sf_schema');
                // Exit early if element doesn't exist on this page
                if (!select) return;
                
                select.innerHTML = '<option value="">-- Select Schema --</option>';
                data.schemas.forEach(schema => {{
                    const opt = document.createElement('option');
                    opt.value = schema;
                    opt.textContent = schema;
                    if (schema === 'PRODUCTION') opt.selected = true;
                    select.appendChild(opt);
                }});
                if (select.value) loadStreamTables();
                // Refresh PIPE dropdown for new schema
                if (typeof loadStreamPipes === 'function') loadStreamPipes();
            }} catch (e) {{
                console.error('Failed to load schemas:', e);
            }}
        }}
        
        async function loadStreamTables() {{
            const dbSelect = document.getElementById('stream_sf_database');
            const schemaSelect = document.getElementById('stream_sf_schema');
            // Exit early if elements don't exist on this page
            if (!dbSelect || !schemaSelect) return;
            
            const db = dbSelect.value;
            const schema = schemaSelect.value;
            if (!db || !schema) return;
            try {{
                const resp = await fetch(`/api/tables/${{db}}/${{schema}}`);
                const data = await resp.json();
                const select = document.getElementById('stream_table');
                // Exit early if element doesn't exist on this page
                if (!select) return;
                
                select.innerHTML = '<option value="">-- Select Table --</option>';
                data.tables.forEach(tbl => {{
                    const opt = document.createElement('option');
                    opt.value = tbl;
                    opt.textContent = tbl;
                    if (tbl === 'AMI_STREAMING_DATA') opt.selected = true;
                    select.appendChild(opt);
                }});
            }} catch (e) {{
                console.error('Failed to load tables:', e);
            }}
        }}
        
        function toggleStreamNewTable() {{
            const checkbox = document.getElementById('stream_create_new');
            const newInput = document.getElementById('stream_new_table');
            const selectTable = document.getElementById('stream_table');
            if (checkbox.checked) {{
                newInput.style.display = 'block';
                selectTable.style.display = 'none';
            }} else {{
                newInput.style.display = 'none';
                selectTable.style.display = 'block';
            }}
            if (typeof syncStreamFields === 'function') syncStreamFields();
        }}
        
        // Sync form fields with UI controls (for production matching)
        function syncFormFields() {{
            // Get values from UI controls
            const fleetSize = document.getElementById('custom_fleet_size')?.value || {fleet_cfg['meters']};
            const productionSource = document.getElementById('production_source')?.value || 'METER_INFRASTRUCTURE';
            const emissionPattern = document.getElementById('emission_pattern')?.value || 'STAGGERED_REALISTIC';
            const segmentFilter = document.getElementById('segment_filter')?.value || '';
            const interval = document.getElementById('reading_interval')?.value || '15';
            const dataFormat = document.getElementById('data_format')?.value || 'standard';
            
            // Update form hidden fields
            if (document.getElementById('form_meters')) {{
                document.getElementById('form_meters').value = fleetSize;
            }}
            if (document.getElementById('form_production_source')) {{
                document.getElementById('form_production_source').value = productionSource;
            }}
            if (document.getElementById('form_emission_pattern')) {{
                document.getElementById('form_emission_pattern').value = emissionPattern;
            }}
            if (document.getElementById('form_segment_filter')) {{
                document.getElementById('form_segment_filter').value = segmentFilter;
            }}
            if (document.getElementById('form_interval')) {{
                document.getElementById('form_interval').value = interval;
            }}
            if (document.getElementById('form_data_format')) {{
                document.getElementById('form_data_format').value = dataFormat;
            }}
            
            // Update preview panel
            if (document.getElementById('preview_fleet_size')) {{
                document.getElementById('preview_fleet_size').textContent = Number(fleetSize).toLocaleString() + ' meters';
            }}
            if (document.getElementById('preview_production_status')) {{
                const statusEl = document.getElementById('preview_production_status');
                if (productionSource === 'SYNTHETIC') {{
                    statusEl.textContent = 'Synthetic IDs (not production matched)';
                    statusEl.style.color = '#f59e0b';
                }} else {{
                    statusEl.textContent = 'Production Matched (' + productionSource + ')';
                    statusEl.style.color = '#22c55e';
                }}
            }}
        }}
        
        // Hook up event listeners
        document.addEventListener('DOMContentLoaded', function() {{
            const fields = ['custom_fleet_size', 'production_source', 'emission_pattern', 'segment_filter', 'reading_interval', 'data_format'];
            fields.forEach(id => {{
                const el = document.getElementById(id);
                if (el) {{
                    el.addEventListener('change', syncFormFields);
                    el.addEventListener('input', syncFormFields);
                }}
            }});
            
            // Initial sync
            syncFormFields();
        }});
        
        // Intercept form submission to ensure fields are synced
        document.getElementById('streaming_form')?.addEventListener('submit', function(e) {{
            syncFormFields();
            if (typeof syncStreamFields === 'function') syncStreamFields();
        }});
        </script>
        '''
    else:
        est_rows = tmpl['estimated_rows']
        preview_content += f'''
        <div style="margin-bottom: 16px;">
            <div style="color: #64748b; font-size: 0.85rem;">Estimated Rows</div>
            <div style="color: #38bdf8; font-size: 1.5rem; font-weight: 700;">~{est_rows}</div>
        </div>
        '''
    
    # Use full-width layout for Stage Landing (dest='stage') to give pipeline config more room
    layout_class = "main-content full-width" if dest == 'stage' else "main-content"
    
    # For stage flows, sidebar becomes a horizontal bottom panel 
    if dest == 'stage':
        sidebar_html = f'''
        <div class="panel" style="grid-column: 1 / -1; max-height: 70vh; display: flex; flex-direction: column;">
            <div style="overflow-y: auto; flex: 1; padding-right: 8px;">
                {preview_content}
            </div>
        </div>
        '''
    else:
        sidebar_html = f'''
        <div class="panel sidebar" style="max-height: calc(100vh - 180px); display: flex; flex-direction: column; min-height: 400px;">
            <div style="overflow-y: auto; flex: 1; padding-right: 4px; min-height: 0;">
                {preview_content}
            </div>
        </div>
        '''
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Generate - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('generate')}
            
            <div class="{layout_class}">
                <div style="min-width: 0; overflow-x: hidden;">
                    <div class="section-header">
                        <span class="section-num">1</span>
                        Select Template
                    </div>
                    <div class="template-grid">
                        {template_btns}
                    </div>
                    <div class="template-desc">{tmpl['description']}</div>
                    
                    <div class="divider"></div>
                    
                    <div class="section-header">
                        <span class="section-num">2</span>
                        Generation Mode
                    </div>
                    <div class="mode-toggle">
                        {mode_btns}
                    </div>
                    {streaming_info}
                    
                    <div class="divider"></div>
                    
                    {config_section}
                    
                    {_build_code_showcase_panel(mechanism)}
                </div>
                
                {sidebar_html}
            </div>
        </div>
        <script>
        // Handle card/button clicks without page reload
        document.addEventListener('DOMContentLoaded', function() {{
            const params = new URLSearchParams(window.location.search);
            
            // Restore scroll position after reload (if saved)
            const savedScroll = sessionStorage.getItem('scrollPos');
            if (savedScroll) {{
                window.scrollTo(0, parseInt(savedScroll));
                sessionStorage.removeItem('scrollPos');
            }}
            
            // Helper to reload while preserving scroll position
            function reloadWithScroll(url) {{
                sessionStorage.setItem('scrollPos', window.scrollY);
                window.location.href = url;
            }}
            
            // Data flows that require stage selection (dest='stage')
            const stageBasedFlows = ['stage_landing'];
            const currentDataFlow = params.get('data_flow') || 'streaming_insert';
            
            // Click handler for all selectable cards
            document.querySelectorAll('[data-param]').forEach(el => {{
                el.style.cursor = 'pointer';
                el.addEventListener('click', function(e) {{
                    e.preventDefault();
                    const param = this.dataset.param;
                    const value = this.dataset.value;
                    
                    // Update URL param
                    params.set(param, value);
                    
                    // Update visual state - remove active from siblings, add to clicked
                    const container = this.parentElement;
                    container.querySelectorAll('[data-param="' + param + '"]').forEach(sib => {{
                        sib.classList.remove('active');
                    }});
                    this.classList.add('active');
                    
                    // Update hidden form field if exists
                    const hiddenField = document.querySelector('input[name="' + param + '"]');
                    if (hiddenField) {{
                        hiddenField.value = value;
                    }}
                    
                    // For fleet presets, update the fleet size input and metrics
                    if (param === 'fleet') {{
                        const fleetPresets = {{
                            'Demo (1K)': 1000,
                            'Substation (10K)': 10000,
                            'District (50K)': 50000,
                            'Service Area (100K)': 100000
                        }};
                        const fleetSize = fleetPresets[value];
                        if (fleetSize) {{
                            const fleetInput = document.getElementById('custom_fleet_size');
                            if (fleetInput) {{
                                fleetInput.value = fleetSize;
                                updateFleetMetrics(fleetSize);
                            }}
                        }}
                    }}
                    
                    // Update URL without reload (for bookmarking/sharing)
                    const newUrl = window.location.pathname + '?' + params.toString();
                    window.history.replaceState(null, '', newUrl);
                    
                    // Only reload for mode changes (batch vs streaming) - these have fundamentally different UIs
                    if (param === 'mode') {{
                        reloadWithScroll(newUrl);
                    }}
                    
                    // For data_flow changes, always reload - each flow has different server-rendered content
                    // (HP SDK code showcase, mechanism-specific config sections, etc.)
                    if (param === 'data_flow' && value !== currentDataFlow) {{
                        reloadWithScroll(newUrl);
                    }}
                }});
            }});
        }});
        </script>
    </body>
    </html>
    """


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_page():
    active_streams = 0
    total_rows = 0
    throughput = "--"
    task_count = 0
    snowpipe_count = 0
    recent_rows_1h = 0
    tasks_html = ""
    snowpipe_html = ""
    recent_data_html = ""
    bronze_preview_html = ""  #  Live preview of bronze table ingestion
    pipes_info = []
    sdk_jobs = []
    auto_select_stage = None  #  For auto-selecting stage in Stage File Preview
    
    try:
        session = get_valid_session()
        if session:
            # ========== SECTION 1: SNOWFLAKE TASKS ==========
            result = session.sql(f"""
                SHOW TASKS LIKE '%AMI_STREAMING%' IN SCHEMA {DB}.PRODUCTION
            """).collect()
            
            started_tasks = []
            suspended_tasks = []
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                task_name = row_dict.get('name', '')
                task_state = row_dict.get('state', 'unknown')
                task_schedule = row_dict.get('schedule', '')
                task_comment = row_dict.get('comment', '')
                task_warehouse = row_dict.get('warehouse', '')
                task_info = {
                    'name': task_name,
                    'state': task_state,
                    'schedule': task_schedule,
                    'comment': task_comment,
                    'warehouse': task_warehouse
                }
                if task_state.lower() == 'started':
                    started_tasks.append(task_info)
                    task_count += 1
                else:
                    suspended_tasks.append(task_info)
            
            # ========== SECTION 2: SNOWPIPE STREAMING ==========
            #  Check for pipes in BOTH PRODUCTION and DEV schemas
            try:
                seen_pipes = set()
                for schema_path in [f"{DB}.PRODUCTION", f"{DB}.DEV"]:
                    try:
                        result = session.sql(f"""
                            SHOW PIPES IN SCHEMA {schema_path}
                        """).collect()
                        for row in result:
                            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                            pipe_name = row_dict.get('name', '')
                            schema_name = row_dict.get('schema_name', schema_path.split('.')[-1])
                            full_name = f"{schema_name}.{pipe_name}"
                            if full_name not in seen_pipes:
                                seen_pipes.add(full_name)
                                definition = row_dict.get('definition', '') or ''
                                pipes_info.append({
                                    'name': pipe_name,
                                    'full_name': f"{DB}.{schema_name}.{pipe_name}",
                                    'schema': schema_name,
                                    'definition': definition[:100] if definition else '',
                                    'notification_channel': row_dict.get('notification_channel', ''),
                                    'owner': row_dict.get('owner', ''),
                                    'is_external': any(x in definition.upper() for x in ['S3://', 'AZURE://', 'GCS://'])
                                })
                    except Exception as e:
                        logger.warning(f"Monitor: Could not load pipes from {schema_path}: {e}")
                # Sort by schema then name
                pipes_info.sort(key=lambda x: (x.get('schema', ''), x.get('name', '')))
            except Exception:
                pass
            
            # Check for SDK streaming jobs - first from in-memory active jobs, then from DB
            active_memory_jobs = []
            with streaming_lock:
                for jid, jdata in active_streaming_jobs.items():
                    if jdata['status'] in ['RUNNING', 'STARTING', 'ERROR']:
                        stats = jdata.get('stats', {})
                        config = jdata.get('config', {})
                        active_memory_jobs.append({
                            'job_id': jid,
                            'mechanism': config.get('mechanism', 'snowpipe_classic'),
                            'target_table': config.get('target_table', '') or config.get('stage_name', '') or stats.get('stage_name', ''),
                            'meters': config.get('meters', 0),
                            'rows_per_sec': config.get('rows_per_sec', 0),
                            'batch_size_mb': 0,
                            'status': jdata['status'],
                            'created_at': str(stats.get('start_time', ''))[:19],
                            'production_matched': config.get('production_source', 'SYNTHETIC') != 'SYNTHETIC',
                            'total_rows_sent': stats.get('total_rows', 0),
                            'batches_sent': stats.get('batches_sent', 0),
                            'errors': stats.get('errors', 0),
                            'last_error': stats.get('last_error', ''),
                            'last_batch': str(stats.get('last_batch_time', ''))[:19] if stats.get('last_batch_time') else 'N/A',
                            'is_live': jdata['status'] != 'ERROR',
                            'is_error': jdata['status'] == 'ERROR',
                        })
                        snowpipe_count += 1
            
            # Also check DB for historical jobs
            #  Jobs marked RUNNING in DB but not in memory are STALE (service restarted)
            try:
                result = session.sql(f"""
                    SELECT JOB_ID, MECHANISM, TARGET_TABLE, METERS, INTERVAL_MINUTES, 
                           ROWS_PER_SEC, BATCH_SIZE_MB, SERVICE_AREA, STATUS, CREATED_AT,
                           PRODUCTION_SOURCE, EMISSION_PATTERN, PRODUCTION_MATCHED
                    FROM {DB}.{SCHEMA_PRODUCTION}.STREAMING_JOBS
                    ORDER BY CREATED_AT DESC
                    LIMIT 10
                """).collect()
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    job_id_db = row_dict.get('JOB_ID', '')
                    # Skip if already in active memory jobs
                    if any(j['job_id'] == job_id_db for j in active_memory_jobs):
                        continue
                    
                    #  Mark DB jobs with RUNNING status as STALE since they're not in memory
                    db_status = row_dict.get('STATUS', '')
                    is_stale = db_status.upper() in ('RUNNING', 'STALE')  # RUNNING or STALE in DB but not in memory
                    display_status = 'STALE' if is_stale else db_status
                    
                    sdk_jobs.append({
                        'job_id': job_id_db,
                        'mechanism': row_dict.get('MECHANISM', ''),
                        'target_table': row_dict.get('TARGET_TABLE', ''),
                        'meters': row_dict.get('METERS', 0),
                        'rows_per_sec': row_dict.get('ROWS_PER_SEC', 0),
                        'batch_size_mb': row_dict.get('BATCH_SIZE_MB', 0),
                        'status': display_status,
                        'created_at': str(row_dict.get('CREATED_AT', ''))[:19],
                        'production_matched': row_dict.get('PRODUCTION_MATCHED', False),
                        'is_live': False,
                        'is_stale': is_stale
                    })
            except Exception:
                pass
            
            # Combine active and historical
            sdk_jobs = active_memory_jobs + sdk_jobs
            
            active_streams = task_count + snowpipe_count
            
            # ========== SECTION 3: DATA METRICS ==========
            # Get recent rows from streaming tables
            for table_name in ['AMI_STREAMING_DATA', 'AMI_STREAMING_READINGS', 'AMI_STREAMING_READINGS_TEXAS_GULF_COAST', 'AMI_STREAMING_READINGS_HOUSTON_METRO']:
                try:
                    result = session.sql(f"""
                        SELECT COUNT(*) as cnt FROM {DB}.{SCHEMA_PRODUCTION}.{table_name}
                        WHERE CREATED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                    """).collect()
                    recent_rows_1h += result[0]['CNT'] if result else 0
                    
                    result = session.sql(f"""
                        SELECT COUNT(*) as cnt FROM {DB}.{SCHEMA_PRODUCTION}.{table_name}
                    """).collect()
                    total_rows += result[0]['CNT'] if result else 0
                except Exception:
                    pass
            
            throughput = f"{recent_rows_1h / 3600:.1f}" if recent_rows_1h > 0 else "--"
            
            # ========== BUILD TASKS HTML ==========
            tasks_html = f'''
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title">{get_material_icon('schedule', '20px', '#38bdf8')} Snowflake Tasks (Scheduled Streaming)</div>
                <p style="color: #64748b; font-size: 0.85rem; margin-bottom: 16px;">
                    Tasks execute SQL at scheduled intervals to insert streaming data into tables.
                </p>
            '''
            
            if started_tasks:
                tasks_html += '<div style="margin-bottom: 16px;"><div style="color: #22c55e; font-weight: 600; margin-bottom: 8px;">● Active Tasks ({0})</div>'.format(len(started_tasks))
                for t in started_tasks:
                    tasks_html += f'''
                    <div style="background: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="color: #e2e8f0; font-weight: 600; font-family: monospace;">{t['name']}</div>
                                <div style="color: #64748b; font-size: 0.8rem; margin-top: 4px;">
                                    Schedule: <span style="color: #38bdf8;">{t['schedule']}</span> | 
                                    Warehouse: <span style="color: #a855f7;">{t['warehouse']}</span>
                                </div>
                                <div style="color: #64748b; font-size: 0.75rem; margin-top: 2px;">{t['comment'][:100] if t['comment'] else 'No description'}...</div>
                            </div>
                            <form action="/api/task/suspend" method="post" style="margin: 0;">
                                <input type="hidden" name="task_name" value="{t['name']}">
                                <button type="submit" class="btn-secondary" style="padding: 6px 12px; font-size: 0.75rem;">Suspend</button>
                            </form>
                        </div>
                    </div>
                    '''
                tasks_html += '</div>'
            
            if suspended_tasks:
                tasks_html += '<div><div style="color: #f59e0b; font-weight: 600; margin-bottom: 8px;">○ Suspended Tasks ({0})</div>'.format(len(suspended_tasks))
                for t in suspended_tasks:
                    tasks_html += f'''
                    <div style="background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.3); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="color: #94a3b8; font-weight: 600; font-family: monospace;">{t['name']}</div>
                                <div style="color: #64748b; font-size: 0.8rem; margin-top: 4px;">
                                    Schedule: {t['schedule']} | Warehouse: {t['warehouse']}
                                </div>
                            </div>
                            <form action="/api/task/resume" method="post" style="margin: 0;">
                                <input type="hidden" name="task_name" value="{t['name']}">
                                <button type="submit" class="btn-primary" style="padding: 6px 12px; font-size: 0.75rem;">Resume</button>
                            </form>
                        </div>
                    </div>
                    '''
                tasks_html += '</div>'
            
            if not started_tasks and not suspended_tasks:
                tasks_html += '''
                <div style="text-align: center; padding: 20px; color: #64748b;">
                    <div style="font-size: 2rem; margin-bottom: 8px;">📋</div>
                    No streaming tasks found. Create one from the <a href="/generate" style="color: #38bdf8;">Generate</a> page.
                </div>
                '''
            
            tasks_html += '</div>'
            
            # ========== BUILD SNOWPIPE HTML ==========
            snowpipe_html = f'''
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title">{get_material_icon('bolt', '20px', '#f59e0b')} Snowpipe Streaming (Real-time Ingestion)</div>
                <p style="color: #64748b; font-size: 0.85rem; margin-bottom: 16px;">
                    Snowpipe Streaming SDK enables sub-second latency data ingestion via the Classic or High-Performance API.
                </p>
            '''
            
            # Show Pipes -  Display full name with schema badge
            if pipes_info:
                snowpipe_html += f'<div style="margin-bottom: 16px;"><div style="color: #a855f7; font-weight: 600; margin-bottom: 8px;">Configured Pipes ({len(pipes_info)})</div>'
                for p in pipes_info:
                    schema_color = '#22c55e' if p.get('schema') == 'PRODUCTION' else '#f59e0b'
                    schema_badge = f'<span style="background: {schema_color}20; color: {schema_color}; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; margin-left: 8px;">{p.get("schema", "")}</span>'
                    external_badge = '<span style="background: #38bdf820; color: #38bdf8; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; margin-left: 4px;">S3</span>' if p.get('is_external') else ''
                    snowpipe_html += f'''
                    <div style="background: rgba(168, 85, 247, 0.1); border: 1px solid rgba(168, 85, 247, 0.3); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                        <div style="display: flex; align-items: center; flex-wrap: wrap;">
                            <span style="color: #e2e8f0; font-weight: 600; font-family: monospace; font-size: 0.85rem; word-break: break-all;">{p['name']}</span>
                            {schema_badge}{external_badge}
                        </div>
                        <div style="color: #64748b; font-size: 0.7rem; margin-top: 4px;">Owner: {p['owner']}</div>
                    </div>
                    '''
                snowpipe_html += '</div>'
            
            # Show SDK Jobs
            if sdk_jobs:
                #  Only truly LIVE jobs count as running (in-memory = is_live=True)
                # DB jobs marked RUNNING are stale if not in memory
                running_jobs = [j for j in sdk_jobs if j.get('is_live', False)]
                error_jobs = [j for j in sdk_jobs if j.get('is_error', False)]
                stale_jobs = [j for j in sdk_jobs if j.get('is_stale', False)]
                other_jobs = [j for j in sdk_jobs if not j.get('is_live', False) and not j.get('is_stale', False) and not j.get('is_error', False)]
                
                #  If there are stale jobs but no live jobs, show a notice
                if stale_jobs and not running_jobs:
                    snowpipe_html += f'''
                    <div style="background: rgba(251, 191, 36, 0.15); border: 1px solid rgba(251, 191, 36, 0.4); border-radius: 8px; padding: 12px; margin-bottom: 16px;">
                        <div style="color: #fbbf24; font-weight: 600; margin-bottom: 8px;">
                            {get_material_icon('warning', '18px', '#fbbf24')} Stale Jobs Detected
                        </div>
                        <div style="color: #94a3b8; font-size: 0.85rem;">
                            {len(stale_jobs)} job(s) were marked as RUNNING but the service has restarted. 
                            These jobs are no longer active. Start a new streaming job from the <a href="/generate" style="color: #38bdf8;">Generate</a> page.
                        </div>
                    </div>
                    '''
                
                if running_jobs:
                    snowpipe_html += f'<div style="margin-bottom: 16px;"><div style="color: #22c55e; font-weight: 600; margin-bottom: 8px;">● Active SDK Jobs ({len(running_jobs)})</div>'
                    for j in running_jobs:
                        # Determine SDK type with proper handling for fallbacks and Stage Landing
                        mechanism = j.get('mechanism', '').lower()
                        sdk_fallback_note = ''
                        if 'stage_json' in mechanism:
                            sdk_type = 'Stage Landing (Snowpipe)' if 'ext' in mechanism else 'Stage Landing (Internal)'
                            sdk_color = '#0ea5e9'  # Blue for stage-based
                        elif 'hp' in mechanism and 'fallback' not in mechanism and 'auto-local' not in mechanism:
                            sdk_type = 'High-Performance'
                            sdk_color = '#22c55e'  # Green for HP
                        elif 'auto-local' in mechanism:
                            sdk_type = 'SQL INSERT (Local Mode)'
                            sdk_color = '#f59e0b'
                            sdk_fallback_note = 'HP SDK requires key-pair auth or SPCS — using SQL INSERT locally'
                        elif 'fallback' in mechanism:
                            sdk_type = 'SQL INSERT (HP Fallback)'
                            sdk_color = '#f59e0b'
                            sdk_fallback_note = 'HP SDK failed to initialize — fell back to SQL INSERT'
                        else:
                            sdk_type = 'SQL INSERT (Snowpark)'
                            sdk_color = '#f59e0b'
                        
                        #  Calculate time since last batch for this job
                        last_batch_str = j.get('last_batch', 'N/A')
                        batching_status = ''
                        batching_color = '#64748b'
                        if j.get('is_live'):
                            total_rows = j.get('total_rows_sent', 0)
                            batches = j.get('batches_sent', 0)
                            errors = j.get('errors', 0)
                            
                            if batches > 0:
                                batching_status = f'✓ {total_rows:,} rows in {batches} batches'
                                batching_color = '#22c55e'
                            elif total_rows > 0:
                                batching_status = f'⏳ Buffering {total_rows:,} rows (waiting for batch flush)'
                                batching_color = '#f59e0b'
                            else:
                                batching_status = '⏳ Building first batch... (Snowpipe buffers data before flush)'
                                batching_color = '#38bdf8'
                            
                            if errors > 0:
                                batching_status += f' | ⚠️ {errors} errors'
                        
                        snowpipe_html += f'''
                        <div style="background: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                            <div style="display: flex; justify-content: space-between; align-items: start;">
                                <div style="flex: 1;">
                                    <div style="color: #e2e8f0; font-weight: 600; font-family: monospace;">{j['job_id']}</div>
                                    <div style="color: #64748b; font-size: 0.8rem; margin-top: 4px;">
                                        SDK: <span style="color: {sdk_color};">{sdk_type}</span> | 
                                        Target: <span style="color: #38bdf8;">{j['target_table']}</span>
                                    </div>
                                    {'<div style="color: #f59e0b; font-size: 0.7rem; margin-top: 2px; padding: 2px 6px; background: rgba(245,158,11,0.1); border-radius: 4px; display: inline-block;">' + sdk_fallback_note + '</div>' if sdk_fallback_note else ''}
                                    <div style="color: #64748b; font-size: 0.75rem; margin-top: 2px;">
                                        Config: {j['meters']:,} meters
                                    </div>
                                </div>
                                <div style="text-align: right;">
                                    <span style="background: #22c55e; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem;">
                                        {'LIVE' if j.get('is_live') else 'RUNNING'}
                                    </span>
                                    <div style="color: #64748b; font-size: 0.7rem; margin-top: 4px;">Started: {j.get('created_at', '')}</div>
                                    <form action="/api/streaming/stop" method="post" style="margin-top: 4px;">
                                        <input type="hidden" name="job_id" value="{j.get('job_id', '')}">
                                        <button type="submit" class="btn-secondary" style="padding: 2px 6px; font-size: 0.65rem;">Stop</button>
                                    </form>
                                </div>
                            </div>
                            <div style="margin-top: 8px; padding: 8px; background: rgba(30, 41, 59, 0.5); border-radius: 4px; font-size: 0.8rem;">
                                <div style="color: {batching_color}; font-weight: 500;">{batching_status}</div>
                                {'<div style="color: #64748b; font-size: 0.7rem; margin-top: 4px;">Last batch: ' + last_batch_str + '</div>' if last_batch_str != 'N/A' else ''}
                            </div>
                            <div style="margin-top: 6px; padding: 6px 8px; background: rgba(56,189,248,0.1); border-radius: 4px; font-size: 0.7rem; color: #94a3b8;">
                                💡 Snowpipe Streaming buffers rows and flushes in batches. "0 rows/sec" is normal during buffering.
                            </div>
                        </div>
                        '''
                    snowpipe_html += '</div>'
                
                # Show ERROR jobs with clear error details
                if error_jobs:
                    snowpipe_html += f'<div style="margin-bottom: 16px;"><div style="color: #ef4444; font-weight: 600; margin-bottom: 8px;">&#x2717; Failed Jobs ({len(error_jobs)})</div>'
                    for j in error_jobs:
                        last_err = j.get('last_error', 'Unknown error')
                        snowpipe_html += f'''
                        <div style="background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                            <div style="display: flex; justify-content: space-between; align-items: start;">
                                <div style="flex: 1;">
                                    <div style="color: #e2e8f0; font-weight: 600; font-family: monospace;">{j['job_id']}</div>
                                    <div style="color: #64748b; font-size: 0.8rem; margin-top: 4px;">
                                        Target: <span style="color: #38bdf8;">{j['target_table']}</span> | 
                                        Errors: <span style="color: #ef4444;">{j.get('errors', 0)}</span>
                                    </div>
                                </div>
                                <div style="text-align: right;">
                                    <span style="background: #ef4444; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem;">ERROR</span>
                                    <form action="/api/streaming/stop" method="post" style="margin-top: 4px;">
                                        <input type="hidden" name="job_id" value="{j.get('job_id', '')}">
                                        <button type="submit" class="btn-secondary" style="padding: 2px 6px; font-size: 0.65rem;">Dismiss</button>
                                    </form>
                                </div>
                            </div>
                            <div style="margin-top: 8px; padding: 8px; background: rgba(239, 68, 68, 0.08); border: 1px solid rgba(239, 68, 68, 0.2); border-radius: 4px; font-size: 0.75rem;">
                                <div style="color: #ef4444; font-weight: 500; margin-bottom: 4px;">Stopped after repeated failures:</div>
                                <div style="color: #fca5a5; font-family: monospace; word-break: break-all;">{last_err}</div>
                            </div>
                        </div>
                        '''
                    snowpipe_html += '</div>'
                
                # Combine stale and other jobs for history display
                history_jobs = stale_jobs + other_jobs
                if history_jobs:
                    snowpipe_html += f'<div><div style="color: #64748b; font-weight: 600; margin-bottom: 8px;">Recent SDK Jobs ({len(history_jobs)})</div>'
                    for j in history_jobs[:5]:
                        status = j.get('status', '')
                        if j.get('is_stale'):
                            status_color = '#fbbf24'  # Amber for stale
                        elif status.upper() == 'FAILED':
                            status_color = '#ef4444'
                        elif status.upper() == 'STOPPED':
                            status_color = '#64748b'
                        else:
                            status_color = '#64748b'
                        snowpipe_html += f'''
                        <div style="background: rgba(100, 116, 139, 0.1); border: 1px solid rgba(100, 116, 139, 0.3); border-radius: 8px; padding: 8px 12px; margin-bottom: 4px; font-size: 0.85rem;">
                            <span style="color: #94a3b8; font-family: monospace;">{j.get('job_id', '')}</span>
                            <span style="color: {status_color}; margin-left: 12px;">{j.get('status', '')}</span>
                            <span style="color: #64748b; margin-left: 12px;">{j.get('created_at', '')}</span>
                        </div>
                        '''
                    snowpipe_html += '</div>'
            
            if not pipes_info and not sdk_jobs:
                snowpipe_html += '''
                <div style="text-align: center; padding: 20px; color: #64748b;">
                    <div style="font-size: 2rem; margin-bottom: 8px;">⚡</div>
                    No Snowpipe Streaming jobs configured. Select "Snowpipe Streaming" as the mechanism on the <a href="/generate" style="color: #38bdf8;">Generate</a> page.
                </div>
                '''
            
            snowpipe_html += '</div>'
            
            # ========== BUILD RECENT DATA HTML ==========
            #  Dynamically show data based on what the user is streaming to
            # - If active job targets a stage → direct user to Stage File Preview
            # - If active job targets a table → query that table dynamically
            # - If no active jobs → show helpful guidance
            
            # Determine the active streaming target
            active_target = None
            active_target_type = None  # 'stage' or 'table'
            if active_memory_jobs:
                # Get the most recent active job's target
                active_target = active_memory_jobs[0].get('target_table', '')
                if active_target.startswith('@'):
                    active_target_type = 'stage'
                else:
                    active_target_type = 'table'
            
            try:
                if active_target_type == 'stage':
                    #  For stage streaming, guide user to the Stage File Preview below
                    stage_name_clean = active_target.replace('@', '')
                    recent_data_html = f'''
                    <div class="panel" style="margin-top: 24px;">
                        <div class="panel-title" style="display: flex; align-items: center; flex-wrap: wrap;">
                            {get_material_icon('cloud_upload', '20px', '#0ea5e9')} Active Streaming Target: External Stage
                        </div>
                        <div style="background: rgba(14, 165, 233, 0.1); border: 1px solid rgba(14, 165, 233, 0.3); border-radius: 8px; padding: 16px; margin-top: 12px;">
                            <div style="color: #e2e8f0; font-weight: 600; font-family: monospace; margin-bottom: 8px;">
                                {active_target}
                            </div>
                            <div style="color: #94a3b8; font-size: 0.85rem;">
                                Data is streaming to an external S3 stage via boto3. Use the <b>Stage File Preview</b> section below 
                                (auto-selected to <span style="color: #38bdf8;">{stage_name_clean}</span>) to see landed files and preview data.
                            </div>
                            <div style="margin-top: 12px; padding: 8px; background: rgba(56,189,248,0.1); border-radius: 4px; font-size: 0.75rem; color: #94a3b8;">
                                💡 External stage data flows: boto3 → S3 → Snowpipe → Bronze Table. Check Snowpipe status in the diagnostics.
                            </div>
                        </div>
                    </div>
                    '''
                    # Set a flag to auto-select this stage in the Stage File Preview
                    auto_select_stage = stage_name_clean
                    
                elif active_target_type == 'table' and active_target:
                    #  Query the actual target table dynamically
                    # Ensure the table name is fully qualified
                    target_table = active_target
                    if '.' not in target_table:
                        target_table = f"{DB}.{SCHEMA_PRODUCTION}.{target_table}"
                    
                    result = session.sql(f"""
                        SELECT METER_ID, READING_TIMESTAMP, USAGE_KWH, VOLTAGE, CUSTOMER_SEGMENT, DATA_QUALITY, PRODUCTION_MATCHED, CREATED_AT
                        FROM {target_table}
                        ORDER BY CREATED_AT DESC
                        LIMIT 10
                    """).collect()
                    
                    # Calculate freshness info
                    newest_record_age = None
                    freshness_color = "#64748b"
                    if result:
                        newest_created = result[0].asDict().get('CREATED_AT') if hasattr(result[0], 'asDict') else dict(result[0]).get('CREATED_AT')
                        if newest_created:
                            from datetime import datetime
                            now = datetime.now()
                            if hasattr(newest_created, 'replace'):
                                try:
                                    newest_created = newest_created.replace(tzinfo=None)
                                except Exception:
                                    pass
                            age_seconds = (now - newest_created).total_seconds()
                            if age_seconds < 60:
                                newest_record_age = f"{int(age_seconds)} seconds ago"
                                freshness_color = "#22c55e"
                            elif age_seconds < 3600:
                                newest_record_age = f"{int(age_seconds // 60)} minutes ago"
                                freshness_color = "#f59e0b" if age_seconds > 300 else "#22c55e"
                            elif age_seconds < 86400:
                                newest_record_age = f"{int(age_seconds // 3600)} hours ago"
                                freshness_color = "#ef4444"
                            else:
                                newest_record_age = f"{int(age_seconds // 86400)} days ago"
                                freshness_color = "#ef4444"
                    
                    if result:
                        freshness_badge = f'''
                            <span style="margin-left: 12px; background: {freshness_color}20; color: {freshness_color}; padding: 4px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 500;">
                                Newest: {newest_record_age}
                            </span>
                        ''' if newest_record_age else ''
                        
                        recent_data_html = f'''
                        <div class="panel" style="margin-top: 24px;">
                            <div class="panel-title" style="display: flex; align-items: center; flex-wrap: wrap;">
                                {get_material_icon('table_rows', '20px', '#22c55e')} Streaming Data: <span style="color: #38bdf8; font-family: monospace; margin-left: 8px;">{target_table}</span>
                                {freshness_badge}
                            </div>
                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
                                    <thead>
                                        <tr style="background: rgba(56,189,248,0.1);">
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">METER_ID</th>
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">READING_TS</th>
                                            <th style="padding: 8px; text-align: left; color: #22c55e;">INGESTED</th>
                                            <th style="padding: 8px; text-align: right; color: #94a3b8;">KWH</th>
                                            <th style="padding: 8px; text-align: right; color: #94a3b8;">VOLTAGE</th>
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">SEGMENT</th>
                                            <th style="padding: 8px; text-align: center; color: #94a3b8;">QUAL</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                        '''
                        for idx, row in enumerate(result):
                            bg = 'rgba(15,23,42,0.5)' if idx % 2 == 0 else 'rgba(30,41,59,0.5)'
                            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                            meter_id = row_dict.get('METER_ID', '-')
                            ts = str(row_dict.get('READING_TIMESTAMP', '-'))[:19]
                            created = row_dict.get('CREATED_AT')
                            created_str = str(created)[:19] if created else '-'
                            usage = row_dict.get('USAGE_KWH', 0) or 0
                            voltage = row_dict.get('VOLTAGE', 0) or 0
                            segment = row_dict.get('CUSTOMER_SEGMENT', '-')
                            quality = row_dict.get('DATA_QUALITY', '-')
                            prod_match = row_dict.get('PRODUCTION_MATCHED', False)
                            
                            meter_color = '#22c55e' if prod_match else '#f59e0b'
                            quality_color = '#22c55e' if quality == 'VALID' else '#ef4444' if quality == 'OUTAGE' else '#f59e0b'
                            quality_short = quality[:3] if quality else '-'
                            
                            recent_data_html += f'''
                            <tr style="background: {bg};">
                                <td style="padding: 8px; color: {meter_color}; font-family: monospace; font-size: 0.8rem;">{meter_id}</td>
                                <td style="padding: 8px; color: #e2e8f0; font-size: 0.8rem;">{ts}</td>
                                <td style="padding: 8px; color: #22c55e; font-size: 0.8rem; font-weight: 500;">{created_str}</td>
                                <td style="padding: 8px; color: #e2e8f0; text-align: right;">{usage:.3f}</td>
                                <td style="padding: 8px; color: #e2e8f0; text-align: right;">{voltage:.1f}</td>
                                <td style="padding: 8px; color: #94a3b8; font-size: 0.8rem;">{segment}</td>
                                <td style="padding: 8px; text-align: center; color: {quality_color};">{quality_short}</td>
                            </tr>
                            '''
                        recent_data_html += '''</tbody></table></div>
                            <div style="margin-top: 8px; padding: 8px; background: rgba(56,189,248,0.1); border-radius: 4px; font-size: 0.75rem; color: #94a3b8;">
                                💡 <b>INGESTED</b> shows when Snowflake received the data. <b>READING_TS</b> is the meter's timestamp.
                            </div>
                        </div>'''
                    else:
                        recent_data_html = f'''
                        <div class="panel" style="margin-top: 24px;">
                            <div class="panel-title">{get_material_icon('table_rows', '20px')} Streaming Data: <span style="color: #38bdf8;">{target_table}</span></div>
                            <div style="text-align: center; padding: 20px; color: #64748b;">
                                No data in target table yet. Data should appear shortly after streaming starts.
                            </div>
                        </div>
                        '''
                    auto_select_stage = None
                else:
                    # No active jobs - show default table with guidance
                    result = session.sql(f"""
                        SELECT METER_ID, READING_TIMESTAMP, USAGE_KWH, VOLTAGE, CUSTOMER_SEGMENT, DATA_QUALITY, PRODUCTION_MATCHED, CREATED_AT
                        FROM {DB}.{SCHEMA_PRODUCTION}.AMI_STREAMING_DATA
                        ORDER BY CREATED_AT DESC
                        LIMIT 10
                    """).collect()
                    
                    newest_record_age = None
                    freshness_color = "#64748b"
                    if result:
                        newest_created = result[0].asDict().get('CREATED_AT') if hasattr(result[0], 'asDict') else dict(result[0]).get('CREATED_AT')
                        if newest_created:
                            from datetime import datetime
                            now = datetime.now()
                            if hasattr(newest_created, 'replace'):
                                try:
                                    newest_created = newest_created.replace(tzinfo=None)
                                except Exception:
                                    pass
                            age_seconds = (now - newest_created).total_seconds()
                            if age_seconds < 60:
                                newest_record_age = f"{int(age_seconds)} seconds ago"
                                freshness_color = "#22c55e"
                            elif age_seconds < 3600:
                                newest_record_age = f"{int(age_seconds // 60)} minutes ago"
                                freshness_color = "#f59e0b" if age_seconds > 300 else "#22c55e"
                            elif age_seconds < 86400:
                                newest_record_age = f"{int(age_seconds // 3600)} hours ago"
                                freshness_color = "#ef4444"
                            else:
                                newest_record_age = f"{int(age_seconds // 86400)} days ago"
                                freshness_color = "#ef4444"
                    
                    if result:
                        freshness_badge = f'''
                            <span style="margin-left: 12px; background: {freshness_color}20; color: {freshness_color}; padding: 4px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 500;">
                                Newest: {newest_record_age}
                            </span>
                        ''' if newest_record_age else ''
                        
                        no_job_notice = '''
                            <div style="background: rgba(251, 191, 36, 0.1); border: 1px solid rgba(251, 191, 36, 0.3); border-radius: 6px; padding: 10px 12px; margin-bottom: 12px; font-size: 0.8rem; color: #fbbf24;">
                                ⚠️ No active streaming job. Showing default table. Start a job from <a href="/generate" style="color: #38bdf8;">Generate</a> to see your streaming target here.
                            </div>
                        '''
                        
                        recent_data_html = f'''
                        <div class="panel" style="margin-top: 24px;">
                            <div class="panel-title" style="display: flex; align-items: center; flex-wrap: wrap;">
                                {get_material_icon('table_rows', '20px', '#22c55e')} Recent Streaming Data (Default Table)
                                {freshness_badge}
                            </div>
                            {no_job_notice}
                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
                                    <thead>
                                        <tr style="background: rgba(56,189,248,0.1);">
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">METER_ID</th>
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">READING_TS</th>
                                            <th style="padding: 8px; text-align: left; color: #22c55e;">INGESTED</th>
                                            <th style="padding: 8px; text-align: right; color: #94a3b8;">KWH</th>
                                            <th style="padding: 8px; text-align: right; color: #94a3b8;">VOLTAGE</th>
                                            <th style="padding: 8px; text-align: left; color: #94a3b8;">SEGMENT</th>
                                            <th style="padding: 8px; text-align: center; color: #94a3b8;">QUAL</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                        '''
                        for idx, row in enumerate(result):
                            bg = 'rgba(15,23,42,0.5)' if idx % 2 == 0 else 'rgba(30,41,59,0.5)'
                            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                            meter_id = row_dict.get('METER_ID', '-')
                            ts = str(row_dict.get('READING_TIMESTAMP', '-'))[:19]
                            created = row_dict.get('CREATED_AT')
                            created_str = str(created)[:19] if created else '-'
                            usage = row_dict.get('USAGE_KWH', 0) or 0
                            voltage = row_dict.get('VOLTAGE', 0) or 0
                            segment = row_dict.get('CUSTOMER_SEGMENT', '-')
                            quality = row_dict.get('DATA_QUALITY', '-')
                            prod_match = row_dict.get('PRODUCTION_MATCHED', False)
                            
                            meter_color = '#22c55e' if prod_match else '#f59e0b'
                            quality_color = '#22c55e' if quality == 'VALID' else '#ef4444' if quality == 'OUTAGE' else '#f59e0b'
                            quality_short = quality[:3] if quality else '-'
                            
                            recent_data_html += f'''
                            <tr style="background: {bg};">
                                <td style="padding: 8px; color: {meter_color}; font-family: monospace; font-size: 0.8rem;">{meter_id}</td>
                                <td style="padding: 8px; color: #e2e8f0; font-size: 0.8rem;">{ts}</td>
                                <td style="padding: 8px; color: #22c55e; font-size: 0.8rem; font-weight: 500;">{created_str}</td>
                                <td style="padding: 8px; color: #e2e8f0; text-align: right;">{usage:.3f}</td>
                                <td style="padding: 8px; color: #e2e8f0; text-align: right;">{voltage:.1f}</td>
                                <td style="padding: 8px; color: #94a3b8; font-size: 0.8rem;">{segment}</td>
                                <td style="padding: 8px; text-align: center; color: {quality_color};">{quality_short}</td>
                            </tr>
                            '''
                        recent_data_html += '''</tbody></table></div>
                            <div style="margin-top: 8px; padding: 8px; background: rgba(56,189,248,0.1); border-radius: 4px; font-size: 0.75rem; color: #94a3b8;">
                                💡 <b>INGESTED</b> shows when Snowflake received the data. <b>READING_TS</b> is the meter's timestamp.
                            </div>
                        </div>'''
                    else:
                        recent_data_html = f'''
                        <div class="panel" style="margin-top: 24px;">
                            <div class="panel-title">{get_material_icon('table_rows', '20px')} Recent Streaming Data</div>
                            <div style="text-align: center; padding: 20px; color: #64748b;">
                                No streaming data yet. Start a streaming job to see data here.
                            </div>
                        </div>
                        '''
                    auto_select_stage = None
                    
            except Exception as e:
                logger.error(f"Error building recent data section: {e}")
                recent_data_html = f'''
                <div class="panel" style="margin-top: 24px;">
                    <div class="panel-title">{get_material_icon('table_rows', '20px')} Recent Streaming Data</div>
                    <div style="text-align: center; padding: 20px; color: #64748b;">
                        No streaming data available. Start a streaming job to see data here.
                    </div>
                </div>
                '''
                auto_select_stage = None
                
    except Exception as e:
        logger.error(f"Monitor page error: {e}")
        tasks_html = f'<div class="info-box red"><div class="title" style="color: #ef4444;">Error</div><div class="desc">{str(e)}</div></div>'
        snowpipe_html = ""
    
    # ========== BUILD BRONZE TABLE PREVIEW HTML ==========
    #  Live preview showing data landing in bronze tables from Snowpipe
    bronze_preview_html = f'''
    <div class="panel" style="margin-top: 24px;">
        <div class="panel-title" style="display: flex; align-items: center; justify-content: space-between;">
            <span>{get_material_icon('database', '20px', '#a855f7')} Bronze Table Live Preview</span>
            <button onclick="refreshBronzePreview()" class="btn-secondary" style="padding: 4px 12px; font-size: 0.75rem;">
                {get_material_icon('refresh', '14px')} Refresh
            </button>
        </div>
        <p style="color: #64748b; font-size: 0.85rem; margin-bottom: 16px;">
            Live view of recently ingested data in bronze/landing tables. Shows the end-to-end flow: S3 → Snowpipe → Bronze Table.
        </p>
        <div style="display: flex; gap: 12px; margin-bottom: 16px; align-items: center;">
            <select id="bronze-table-select" onchange="loadBronzePreview()" style="
                flex: 1;
                background: rgba(15,23,42,0.8);
                border: 1px solid rgba(168,85,247,0.3);
                border-radius: 6px;
                padding: 10px 12px;
                color: #e2e8f0;
                font-family: monospace;
                font-size: 0.9rem;
            ">
                <option value="">Select a bronze table to preview...</option>
            </select>
            <label style="display: flex; align-items: center; gap: 6px; color: #94a3b8; font-size: 0.8rem; cursor: pointer;">
                <input type="checkbox" id="bronze-auto-refresh" checked onchange="toggleBronzeAutoRefresh()" style="accent-color: #a855f7;">
                Auto-refresh
            </label>
        </div>
        <div id="bronze-preview-content" style="
            background: rgba(15,23,42,0.6);
            border: 1px solid rgba(100,116,139,0.3);
            border-radius: 8px;
            padding: 16px;
            min-height: 100px;
        ">
            <div style="text-align: center; color: #64748b;">
                Select a bronze table above to see recently ingested data
            </div>
        </div>
        <div id="bronze-freshness" style="margin-top: 8px; text-align: right; font-size: 0.75rem; color: #64748b;"></div>
    </div>
    
    <script>
        const BRONZE_STORAGE_KEY = 'flux_monitor_selected_bronze_table';
        let bronzeRefreshInterval = null;
        
        document.addEventListener('DOMContentLoaded', function() {{
            loadBronzeTables();
        }});
        
        async function loadBronzeTables() {{
            try {{
                const resp = await fetch('/api/bronze-tables');
                const data = await resp.json();
                const select = document.getElementById('bronze-table-select');
                
                // Clear existing options except the first one
                while (select.options.length > 1) {{
                    select.remove(1);
                }}
                
                // Add table options
                if (data.tables) {{
                    data.tables.forEach(function(t) {{
                        const opt = document.createElement('option');
                        opt.value = t.full_name;
                        opt.textContent = t.full_name + (t.row_count !== undefined ? ' (' + t.row_count.toLocaleString() + ' rows)' : '');
                        select.appendChild(opt);
                    }});
                }}
                
                // Restore saved selection
                const saved = localStorage.getItem(BRONZE_STORAGE_KEY);
                if (saved) {{
                    select.value = saved;
                    if (select.value === saved) {{
                        loadBronzePreview();
                    }}
                }}
                
                // Start auto-refresh if enabled
                if (document.getElementById('bronze-auto-refresh').checked) {{
                    startBronzeAutoRefresh();
                }}
            }} catch (err) {{
                console.error('Failed to load bronze tables:', err);
            }}
        }}
        
        async function loadBronzePreview() {{
            const select = document.getElementById('bronze-table-select');
            const tableName = select.value;
            const contentDiv = document.getElementById('bronze-preview-content');
            const freshnessDiv = document.getElementById('bronze-freshness');
            
            if (!tableName) {{
                contentDiv.innerHTML = '<div style="text-align: center; color: #64748b;">Select a bronze table above to see recently ingested data</div>';
                freshnessDiv.innerHTML = '';
                return;
            }}
            
            // Save selection
            localStorage.setItem(BRONZE_STORAGE_KEY, tableName);
            
            contentDiv.innerHTML = '<div style="text-align: center; color: #64748b;">Loading...</div>';
            
            try {{
                const resp = await fetch('/api/bronze-preview?table=' + encodeURIComponent(tableName));
                const data = await resp.json();
                
                if (data.error) {{
                    contentDiv.innerHTML = '<div style="color: #ef4444;">Error: ' + data.error + '</div>';
                    freshnessDiv.innerHTML = '';
                    return;
                }}
                
                if (!data.rows || data.rows.length === 0) {{
                    contentDiv.innerHTML = '<div style="text-align: center; color: #64748b;">No data found in this table yet. Snowpipe is still ingesting...</div>';
                    freshnessDiv.innerHTML = '';
                    return;
                }}
                
                // Build table HTML
                let html = '<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse; font-size: 0.8rem;">';
                html += '<thead><tr style="background: rgba(168,85,247,0.1);">';
                
                // Get columns from first row
                const cols = data.columns || Object.keys(data.rows[0]);
                cols.forEach(function(col) {{
                    html += '<th style="padding: 8px; text-align: left; color: #a855f7; font-weight: 600; white-space: nowrap;">' + col + '</th>';
                }});
                html += '</tr></thead><tbody>';
                
                // Add data rows
                data.rows.forEach(function(row, idx) {{
                    const bg = idx % 2 === 0 ? 'rgba(15,23,42,0.5)' : 'rgba(30,41,59,0.5)';
                    html += '<tr style="background: ' + bg + ';">';
                    cols.forEach(function(col) {{
                        let val = row[col];
                        if (val === null || val === undefined) val = '<span style="color: #64748b;">NULL</span>';
                        else if (typeof val === 'string' && val.length > 30) val = val.substring(0, 30) + '...';
                        html += '<td style="padding: 8px; color: #e2e8f0; white-space: nowrap;">' + val + '</td>';
                    }});
                    html += '</tr>';
                }});
                
                html += '</tbody></table></div>';
                
                // Add summary
                if (data.total_count !== undefined) {{
                    html += '<div style="margin-top: 12px; padding: 8px; background: rgba(168,85,247,0.1); border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">';
                    html += '<span style="color: #94a3b8; font-size: 0.75rem;">Showing ' + data.rows.length + ' of ' + data.total_count.toLocaleString() + ' rows</span>';
                    if (data.newest_age) {{
                        const ageColor = data.newest_age_seconds < 60 ? '#22c55e' : (data.newest_age_seconds < 300 ? '#f59e0b' : '#ef4444');
                        html += '<span style="color: ' + ageColor + '; font-size: 0.75rem; font-weight: 600;">Newest: ' + data.newest_age + '</span>';
                    }}
                    html += '</div>';
                }}
                
                contentDiv.innerHTML = html;
                freshnessDiv.innerHTML = 'Last updated: ' + new Date().toLocaleTimeString();
                
            }} catch (err) {{
                contentDiv.innerHTML = '<div style="color: #ef4444;">Failed to load preview: ' + err.message + '</div>';
                freshnessDiv.innerHTML = '';
            }}
        }}
        
        function startBronzeAutoRefresh() {{
            if (bronzeRefreshInterval) clearInterval(bronzeRefreshInterval);
            bronzeRefreshInterval = setInterval(function() {{
                const select = document.getElementById('bronze-table-select');
                if (select.value) {{
                    loadBronzePreview();
                }}
            }}, 5000);  // Refresh every 5 seconds
        }}
        
        function stopBronzeAutoRefresh() {{
            if (bronzeRefreshInterval) {{
                clearInterval(bronzeRefreshInterval);
                bronzeRefreshInterval = null;
            }}
        }}
        
        function toggleBronzeAutoRefresh() {{
            const enabled = document.getElementById('bronze-auto-refresh').checked;
            if (enabled) {{
                startBronzeAutoRefresh();
            }} else {{
                stopBronzeAutoRefresh();
            }}
        }}
        
        function refreshBronzePreview() {{
            loadBronzePreview();
        }}
    </script>
    '''
    
    # ========== BUILD STAGE PREVIEW HTML ==========
    #  S3 Select-style live preview of raw files landing in stages
    stage_preview_html = f'''
    <div class="panel" style="margin-top: 24px;">
        <div class="panel-title">{get_material_icon('folder_open', '20px', '#0ea5e9')} Stage File Preview (S3 Select-Style)</div>
        <p style="color: #64748b; font-size: 0.85rem; margin-bottom: 16px;">
            Query raw files directly from stages - similar to AWS S3 Select. Select a stage to preview landed data.
        </p>
        <div style="display: flex; gap: 12px; margin-bottom: 16px; align-items: center;">
            <select id="stage-preview-select" onchange="loadStagePreview()" style="
                flex: 1;
                background: rgba(15,23,42,0.8);
                border: 1px solid rgba(56,189,248,0.3);
                border-radius: 6px;
                padding: 10px 12px;
                color: #e2e8f0;
                font-family: monospace;
                font-size: 0.9rem;
            ">
                <option value="">Select a stage to preview...</option>
            </select>
            <button onclick="loadStagePreview()" class="btn-primary" style="padding: 10px 16px;">
                {get_material_icon('refresh', '16px')} Refresh
            </button>
        </div>
        <div id="stage-preview-content" style="
            background: rgba(15,23,42,0.6);
            border: 1px solid rgba(100,116,139,0.3);
            border-radius: 8px;
            padding: 16px;
            min-height: 100px;
        ">
            <div style="text-align: center; color: #64748b;">
                Select a stage above to preview files
            </div>
        </div>
    </div>
    
    <script>
        //  Preserve stage selection across page refreshes using localStorage
        const STAGE_STORAGE_KEY = 'flux_monitor_selected_stage';
        
        //  Server-provided auto-select stage (from active streaming job)
        const AUTO_SELECT_STAGE = '{auto_select_stage if auto_select_stage else ""}';
        
        // Load stages on page load and restore previous selection
        document.addEventListener('DOMContentLoaded', function() {{
            loadStageOptions();
            startAutoRefresh();
        }});
        
        async function loadStageOptions() {{
            try {{
                const resp = await fetch('/api/stages');
                const data = await resp.json();
                const select = document.getElementById('stage-preview-select');
                
                // Clear existing options except the first one
                while (select.options.length > 1) {{
                    select.remove(1);
                }}
                
                // Add external stages
                if (data.stages && data.stages.external && data.stages.external.length > 0) {{
                    const extGroup = document.createElement('optgroup');
                    extGroup.label = '☁️ External Stages';
                    data.stages.external.forEach(stage => {{
                        const opt = document.createElement('option');
                        opt.value = stage.full_name;
                        opt.textContent = stage.full_name + ' (' + stage.cloud_provider + ')';
                        extGroup.appendChild(opt);
                    }});
                    select.appendChild(extGroup);
                }}
                
                // Add internal stages
                if (data.stages && data.stages.internal && data.stages.internal.length > 0) {{
                    const intGroup = document.createElement('optgroup');
                    intGroup.label = '❄️ Internal Stages';
                    data.stages.internal.forEach(stage => {{
                        const opt = document.createElement('option');
                        opt.value = stage.full_name;
                        opt.textContent = stage.full_name;
                        intGroup.appendChild(opt);
                    }});
                    select.appendChild(intGroup);
                }}
                
                //  Priority order for stage selection:
                // 1. Server-provided AUTO_SELECT_STAGE (from active streaming job) - highest priority
                // 2. User's previously saved selection from localStorage
                // 3. Default to first option
                let stageToSelect = null;
                
                if (AUTO_SELECT_STAGE) {{
                    // Active job is streaming to a stage - auto-select it
                    stageToSelect = AUTO_SELECT_STAGE;
                    // Also show a visual indicator
                    console.log('Auto-selecting stage from active job:', AUTO_SELECT_STAGE);
                }} else {{
                    // No active job - try to restore saved selection
                    stageToSelect = localStorage.getItem(STAGE_STORAGE_KEY);
                }}
                
                if (stageToSelect) {{
                    // Find and select the stage
                    for (let i = 0; i < select.options.length; i++) {{
                        if (select.options[i].value === stageToSelect) {{
                            select.value = stageToSelect;
                            loadStagePreview();
                            break;
                        }}
                    }}
                }}
            }} catch (err) {{
                console.error('Failed to load stages:', err);
            }}
        }}
        
        async function loadStagePreview() {{
            const select = document.getElementById('stage-preview-select');
            const stageName = select.value;
            const contentDiv = document.getElementById('stage-preview-content');
            
            //  Save selection to localStorage for persistence across refreshes
            if (stageName) {{
                localStorage.setItem(STAGE_STORAGE_KEY, stageName);
            }} else {{
                localStorage.removeItem(STAGE_STORAGE_KEY);
            }}
            
            if (!stageName) {{
                contentDiv.innerHTML = '<div style="text-align: center; color: #64748b;">Select a stage above to preview files</div>';
                return;
            }}
            
            contentDiv.innerHTML = '<div style="text-align: center; color: #38bdf8;"><span class="loading-spinner"></span> Loading stage preview...</div>';
            
            try {{
                const resp = await fetch('/api/stage/preview/' + encodeURIComponent(stageName) + '?limit=10');
                const data = await resp.json();
                
                if (data.error) {{
                    contentDiv.innerHTML = '<div style="color: #ef4444;">Error: ' + data.error + '</div>';
                    return;
                }}
                
                let html = '';
                
                // File listing section
                html += '<div style="margin-bottom: 16px;">';
                html += '<div style="color: #94a3b8; font-weight: 600; margin-bottom: 8px;">📁 Files in Stage (' + data.file_count + ' found)</div>';
                if (data.files && data.files.length > 0) {{
                    html += '<div style="max-height: 150px; overflow-y: auto;">';
                    data.files.forEach(f => {{
                        const sizeKB = (f.size / 1024).toFixed(1);
                        html += '<div style="background: rgba(30,41,59,0.5); padding: 6px 10px; margin-bottom: 4px; border-radius: 4px; font-size: 0.8rem; display: flex; justify-content: space-between;">';
                        html += '<span style="color: #38bdf8; font-family: monospace;">' + f.name.split('/').pop() + '</span>';
                        html += '<span style="color: #64748b;">' + sizeKB + ' KB</span>';
                        html += '</div>';
                    }});
                    html += '</div>';
                }} else {{
                    html += '<div style="color: #64748b; font-style: italic;">No files found in stage</div>';
                }}
                html += '</div>';
                
                // Data preview section (S3 Select style)
                html += '<div>';
                html += '<div style="color: #94a3b8; font-weight: 600; margin-bottom: 8px;">🔍 Data Preview (Query: ' + (data.query_method || 'N/A') + ')</div>';
                if (data.preview_data && data.preview_data.length > 0) {{
                    html += '<div style="overflow-x: auto;">';
                    html += '<table style="width: 100%; border-collapse: collapse; font-size: 0.8rem;">';
                    html += '<thead><tr style="background: rgba(14,165,233,0.15);">';
                    html += '<th style="padding: 6px 8px; text-align: left; color: #0ea5e9;">METER_ID</th>';
                    html += '<th style="padding: 6px 8px; text-align: left; color: #0ea5e9;">TIMESTAMP</th>';
                    html += '<th style="padding: 6px 8px; text-align: right; color: #0ea5e9;">USAGE</th>';
                    html += '<th style="padding: 6px 8px; text-align: right; color: #0ea5e9;">VOLTAGE</th>';
                    html += '<th style="padding: 6px 8px; text-align: left; color: #0ea5e9;">SEGMENT</th>';
                    html += '<th style="padding: 6px 8px; text-align: left; color: #0ea5e9;">SOURCE FILE</th>';
                    html += '</tr></thead><tbody>';
                    
                    data.preview_data.forEach((row, idx) => {{
                        const bg = idx % 2 === 0 ? 'rgba(15,23,42,0.5)' : 'rgba(30,41,59,0.5)';
                        html += '<tr style="background: ' + bg + ';">';
                        html += '<td style="padding: 6px 8px; color: #22c55e; font-family: monospace;">' + (row.meter_id || '-') + '</td>';
                        html += '<td style="padding: 6px 8px; color: #e2e8f0;">' + (row.reading_timestamp || '-') + '</td>';
                        html += '<td style="padding: 6px 8px; color: #e2e8f0; text-align: right;">' + (row.usage_kwh || 0).toFixed(4) + '</td>';
                        html += '<td style="padding: 6px 8px; color: #e2e8f0; text-align: right;">' + (row.voltage || 0).toFixed(2) + '</td>';
                        html += '<td style="padding: 6px 8px; color: #94a3b8;">' + (row.customer_segment || '-') + '</td>';
                        const shortFile = (row.source_file || '').split('/').pop().substring(0, 25);
                        html += '<td style="padding: 6px 8px; color: #64748b; font-family: monospace; font-size: 0.7rem;">' + shortFile + '</td>';
                        html += '</tr>';
                    }});
                    html += '</tbody></table></div>';
                }} else {{
                    html += '<div style="color: #64748b; font-style: italic; text-align: center; padding: 20px;">No data records found - stage may be empty or use a different format</div>';
                }}
                html += '</div>';
                
                contentDiv.innerHTML = html;
            }} catch (err) {{
                contentDiv.innerHTML = '<div style="color: #ef4444;">Failed to load preview: ' + err.message + '</div>';
            }}
        }}
        
        //  Smart auto-refresh that updates metrics via AJAX without full page reload
        // This preserves stage selection and user interactions
        let refreshCountdown = 30;
        let refreshInterval = null;
        
        function startAutoRefresh() {{
            // Update countdown every second
            const countdownEl = document.getElementById('refresh-countdown');
            refreshInterval = setInterval(() => {{
                refreshCountdown--;
                if (countdownEl) countdownEl.textContent = refreshCountdown + 's';
                
                if (refreshCountdown <= 0) {{
                    refreshMetrics();
                    refreshCountdown = 30;
                }}
            }}, 1000);
        }}
        
        async function refreshMetrics() {{
            try {{
                // Fetch updated metrics from API
                const resp = await fetch('/api/monitor/metrics');
                if (!resp.ok) return;
                
                const data = await resp.json();
                
                // Update metric cards
                const activeEl = document.getElementById('metric-active-streams');
                const throughputEl = document.getElementById('metric-throughput');
                const totalEl = document.getElementById('metric-total-rows');
                const healthEl = document.getElementById('health-indicator');
                const healthDetailEl = document.getElementById('health-detail');
                
                if (activeEl && data.active_streams !== undefined) {{
                    activeEl.textContent = data.active_streams;
                }}
                if (throughputEl && data.throughput !== undefined) {{
                    throughputEl.textContent = data.throughput;
                }}
                if (totalEl && data.total_rows !== undefined) {{
                    totalEl.textContent = data.total_rows_formatted || data.total_rows;
                }}
                
                // Update health indicator if data available
                if (healthEl && data.health) {{
                    healthEl.className = 'health-indicator ' + data.health.status.toLowerCase();
                    const iconEl = healthEl.querySelector('.health-icon');
                    const statusEl = healthEl.querySelector('.health-status');
                    if (iconEl) {{
                        iconEl.textContent = data.health.icon;
                        iconEl.style.color = data.health.color;
                    }}
                    if (statusEl) {{
                        statusEl.textContent = data.health.status;
                        statusEl.style.color = data.health.color;
                    }}
                    if (healthDetailEl) {{
                        healthDetailEl.textContent = data.health.detail;
                    }}
                }}
                
                // Update grid topology widget
                const topoEl = document.getElementById('grid-topology-widget');
                if (topoEl && data.grid_topology) {{
                    const t = data.grid_topology;
                    const hasEvents = t.substations_active > 0 || t.circuits_active > 0 || t.transformers_active > 0;
                    topoEl.style.display = 'block';
                    topoEl.innerHTML = `
                        <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">
                            <span class="material-symbols-rounded" style="font-size:16px;color:${{hasEvents ? '#f59e0b' : '#22c55e'}};">${{hasEvents ? 'warning' : 'check_circle'}}</span>
                            <span style="color:#e2e8f0;font-weight:600;font-size:0.8rem;">Grid Topology</span>
                            <span style="color:${{hasEvents ? '#f59e0b' : '#64748b'}};font-size:0.7rem;margin-left:auto;">${{hasEvents ? 'ACTIVE EVENTS' : 'NOMINAL'}}</span>
                        </div>
                        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;text-align:center;">
                            <div style="background:rgba(30,41,59,0.5);border-radius:6px;padding:8px 4px;">
                                <div style="color:${{t.substations_active > 0 ? '#ef4444' : '#94a3b8'}};font-size:1.1rem;font-weight:700;">${{t.substations_active}}<span style="color:#64748b;font-weight:400;">/${{t.substations_total}}</span></div>
                                <div style="color:#64748b;font-size:0.65rem;margin-top:2px;">Substations</div>
                            </div>
                            <div style="background:rgba(30,41,59,0.5);border-radius:6px;padding:8px 4px;">
                                <div style="color:${{t.circuits_active > 0 ? '#f59e0b' : '#94a3b8'}};font-size:1.1rem;font-weight:700;">${{t.circuits_active}}<span style="color:#64748b;font-weight:400;">/${{t.circuits_total}}</span></div>
                                <div style="color:#64748b;font-size:0.65rem;margin-top:2px;">Circuits</div>
                            </div>
                            <div style="background:rgba(30,41,59,0.5);border-radius:6px;padding:8px 4px;">
                                <div style="color:${{t.transformers_active > 0 ? '#ef4444' : '#94a3b8'}};font-size:1.1rem;font-weight:700;">${{t.transformers_active}}<span style="color:#64748b;font-weight:400;">/${{t.transformers_total}}</span></div>
                                <div style="color:#64748b;font-size:0.65rem;margin-top:2px;">Transformers</div>
                            </div>
                        </div>
                    `;
                }} else if (topoEl) {{
                    topoEl.style.display = 'none';
                }}
                
                // Flash indicator to show refresh happened
                const indicator = document.querySelector('.refresh-indicator');
                if (indicator) {{
                    indicator.style.borderColor = 'rgba(34,197,94,0.8)';
                    setTimeout(() => {{ indicator.style.borderColor = 'rgba(56,189,248,0.3)'; }}, 300);
                }}
            }} catch (err) {{
                console.error('Auto-refresh failed:', err);
            }}
        }}
        
        // Manual refresh button
        function manualRefresh() {{
            refreshMetrics();
            refreshCountdown = 30;
        }}
    </script>
    '''
    
    # Insight: Calculate stream health status for at-a-glance understanding
    # Health states: HEALTHY (data flowing), BUFFERING (waiting for batch), STALLED (no data 5+ min), IDLE (no jobs)
    stream_health = "IDLE"
    health_color = "#64748b"
    health_icon = "pause_circle"
    health_detail = "No active streaming jobs"
    
    if active_streams > 0:
        # Check recent activity from in-memory jobs
        most_recent_batch = None
        has_active_batching = False
        with streaming_lock:
            for jid, jdata in active_streaming_jobs.items():
                if jdata['status'] in ['RUNNING', 'STARTING']:
                    stats = jdata.get('stats', {})
                    last_batch = stats.get('last_batch_time')
                    if last_batch:
                        if most_recent_batch is None or last_batch > most_recent_batch:
                            most_recent_batch = last_batch
                    # Check if actively batching (rows generated but waiting for batch flush)
                    if stats.get('total_rows', 0) > 0 or stats.get('batches_sent', 0) > 0:
                        has_active_batching = True
        
        from datetime import datetime, timedelta
        now = datetime.now()
        
        if most_recent_batch:
            time_since_batch = (now - most_recent_batch).total_seconds()
            if time_since_batch < 60:  # Data in last 60 seconds
                stream_health = "HEALTHY"
                health_color = "#22c55e"
                health_icon = "check_circle"
                health_detail = f"Data flowing ({int(time_since_batch)}s since last batch)"
            elif time_since_batch < 300:  # 1-5 minutes
                stream_health = "BUFFERING"
                health_color = "#f59e0b"
                health_icon = "hourglass_top"
                health_detail = f"Buffering data ({int(time_since_batch)}s since last batch)"
            else:  # 5+ minutes
                stream_health = "STALLED"
                health_color = "#ef4444"
                health_icon = "warning"
                health_detail = f"No data for {int(time_since_batch//60)}+ minutes"
        elif has_active_batching:
            stream_health = "STARTING"
            health_color = "#38bdf8"
            health_icon = "play_circle"
            health_detail = "Stream starting, building first batch..."
        else:
            stream_health = "WAITING"
            health_color = "#a855f7"
            health_icon = "schedule"
            health_detail = "Jobs active, awaiting first data"
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Monitor - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <!--  Removed meta refresh - using AJAX to preserve UI state -->
        {get_base_styles()}
        <style>
            /* Stream Health Indicator Styles */
            .health-indicator {{
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 12px;
                padding: 16px 24px;
                border-radius: 12px;
                margin-bottom: 24px;
                animation: health-pulse 2s ease-in-out infinite;
            }}
            .health-indicator.healthy {{ background: linear-gradient(135deg, rgba(34,197,94,0.2), rgba(34,197,94,0.1)); border: 2px solid rgba(34,197,94,0.5); }}
            .health-indicator.buffering {{ background: linear-gradient(135deg, rgba(245,158,11,0.2), rgba(245,158,11,0.1)); border: 2px solid rgba(245,158,11,0.5); }}
            .health-indicator.stalled {{ background: linear-gradient(135deg, rgba(239,68,68,0.2), rgba(239,68,68,0.1)); border: 2px solid rgba(239,68,68,0.5); animation: health-warning 1s ease-in-out infinite; }}
            .health-indicator.idle {{ background: linear-gradient(135deg, rgba(100,116,139,0.2), rgba(100,116,139,0.1)); border: 2px solid rgba(100,116,139,0.5); }}
            .health-indicator.starting {{ background: linear-gradient(135deg, rgba(56,189,248,0.2), rgba(56,189,248,0.1)); border: 2px solid rgba(56,189,248,0.5); }}
            .health-indicator.waiting {{ background: linear-gradient(135deg, rgba(168,85,247,0.2), rgba(168,85,247,0.1)); border: 2px solid rgba(168,85,247,0.5); }}
            
            @keyframes health-pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.85; }} }}
            @keyframes health-warning {{ 0%, 100% {{ border-color: rgba(239,68,68,0.5); }} 50% {{ border-color: rgba(239,68,68,0.9); }} }}
            
            .health-status {{ font-size: 1.5rem; font-weight: 700; letter-spacing: 2px; }}
            .health-detail {{ font-size: 0.9rem; color: #94a3b8; }}
            .health-icon {{ font-size: 2rem; }}
            
            /* Auto-refresh indicator */
            .refresh-indicator {{
                position: fixed;
                bottom: 20px;
                right: 20px;
                background: rgba(15,23,42,0.95);
                border: 1px solid rgba(56,189,248,0.3);
                border-radius: 8px;
                padding: 8px 16px;
                font-size: 0.8rem;
                color: #64748b;
                z-index: 1000;
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            .refresh-countdown {{ color: #38bdf8; font-family: monospace; }}
        </style>
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('monitor')}
            
            <!--  Prominent Stream Health Indicator - answers "is my stream working?" at a glance -->
            <div class="health-indicator {stream_health.lower()}" id="health-indicator">
                <span class="material-symbols-outlined health-icon" style="color: {health_color};">{health_icon}</span>
                <div>
                    <div class="health-status" style="color: {health_color};">{stream_health}</div>
                    <div class="health-detail" id="health-detail">{health_detail}</div>
                </div>
            </div>
            
            <div class="monitor-grid">
                <div class="monitor-card">
                    <h3>{get_material_icon('show_chart', '20px', '#38bdf8')} Active Streams</h3>
                    <div class="metric-value" style="font-size: 3rem;" id="metric-active-streams">{active_streams}</div>
                    <div class="metric-label" id="metric-streams-label">{'No active streaming jobs' if active_streams == 0 else f'{task_count} task(s), {snowpipe_count} Snowpipe job(s)'}</div>
                </div>
                <div class="monitor-card">
                    <h3>{get_material_icon('speed', '20px', '#22c55e')} Throughput</h3>
                    <div class="metric-value" style="font-size: 3rem;" id="metric-throughput">{throughput}</div>
                    <div class="metric-label">rows/second (last hour)</div>
                </div>
                <div class="monitor-card">
                    <h3>{get_material_icon('trending_up', '20px', '#a855f7')} Total Rows</h3>
                    <div class="metric-value" style="font-size: 3rem;" id="metric-total-rows">{format_number(total_rows)}</div>
                    <div class="metric-label">rows in streaming tables</div>
                </div>
            </div>
            
            <!-- Grid Topology Status (populated via AJAX auto-refresh) -->
            <div id="grid-topology-widget" class="panel" style="display:none; margin-bottom: 16px; padding: 14px 16px;"></div>
            
            <!--  Section order follows data pipeline flow: Generator → S3 Stage → Snowpipe → Bronze Table → Tasks -->
            
            {recent_data_html}
            
            {snowpipe_html}
            
            {stage_preview_html}
            
            {bronze_preview_html}
            
            {tasks_html}
            
            <!--  External Stage Diagnostics Panel - pre-flight check for S3 streaming -->
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title" style="display: flex; align-items: center; gap: 8px;">
                    {get_material_icon('health_and_safety', '20px', '#f59e0b')} External Stage Health Check
                    <button onclick="runDiagnostics()" class="btn-secondary" style="margin-left: auto; padding: 4px 12px; font-size: 0.75rem;">
                        Run Diagnostics
                    </button>
                </div>
                <p style="color: #64748b; font-size: 0.85rem; margin-bottom: 16px;">
                    Pre-flight check for S3 external stage streaming. Validates AWS credentials, role assumption, and bucket access.
                </p>
                <div id="diagnostics-results" style="display: none;">
                    <div id="diagnostics-status" style="padding: 12px; border-radius: 8px; margin-bottom: 16px;"></div>
                    <div id="diagnostics-checks"></div>
                </div>
                <div id="diagnostics-placeholder" style="text-align: center; padding: 20px; color: #64748b;">
                    Click "Run Diagnostics" to validate external stage streaming configuration
                </div>
            </div>
            
            <script>
            async function runDiagnostics() {{
                const placeholder = document.getElementById('diagnostics-placeholder');
                const results = document.getElementById('diagnostics-results');
                const statusEl = document.getElementById('diagnostics-status');
                const checksEl = document.getElementById('diagnostics-checks');
                
                placeholder.innerHTML = '<div style="color: #38bdf8;">Running diagnostics...</div>';
                
                try {{
                    const resp = await fetch('/api/external-stage/diagnostics');
                    const data = await resp.json();
                    
                    placeholder.style.display = 'none';
                    results.style.display = 'block';
                    
                    // Status banner - handle READY, READY_WITH_WARNINGS, NOT_READY
                    const isReady = data.overall_status === 'READY' || data.overall_status === 'READY_WITH_WARNINGS';
                    const hasWarnings = data.overall_status === 'READY_WITH_WARNINGS';
                    const bannerColor = isReady ? (hasWarnings ? 'rgba(245,158,11,0.2)' : 'rgba(34,197,94,0.2)') : 'rgba(239,68,68,0.2)';
                    const borderColor = isReady ? (hasWarnings ? 'rgba(245,158,11,0.5)' : 'rgba(34,197,94,0.5)') : 'rgba(239,68,68,0.5)';
                    const textColor = isReady ? (hasWarnings ? '#f59e0b' : '#22c55e') : '#ef4444';
                    const statusIcon = isReady ? (hasWarnings ? '⚠️' : '✅') : '❌';
                    
                    statusEl.style.background = bannerColor;
                    statusEl.style.border = `1px solid ${{borderColor}}`;
                    statusEl.innerHTML = `
                        <div style="font-weight: 600; font-size: 1.1rem; color: ${{textColor}};">
                            ${{statusIcon}} ${{data.overall_status.replace(/_/g, ' ')}}
                        </div>
                        <div style="color: #94a3b8; margin-top: 4px;">${{data.summary}}</div>
                        ${{data.action_required ? `<div style="color: #f59e0b; margin-top: 8px; font-size: 0.85rem;">⚠️ ${{data.action_required}}</div>` : ''}}
                    `;
                    
                    // Individual checks
                    let checksHtml = '';
                    for (const check of data.checks) {{
                        const statusIcon = check.status === 'PASS' ? '✅' : check.status === 'WARN' ? '⚠️' : '❌';
                        const statusColor = check.status === 'PASS' ? '#22c55e' : check.status === 'WARN' ? '#f59e0b' : '#ef4444';
                        
                        // Special handling for Snowpipe Configuration check
                        let extraContent = '';
                        if (check.name === 'Snowpipe Configuration') {{
                            if (check.pipes && check.pipes.length > 0) {{
                                // Show existing pipes with actions
                                extraContent = `
                                    <div style="margin-top: 12px; padding: 12px; background: rgba(34,197,94,0.1); border-radius: 6px;">
                                        <div style="color: #22c55e; font-weight: 500; margin-bottom: 8px;">Configured Pipes:</div>
                                        ${{check.pipes.map(p => `
                                            <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px; background: rgba(15,23,42,0.5); border-radius: 4px; margin-bottom: 4px;">
                                                <div>
                                                    <span style="color: #38bdf8; font-family: monospace;">${{p.name}}</span>
                                                    ${{p.auto_ingest ? '<span style="background: #22c55e; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; margin-left: 8px;">AUTO-INGEST</span>' : ''}}
                                                </div>
                                                <div style="display: flex; gap: 8px;">
                                                    <button onclick="refreshPipe('${{p.name}}')" class="btn-secondary" style="padding: 4px 8px; font-size: 0.7rem;">Refresh Files</button>
                                                    <button onclick="checkPipeStatus('${{p.name}}')" class="btn-secondary" style="padding: 4px 8px; font-size: 0.7rem;">Status</button>
                                                </div>
                                            </div>
                                            <div id="pipe-status-${{p.name.replace(/\\./g, '-')}}" style="display: none; margin-bottom: 8px;"></div>
                                        `).join('')}}
                                    </div>
                                `;
                            }} else if (check.can_create_pipe) {{
                                // Show pipe creation form
                                const stageInfo = data.stage_info || {{}};
                                extraContent = `
                                    <div style="margin-top: 12px; padding: 16px; background: rgba(56,189,248,0.1); border: 1px dashed rgba(56,189,248,0.3); border-radius: 8px;">
                                        <div style="color: #38bdf8; font-weight: 600; margin-bottom: 12px;">
                                            🔧 Create Snowpipe for Auto-Ingestion
                                        </div>
                                        <div id="pipe-create-form">
                                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px;">
                                                <div>
                                                    <label style="color: #94a3b8; font-size: 0.8rem; display: block; margin-bottom: 4px;">Target Schema</label>
                                                    <select id="new-pipe-schema" style="width: 100%; padding: 8px; background: rgba(15,23,42,0.8); border: 1px solid rgba(100,116,139,0.3); border-radius: 6px; color: #e2e8f0;">
                                                        <option value="DEV">DEV</option>
                                                        <option value="PRODUCTION">PRODUCTION</option>
                                                    </select>
                                                </div>
                                                <div>
                                                    <label style="color: #94a3b8; font-size: 0.8rem; display: block; margin-bottom: 4px;">Target Table</label>
                                                    <input type="text" id="new-pipe-table" placeholder="AMI_BRONZE_RAW" style="width: 100%; padding: 8px; background: rgba(15,23,42,0.8); border: 1px solid rgba(100,116,139,0.3); border-radius: 6px; color: #e2e8f0;">
                                                </div>
                                            </div>
                                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px;">
                                                <div>
                                                    <label style="color: #94a3b8; font-size: 0.8rem; display: block; margin-bottom: 4px;">Pipe Name</label>
                                                    <input type="text" id="new-pipe-name" placeholder="AMI_RAW_INGEST_PIPE" style="width: 100%; padding: 8px; background: rgba(15,23,42,0.8); border: 1px solid rgba(100,116,139,0.3); border-radius: 6px; color: #e2e8f0;">
                                                </div>
                                                <div>
                                                    <label style="color: #94a3b8; font-size: 0.8rem; display: block; margin-bottom: 4px;">Source Stage</label>
                                                    <input type="text" id="new-pipe-stage" value="${{stageInfo.name || '{DB}.PRODUCTION.EXT_RAW_AMI'}}" readonly style="width: 100%; padding: 8px; background: rgba(15,23,42,0.5); border: 1px solid rgba(100,116,139,0.2); border-radius: 6px; color: #94a3b8;">
                                                </div>
                                            </div>
                                            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 12px;">
                                                <label style="display: flex; align-items: center; gap: 6px; color: #94a3b8; font-size: 0.85rem; cursor: pointer;">
                                                    <input type="checkbox" id="new-pipe-autoingest" checked style="width: 16px; height: 16px;">
                                                    Enable Auto-Ingest (requires S3 event notification setup)
                                                </label>
                                            </div>
                                            <button onclick="createPipeFromDiagnostics()" class="btn-primary" style="padding: 10px 20px;">
                                                Create Snowpipe
                                            </button>
                                            <div id="pipe-create-result" style="margin-top: 12px;"></div>
                                        </div>
                                    </div>
                                `;
                            }}
                        }}
                        
                        checksHtml += `
                            <div style="background: rgba(30,41,59,0.5); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                                <div style="display: flex; justify-content: space-between; align-items: start;">
                                    <div>
                                        <div style="font-weight: 600; color: #e2e8f0;">
                                            ${{statusIcon}} ${{check.name}}
                                        </div>
                                        <div style="color: #64748b; font-size: 0.8rem; margin-top: 2px;">
                                            ${{check.description}}
                                        </div>
                                    </div>
                                    <span style="color: ${{statusColor}}; font-size: 0.8rem; font-weight: 500;">
                                        ${{check.status}}
                                    </span>
                                </div>
                                <div style="color: #94a3b8; font-size: 0.8rem; margin-top: 8px; font-family: monospace; background: rgba(15,23,42,0.5); padding: 8px; border-radius: 4px;">
                                    ${{check.detail}}
                                </div>
                                ${{check.fix ? `<div style="color: #f59e0b; font-size: 0.75rem; margin-top: 6px;">💡 Fix: ${{check.fix}}</div>` : ''}}
                                ${{extraContent}}
                            </div>
                        `;
                    }}
                    checksEl.innerHTML = checksHtml;
                    
                }} catch (e) {{
                    placeholder.innerHTML = `<div style="color: #ef4444;">Error running diagnostics: ${{e.message}}</div>`;
                }}
            }}
            
            async function createPipeFromDiagnostics() {{
                const schema = document.getElementById('new-pipe-schema').value;
                const table = document.getElementById('new-pipe-table').value;
                const pipeName = document.getElementById('new-pipe-name').value;
                const stage = document.getElementById('new-pipe-stage').value;
                const autoIngest = document.getElementById('new-pipe-autoingest').checked;
                const resultDiv = document.getElementById('pipe-create-result');
                
                if (!table || !pipeName) {{
                    resultDiv.innerHTML = '<div style="color: #ef4444;">Please fill in Target Table and Pipe Name</div>';
                    return;
                }}
                
                resultDiv.innerHTML = '<div style="color: #38bdf8;">Creating pipe...</div>';
                
                try {{
                    const formData = new FormData();
                    formData.append('pipe_name', pipeName);
                    formData.append('target_database', '{DB}');
                    formData.append('target_schema', schema);
                    formData.append('target_table', table);
                    formData.append('source_stage', stage);
                    formData.append('file_format', 'JSON');
                    formData.append('auto_ingest', autoIngest ? 'true' : 'false');
                    formData.append('strip_outer_array', 'true');
                    
                    const resp = await fetch('/api/pipes/create', {{
                        method: 'POST',
                        body: formData
                    }});
                    const data = await resp.json();
                    
                    if (resp.ok && data.success) {{
                        resultDiv.innerHTML = `
                            <div style="background: rgba(34,197,94,0.2); border: 1px solid rgba(34,197,94,0.5); border-radius: 6px; padding: 12px; margin-top: 8px;">
                                <div style="color: #22c55e; font-weight: 600;">✅ ${{data.message}}</div>
                                <div style="color: #94a3b8; font-size: 0.85rem; margin-top: 8px;">
                                    <div><b>Pipe:</b> ${{data.pipe_name}}</div>
                                    <div><b>Target:</b> ${{data.target_table}}</div>
                                    ${{data.notification_channel ? `<div style="margin-top: 8px;"><b>SQS Queue:</b> <span style="font-family: monospace; font-size: 0.75rem; word-break: break-all;">${{data.notification_channel}}</span></div>` : ''}}
                                </div>
                                <div style="margin-top: 12px;">
                                    <button onclick="refreshPipe('${{data.pipe_name}}')" class="btn-secondary" style="padding: 6px 12px; font-size: 0.8rem;">
                                        Load Existing Files Now
                                    </button>
                                </div>
                            </div>
                        `;
                    }} else {{
                        throw new Error(data.detail || 'Failed to create pipe');
                    }}
                }} catch (e) {{
                    resultDiv.innerHTML = `<div style="color: #ef4444;">Error: ${{e.message}}</div>`;
                }}
            }}
            
            async function refreshPipe(pipeName) {{
                try {{
                    const resp = await fetch(`/api/pipes/refresh/${{encodeURIComponent(pipeName)}}`, {{ method: 'POST' }});
                    const data = await resp.json();
                    if (data.success) {{
                        alert(`✅ Refresh triggered for ${{data.files_sent}} file(s)!\\n\\nFiles will be ingested by Snowpipe shortly.`);
                    }} else {{
                        alert('❌ Failed to refresh: ' + (data.detail || 'Unknown error'));
                    }}
                }} catch (e) {{
                    alert('❌ Error: ' + e.message);
                }}
            }}
            
            async function checkPipeStatus(pipeName) {{
                const statusDiv = document.getElementById('pipe-status-' + pipeName.replace(/\\./g, '-'));
                statusDiv.style.display = 'block';
                statusDiv.innerHTML = '<div style="color: #38bdf8; font-size: 0.8rem; padding: 8px;">Loading status...</div>';
                
                try {{
                    const resp = await fetch(`/api/pipe/status/${{encodeURIComponent(pipeName)}}`);
                    const data = await resp.json();
                    
                    if (data.status) {{
                        const s = data.status;
                        statusDiv.innerHTML = `
                            <div style="background: rgba(15,23,42,0.8); border-radius: 6px; padding: 10px; font-size: 0.8rem;">
                                <div style="display: flex; flex-wrap: wrap; gap: 12px;">
                                    <div><span style="color: #64748b;">State:</span> <span style="color: ${{s.executionState === 'RUNNING' ? '#22c55e' : '#f59e0b'}};">${{s.executionState}}</span></div>
                                    <div><span style="color: #64748b;">Pending:</span> <span style="color: #e2e8f0;">${{s.pendingFileCount}} files</span></div>
                                    <div><span style="color: #64748b;">Last:</span> <span style="color: #e2e8f0;">${{s.lastIngestedTimestamp ? s.lastIngestedTimestamp.substring(0,19) : 'N/A'}}</span></div>
                                </div>
                                ${{s.lastIngestedFilePath ? `<div style="margin-top: 6px; color: #64748b;">Last file: <span style="font-family: monospace; color: #94a3b8;">${{s.lastIngestedFilePath}}</span></div>` : ''}}
                            </div>
                        `;
                    }} else {{
                        statusDiv.innerHTML = '<div style="color: #64748b; font-size: 0.8rem; padding: 8px;">No status available</div>';
                    }}
                }} catch (e) {{
                    statusDiv.innerHTML = `<div style="color: #ef4444; font-size: 0.8rem; padding: 8px;">Error: ${{e.message}}</div>`;
                }}
            }}
            </script>
            
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title">{get_material_icon('monitoring', '20px')} Snowpipe Streaming SDK Reference</div>
                <table class="sdk-limits-table">
                    <tr>
                        <th>Parameter</th>
                        <th>Classic SDK</th>
                        <th>High-Performance SDK</th>
                    </tr>
                    <tr>
                        <td>Max Throughput</td>
                        <td>Variable (connection-based)</td>
                        <td class="value">10 GB/s per table</td>
                    </tr>
                    <tr>
                        <td>Optimal Batch Size</td>
                        <td>10-16 MB</td>
                        <td class="value">10-16 MB</td>
                    </tr>
                    <tr>
                        <td>MAX_CLIENT_LAG Default</td>
                        <td>1 second</td>
                        <td>1 second (30s for Iceberg)</td>
                    </tr>
                    <tr>
                        <td>MAX_CLIENT_LAG Range</td>
                        <td>1 - 600 seconds</td>
                        <td>1 - 600 seconds</td>
                    </tr>
                    <tr>
                        <td>Channel Timeout</td>
                        <td>30 days inactive</td>
                        <td>30 days inactive</td>
                    </tr>
                    <tr>
                        <td>PIPE Object Required</td>
                        <td>No (direct table)</td>
                        <td class="value">Yes</td>
                    </tr>
                    <tr>
                        <td>Schema Validation</td>
                        <td>Client-side</td>
                        <td>Server-side</td>
                    </tr>
                    <tr>
                        <td>Billing Model</td>
                        <td>Compute + connections</td>
                        <td class="value">Throughput-based (per GB)</td>
                    </tr>
                </table>
            </div>
            
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title">{get_material_icon('tips_and_updates', '20px')} Best Practices</div>
                <ul style="color: #94a3b8; line-height: 1.8; padding-left: 20px;">
                    <li>Use <b>insertRows()</b> instead of multiple insertRow() calls for better efficiency</li>
                    <li>Keep batch sizes between <b>10-16 MB</b> for optimal performance</li>
                    <li>Set <b>MAX_CLIENT_LAG</b> as high as latency requirements allow to reduce partition fragmentation</li>
                    <li>Use <b>OnErrorOption.CONTINUE</b> and manually check return values for better performance</li>
                    <li>Channels should be <b>long-lived</b> - don't close after each batch</li>
                    <li>For Iceberg tables, use <b>30 second</b> default MAX_CLIENT_LAG for better Parquet files</li>
                </ul>
            </div>
            
        </div>
        
        <!--  Smart refresh indicator - shows countdown, doesn't reset page state -->
        <div class="refresh-indicator" onclick="manualRefresh()" style="cursor: pointer;" title="Click to refresh now">
            <span class="material-symbols-outlined" style="font-size: 16px;">sync</span>
            <span>Auto-refresh in <span id="refresh-countdown" class="refresh-countdown">30s</span></span>
        </div>
    </body>
    </html>
    """


@app.get("/validate", response_class=HTMLResponse)
async def validate_page():
    databases = []
    try:
        if snowflake_session:
            result = snowflake_session.sql("SHOW DATABASES").collect()
            databases = [r['name'] for r in result if not r['name'].startswith('SNOWFLAKE')]
    except Exception:
        databases = [DB]
    
    db_options = "".join([f'<option value="{db}">{db}</option>' for db in databases])
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Validate - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('validate')}
            
            <div class="panel">
                <div class="panel-title">{get_material_icon('check_circle', '20px', '#22c55e')} Schema Validation</div>
                <p style="color: #94a3b8; margin-bottom: 20px;">
                    Compare generated data schema against your target Snowflake table to ensure compatibility.
                </p>
                
                <form action="/api/validate" method="post">
                    <div class="form-row">
                        <div class="form-group">
                            <label class="form-label">Database</label>
                            <select name="database">
                                {db_options}
                            </select>
                        </div>
                        <div class="form-group">
                            <label class="form-label">Schema</label>
                            <input type="text" name="schema" value="PRODUCTION">
                        </div>
                        <div class="form-group">
                            <label class="form-label">Table</label>
                            <input type="text" name="table" value="AMI_INTERVAL_READINGS">
                        </div>
                    </div>
                    
                    <button type="submit" class="btn-primary" style="max-width: 300px;">{get_material_icon('search', '20px')} Validate Schema</button>
                </form>
            </div>
            
            <div class="panel" style="margin-top: 24px;">
                <div class="panel-title">{get_material_icon('table_chart', '20px')} Expected AMI Schema</div>
                <table class="sdk-limits-table">
                    <tr><th>Column</th><th>Type</th><th>Description</th></tr>
                    <tr><td>ENTITY_ID</td><td>NUMBER(38,0)</td><td>Auto-increment primary key</td></tr>
                    <tr><td>READING_ID</td><td>VARCHAR(50)</td><td>Unique reading identifier</td></tr>
                    <tr><td>METER_ID</td><td>VARCHAR(50)</td><td>Smart meter identifier</td></tr>
                    <tr><td>CUSTOMER_SEGMENT</td><td>VARCHAR(50)</td><td>RESIDENTIAL/COMMERCIAL/INDUSTRIAL</td></tr>
                    <tr><td>TIMESTAMP</td><td>TIMESTAMP_NTZ</td><td>Reading timestamp</td></tr>
                    <tr><td>USAGE_KWH</td><td>FLOAT</td><td>Energy consumption in kWh</td></tr>
                    <tr><td>VOLTAGE_READING</td><td>FLOAT</td><td>Voltage measurement</td></tr>
                    <tr><td>TEMPERATURE_CELSIUS</td><td>FLOAT</td><td>Ambient temperature</td></tr>
                    <tr><td>TRANSFORMER_ID</td><td>VARCHAR(50)</td><td>Associated transformer</td></tr>
                    <tr><td>SUBSTATION_ID</td><td>VARCHAR(50)</td><td>Associated substation</td></tr>
                </table>
            </div>
        </div>
    </body>
    </html>
    """


@app.get("/history", response_class=HTMLResponse)
async def history_page():
    history_rows = []
    try:
        if snowflake_session:
            result = snowflake_session.sql(f"""
                SELECT JOB_ID, CREATED_AT, MODE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                       METERS, DAYS, ROWS_GENERATED, DURATION_SECONDS, STATUS
                FROM {DB}.{SCHEMA_APPLICATIONS}.FLUX_GENERATION_HISTORY
                ORDER BY CREATED_AT DESC
                LIMIT 50
            """).collect()
            history_rows = result
    except Exception:
        pass
    
    if history_rows:
        table_rows = ""
        for row in history_rows:
            status_color = "#22c55e" if row['STATUS'] == 'SUCCESS' else "#ef4444"
            table_rows += f"""
            <tr>
                <td>{row['JOB_ID']}</td>
                <td>{row['CREATED_AT']}</td>
                <td>{row['MODE']}</td>
                <td>{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['TABLE_NAME']}</td>
                <td>{row['METERS']:,}</td>
                <td>{format_number(row['ROWS_GENERATED'] or 0)}</td>
                <td>{row['DURATION_SECONDS']:.1f}s</td>
                <td style="color: {status_color};">{row['STATUS']}</td>
            </tr>
            """
        
        history_content = f"""
        <table class="sdk-limits-table">
            <tr>
                <th>Job ID</th>
                <th>Created</th>
                <th>Mode</th>
                <th>Target</th>
                <th>Meters</th>
                <th>Rows</th>
                <th>Duration</th>
                <th>Status</th>
            </tr>
            {table_rows}
        </table>
        """
    else:
        history_content = """
        <p style="color: #64748b; text-align: center; padding: 40px;">
            No generation history available. Complete a data generation job to see history.
        </p>
        """
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>History - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('history')}
            
            <div class="panel">
                <div class="panel-title">{get_material_icon('history', '20px')} Generation History</div>
                {history_content}
            </div>
        </div>
    </body>
    </html>
    """


@app.get("/openflow", response_class=HTMLResponse)
async def openflow_page():
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Openflow Designer - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
        <style>
            .of-badge {{
                display: inline-block;
                background: rgba(245,158,11,0.15);
                color: #f59e0b;
                font-size: 0.75rem;
                font-weight: 600;
                padding: 3px 10px;
                border-radius: 20px;
            }}
            .of-section-header {{
                display: flex;
                align-items: center;
                gap: 12px;
                margin-bottom: 16px;
                font-size: 1.15rem;
                font-weight: 600;
                color: #f1f5f9;
            }}
            .of-section-num {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 32px;
                height: 32px;
                border-radius: 50%;
                background: rgba(245,158,11,0.15);
                color: #f59e0b;
                font-size: 0.9rem;
                font-weight: 700;
                flex-shrink: 0;
            }}
            .of-template-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                gap: 14px;
                margin-bottom: 24px;
            }}
            .of-template-card {{
                background: rgba(30,41,59,0.7);
                border: 1px solid rgba(148,163,184,0.1);
                border-radius: 10px;
                padding: 18px;
                cursor: pointer;
                transition: all 0.2s;
                position: relative;
            }}
            .of-template-card:hover {{
                border-color: rgba(245,158,11,0.4);
                background: rgba(30,41,59,0.9);
                transform: translateY(-1px);
            }}
            .of-template-card.active {{
                border-color: #f59e0b;
                background: rgba(245,158,11,0.08);
                box-shadow: 0 0 0 1px rgba(245,158,11,0.3);
            }}
            .of-card-icon {{
                font-size: 1.6rem;
                margin-bottom: 8px;
            }}
            .of-card-title {{
                font-size: 1rem;
                font-weight: 600;
                color: #f1f5f9;
                margin-bottom: 4px;
            }}
            .of-card-desc {{
                font-size: 0.85rem;
                color: #94a3b8;
                line-height: 1.4;
                margin-bottom: 10px;
            }}
            .of-card-processors {{
                display: flex;
                flex-wrap: wrap;
                gap: 4px;
            }}
            .of-proc-tag {{
                font-size: 0.7rem;
                padding: 2px 7px;
                border-radius: 4px;
                background: rgba(148,163,184,0.1);
                color: #94a3b8;
                font-family: 'SF Mono', 'Fira Code', monospace;
            }}
            .of-connector-badge {{
                position: absolute;
                top: 10px;
                right: 10px;
                font-size: 0.65rem;
                padding: 2px 8px;
                border-radius: 10px;
                background: rgba(245,158,11,0.15);
                color: #f59e0b;
                font-weight: 600;
            }}
            .of-step-section {{
                margin-top: 24px;
                padding: 24px;
                border-radius: 12px;
                background: rgba(15,23,42,0.5);
                border: 1px solid rgba(148,163,184,0.08);
            }}
            .of-step-locked {{
                opacity: 0.35;
                pointer-events: none;
                position: relative;
            }}
            .of-lock-msg {{
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                color: #64748b;
                font-size: 0.9rem;
                text-align: center;
                z-index: 2;
            }}
            .of-selected-summary {{
                margin-top: 12px;
                padding: 12px 16px;
                border-radius: 8px;
                background: rgba(245,158,11,0.06);
                border: 1px solid rgba(245,158,11,0.15);
                color: #f59e0b;
                font-size: 0.9rem;
                display: none;
            }}
            .of-toggle-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
                gap: 12px;
                margin-top: 12px;
            }}
            .of-toggle-card {{
                display: flex;
                align-items: flex-start;
                gap: 12px;
                padding: 14px;
                border-radius: 8px;
                background: rgba(30,41,59,0.5);
                border: 1px solid rgba(148,163,184,0.08);
                cursor: pointer;
                transition: all 0.2s;
            }}
            .of-toggle-card:hover {{
                border-color: rgba(245,158,11,0.3);
            }}
            .of-toggle-card.active {{
                border-color: #f59e0b;
                background: rgba(245,158,11,0.06);
            }}
            .of-toggle-cb {{
                width: 18px;
                height: 18px;
                border-radius: 4px;
                border: 2px solid rgba(148,163,184,0.3);
                flex-shrink: 0;
                margin-top: 2px;
                display: flex;
                align-items: center;
                justify-content: center;
                transition: all 0.15s;
            }}
            .of-toggle-card.active .of-toggle-cb {{
                background: #f59e0b;
                border-color: #f59e0b;
            }}
            .of-toggle-title {{
                font-size: 0.9rem;
                font-weight: 600;
                color: #e2e8f0;
                margin-bottom: 2px;
            }}
            .of-toggle-desc {{
                font-size: 0.8rem;
                color: #94a3b8;
                line-height: 1.3;
            }}
            .of-dest-form {{
                display: grid;
                grid-template-columns: 1fr 1fr 1fr;
                gap: 14px;
                margin-top: 12px;
            }}
            .of-dest-form label {{
                display: block;
                color: #94a3b8;
                font-size: 0.82rem;
                margin-bottom: 4px;
            }}
            .of-dest-form select, .of-dest-form input {{
                width: 100%;
                padding: 9px 12px;
                border-radius: 6px;
                border: 1px solid rgba(148,163,184,0.15);
                background: rgba(15,23,42,0.6);
                color: #e2e8f0;
                font-size: 0.9rem;
            }}
            .of-dest-row2 {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 14px;
                margin-top: 14px;
            }}
            .of-validate-btn {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 10px 20px;
                border: none;
                border-radius: 8px;
                background: linear-gradient(135deg, #f59e0b, #d97706);
                color: #fff;
                font-weight: 600;
                font-size: 0.9rem;
                cursor: pointer;
                margin-top: 16px;
                transition: all 0.2s;
            }}
            .of-validate-btn:hover {{
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(245,158,11,0.3);
            }}
            .of-layout {{
                display: grid;
                grid-template-columns: 1fr 260px;
                gap: 20px;
                align-items: start;
            }}
            @media (max-width: 900px) {{
                .of-layout {{ grid-template-columns: 1fr; }}
                .of-sidebar {{ display: none; }}
            }}
            .of-sidebar {{
                position: sticky;
                top: 24px;
                background: rgba(15,23,42,0.6);
                border: 1px solid rgba(148,163,184,0.08);
                border-radius: 12px;
                padding: 20px;
            }}
            .of-sidebar-title {{
                font-size: 0.85rem;
                font-weight: 600;
                color: #94a3b8;
                margin-bottom: 16px;
                text-transform: uppercase;
                letter-spacing: 0.05em;
            }}
            .of-progress-step {{
                display: flex;
                align-items: flex-start;
                gap: 10px;
                margin-bottom: 14px;
                cursor: pointer;
            }}
            .of-progress-step:last-child {{ margin-bottom: 0; }}
            .of-progress-dot {{
                width: 24px;
                height: 24px;
                border-radius: 50%;
                border: 2px solid rgba(148,163,184,0.2);
                display: flex;
                align-items: center;
                justify-content: center;
                flex-shrink: 0;
                font-size: 0.7rem;
                font-weight: 700;
                color: #64748b;
                transition: all 0.2s;
            }}
            .of-progress-step.active .of-progress-dot {{
                border-color: #f59e0b;
                color: #f59e0b;
                background: rgba(245,158,11,0.1);
            }}
            .of-progress-step.done .of-progress-dot {{
                border-color: #22c55e;
                color: #22c55e;
                background: rgba(34,197,94,0.1);
            }}
            .of-progress-label {{
                font-size: 0.82rem;
                color: #64748b;
                line-height: 1.3;
                padding-top: 3px;
            }}
            .of-progress-step.active .of-progress-label {{
                color: #f1f5f9;
                font-weight: 600;
            }}
            .of-progress-step.done .of-progress-label {{
                color: #94a3b8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('openflow')}

            <div class="of-layout">
            <div class="of-main">

            <!-- Step 1: Flow Template -->
            <div class="of-step-section" id="of-step-1">
                <div class="of-section-header">
                    <span class="of-section-num">1</span>
                    Flow Template
                </div>
                <p style="color: #94a3b8; font-size: 0.9rem; margin: 0 0 16px 0;">
                    Choose a pre-built pattern or start from scratch. Each template configures the right processors and controller services automatically.
                </p>

                <div class="of-template-grid">
                    <!-- Kafka / EventHub -->
                    <div class="of-template-card" onclick="ofSelectTemplate('kafka')" id="of-tpl-kafka">
                        <span class="of-connector-badge">Connector</span>
                        <div class="of-card-icon">{get_material_icon('stream', '28px')}</div>
                        <div class="of-card-title">Kafka / EventHub Ingest</div>
                        <div class="of-card-desc">Stream messages from Kafka or Azure EventHub into Snowflake via Snowpipe Streaming.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">ConsumeKafka</span>
                            <span class="of-proc-tag">ConvertRecord</span>
                            <span class="of-proc-tag">PutSnowpipeStreaming</span>
                        </div>
                    </div>

                    <!-- REST API -->
                    <div class="of-template-card" onclick="ofSelectTemplate('rest')" id="of-tpl-rest">
                        <div class="of-card-icon">{get_material_icon('api', '28px')}</div>
                        <div class="of-card-title">REST API Ingest</div>
                        <div class="of-card-desc">Poll or paginate a REST API and land JSON responses into Snowflake tables.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">InvokeHTTP</span>
                            <span class="of-proc-tag">SplitJson</span>
                            <span class="of-proc-tag">PutSnowpipeStreaming</span>
                        </div>
                    </div>

                    <!-- Database CDC -->
                    <div class="of-template-card" onclick="ofSelectTemplate('cdc')" id="of-tpl-cdc">
                        <span class="of-connector-badge">Connector</span>
                        <div class="of-card-icon">{get_material_icon('sync_alt', '28px')}</div>
                        <div class="of-card-title">Database CDC</div>
                        <div class="of-card-desc">Capture changes from PostgreSQL, MySQL, or SQL Server and replicate into Snowflake.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">CDC Source</span>
                            <span class="of-proc-tag">ConvertRecord</span>
                            <span class="of-proc-tag">UpdateSnowflakeDatabase</span>
                        </div>
                    </div>

                    <!-- File Processing -->
                    <div class="of-template-card" onclick="ofSelectTemplate('files')" id="of-tpl-files">
                        <div class="of-card-icon">{get_material_icon('folder_open', '28px')}</div>
                        <div class="of-card-title">File Processing</div>
                        <div class="of-card-desc">List and fetch files from S3, GCS, or Azure Blob, convert formats, and load into Snowflake.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">ListS3 / ListGCS</span>
                            <span class="of-proc-tag">FetchObject</span>
                            <span class="of-proc-tag">PutSnowpipeStreaming</span>
                        </div>
                    </div>

                    <!-- Data Generation -->
                    <div class="of-template-card" onclick="ofSelectTemplate('datagen')" id="of-tpl-datagen">
                        <div class="of-card-icon">{get_material_icon('science', '28px')}</div>
                        <div class="of-card-title">Data Generation</div>
                        <div class="of-card-desc">Generate synthetic data with DataFaker expressions for testing and development pipelines.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">GenerateJSON</span>
                            <span class="of-proc-tag">MergeRecord</span>
                            <span class="of-proc-tag">PutSnowpipeStreaming</span>
                        </div>
                    </div>

                    <!-- Custom Flow -->
                    <div class="of-template-card" onclick="ofSelectTemplate('custom')" id="of-tpl-custom">
                        <div class="of-card-icon">{get_material_icon('draw', '28px')}</div>
                        <div class="of-card-title">Custom Flow</div>
                        <div class="of-card-desc">Start from a blank canvas and pick your own processors. Full control over every component.</div>
                        <div class="of-card-processors">
                            <span class="of-proc-tag">You choose</span>
                        </div>
                    </div>
                </div>

                <div class="of-selected-summary" id="of-tpl-summary">
                    Template selected: <strong id="of-tpl-name"></strong>
                </div>
            </div>

            <!-- Step 2: Source Configuration (locked) -->
            <div class="of-step-section of-step-locked" id="of-step-2">
                <div class="of-lock-msg">{get_material_icon('lock', '20px')} Select a template above to configure the source</div>
                <div class="of-section-header">
                    <span class="of-section-num">2</span>
                    Source Configuration
                </div>
                <div id="of-source-panel">
                    <p style="color: #64748b; font-size: 0.9rem;">Source configuration will appear here based on your template selection.</p>
                </div>
            </div>

            <!-- Step 3: Transformation (locked) -->
            <div class="of-step-section of-step-locked" id="of-step-3">
                <div class="of-lock-msg">{get_material_icon('lock', '20px')} Complete source configuration first</div>
                <div class="of-section-header">
                    <span class="of-section-num">3</span>
                    Transformation
                </div>
                <p style="color: #94a3b8; font-size: 0.85rem; margin: 0 0 4px;">
                    Select optional transformation processors to add between source and destination.
                </p>
                <div class="of-toggle-grid">
                    <div class="of-toggle-card" onclick="ofToggleTransform(this, 'convert')" data-transform="convert">
                        <div class="of-toggle-cb"></div>
                        <div>
                            <div class="of-toggle-title">{get_material_icon('swap_horiz', '16px')} Format Conversion</div>
                            <div class="of-toggle-desc">ConvertRecord &mdash; transform between JSON, Avro, CSV, and Parquet formats.</div>
                        </div>
                    </div>
                    <div class="of-toggle-card" onclick="ofToggleTransform(this, 'jolt')" data-transform="jolt">
                        <div class="of-toggle-cb"></div>
                        <div>
                            <div class="of-toggle-title">{get_material_icon('schema', '16px')} Field Mapping (JOLT)</div>
                            <div class="of-toggle-desc">JoltTransformJSON &mdash; rename, restructure, or flatten nested JSON fields.</div>
                        </div>
                    </div>
                    <div class="of-toggle-card" onclick="ofToggleTransform(this, 'filter')" data-transform="filter">
                        <div class="of-toggle-cb"></div>
                        <div>
                            <div class="of-toggle-title">{get_material_icon('filter_alt', '16px')} Filtering</div>
                            <div class="of-toggle-desc">RouteOnAttribute &mdash; keep or discard records based on field values or expressions.</div>
                        </div>
                    </div>
                    <div class="of-toggle-card" onclick="ofToggleTransform(this, 'enrich')" data-transform="enrich">
                        <div class="of-toggle-cb"></div>
                        <div>
                            <div class="of-toggle-title">{get_material_icon('add_circle', '16px')} Enrichment</div>
                            <div class="of-toggle-desc">UpdateAttribute &mdash; add computed fields, timestamps, or static metadata columns.</div>
                        </div>
                    </div>
                </div>
                <div id="of-transform-detail" style="margin-top:14px;"></div>
            </div>

            <!-- Step 4: Snowflake Destination (locked) -->
            <div class="of-step-section of-step-locked" id="of-step-4">
                <div class="of-lock-msg">{get_material_icon('lock', '20px')} Complete source configuration first</div>
                <div class="of-section-header">
                    <span class="of-section-num">4</span>
                    Snowflake Destination
                </div>
                <p style="color: #94a3b8; font-size: 0.85rem; margin: 0 0 4px;">
                    Select the target database, schema, and table for incoming data.
                </p>
                <div class="of-dest-form">
                    <div>
                        <label>Database</label>
                        <select id="of_dest_db" onchange="ofLoadSchemas()">
                            <option value="">Loading...</option>
                        </select>
                    </div>
                    <div>
                        <label>Schema</label>
                        <select id="of_dest_schema" onchange="ofLoadTables()">
                            <option value="">Select database first</option>
                        </select>
                    </div>
                    <div>
                        <label>Table</label>
                        <select id="of_dest_table">
                            <option value="">Select schema first</option>
                        </select>
                    </div>
                </div>
                <div class="of-dest-row2">
                    <div>
                        <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">Write Method</label>
                        <select id="of_dest_method" style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                            <option value="snowpipe_streaming">PutSnowpipeStreaming (low latency)</option>
                            <option value="update_db">UpdateSnowflakeDatabase (merge/upsert)</option>
                        </select>
                    </div>
                    <div>
                        <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">New Table Name (optional)</label>
                        <input id="of_dest_new_table" type="text" placeholder="Leave blank to use existing table"
                            style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                    </div>
                </div>
                <button class="of-validate-btn" onclick="ofValidateDestination()">
                    {get_material_icon('check_circle', '18px')} Validate &amp; Continue
                </button>
            </div>

            <!-- Step 5: Network & Auth (locked) -->
            <div class="of-step-section of-step-locked" id="of-step-5">
                <div class="of-lock-msg">{get_material_icon('lock', '20px')} Complete destination configuration first</div>
                <div class="of-section-header">
                    <span class="of-section-num">5</span>
                    Network &amp; Auth
                </div>
                <p style="color: #94a3b8; font-size: 0.85rem; margin: 0 0 12px;">
                    Configure External Access Integration (EAI) so Openflow on SPCS can reach external sources.
                </p>
                <div style="margin-bottom:14px;">
                    <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">EAI Name</label>
                    <input id="of_eai_name" type="text" placeholder="OPENFLOW_EAI" value="OPENFLOW_EAI"
                        style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                </div>
                <div style="margin-bottom:14px;">
                    <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">Allowed Hosts (comma-separated)</label>
                    <input id="of_eai_hosts" type="text" placeholder="broker1:9092, api.example.com"
                        style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                    <p style="color:#64748b;font-size:0.78rem;margin:4px 0 0;">Required for SPCS to connect to external Kafka, REST APIs, or databases.</p>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
                    <div>
                        <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">Network Rule Name</label>
                        <input id="of_net_rule" type="text" placeholder="OPENFLOW_NETWORK_RULE" value="OPENFLOW_NETWORK_RULE"
                            style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                    </div>
                    <div>
                        <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">Secret Name (optional)</label>
                        <input id="of_secret_name" type="text" placeholder="OPENFLOW_SECRET"
                            style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                    </div>
                </div>
            </div>

            <!-- Step 6: Review & Generate (locked) -->
            <div class="of-step-section of-step-locked" id="of-step-6">
                <div class="of-lock-msg">{get_material_icon('lock', '20px')} Complete all previous steps first</div>
                <div class="of-section-header">
                    <span class="of-section-num">6</span>
                    Review &amp; Generate
                </div>
                <p style="color: #94a3b8; font-size: 0.85rem; margin: 0 0 12px;">
                    Review your pipeline configuration and generate deployment SQL.
                </p>

                <!-- Pipeline visualization -->
                <div id="of-pipeline-viz" style="padding:16px;border-radius:8px;background:rgba(15,23,42,0.5);border:1px solid rgba(148,163,184,0.08);margin-bottom:16px;">
                    <div style="color:#94a3b8;font-size:0.82rem;margin-bottom:8px;font-weight:600;">Pipeline Flow</div>
                    <div id="of-pipeline-flow" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:0.85rem;"></div>
                </div>

                <!-- Config summary -->
                <div id="of-config-summary" style="padding:16px;border-radius:8px;background:rgba(15,23,42,0.5);border:1px solid rgba(148,163,184,0.08);margin-bottom:16px;">
                    <div style="color:#94a3b8;font-size:0.82rem;margin-bottom:8px;font-weight:600;">Configuration Summary</div>
                    <div id="of-summary-body" style="font-size:0.85rem;color:#e2e8f0;"></div>
                </div>

                <button class="of-validate-btn" onclick="ofGenerate()" style="font-size:1rem;padding:12px 28px;">
                    {get_material_icon('code', '20px')} Generate Deployment SQL
                </button>

                <!-- Generated SQL output -->
                <div id="of-sql-output" style="display:none;margin-top:16px;">
                    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                        <span style="color:#94a3b8;font-size:0.82rem;font-weight:600;">Generated SQL</span>
                        <button onclick="ofCopySql()" style="padding:5px 12px;border:1px solid rgba(148,163,184,0.15);border-radius:6px;background:rgba(30,41,59,0.7);color:#94a3b8;font-size:0.8rem;cursor:pointer;">Copy</button>
                    </div>
                    <pre id="of-sql-pre" style="padding:16px;border-radius:8px;background:rgba(15,23,42,0.8);border:1px solid rgba(148,163,184,0.08);color:#e2e8f0;font-size:0.82rem;font-family:'SF Mono','Fira Code',monospace;overflow-x:auto;white-space:pre-wrap;max-height:500px;overflow-y:auto;"></pre>
                </div>
            </div>
            </div><!-- /of-main -->

            <!-- Sidebar: Wizard Progress -->
            <div class="of-sidebar">
                <div class="of-sidebar-title">Wizard Progress</div>
                <div class="of-progress-step active" id="of-prog-1" onclick="document.getElementById('of-step-1')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">1</div>
                    <div class="of-progress-label">Flow Template</div>
                </div>
                <div class="of-progress-step" id="of-prog-2" onclick="document.getElementById('of-step-2')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">2</div>
                    <div class="of-progress-label">Source Config</div>
                </div>
                <div class="of-progress-step" id="of-prog-3" onclick="document.getElementById('of-step-3')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">3</div>
                    <div class="of-progress-label">Transformation</div>
                </div>
                <div class="of-progress-step" id="of-prog-4" onclick="document.getElementById('of-step-4')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">4</div>
                    <div class="of-progress-label">Destination</div>
                </div>
                <div class="of-progress-step" id="of-prog-5" onclick="document.getElementById('of-step-5')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">5</div>
                    <div class="of-progress-label">Network &amp; Auth</div>
                </div>
                <div class="of-progress-step" id="of-prog-6" onclick="document.getElementById('of-step-6')?.scrollIntoView({{behavior:'smooth',block:'start'}})">
                    <div class="of-progress-dot">6</div>
                    <div class="of-progress-label">Review &amp; Generate</div>
                </div>
            </div>

            </div><!-- /of-layout -->
        </div>

        <script>
        // ── Openflow Wizard State ──
        const ofState = {{
            template: null,
            sourceValid: false,
        }};

        const ofTemplateLabels = {{
            kafka: 'Kafka / EventHub Ingest',
            rest: 'REST API Ingest',
            cdc: 'Database CDC',
            files: 'File Processing',
            datagen: 'Data Generation',
            custom: 'Custom Flow',
        }};

        // ── Form field helper ──
        function ofField(id, label, placeholder, type='text', opts) {{
            if (type === 'select' && opts) {{
                const options = opts.map(o => `<option value="${{o[0]}}">${{o[1]}}</option>`).join('');
                return `<div style="margin-bottom:14px;">
                    <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">${{label}}</label>
                    <select id="${{id}}" style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
                        ${{options}}
                    </select>
                </div>`;
            }}
            if (type === 'textarea') {{
                return `<div style="margin-bottom:14px;">
                    <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">${{label}}</label>
                    <textarea id="${{id}}" placeholder="${{placeholder}}" rows="6"
                        style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.85rem;font-family:'SF Mono','Fira Code',monospace;resize:vertical;">${{placeholder}}</textarea>
                </div>`;
            }}
            return `<div style="margin-bottom:14px;">
                <label style="display:block;color:#94a3b8;font-size:0.82rem;margin-bottom:4px;">${{label}}</label>
                <input id="${{id}}" type="${{type}}" placeholder="${{placeholder}}"
                    style="width:100%;padding:9px 12px;border-radius:6px;border:1px solid rgba(148,163,184,0.15);background:rgba(15,23,42,0.6);color:#e2e8f0;font-size:0.9rem;">
            </div>`;
        }}

        // ── Source panels per template ──
        function ofGetSourcePanel(tpl) {{
            const F = ofField;
            const row2 = (a,b) => `<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${{a}}${{b}}</div>`;
            const validateBtn = `<button class="of-validate-btn" onclick="ofValidateSource()">Validate Source &amp; Continue</button>`;

            switch (tpl) {{
                case 'kafka':
                    return `<p style="color:#94a3b8;font-size:0.85rem;margin:0 0 16px;">Configure your Kafka or Azure EventHub source connection.</p>`
                        + F('of_kafka_bootstrap', 'Bootstrap Servers', 'broker1:9092, broker2:9092')
                        + F('of_kafka_topic', 'Topic Name', 'my-events-topic')
                        + row2(
                            F('of_kafka_group', 'Consumer Group', 'openflow-consumer-1'),
                            F('of_kafka_auth', 'Authentication', '', 'select', [['none','None'],['sasl_plain','SASL/PLAIN'],['sasl_scram','SASL/SCRAM-256'],['mtls','mTLS']])
                        )
                        + `<div id="of_kafka_auth_fields"></div>`
                        + F('of_kafka_schema_reg', 'Schema Registry URL (optional)', 'https://schema-registry:8081')
                        + validateBtn;

                case 'rest':
                    return `<p style="color:#94a3b8;font-size:0.85rem;margin:0 0 16px;">Configure <strong>InvokeHTTP</strong> to poll a REST API endpoint. Responses are routed through SplitJson if the API returns arrays.</p>`
                        + F('of_rest_url', 'API URL', 'https://api.example.com/v1/data')
                        + row2(
                            F('of_rest_method', 'HTTP Method', '', 'select', [['GET','GET'],['POST','POST']]),
                            F('of_rest_auth', 'Authentication', '', 'select', [['none','None'],['bearer','Bearer Token'],['apikey','API Key'],['basic','Basic Auth'],['oauth2','OAuth2 Token Refresh']])
                        )
                        + `<div id="of_rest_auth_fields"></div>`
                        + F('of_rest_headers', 'Custom Headers (JSON)', '{{"Accept": "application/json"}}', 'textarea')
                        + row2(
                            F('of_rest_pagination', 'Pagination', '', 'select', [['none','None'],['offset','Offset/Limit'],['cursor','Cursor-based'],['link','Link Header']]),
                            F('of_rest_schedule', 'Poll Interval (seconds)', '300')
                        )
                        + `<p style="color:#64748b;font-size:0.78rem;margin:0 0 12px;">Tip: For rate-limited APIs, add a ControlRate processor in the Transforms step to throttle throughput.</p>`
                        + validateBtn;

                case 'cdc':
                    return `<p style="color:#94a3b8;font-size:0.85rem;margin:0 0 16px;">Configure a <strong>CDC Connector</strong> (postgresql/mysql/sqlserver) for database replication.</p>
                        <p style="color:#64748b;font-size:0.78rem;margin:0 0 12px;">Requires: replication enabled on source, JDBC driver uploaded, EAI for network access. See Snowflake docs for full prerequisites.</p>`
                        + F('of_cdc_dbtype', 'Database Type', '', 'select', [['postgres','PostgreSQL (flow: postgresql)'],['mysql','MySQL (flow: mysql)'],['sqlserver','SQL Server (flow: sqlserver)']])
                        + F('of_cdc_jdbc', 'JDBC Connection URL', 'jdbc:postgresql://host:5432/mydb')
                        + row2(
                            F('of_cdc_user', 'Username', 'replication_user'),
                            F('of_cdc_pass', 'Password', '', 'password')
                        )
                        + F('of_cdc_tables', 'Included Tables (comma-separated or regex)', 'public.users, public.orders')
                        + F('of_cdc_publication', 'Publication Name (PostgreSQL only)', 'my_publication')
                        + F('of_cdc_case', 'Object Identifier Resolution', '', 'select', [['CASE_SENSITIVE','CASE_SENSITIVE (preserve source casing, requires quoted identifiers)'],['CASE_INSENSITIVE','CASE_INSENSITIVE (uppercase all names, Snowflake convention)']])
                        + validateBtn;

                case 'files':
                    return `<p style="color:#94a3b8;font-size:0.85rem;margin:0 0 16px;">Configure cloud storage source using the <strong>List + Fetch</strong> pattern (ListS3/ListGCSBucket + FetchS3Object/FetchGCSObject).</p>
                        <p style="color:#64748b;font-size:0.78rem;margin:0 0 12px;">Files are listed, fetched, then parsed with ConvertRecord before loading to Snowflake. Check if a pre-built Connector exists before building custom.</p>`
                        + F('of_file_provider', 'Cloud Provider', '', 'select', [['s3','Amazon S3 (ListS3 + FetchS3Object)'],['gcs','Google Cloud Storage (ListGCSBucket + FetchGCSObject)'],['azure','Azure Blob Storage']])
                        + F('of_file_bucket', 'Bucket / Container', 'my-data-bucket')
                        + F('of_file_prefix', 'Prefix (optional)', 'raw/events/')
                        + row2(
                            F('of_file_format', 'File Format', '', 'select', [['json','JSON'],['csv','CSV'],['parquet','Parquet'],['avro','Avro']]),
                            F('of_file_strategy', 'After Processing', '', 'select', [['none','Track only (no delete)'],['delete','Delete processed files'],['move','Move to archive prefix']])
                        )
                        + validateBtn;

                case 'datagen':
                    return `<p style="color:#94a3b8;font-size:0.85rem;margin:0 0 16px;">Configure synthetic data generation using <strong>GenerateJSON</strong> with JSON Schema and DataFaker format fields.</p>
                        <p style="color:#64748b;font-size:0.78rem;margin:0 0 12px;">GenerateJSON uses standard JSON Schema with DataFaker expressions in the <code>format</code> field &mdash; NOT NiFi Expression Language <code>$&#123;...&#125;</code> syntax.</p>`
                        + F('of_dg_schema', 'JSON Schema (DataFaker format fields)', `{{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {{
    "id": {{"type": "string", "format": "Internet.uuid"}},
    "timestamp": {{"type": "string", "format": "TimeAndDate.past '30','DAYS','yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\''" }},
    "customer_id": {{"type": "string", "format": "regexify 'CUST[0-9]{{6}}'"}},
    "amount": {{"type": "number", "minimum": 10, "maximum": 9999}},
    "status": {{"type": "string", "enum": ["active", "pending", "cancelled"]}}
  }}
}}`, 'textarea')
                        + row2(
                            F('of_dg_batch', 'Batch Size', '100'),
                            F('of_dg_interval', 'Schedule (seconds)', '60')
                        )
                        + validateBtn;

                case 'custom':
                    return `<div style="padding:24px;text-align:center;border:1px dashed rgba(148,163,184,0.15);border-radius:8px;background:rgba(15,23,42,0.3);">
                        <p style="color:#94a3b8;font-size:0.9rem;margin:0 0 8px;">Custom flows are configured directly in the Openflow UI.</p>
                        <p style="color:#64748b;font-size:0.82rem;margin:0;">The wizard will generate the setup SQL (EAI, grants) and you add processors manually.</p>
                    </div>`;

                default:
                    return `<p style="color:#64748b;font-size:0.9rem;">Select a template to see source configuration.</p>`;
            }}
        }}

        function ofSelectTemplate(tpl) {{
            // Deselect all cards
            document.querySelectorAll('.of-template-card').forEach(c => c.classList.remove('active'));
            // Activate selected
            const card = document.getElementById('of-tpl-' + tpl);
            if (card) card.classList.add('active');

            ofState.template = tpl;

            // Show summary
            const summary = document.getElementById('of-tpl-summary');
            const nameEl = document.getElementById('of-tpl-name');
            if (summary && nameEl) {{
                nameEl.textContent = ofTemplateLabels[tpl] || tpl;
                summary.style.display = 'block';
            }}

            // Unlock Step 2 and inject source panel
            const step2 = document.getElementById('of-step-2');
            if (step2) {{
                step2.classList.remove('of-step-locked');
                const lockMsg = step2.querySelector('.of-lock-msg');
                if (lockMsg) lockMsg.style.display = 'none';
            }}
            const panel = document.getElementById('of-source-panel');
            if (panel) {{
                panel.innerHTML = ofGetSourcePanel(tpl);
            }}

            // For custom template, auto-unlock steps 3 & 4 since no source config needed
            if (tpl === 'custom') {{
                ofUnlockStep(3);
                ofUnlockStep(4);
                ofState.sourceValid = true;
                ofLoadDatabases();
            }} else {{
                ofState.sourceValid = false;
            }}

            // Scroll to step 2
            step2?.scrollIntoView({{ behavior: 'smooth', block: 'start' }});

            // Update sidebar progress
            ofUpdateProgress(2);
        }}

        function ofUnlockStep(n) {{
            const el = document.getElementById('of-step-' + n);
            if (el) {{
                el.classList.remove('of-step-locked');
                const lockMsg = el.querySelector('.of-lock-msg');
                if (lockMsg) lockMsg.style.display = 'none';
            }}
        }}

        // ── Transformation toggles ──
        function ofToggleTransform(card, key) {{
            card.classList.toggle('active');
            const isActive = card.classList.contains('active');
            if (isActive) {{
                if (!ofState.transforms) ofState.transforms = [];
                if (!ofState.transforms.includes(key)) ofState.transforms.push(key);
            }} else {{
                if (ofState.transforms) {{
                    ofState.transforms = ofState.transforms.filter(t => t !== key);
                }}
            }}
        }}

        // ── Source validation → unlock steps 3 & 4 ──
        function ofValidateSource() {{
            ofState.sourceValid = true;
            ofUnlockStep(3);
            ofUnlockStep(4);
            // Load databases for destination dropdown
            ofLoadDatabases();
            ofUpdateProgress(3);
            document.getElementById('of-step-3')?.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
        }}

        // ── Destination dropdown cascading ──
        async function ofLoadDatabases() {{
            try {{
                const resp = await fetch('/api/databases');
                const data = await resp.json();
                const sel = document.getElementById('of_dest_db');
                if (!sel) return;
                sel.innerHTML = '<option value="">-- Select Database --</option>';
                (data.databases || data || []).forEach(db => {{
                    const name = typeof db === 'string' ? db : (db.name || db.database_name || db);
                    sel.innerHTML += `<option value="${{name}}">${{name}}</option>`;
                }});
            }} catch(e) {{
                console.error('Failed to load databases', e);
            }}
        }}

        async function ofLoadSchemas() {{
            const db = document.getElementById('of_dest_db')?.value;
            const sel = document.getElementById('of_dest_schema');
            const tblSel = document.getElementById('of_dest_table');
            if (!sel) return;
            sel.innerHTML = '<option value="">Loading...</option>';
            if (tblSel) tblSel.innerHTML = '<option value="">Select schema first</option>';
            if (!db) {{ sel.innerHTML = '<option value="">Select database first</option>'; return; }}
            try {{
                const resp = await fetch(`/api/schemas/${{db}}`);
                const data = await resp.json();
                sel.innerHTML = '<option value="">-- Select Schema --</option>';
                (data.schemas || data || []).forEach(s => {{
                    const name = typeof s === 'string' ? s : (s.name || s.schema_name || s);
                    sel.innerHTML += `<option value="${{name}}">${{name}}</option>`;
                }});
            }} catch(e) {{
                console.error('Failed to load schemas', e);
            }}
        }}

        async function ofLoadTables() {{
            const db = document.getElementById('of_dest_db')?.value;
            const schema = document.getElementById('of_dest_schema')?.value;
            const sel = document.getElementById('of_dest_table');
            if (!sel) return;
            sel.innerHTML = '<option value="">Loading...</option>';
            if (!db || !schema) {{ sel.innerHTML = '<option value="">Select schema first</option>'; return; }}
            try {{
                const resp = await fetch(`/api/tables/${{db}}/${{schema}}`);
                const data = await resp.json();
                sel.innerHTML = '<option value="">(Create New Table)</option>';
                (data.tables || data || []).forEach(t => {{
                    const name = typeof t === 'string' ? t : (t.name || t.table_name || t);
                    sel.innerHTML += `<option value="${{name}}">${{name}}</option>`;
                }});
            }} catch(e) {{
                console.error('Failed to load tables', e);
            }}
        }}

        // ── Destination validation → unlock steps 5 & 6 ──
        function ofValidateDestination() {{
            const db = document.getElementById('of_dest_db')?.value;
            const schema = document.getElementById('of_dest_schema')?.value;
            const table = document.getElementById('of_dest_table')?.value;
            const newTable = document.getElementById('of_dest_new_table')?.value;

            if (!db || !schema) {{
                alert('Please select a database and schema.');
                return;
            }}
            if (!table && !newTable) {{
                alert('Please select an existing table or enter a new table name.');
                return;
            }}

            ofState.destDb = db;
            ofState.destSchema = schema;
            ofState.destTable = table || newTable;
            ofState.destMethod = document.getElementById('of_dest_method')?.value || 'snowpipe_streaming';

            ofUnlockStep(5);
            ofUnlockStep(6);
            ofUpdateProgress(5);
            document.getElementById('of-step-5')?.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
        }}

        // ── On page load: add validate button to step 2 for non-custom templates ──
        // (This is handled dynamically when source panel is injected)

        // ── Pipeline visualization + Summary (called when step 6 unlocks) ──
        function ofBuildPipelineViz() {{
            const flow = document.getElementById('of-pipeline-flow');
            const summaryBody = document.getElementById('of-summary-body');
            if (!flow || !summaryBody) return;

            // Build processor chain based on template + transforms
            const processors = [];
            const tpl = ofState.template;
            const arrow = `<span style="color:#64748b;">\u2192</span>`;
            const chip = (name, color) => `<span style="padding:4px 10px;border-radius:6px;background:rgba(${{color}},0.12);color:rgb(${{color}});font-family:'SF Mono','Fira Code',monospace;font-size:0.8rem;">${{name}}</span>`;

            // Source processor
            const srcMap = {{
                kafka: 'ConsumeKafka', rest: 'InvokeHTTP', cdc: 'CDC Connector',
                files: 'ListS3 + FetchS3Object', datagen: 'GenerateJSON', custom: 'Custom Source'
            }};
            processors.push(chip(srcMap[tpl] || 'Source', '245,158,11'));

            // Transform processors
            const tLabels = {{ convert: 'ConvertRecord', jolt: 'JoltTransformJSON', filter: 'RouteOnAttribute', enrich: 'UpdateAttribute' }};
            (ofState.transforms || []).forEach(t => {{
                processors.push(chip(tLabels[t] || t, '99,102,241'));
            }});

            // Destination processor
            const method = ofState.destMethod || 'snowpipe_streaming';
            const destProc = method === 'update_db' ? 'UpdateSnowflakeDatabase' : 'PutSnowpipeStreaming';
            processors.push(chip(destProc, '34,197,94'));

            flow.innerHTML = processors.join(arrow);

            // Build config summary
            const lines = [];
            lines.push(`<div style="margin-bottom:6px;"><strong>Template:</strong> ${{ofTemplateLabels[tpl] || tpl}}</div>`);
            lines.push(`<div style="margin-bottom:6px;"><strong>Destination:</strong> ${{ofState.destDb || '?'}}.${{ofState.destSchema || '?'}}.${{ofState.destTable || '?'}}</div>`);
            lines.push(`<div style="margin-bottom:6px;"><strong>Write Method:</strong> ${{destProc}}</div>`);
            if ((ofState.transforms || []).length > 0) {{
                lines.push(`<div style="margin-bottom:6px;"><strong>Transforms:</strong> ${{ofState.transforms.map(t => tLabels[t] || t).join(', ')}}</div>`);
            }}
            const eaiName = document.getElementById('of_eai_name')?.value || 'OPENFLOW_EAI';
            const eaiHosts = document.getElementById('of_eai_hosts')?.value || '';
            if (eaiHosts) {{
                lines.push(`<div style="margin-bottom:6px;"><strong>EAI:</strong> ${{eaiName}} &mdash; ${{eaiHosts}}</div>`);
            }}
            summaryBody.innerHTML = lines.join('');
        }}

        // Observer: build viz when step 6 becomes visible
        const step6Observer = new MutationObserver(() => {{
            const s6 = document.getElementById('of-step-6');
            if (s6 && !s6.classList.contains('of-step-locked')) {{
                ofBuildPipelineViz();
            }}
        }});
        const s6el = document.getElementById('of-step-6');
        if (s6el) step6Observer.observe(s6el, {{ attributes: true, attributeFilter: ['class'] }});

        // ── Generate SQL ──
        async function ofGenerate() {{
            ofBuildPipelineViz();
            const payload = {{
                template: ofState.template,
                dest_db: ofState.destDb,
                dest_schema: ofState.destSchema,
                dest_table: ofState.destTable,
                dest_method: ofState.destMethod || 'snowpipe_streaming',
                transforms: ofState.transforms || [],
                eai_name: document.getElementById('of_eai_name')?.value || 'OPENFLOW_EAI',
                eai_hosts: document.getElementById('of_eai_hosts')?.value || '',
                network_rule: document.getElementById('of_net_rule')?.value || 'OPENFLOW_NETWORK_RULE',
                secret_name: document.getElementById('of_secret_name')?.value || '',
            }};
            try {{
                const resp = await fetch('/api/openflow/generate', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(payload),
                }});
                const data = await resp.json();
                if (data.sql) {{
                    document.getElementById('of-sql-pre').textContent = data.sql;
                    document.getElementById('of-sql-output').style.display = 'block';
                }} else {{
                    alert(data.error || 'Failed to generate SQL');
                }}
            }} catch(e) {{
                alert('Error: ' + e.message);
            }}
        }}

        function ofCopySql() {{
            const sql = document.getElementById('of-sql-pre')?.textContent || '';
            navigator.clipboard.writeText(sql).then(() => {{
                const btn = event.target;
                btn.textContent = 'Copied!';
                setTimeout(() => btn.textContent = 'Copy', 2000);
            }});
        }}

        // ── Sidebar progress tracking ──
        function ofUpdateProgress(activeStep) {{
            for (let i = 1; i <= 6; i++) {{
                const prog = document.getElementById('of-prog-' + i);
                if (!prog) continue;
                prog.classList.remove('active', 'done');
                if (i < activeStep) {{
                    prog.classList.add('done');
                    prog.querySelector('.of-progress-dot').textContent = '\u2713';
                }} else if (i === activeStep) {{
                    prog.classList.add('active');
                    prog.querySelector('.of-progress-dot').textContent = i;
                }} else {{
                    prog.querySelector('.of-progress-dot').textContent = i;
                }}
            }}
        }}
        </script>
    </body>
    </html>
    """


@app.post("/api/openflow/generate")
async def openflow_generate(request: Request):
    """Generate deployment SQL for an Openflow pipeline based on wizard configuration."""
    try:
        body = await request.json()
        template = body.get("template", "custom")
        dest_db = body.get("dest_db", "MY_DB")
        dest_schema = body.get("dest_schema", "PUBLIC")
        dest_table = body.get("dest_table", "MY_TABLE")
        dest_method = body.get("dest_method", "snowpipe_streaming")
        transforms = body.get("transforms", [])
        eai_name = body.get("eai_name", "OPENFLOW_EAI")
        eai_hosts = body.get("eai_hosts", "")
        network_rule = body.get("network_rule", "OPENFLOW_NETWORK_RULE")
        secret_name = body.get("secret_name", "")
        runtime_role = body.get("runtime_role", "<OPENFLOW_RUNTIME_ROLE>")

        sql_parts = []
        sql_parts.append(f"-- =============================================================")
        sql_parts.append(f"-- Openflow Pipeline Deployment SQL")
        sql_parts.append(f"-- Template: {template}")
        sql_parts.append(f"-- Generated by FLUX Data Forge")
        sql_parts.append(f"-- =============================================================\n")

        # 1. Network rule + EAI (requires SECURITYADMIN per platform-eai.md)
        if eai_hosts.strip():
            # Validate and format host:port entries
            raw_hosts = [h.strip() for h in eai_hosts.split(",") if h.strip()]
            hosts_list = ", ".join(f"'{h}'" for h in raw_hosts)

            sql_parts.append(f"-- Step 1: Network Rule & External Access Integration")
            sql_parts.append(f"-- NOTE: Network rules and EAI require SECURITYADMIN role.")
            sql_parts.append(f"-- Ensure hosts use host:port format (e.g. broker1:9092, api.example.com:443).\n")
            sql_parts.append(f"USE ROLE SECURITYADMIN;\n")

            sql_parts.append(f"CREATE OR REPLACE NETWORK RULE {dest_db}.{dest_schema}.{network_rule}")
            sql_parts.append(f"  MODE = EGRESS")
            sql_parts.append(f"  TYPE = HOST_PORT")
            sql_parts.append(f"  VALUE_LIST = ({hosts_list});")
            sql_parts.append(f"")
            sql_parts.append(f"-- Verify network rule was created correctly:")
            sql_parts.append(f"DESCRIBE NETWORK RULE {dest_db}.{dest_schema}.{network_rule};\n")

        # 2. Secret (if provided)
        if secret_name.strip():
            sql_parts.append(f"-- Step 2: Create Secret (update credentials as needed)")
            sql_parts.append(f"CREATE OR REPLACE SECRET {dest_db}.{dest_schema}.{secret_name}")
            sql_parts.append(f"  TYPE = GENERIC_STRING")
            sql_parts.append(f"  SECRET_STRING = '<UPDATE_WITH_ACTUAL_SECRET>';\n")

        # 3. External Access Integration
        if eai_hosts.strip():
            eai_secrets_clause = ""
            if secret_name.strip():
                eai_secrets_clause = f"\n  ALLOWED_AUTHENTICATION_SECRETS = ({dest_db}.{dest_schema}.{secret_name})"
            sql_parts.append(f"CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {eai_name}")
            sql_parts.append(f"  ALLOWED_NETWORK_RULES = ({dest_db}.{dest_schema}.{network_rule}){eai_secrets_clause}")
            sql_parts.append(f"  ENABLED = TRUE")
            sql_parts.append(f"  COMMENT = 'Openflow EAI for {template} pipeline - generated by FLUX Data Forge';")
            sql_parts.append(f"")
            sql_parts.append(f"-- Verify EAI was created correctly:")
            sql_parts.append(f"DESCRIBE EXTERNAL ACCESS INTEGRATION {eai_name};")
            sql_parts.append(f"")
            sql_parts.append(f"-- Grant EAI usage to Openflow runtime role:")
            sql_parts.append(f"GRANT USAGE ON INTEGRATION {eai_name} TO ROLE {runtime_role};\n")
            sql_parts.append(f"-- IMPORTANT: After running these statements, attach the EAI to")
            sql_parts.append(f"-- the Openflow runtime via the Snowflake UI (Openflow > Settings).\n")

        # 4. Destination table & grants (switch back to a role with DDL privileges)
        sql_parts.append(f"-- Step 3: Create destination table and grant permissions")
        sql_parts.append(f"-- Switch to a role with CREATE TABLE privileges on the target schema.\n")
        sql_parts.append(f"USE ROLE SYSADMIN;  -- Or your preferred DDL role\n")
        sql_parts.append(f"CREATE TABLE IF NOT EXISTS {dest_db}.{dest_schema}.{dest_table} (")
        sql_parts.append(f"  RECORD_CONTENT VARIANT,")
        sql_parts.append(f"  RECORD_METADATA VARIANT,")
        sql_parts.append(f"  _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
        sql_parts.append(f");\n")

        # 5. Grants — per author-snowflake-destination.md
        sql_parts.append(f"-- Step 4: Grant permissions to Openflow runtime role")
        sql_parts.append(f"-- These are the minimum privileges required per Snowflake documentation.\n")
        sql_parts.append(f"GRANT USAGE ON DATABASE {dest_db} TO ROLE {runtime_role};")
        sql_parts.append(f"GRANT USAGE ON SCHEMA {dest_db}.{dest_schema} TO ROLE {runtime_role};")
        sql_parts.append(f"GRANT CREATE TABLE ON SCHEMA {dest_db}.{dest_schema} TO ROLE {runtime_role};")
        sql_parts.append(f"GRANT INSERT, SELECT ON TABLE {dest_db}.{dest_schema}.{dest_table} TO ROLE {runtime_role};")
        sql_parts.append("")

        # 6. Processor config hints as comments
        src_map = {
            "kafka": "ConsumeKafka_2_6 -> bootstrap.servers, topic, group.id, security.protocol",
            "rest": "InvokeHTTP -> Remote URL, HTTP Method, Request Headers",
            "cdc": "CDC Connector (postgresql/mysql/sqlserver) -> connection URL, credentials, table.include.list",
            "files": "ListS3 + FetchS3Object -> Bucket, Prefix, Region (or ListGCSBucket + FetchGCSObject)",
            "datagen": "GenerateJSON -> JSON Schema with DataFaker format fields, Batch Size, Schedule",
            "custom": "Configure processors manually in the Openflow UI",
        }
        sql_parts.append(f"-- =============================================================")
        sql_parts.append(f"-- Openflow Processor Configuration Reference")
        sql_parts.append(f"-- =============================================================")
        sql_parts.append(f"-- Source: {src_map.get(template, 'Custom')}")

        if transforms:
            transform_map = {
                "convert": "ConvertRecord (JsonTreeReader -> JsonRecordSetWriter or AvroRecordSetWriter)",
                "jolt": "JoltTransformJSON (shift/default/remove specs)",
                "filter": "RouteOnAttribute (expression-based filtering)",
                "enrich": "UpdateAttribute (add timestamp, static fields, computed values)",
            }
            for t in transforms:
                sql_parts.append(f"-- Transform: {transform_map.get(t, t)}")

        dest_proc = "UpdateSnowflakeDatabase" if dest_method == "update_db" else "PutSnowpipeStreaming"
        sql_parts.append(f"-- Destination: {dest_proc} -> {dest_db}.{dest_schema}.{dest_table}")
        sql_parts.append(f"--")
        sql_parts.append(f"-- Next steps:")
        sql_parts.append(f"--   1. Run this SQL in Snowflake (Snowsight or SnowSQL)")
        sql_parts.append(f"--   2. In the Openflow UI, deploy or create a flow")
        sql_parts.append(f"--   3. Configure processor properties using the parameter context")
        sql_parts.append(f"--   4. Attach the EAI in Openflow > Settings (if applicable)")
        sql_parts.append(f"--   5. Verify controllers, then start the flow")

        sql = "\n".join(sql_parts)
        return {"sql": sql}

    except Exception as e:
        return {"error": str(e)}


def create_streaming_task_sql(database: str, schema: str, task_name: str, table_name: str, 
                               num_meters: int, interval_minutes: int, service_area: str) -> str:
    return f"""
CREATE OR REPLACE TABLE {database}.{schema}.{table_name} IF NOT EXISTS (
    METER_ID VARCHAR(50),
    READING_TIMESTAMP TIMESTAMP_NTZ,
    USAGE_KWH FLOAT,
    VOLTAGE FLOAT,
    POWER_FACTOR FLOAT,
    TEMPERATURE_C FLOAT,
    SERVICE_AREA VARCHAR(100),
    CUSTOMER_SEGMENT VARCHAR(50),
    IS_OUTAGE BOOLEAN,
    DATA_QUALITY VARCHAR(20),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK {database}.{schema}.{task_name}
    WAREHOUSE = SI_AMI_PIPELINE_WH
    SCHEDULE = '{interval_minutes} MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'AMI streaming simulator - {num_meters} meters every {interval_minutes} min - {service_area}'
AS
INSERT INTO {database}.{schema}.{table_name} (
    METER_ID, READING_TIMESTAMP, USAGE_KWH, VOLTAGE, POWER_FACTOR, 
    TEMPERATURE_C, SERVICE_AREA, CUSTOMER_SEGMENT, IS_OUTAGE, DATA_QUALITY
)
WITH meters AS (
    SELECT 
        'MTR-{service_area[:3]}-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::VARCHAR, 6, '0') AS METER_ID,
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS METER_NUM,
        CASE MOD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 4)
            WHEN 0 THEN 'RESIDENTIAL_STANDARD'
            WHEN 1 THEN 'RESIDENTIAL_HIGH_USAGE'
            WHEN 2 THEN 'COMMERCIAL_SMALL'
            ELSE 'COMMERCIAL_LARGE'
        END AS SEGMENT
    FROM TABLE(GENERATOR(ROWCOUNT => {num_meters}))
)
SELECT 
    m.METER_ID,
    DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP()) AS READING_TIMESTAMP,
    ROUND(CASE 
        WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 14 AND 19 THEN UNIFORM(1.5, 3.5, RANDOM())
        WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 6 AND 9 THEN UNIFORM(1.0, 2.5, RANDOM())
        ELSE UNIFORM(0.3, 1.5, RANDOM())
    END * CASE m.SEGMENT 
        WHEN 'COMMERCIAL_LARGE' THEN 15 
        WHEN 'COMMERCIAL_SMALL' THEN 5 
        WHEN 'RESIDENTIAL_HIGH_USAGE' THEN 2 
        ELSE 1 
    END, 4) AS USAGE_KWH,
    ROUND(CASE WHEN UNIFORM(1, 100, RANDOM()) <= 2 THEN UNIFORM(105, 110, RANDOM())
               WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN UNIFORM(128, 135, RANDOM())
               ELSE NORMAL(120, 2, RANDOM()) END, 2) AS VOLTAGE,
    ROUND(UNIFORM(0.92, 0.99, RANDOM()), 3) AS POWER_FACTOR,
    ROUND(NORMAL(25, 8, RANDOM()), 1) AS TEMPERATURE_C,
    '{service_area}' AS SERVICE_AREA,
    m.SEGMENT AS CUSTOMER_SEGMENT,
    UNIFORM(1, 100, RANDOM()) <= 1 AS IS_OUTAGE,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 1 THEN 'OUTAGE' 
         WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN 'ANOMALY' 
         ELSE 'VALID' END AS DATA_QUALITY
FROM meters m;

ALTER TASK {database}.{schema}.{task_name} RESUME;
"""


@app.post("/api/task/suspend")
async def suspend_task(task_name: str = Form(...)):
    """Suspend a running streaming task"""
    try:
        validated_task = validate_identifier(task_name, allow_qualified=False)
        session = get_valid_session()
        if session:
            session.sql(f"ALTER TASK {DB}.{SCHEMA_PRODUCTION}.{validated_task} SUSPEND").collect()
            return RedirectResponse(url="/monitor", status_code=303)
    except Exception as e:
        logger.error(f"Failed to suspend task {task_name}: {e}")
        return HTMLResponse(f"<script>alert('Failed to suspend task: {html.escape(str(e))}'); window.location='/monitor';</script>")


@app.post("/api/task/resume")
async def resume_task(task_name: str = Form(...)):
    """Resume a suspended streaming task"""
    try:
        session = get_valid_session()
        if session:
            session.sql(f"ALTER TASK {DB}.{SCHEMA_PRODUCTION}.{task_name} RESUME").collect()
            return RedirectResponse(url="/monitor", status_code=303)
    except Exception as e:
        logger.error(f"Failed to resume task {task_name}: {e}")
        return HTMLResponse(f"<script>alert('Failed to resume task: {html.escape(str(e))}'); window.location='/monitor';</script>")


@app.post("/api/streaming/stop")
async def stop_streaming_job(job_id: str = Form(...)):
    """Stop an active Snowpipe Streaming job"""
    global active_streaming_jobs
    
    try:
        with streaming_lock:
            if job_id in active_streaming_jobs:
                active_streaming_jobs[job_id]['status'] = 'STOPPING'
                logger.info(f"Stopping streaming job: {job_id}")
        
        # Update DB status
        session = get_valid_session()
        if session:
            try:
                session.sql(f"""
                    UPDATE {DB}.{SCHEMA_PRODUCTION}.STREAMING_JOBS 
                    SET STATUS = 'STOPPED', UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE JOB_ID = '{sanitize_sql_value(job_id)}'
                """).collect()
            except Exception:
                pass
        
        return RedirectResponse(url="/monitor", status_code=303)
    except Exception as e:
        logger.error(f"Failed to stop streaming job {job_id}: {e}")
        return HTMLResponse(f"<script>alert('Failed to stop job: {html.escape(str(e))}'); window.location='/monitor';</script>")


def _compute_latency_percentiles(latencies: list) -> dict:
    """Compute P50/P95/P99/min/max/avg from a list of batch latency values (ms)."""
    if not latencies:
        return {}
    lats = sorted(latencies)
    n = len(lats)
    return {
        "p50_ms": round(lats[int(n * 0.50)], 1),
        "p95_ms": round(lats[min(int(n * 0.95), n - 1)], 1),
        "p99_ms": round(lats[min(int(n * 0.99), n - 1)], 1),
        "min_ms": round(lats[0], 1),
        "max_ms": round(lats[-1], 1),
        "avg_ms": round(sum(lats) / n, 1),
        "samples": n,
    }


@app.get("/api/streaming/status")
async def get_streaming_status():
    """Get status of all active streaming jobs"""
    global active_streaming_jobs
    
    jobs = []
    with streaming_lock:
        for job_id, job_data in active_streaming_jobs.items():
            stats = job_data.get('stats', {})
            config = job_data.get('config', {})
            # Retrieve HP SDK channel-level metrics if available
            hp_sdk_status = None
            hp_client = job_data.get('hp_client')
            if hp_client:
                try:
                    hp_sdk_status = hp_client.get_status()
                except Exception:
                    hp_sdk_status = None
            jobs.append({
                'job_id': job_id,
                'status': job_data.get('status', 'UNKNOWN'),
                'mechanism': config.get('mechanism', ''),
                'target_table': config.get('target_table', ''),
                'meters': config.get('meters', 0),
                'rows_per_sec': config.get('rows_per_sec', 0),
                'total_rows_sent': stats.get('total_rows', 0),
                'batches_sent': stats.get('batches_sent', 0),
                'errors': stats.get('errors', 0),
                'peak_rps': round(stats.get('peak_rps', 0), 1),
                'start_time': str(stats.get('start_time', ''))[:19],
                'last_batch_time': str(stats.get('last_batch_time', ''))[:19] if stats.get('last_batch_time') else None,
                'perf': _compute_latency_percentiles(stats.get('batch_latencies', [])),
                'source': stats.get('source', config.get('stream_source', 'test')),
                # HP SDK channel metrics (rows_inserted, rows_parsed, errors from Snowflake)
                'hp_sdk_channel': hp_sdk_status,
                # Grid topology state (correlated outages & voltage)
                'grid_topology': stats.get('grid_topology'),
                # Comparison mode fields (only populated when mechanism == 'comparison')
                'comparison': {
                    'hp_sdk_perf': _compute_latency_percentiles(stats.get('hp_sdk_latencies', [])),
                    'sql_insert_perf': _compute_latency_percentiles(stats.get('sql_insert_latencies', [])),
                    'hp_sdk_rows': stats.get('hp_sdk_rows', 0),
                    'sql_insert_rows': stats.get('sql_insert_rows', 0),
                    'hp_sdk_errors': stats.get('hp_sdk_errors', 0),
                    'sql_insert_errors': stats.get('sql_insert_errors', 0),
                } if stats.get('mechanism') == 'comparison' else None,
            })
    
    return JSONResponse({'active_jobs': jobs, 'count': len(jobs)})


@app.post("/api/stream")
async def start_stream(
    mode: str = Form(...),
    data_flow: str = Form("streaming_insert"),
    meters: int = Form(1000),
    interval: int = Form(15),
    service_area: str = Form("TEXAS_GULF_COAST"),
    rows_per_sec: int = Form(1000),
    batch_size_mb: int = Form(10),
    max_client_lag: int = Form(1),
    batch_size_rows: int = Form(0),  # 0 = auto (min(rows_per_sec, 500))
    table: str = Form(f"{DB}.{SCHEMA_PRODUCTION}.AMI_STREAMING_DATA"),
    new_table: str = Form(None),
    # New production matching parameters
    production_source: str = Form("METER_INFRASTRUCTURE"),
    emission_pattern: str = Form("STAGGERED_REALISTIC"),
    segment_filter: str = Form(None),
    data_format: str = Form("standard"),
    # Stage parameters (for internal/external stage streaming)
    stage_name: str = Form(None),
    new_stage_name: str = Form(None),  # For creating new stages
    stage_file_format: str = Form("json"),
    # HP Streaming PIPE parameter
    pipe_name: str = Form("AMI_STREAMING_PIPE"),
    # Source & comparison parameters
    stream_source: str = Form("test"),
    eh_conn_str: str = Form(""),
    eh_name: str = Form(""),
    eh_consumer_group: str = Form("$Default"),
    ps_project: str = Form(""),
    ps_subscription: str = Form(""),
    compare_mode: str = Form(""),
):
    """
    Start a streaming job with production-matched meter data.
    
    Parameters:
    - data_flow: Unified flow choice (snowflake_task, streaming_insert, streaming_hp, stage_landing, dual_write)
    - production_source: METER_INFRASTRUCTURE, AMI_METADATA_SEARCH, or SYNTHETIC
    - emission_pattern: UNIFORM, STAGGERED_REALISTIC, PARTIAL_REPORTING, DEGRADED_NETWORK
    - segment_filter: Optional filter for RESIDENTIAL, COMMERCIAL, INDUSTRIAL
    - data_format: standard, raw_ami (with VARIANT), or minimal
    - stage_name: External stage for stage destination
    - stage_file_format: parquet, json, or csv
    """
    # Decode base64-encoded connection string (semicolons get mangled in form encoding)
    if eh_conn_str.startswith('b64:'):
        import base64
        eh_conn_str = base64.b64decode(eh_conn_str[4:]).decode('utf-8')

    # Extract mechanism and dest from data_flow for backward compatibility
    flow_cfg = DATA_FLOWS.get(data_flow, DATA_FLOWS['streaming_insert'])
    mechanism = flow_cfg.get('mechanism', 'snowpipe_classic')
    dest = flow_cfg.get('dest', 'snowflake')
    
    job_id = f"flux_stream_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    target_table = new_table if new_table else table
    
    error_message = None
    task_created = False
    production_matched = False
    
    if snowflake_session:
        try:
            database = DB
            schema = SCHEMA_PRODUCTION
            
            # Get emission pattern config
            pattern_cfg = EMISSION_PATTERNS.get(emission_pattern, EMISSION_PATTERNS['STAGGERED_REALISTIC'])
            report_pct = pattern_cfg.get('meter_report_pct', 100)
            
            # Determine meter source query
            if production_source == "SYNTHETIC" or production_source not in PRODUCTION_DATA_SOURCES:
                # Use synthetic meter generation (original behavior)
                meter_source_sql = f"""
                    SELECT 
                        'MTR-{service_area[:3]}-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::VARCHAR, 6, '0') AS METER_ID,
                        'XFMR-{service_area[:3]}-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 10)::VARCHAR, 5, '0') AS TRANSFORMER_ID,
                        'CIRCUIT-{service_area[:3]}-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 100)::VARCHAR, 4, '0') AS CIRCUIT_ID,
                        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS METER_NUM,
                        CASE MOD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 10)
                            WHEN 0 THEN 'COMMERCIAL'
                            WHEN 1 THEN 'INDUSTRIAL'
                            ELSE 'RESIDENTIAL'
                        END AS CUSTOMER_SEGMENT,
                        29.7604 + UNIFORM(-0.5, 0.5, RANDOM()) AS LATITUDE,
                        -95.3698 + UNIFORM(-0.5, 0.5, RANDOM()) AS LONGITUDE,
                        'SUB-{service_area[:3]}-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 1000)::VARCHAR, 3, '0') AS SUBSTATION_ID
                    FROM TABLE(GENERATOR(ROWCOUNT => {meters}))
                """
                production_matched = False
            else:
                # Use production-matched meters
                src_cfg = PRODUCTION_DATA_SOURCES[production_source]
                segment_where = f"WHERE {src_cfg.get('segment_col', 'CUSTOMER_SEGMENT_ID')} = '{segment_filter}'" if segment_filter else ""
                
                meter_source_sql = f"""
                    SELECT 
                        m.{src_cfg['meter_col']} AS METER_ID,
                        m.{src_cfg.get('transformer_col', 'TRANSFORMER_ID')} AS TRANSFORMER_ID,
                        m.{src_cfg.get('circuit_col', 'CIRCUIT_ID')} AS CIRCUIT_ID,
                        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS METER_NUM,
                        COALESCE(m.{src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') AS CUSTOMER_SEGMENT,
                        m.{src_cfg.get('lat_col', 'NULL')} AS LATITUDE,
                        m.{src_cfg.get('lon_col', 'NULL')} AS LONGITUDE,
                        m.{src_cfg.get('substation_col', 'NULL')} AS SUBSTATION_ID
                    FROM {src_cfg['table']} m
                    {segment_where}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                """
                production_matched = True
            
            if mechanism == "task":
                task_name = f"AMI_STREAMING_TASK_{service_area}"
                table_name = target_table.split('.')[-1] if '.' in target_table else target_table
                
                # Enhanced table schema with more columns
                table_ddl = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                    METER_ID VARCHAR(50),
                    TRANSFORMER_ID VARCHAR(50),
                    CIRCUIT_ID VARCHAR(50),
                    SUBSTATION_ID VARCHAR(50),
                    READING_TIMESTAMP TIMESTAMP_NTZ,
                    USAGE_KWH FLOAT,
                    VOLTAGE FLOAT,
                    POWER_FACTOR FLOAT,
                    TEMPERATURE_C FLOAT,
                    SERVICE_AREA VARCHAR(100),
                    CUSTOMER_SEGMENT VARCHAR(50),
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    IS_OUTAGE BOOLEAN,
                    DATA_QUALITY VARCHAR(20),
                    PRODUCTION_MATCHED BOOLEAN DEFAULT {str(production_matched).upper()},
                    EMISSION_PATTERN VARCHAR(50),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
                snowflake_session.sql(table_ddl).collect()
                
                # Simpler meter source that doesn't need METER_NUM
                if production_source == "SYNTHETIC" or production_source not in PRODUCTION_DATA_SOURCES:
                    meter_source_for_task = f"""
                    SELECT 
                        'MTR-{service_area[:3]}-' || LPAD(SEQ4()::VARCHAR, 6, '0') AS METER_ID,
                        'XFMR-{service_area[:3]}-' || LPAD((SEQ4() / 10)::VARCHAR, 5, '0') AS TRANSFORMER_ID,
                        'CIRCUIT-{service_area[:3]}-' || LPAD((SEQ4() / 100)::VARCHAR, 4, '0') AS CIRCUIT_ID,
                        CASE MOD(SEQ4(), 10)
                            WHEN 0 THEN 'COMMERCIAL'
                            WHEN 1 THEN 'INDUSTRIAL'
                            ELSE 'RESIDENTIAL'
                        END AS CUSTOMER_SEGMENT,
                        29.7604 + UNIFORM(-0.5::FLOAT, 0.5::FLOAT, RANDOM()) AS LATITUDE,
                        -95.3698 + UNIFORM(-0.5::FLOAT, 0.5::FLOAT, RANDOM()) AS LONGITUDE,
                        'SUB-{service_area[:3]}-' || LPAD((SEQ4() / 1000)::VARCHAR, 3, '0') AS SUBSTATION_ID
                    FROM TABLE(GENERATOR(ROWCOUNT => {meters}))
                    """
                else:
                    src_cfg = PRODUCTION_DATA_SOURCES[production_source]
                    segment_where = f"WHERE {src_cfg.get('segment_col', 'CUSTOMER_SEGMENT_ID')} = '{segment_filter}'" if segment_filter else ""
                    meter_source_for_task = f"""
                    SELECT 
                        m.{src_cfg['meter_col']} AS METER_ID,
                        m.{src_cfg.get('transformer_col', 'TRANSFORMER_ID')} AS TRANSFORMER_ID,
                        m.{src_cfg.get('circuit_col', 'CIRCUIT_ID')} AS CIRCUIT_ID,
                        COALESCE(m.{src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') AS CUSTOMER_SEGMENT,
                        m.{src_cfg.get('lat_col', 'NULL')} AS LATITUDE,
                        m.{src_cfg.get('lon_col', 'NULL')} AS LONGITUDE,
                        m.{src_cfg.get('substation_col', 'NULL')} AS SUBSTATION_ID
                    FROM {src_cfg['table']} m
                    {segment_where}
                    ORDER BY RANDOM()
                    LIMIT {meters}
                    """
                
                # Create task with corrected RANDOM() usage (no arguments)
                task_ddl = f"""
                CREATE OR REPLACE TASK {database}.{schema}.{task_name}
                    WAREHOUSE = {WAREHOUSE}
                    SCHEDULE = '{interval} MINUTE'
                    ALLOW_OVERLAPPING_EXECUTION = FALSE
                    COMMENT = 'AMI streaming - {meters} meters every {interval} min - {service_area} - Production:{production_matched} - Job: {job_id}'
                AS
                INSERT INTO {database}.{schema}.{table_name} (
                    METER_ID, TRANSFORMER_ID, CIRCUIT_ID, SUBSTATION_ID,
                    READING_TIMESTAMP, USAGE_KWH, VOLTAGE, POWER_FACTOR, 
                    TEMPERATURE_C, SERVICE_AREA, CUSTOMER_SEGMENT, LATITUDE, LONGITUDE,
                    IS_OUTAGE, DATA_QUALITY, PRODUCTION_MATCHED, EMISSION_PATTERN
                )
                WITH meter_source AS (
                    {meter_source_for_task}
                )
                SELECT 
                    m.METER_ID,
                    m.TRANSFORMER_ID,
                    m.CIRCUIT_ID,
                    m.SUBSTATION_ID,
                    DATEADD(SECOND, UNIFORM(0, {pattern_cfg.get('stagger_seconds', 900)}, RANDOM()), DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP())) AS READING_TIMESTAMP,
                    ROUND(UNIFORM(0.3::FLOAT, 3.5::FLOAT, RANDOM()) * 
                        CASE m.CUSTOMER_SEGMENT 
                            WHEN 'INDUSTRIAL' THEN 15 
                            WHEN 'COMMERCIAL' THEN 5 
                            ELSE 1 
                        END, 4) AS USAGE_KWH,
                    ROUND(UNIFORM(118::FLOAT, 122::FLOAT, RANDOM()), 2) AS VOLTAGE,
                    ROUND(UNIFORM(0.92::FLOAT, 0.99::FLOAT, RANDOM()), 3) AS POWER_FACTOR,
                    ROUND(UNIFORM(15::FLOAT, 35::FLOAT, RANDOM()), 1) AS TEMPERATURE_C,
                    '{service_area}' AS SERVICE_AREA,
                    m.CUSTOMER_SEGMENT,
                    m.LATITUDE,
                    m.LONGITUDE,
                    UNIFORM(1, 100, RANDOM()) <= 1 AS IS_OUTAGE,
                    CASE 
                        WHEN UNIFORM(1, 100, RANDOM()) <= 1 THEN 'OUTAGE' 
                        WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN 'ANOMALY' 
                        ELSE 'VALID' 
                    END AS DATA_QUALITY,
                    {str(production_matched).upper()} AS PRODUCTION_MATCHED,
                    '{emission_pattern}' AS EMISSION_PATTERN
                FROM meter_source m
                """
                snowflake_session.sql(task_ddl).collect()
                snowflake_session.sql(f"ALTER TASK {database}.{schema}.{task_name} RESUME").collect()
                task_created = True
                logger.info(f"Created and started Task: {task_name} (Production Matched: {production_matched})")
                
            elif mechanism in ["snowpipe_classic", "snowpipe_hp"]:
                table_name = target_table.split('.')[-1] if '.' in target_table else target_table
                full_table_name = f"{database}.{schema}.{table_name}"
                
                table_ddl = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    METER_ID VARCHAR(50),
                    TRANSFORMER_ID VARCHAR(50),
                    CIRCUIT_ID VARCHAR(50),
                    SUBSTATION_ID VARCHAR(50),
                    READING_TIMESTAMP TIMESTAMP_NTZ,
                    USAGE_KWH FLOAT,
                    VOLTAGE FLOAT,
                    POWER_FACTOR FLOAT,
                    TEMPERATURE_C FLOAT,
                    SERVICE_AREA VARCHAR(100),
                    CUSTOMER_SEGMENT VARCHAR(50),
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    IS_OUTAGE BOOLEAN,
                    DATA_QUALITY VARCHAR(20),
                    PRODUCTION_MATCHED BOOLEAN DEFAULT FALSE,
                    EMISSION_PATTERN VARCHAR(50),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
                snowflake_session.sql(table_ddl).collect()
                
                sdk_type = "High-Performance" if mechanism == "snowpipe_hp" else "Classic"
                
                # Create STREAMING_JOBS tracking table and record
                job_record = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.STREAMING_JOBS (
                    JOB_ID VARCHAR(100),
                    MECHANISM VARCHAR(50),
                    TARGET_TABLE VARCHAR(200),
                    METERS INT,
                    INTERVAL_MINUTES INT,
                    ROWS_PER_SEC INT,
                    BATCH_SIZE_MB INT,
                    SERVICE_AREA VARCHAR(100),
                    PRODUCTION_SOURCE VARCHAR(100),
                    EMISSION_PATTERN VARCHAR(50),
                    PRODUCTION_MATCHED BOOLEAN,
                    STATUS VARCHAR(20),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                INSERT INTO {database}.{schema}.STREAMING_JOBS 
                (JOB_ID, MECHANISM, TARGET_TABLE, METERS, INTERVAL_MINUTES, ROWS_PER_SEC, BATCH_SIZE_MB, SERVICE_AREA, PRODUCTION_SOURCE, EMISSION_PATTERN, PRODUCTION_MATCHED, STATUS)
                VALUES ('{job_id}', '{mechanism}', '{table_name}', {meters}, {interval}, {rows_per_sec}, {batch_size_mb}, '{service_area}', '{production_source}', '{emission_pattern}', {str(production_matched).upper()}, 'RUNNING')
                """
                for stmt in job_record.split(';'):
                    if stmt.strip():
                        snowflake_session.sql(stmt).collect()
                
                # Start the actual streaming worker thread
                streaming_config = {
                    'meters': meters,
                    'rows_per_sec': rows_per_sec,
                    'batch_size': batch_size_rows if batch_size_rows > 0 else min(rows_per_sec, 500),
                    'service_area': service_area,
                    'emission_pattern': emission_pattern,
                    'production_source': production_source,
                    'target_table': full_table_name,
                    'mechanism': mechanism,
                    'sdk_type': sdk_type,
                    'pipe_name': pipe_name,
                    # Source-specific config
                    'stream_source': stream_source,
                    'eh_conn_str': eh_conn_str,
                    'eh_name': eh_name,
                    'eh_consumer_group': eh_consumer_group,
                    'ps_project': ps_project,
                    'ps_subscription': ps_subscription,
                    'compare_mode': compare_mode,
                }
                
                # Choose worker based on source and comparison mode
                if compare_mode == "1" and mechanism == 'snowpipe_hp':
                    worker_target = comparison_streaming_worker
                    worker_label = "HP SDK vs SQL INSERT Comparison"
                elif stream_source == "eventhub_produce":
                    worker_target = eventhub_producer_worker
                    worker_label = "Generate Data → Azure EventHub"
                elif stream_source == "eventhub":
                    worker_target = eventhub_streaming_worker
                    worker_label = "EventHub → Snowflake"
                elif stream_source == "pubsub":
                    worker_target = pubsub_streaming_worker
                    worker_label = "PubSub → Snowflake"
                else:
                    worker_target = snowpipe_streaming_worker
                    worker_label = f"Snowpipe Streaming ({sdk_type})"
                
                # Create and start streaming thread
                worker_thread = threading.Thread(
                    target=worker_target,
                    args=(job_id, streaming_config),
                    daemon=True
                )
                
                with streaming_lock:
                    active_streaming_jobs[job_id] = {
                        'thread': worker_thread,
                        'status': 'STARTING',
                        'config': streaming_config,
                        'stats': {
                            'total_rows': 0,
                            'batches_sent': 0,
                            'errors': 0,
                            'start_time': datetime.now(),
                            'last_batch_time': None
                        }
                    }
                
                worker_thread.start()
                task_created = True
                logger.info(f"Started {worker_label} worker: {job_id} ({rows_per_sec} rows/sec, Production Matched: {production_matched})")
            
            elif mechanism == "raw_json_s3":
                # Raw JSON streaming to S3 - Snowpipe auto-ingests
                if not BOTO3_AVAILABLE:
                    raise Exception("boto3 not available - cannot stream to S3")
                
                # Get AWS credentials from environment or secrets
                aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '')
                aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '')
                s3_bucket = os.getenv('S3_BUCKET', 'your-s3-bucket')
                s3_prefix = os.getenv('S3_PREFIX', 'raw/ami/')
                
                # Record job in tracking table
                job_record = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.STREAMING_JOBS (
                    JOB_ID VARCHAR(100),
                    MECHANISM VARCHAR(50),
                    TARGET_TABLE VARCHAR(200),
                    METERS INT,
                    INTERVAL_MINUTES INT,
                    ROWS_PER_SEC INT,
                    BATCH_SIZE_MB INT,
                    SERVICE_AREA VARCHAR(100),
                    PRODUCTION_SOURCE VARCHAR(100),
                    EMISSION_PATTERN VARCHAR(50),
                    PRODUCTION_MATCHED BOOLEAN,
                    STATUS VARCHAR(20),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                INSERT INTO {database}.{schema}.STREAMING_JOBS 
                (JOB_ID, MECHANISM, TARGET_TABLE, METERS, INTERVAL_MINUTES, ROWS_PER_SEC, BATCH_SIZE_MB, SERVICE_AREA, PRODUCTION_SOURCE, EMISSION_PATTERN, PRODUCTION_MATCHED, STATUS)
                VALUES ('{job_id}', 'raw_json_s3', 's3://{s3_bucket}/{s3_prefix}', {meters}, {interval}, {rows_per_sec}, {batch_size_mb}, '{service_area}', '{production_source}', '{emission_pattern}', {str(production_matched).upper()}, 'RUNNING')
                """
                for stmt in job_record.split(';'):
                    if stmt.strip():
                        snowflake_session.sql(stmt).collect()
                
                # Configure S3 streaming
                streaming_config = {
                    'meters': meters,
                    'rows_per_batch': min(rows_per_sec * 10, 1000),  # 10 seconds worth or 1000 max
                    'batch_interval_sec': 10,  # Write to S3 every 10 seconds
                    'service_area': service_area,
                    'emission_pattern': emission_pattern,
                    'production_source': production_source,
                    's3_bucket': s3_bucket,
                    's3_prefix': s3_prefix,
                    'aws_access_key_id': aws_access_key,
                    'aws_secret_access_key': aws_secret_key,
                    'aws_region': 'us-west-2',
                    'aws_role_arn': os.getenv('AWS_ROLE_ARN', ''),
                    'mechanism': 'raw_json_s3',
                }
                
                # Create and start S3 streaming thread
                worker_thread = threading.Thread(
                    target=raw_json_s3_streaming_worker,
                    args=(job_id, streaming_config),
                    daemon=True
                )
                
                with streaming_lock:
                    active_streaming_jobs[job_id] = {
                        'thread': worker_thread,
                        'status': 'STARTING',
                        'config': streaming_config,
                        'stats': {
                            'total_rows': 0,
                            'files_written': 0,
                            'errors': 0,
                            'start_time': datetime.now(),
                            'last_file_time': None
                        }
                    }
                
                worker_thread.start()
                task_created = True
                logger.info(f"Started Raw JSON S3 Streaming: {job_id} → s3://{s3_bucket}/{s3_prefix}")
            
            elif mechanism == "stage_json":
                # Unified Stage JSON streaming - handles both internal and external stages
                #  Critical for utility demo showing raw data landing before transformation
                
                # Determine target stage - use new_stage_name if creating new, else use selected or default
                if stage_name == '__create_new__' and new_stage_name:
                    # User wants to create a new internal stage
                    target_stage = f"{database}.{schema}.{new_stage_name.upper()}"
                    is_external_stage = False
                    logger.info(f"Creating new internal stage: {target_stage}")
                elif stage_name and stage_name != '__create_new__':
                    target_stage = stage_name
                    # Check if this is an external stage by querying stage properties
                    is_external_stage = False
                    try:
                        stage_info = snowflake_session.sql(f"DESC STAGE {target_stage}").collect()
                        for row in stage_info:
                            row_dict = row.asDict() if hasattr(row, 'asDict') else row
                            prop = row_dict.get('property', '') or str(row_dict)
                            if 'URL' in prop or 'EXTERNAL' in str(row_dict).upper():
                                url_val = row_dict.get('property_value', '') or ''
                                if 's3://' in url_val.lower() or 'azure://' in url_val.lower() or 'gcs://' in url_val.lower():
                                    is_external_stage = True
                                    break
                    except Exception as check_err:
                        logger.warning(f"Could not determine stage type, assuming internal: {check_err}")
                        is_external_stage = False
                else:
                    target_stage = f"{database}.{schema}.STG_AMI_RAW_JSON"
                    is_external_stage = False
                
                stage_file_fmt = stage_file_format if stage_file_format else 'json'
                
                # For internal stages, ensure the stage exists
                if not is_external_stage:
                    stage_ddl = f"""
                    CREATE STAGE IF NOT EXISTS {target_stage}
                        FILE_FORMAT = (TYPE = 'JSON')
                        COMMENT = 'Internal stage for raw AMI JSON data landing'
                    """
                    try:
                        snowflake_session.sql(stage_ddl).collect()
                    except Exception as stage_err:
                        logger.warning(f"Stage creation warning (may already exist): {stage_err}")
                
                # Record job in tracking table
                mechanism_type = 'stage_json_ext' if is_external_stage else 'stage_json'
                job_record = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.STREAMING_JOBS (
                    JOB_ID VARCHAR(100),
                    MECHANISM VARCHAR(50),
                    TARGET_TABLE VARCHAR(200),
                    METERS INT,
                    INTERVAL_MINUTES INT,
                    ROWS_PER_SEC INT,
                    BATCH_SIZE_MB INT,
                    SERVICE_AREA VARCHAR(100),
                    PRODUCTION_SOURCE VARCHAR(100),
                    EMISSION_PATTERN VARCHAR(50),
                    PRODUCTION_MATCHED BOOLEAN,
                    STATUS VARCHAR(20),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                INSERT INTO {database}.{schema}.STREAMING_JOBS 
                (JOB_ID, MECHANISM, TARGET_TABLE, METERS, INTERVAL_MINUTES, ROWS_PER_SEC, BATCH_SIZE_MB, SERVICE_AREA, PRODUCTION_SOURCE, EMISSION_PATTERN, PRODUCTION_MATCHED, STATUS)
                VALUES ('{job_id}', '{mechanism_type}', '@{target_stage}', {meters}, {interval}, {rows_per_sec}, {batch_size_mb}, '{service_area}', '{production_source}', '{emission_pattern}', {str(production_matched).upper()}, 'RUNNING')
                """
                for stmt in job_record.split(';'):
                    if stmt.strip():
                        snowflake_session.sql(stmt).collect()
                
                # Configure stage streaming
                streaming_config = {
                    'meters': meters,
                    'rows_per_batch': min(rows_per_sec * 10, 1000),  # 10 seconds worth or 1000 max
                    'batch_interval_sec': 10,  # Write to stage every 10 seconds
                    'service_area': service_area,
                    'emission_pattern': emission_pattern,
                    'production_source': production_source,
                    'stage_name': target_stage,
                    'target_table': f"@{target_stage}",  # For display consistency (stages use @ prefix)
                    'stage_file_format': stage_file_fmt,
                    'mechanism': mechanism_type,
                    'is_external': is_external_stage,
                }
                
                # Choose appropriate worker based on stage type
                worker_func = external_stage_streaming_worker if is_external_stage else internal_stage_streaming_worker
                worker_thread = threading.Thread(
                    target=worker_func,
                    args=(job_id, streaming_config),
                    daemon=True
                )
                
                with streaming_lock:
                    active_streaming_jobs[job_id] = {
                        'thread': worker_thread,
                        'status': 'STARTING',
                        'config': streaming_config,
                        'stats': {
                            'total_rows': 0,
                            'files_written': 0,
                            'errors': 0,
                            'start_time': datetime.now(),
                            'last_file_time': None,
                            'stage_name': target_stage,
                            'stage_type': 'external' if is_external_stage else 'internal'
                        }
                    }
                
                worker_thread.start()
                task_created = True
                stage_type_label = "External" if is_external_stage else "Internal"
                logger.info(f"Started {stage_type_label} Stage JSON Streaming: {job_id} → @{target_stage}")
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to create streaming job: {e}")
    
    status_icon = "check_circle" if task_created else ("error" if error_message else "hourglass_empty")
    status_color = "#22c55e" if task_created else ("#ef4444" if error_message else "#f59e0b")
    status_text = "Streaming Job Started!" if task_created else ("Failed to Start" if error_message else "Job Registered (Manual Start Required)")
    
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Streaming Started - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('generate')}
            
            <div class="panel" style="text-align: center; padding: 40px;">
                <div style="font-size: 4rem; margin-bottom: 20px;">
                    {get_material_icon(status_icon, '80px', status_color)}
                </div>
                <h2 style="color: {status_color}; margin-bottom: 16px;">{status_text}</h2>
                <p style="color: #94a3b8; margin-bottom: 24px;">{'Error: ' + error_message if error_message else ('Task-based streaming is now running in Snowflake.' if mechanism == 'task' else 'Snowpipe SDK job registered - requires SDK client to run.')}</p>
                
                <div style="background: #1e293b; border-radius: 8px; padding: 20px; text-align: left; max-width: 500px; margin: 0 auto 24px;">
                    <div style="display: grid; gap: 12px;">
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Job ID</span>
                            <span style="color: #38bdf8; font-family: monospace;">{job_id}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Target Table</span>
                            <span style="color: #f8fafc;">{target_table}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Meters</span>
                            <span style="color: #f8fafc;">{meters:,}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Interval</span>
                            <span style="color: #f8fafc;">{interval} minutes</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Data Flow</span>
                            <span style="color: #f8fafc;">{data_flow}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Rows/sec</span>
                            <span style="color: #f8fafc;">{rows_per_sec:,}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Batch Size</span>
                            <span style="color: #f8fafc;">{batch_size_mb} MB</span>
                        </div>
                    </div>
                </div>
                
                <div style="display: flex; gap: 12px; justify-content: center;">
                    <a href="/monitor" class="btn-primary" style="text-decoration: none;">
                        {get_material_icon('monitoring', '20px')} View Monitor
                    </a>
                    <a href="/generate" class="btn-secondary" style="text-decoration: none;">
                        {get_material_icon('arrow_back', '20px')} Back to Generate
                    </a>
                </div>
            </div>
        </div>
    </body>
    </html>
    """)


@app.post("/api/generate")
async def generate_batch(
    mode: str = Form(...),
    mechanism: str = Form("task"),
    dest: str = Form("snowflake"),
    meters: int = Form(1000),
    days: int = Form(90),
    interval: int = Form(15),
    start_date: str = Form(None),
    service_area: str = Form("TEXAS_GULF_COAST"),
    meter_prefix: str = Form("MTR"),
    table: str = Form(f"{DB}.{SCHEMA_PRODUCTION}.AMI_INTERVAL_READINGS"),
    include_variant: str = Form("false"),
    gen_asset360: str = Form(None),
    gen_work_orders: str = Form(None),
    gen_power_quality: str = Form(None),
    gen_erm: str = Form(None)
):
    job_id = f"flux_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    est_rows = meters * (days * 24 * 60 // interval)
    
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Batch Generation Started - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('generate')}
            
            <div class="panel" style="text-align: center; padding: 40px;">
                <div style="font-size: 4rem; margin-bottom: 20px;">
                    {get_material_icon('check_circle', '80px', '#22c55e')}
                </div>
                <h2 style="color: #22c55e; margin-bottom: 16px;">Batch Generation Started!</h2>
                <p style="color: #94a3b8; margin-bottom: 24px;">Your data generation job is now running.</p>
                
                <div style="background: #1e293b; border-radius: 8px; padding: 20px; text-align: left; max-width: 500px; margin: 0 auto 24px;">
                    <div style="display: grid; gap: 12px;">
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Job ID</span>
                            <span style="color: #38bdf8; font-family: monospace;">{job_id}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Target Table</span>
                            <span style="color: #f8fafc;">{table}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Meters</span>
                            <span style="color: #f8fafc;">{meters:,}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Days</span>
                            <span style="color: #f8fafc;">{days}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Interval</span>
                            <span style="color: #f8fafc;">{interval} minutes</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Est. Rows</span>
                            <span style="color: #f8fafc;">{est_rows:,}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: #64748b;">Service Area</span>
                            <span style="color: #f8fafc;">{service_area}</span>
                        </div>
                    </div>
                </div>
                
                <div style="display: flex; gap: 12px; justify-content: center;">
                    <a href="/history" class="btn-primary" style="text-decoration: none;">
                        {get_material_icon('history', '20px')} View History
                    </a>
                    <a href="/generate" class="btn-secondary" style="text-decoration: none;">
                        {get_material_icon('arrow_back', '20px')} Back to Generate
                    </a>
                </div>
            </div>
        </div>
    </body>
    </html>
    """)


@app.post("/api/validate")
async def validate_schema(
    database: str = Form(...),
    schema: str = Form(...),
    table: str = Form(...)
):
    try:
        qualified = validate_identifier(f"{database}.{schema}.{table}")
        if snowflake_session:
            result = snowflake_session.sql(f"DESCRIBE TABLE {qualified}").collect()
            columns = [{"name": r['name'], "type": r['type']} for r in result]
            return JSONResponse({
                "status": "success",
                "table": f"{database}.{schema}.{table}",
                "columns": columns
            })
    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": str(e)
        }, status_code=400)


@app.get("/api/databases")
async def list_databases():
    """List all accessible databases for the current user"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql("SHOW DATABASES").collect()
        # Filter out system databases and sort
        databases = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name'] 
            for r in result 
            if not (r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']).startswith('SNOWFLAKE')
        ])
        return {"databases": databases, "count": len(databases)}
    except Exception as e:
        logger.warning(f"Failed to list databases: {e}")
        return {"databases": [DB], "error": str(e)}


@app.get("/api/schemas/{database}")
async def list_schemas(database: str):
    """List all schemas in a database"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        validated_db = validate_identifier(database, allow_qualified=False)
        result = session.sql(f"SHOW SCHEMAS IN DATABASE {validated_db}").collect()
        schemas = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result 
            if (r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']) not in ['INFORMATION_SCHEMA']
        ])
        return {"schemas": schemas, "database": database, "count": len(schemas)}
    except Exception as e:
        logger.warning(f"Failed to list schemas in {database}: {e}")
        return {"schemas": ["PUBLIC"], "error": str(e)}


@app.get("/api/tables/{database}/{schema}")
async def list_tables(database: str, schema: str):
    """List all tables in a schema"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        validated_ref = validate_identifier(f"{database}.{schema}")
        result = session.sql(f"SHOW TABLES IN {validated_ref}").collect()
        tables = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result
        ])
        return {"tables": tables, "database": database, "schema": schema, "count": len(tables)}
    except Exception as e:
        logger.warning(f"Failed to list tables in {database}.{schema}: {e}")
        return {"tables": [], "error": str(e)}


@app.get("/api/tables/bronze")
async def list_bronze_tables():
    """
    List tables suitable for bronze/raw data landing (tables with VARIANT columns).
    Advanced Mode: Returns tables that can serve as Snowpipe targets.
    Uses preloaded cache for instant response when available.
    """
    global dependency_cache
    
    #  Use cached data if available for instant response
    with dependency_cache['lock']:
        if dependency_cache['tables'] is not None:
            cached_tables = dependency_cache['tables']
            logger.info(f"list_bronze_tables: Returning {len(cached_tables)} tables from cache (instant)")
            return {
                "tables": cached_tables,
                "count": len(cached_tables),
                "cached": True
            }
    
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        # Query for tables with VARIANT columns (typical bronze table pattern)
        # Check common locations: FLUX_DB.PRODUCTION and current database/schema
        bronze_tables = []
        
        # Get current context
        try:
            ctx = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
            current_db = ctx[0]
            current_schema = ctx[1]
        except Exception:
            current_db = DB
            current_schema = SCHEMA_PRODUCTION
        
        # Search in current database/schema plus all accessible schemas in current database
        # This ensures we find tables in DEV, PRODUCTION, and any other schemas
        search_locations = [
            (current_db, current_schema),
            (DB, "PRODUCTION"),
            (DB, "DEV"),
        ]
        
        # Dynamically discover all schemas in the current database
        try:
            schemas_result = session.sql(f"SHOW SCHEMAS IN DATABASE {current_db}").collect()
            for schema_row in schemas_result:
                schema_dict = schema_row.asDict() if hasattr(schema_row, 'asDict') else dict(schema_row)
                schema_name = schema_dict.get('name', '')
                if schema_name and schema_name not in ['INFORMATION_SCHEMA']:
                    search_locations.append((current_db, schema_name))
        except Exception:
            pass
        
        # Deduplicate while preserving order
        seen = set()
        unique_locations = []
        for loc in search_locations:
            if loc not in seen:
                seen.add(loc)
                unique_locations.append(loc)
        search_locations = unique_locations
        
        for db, schema in search_locations:
            try:
                # Get tables in this schema
                tables_result = session.sql(f"SHOW TABLES IN {db}.{schema}").collect()
                
                for tbl_row in tables_result:
                    tbl_dict = tbl_row.asDict() if hasattr(tbl_row, 'asDict') else dict(tbl_row)
                    tbl_name = tbl_dict.get('name', '')
                    
                    # Check if table has VARIANT column (bronze table indicator)
                    try:
                        cols_result = session.sql(f"DESC TABLE {db}.{schema}.{tbl_name}").collect()
                        has_variant = any(
                            'VARIANT' in str(col.asDict().get('type', '') if hasattr(col, 'asDict') else col.get('type', '')).upper()
                            for col in cols_result
                        )
                        
                        if has_variant:
                            bronze_tables.append({
                                "name": tbl_name,
                                "full_name": f"{db}.{schema}.{tbl_name}",
                                "database": db,
                                "schema": schema,
                                "has_variant": True
                            })
                    except Exception as col_err:
                        # Skip tables we can't describe
                        continue
                        
            except Exception as loc_err:
                logger.debug(f"Could not search {db}.{schema}: {loc_err}")
                continue
        
        # Also add common bronze table names even if they don't exist yet (as suggestions)
        common_names = ["AMI_BRONZE_RAW", "AMI_RAW_DATA", "RAW_AMI_DATA", "BRONZE_AMI"]
        existing_names = {t['name'] for t in bronze_tables}
        
        return {
            "tables": bronze_tables,
            "count": len(bronze_tables),
            "suggestions": [n for n in common_names if n not in existing_names]
        }
        
    except Exception as e:
        logger.error(f"Failed to list bronze tables: {e}")
        return {"tables": [], "count": 0, "error": str(e)}


@app.get("/api/bronze-tables")
async def get_bronze_tables_for_monitor():
    """
     Get list of bronze/landing tables for the Monitor page dropdown.
    Returns simplified list with row counts for quick selection.
    Includes both VARIANT-based bronze tables AND explicitly named raw/bronze tables.
    """
    session = get_valid_session()
    if not session:
        return {"tables": [], "error": "Not connected to Snowflake"}
    
    try:
        bronze_tables = []
        seen_tables = set()
        
        # Search in both PRODUCTION and DEV schemas
        for schema_path in [f"{DB}.PRODUCTION", f"{DB}.DEV"]:
            try:
                db, schema = schema_path.split('.')
                result = session.sql(f"SHOW TABLES IN {schema_path}").collect()
                
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    tbl_name = row_dict.get('name', '')
                    full_name = f"{schema_path}.{tbl_name}"
                    
                    if full_name in seen_tables:
                        continue
                    
                    # Include tables that are likely bronze/raw landing tables
                    is_bronze = any(x in tbl_name.upper() for x in ['BRONZE', 'RAW', 'LANDING', 'AMI_STREAMING', 'INGEST'])
                    
                    # Also check for VARIANT column (classic bronze pattern)
                    has_variant = False
                    if not is_bronze:
                        try:
                            cols = session.sql(f"DESC TABLE {full_name}").collect()
                            has_variant = any('VARIANT' in str(c.asDict().get('type', '')).upper() if hasattr(c, 'asDict') else 'VARIANT' in str(c.get('type', '')).upper() for c in cols)
                        except Exception:
                            pass
                    
                    if is_bronze or has_variant:
                        seen_tables.add(full_name)
                        # Get row count
                        try:
                            cnt_result = session.sql(f"SELECT COUNT(*) as CNT FROM {full_name}").collect()
                            row_count = cnt_result[0]['CNT'] if cnt_result else 0
                        except Exception:
                            row_count = None
                        
                        bronze_tables.append({
                            "name": tbl_name,
                            "full_name": full_name,
                            "schema": schema,
                            "row_count": row_count,
                            "has_variant": has_variant
                        })
            except Exception as e:
                logger.warning(f"Could not search {schema_path} for bronze tables: {e}")
        
        # Sort by schema then name
        bronze_tables.sort(key=lambda x: (x.get('schema', ''), x.get('name', '')))
        
        return {"tables": bronze_tables, "count": len(bronze_tables)}
        
    except Exception as e:
        logger.error(f"Failed to get bronze tables for monitor: {e}")
        return {"tables": [], "error": str(e)}


@app.get("/api/bronze-preview")
async def get_bronze_preview(table: str):
    """
     Get live preview of data in a bronze/landing table.
    Returns recent rows ordered by ingestion time for the Monitor page.
    """
    if not table:
        return {"error": "No table specified"}
    
    session = get_valid_session()
    if not session:
        return {"error": "Not connected to Snowflake"}
    
    try:
        # Validate table name (basic SQL injection prevention)
        if any(x in table.upper() for x in [';', '--', 'DROP', 'DELETE', 'INSERT', 'UPDATE']):
            return {"error": "Invalid table name"}
        
        # Get columns first
        try:
            cols_result = session.sql(f"DESC TABLE {table}").collect()
            columns = [c.asDict().get('name', '') if hasattr(c, 'asDict') else c.get('name', '') for c in cols_result]
        except Exception as e:
            return {"error": f"Cannot describe table: {e}"}
        
        # Find the timestamp column for ordering (prefer CREATED_AT, INGESTED_AT, etc.)
        timestamp_col = None
        for preferred in ['CREATED_AT', 'INGESTED_AT', 'LOADED_AT', 'TIMESTAMP', 'TS', 'READING_TIMESTAMP']:
            if preferred in [c.upper() for c in columns]:
                timestamp_col = preferred
                break
        
        # Build query
        if timestamp_col:
            query = f"SELECT * FROM {table} ORDER BY {timestamp_col} DESC LIMIT 15"
        else:
            query = f"SELECT * FROM {table} LIMIT 15"
        
        result = session.sql(query).collect()
        
        # Convert to list of dicts
        rows = []
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            # Clean up values for JSON serialization
            clean_row = {}
            for k, v in row_dict.items():
                if v is None:
                    clean_row[k] = None
                elif hasattr(v, 'isoformat'):
                    clean_row[k] = v.isoformat()
                elif isinstance(v, (int, float, str, bool)):
                    clean_row[k] = v
                else:
                    clean_row[k] = str(v)[:100]  # Truncate long values
            rows.append(clean_row)
        
        # Get total count
        try:
            cnt_result = session.sql(f"SELECT COUNT(*) as CNT FROM {table}").collect()
            total_count = cnt_result[0]['CNT'] if cnt_result else 0
        except Exception:
            total_count = None
        
        # Calculate freshness if we have a timestamp column
        newest_age = None
        newest_age_seconds = None
        if timestamp_col and rows:
            try:
                ts_val = rows[0].get(timestamp_col)
                if ts_val:
                    from datetime import datetime
                    if isinstance(ts_val, str):
                        ts_val = datetime.fromisoformat(ts_val.replace('Z', '+00:00').replace('+00:00', ''))
                    now = datetime.now()
                    if hasattr(ts_val, 'replace'):
                        try:
                            ts_val = ts_val.replace(tzinfo=None)
                        except Exception:
                            pass
                    age_seconds = (now - ts_val).total_seconds()
                    newest_age_seconds = age_seconds
                    if age_seconds < 60:
                        newest_age = f"{int(age_seconds)} seconds ago"
                    elif age_seconds < 3600:
                        newest_age = f"{int(age_seconds // 60)} minutes ago"
                    elif age_seconds < 86400:
                        newest_age = f"{int(age_seconds // 3600)} hours ago"
                    else:
                        newest_age = f"{int(age_seconds // 86400)} days ago"
            except Exception as e:
                logger.debug(f"Could not calculate freshness: {e}")
        
        return {
            "rows": rows,
            "columns": columns,
            "total_count": total_count,
            "newest_age": newest_age,
            "newest_age_seconds": newest_age_seconds,
            "timestamp_column": timestamp_col
        }
        
    except Exception as e:
        logger.error(f"Failed to get bronze preview: {e}")
        return {"error": str(e)}


def check_pipe_exists_for_table(session, database: str, schema: str, table_name: str):
    """
     Check if a Snowpipe exists that loads into the specified table.
    Returns dict with pipe info or None if no pipe found.
    """
    try:
        # Check for pipes in the same schema that target this table
        result = session.sql(f"SHOW PIPES IN {database}.{schema}").collect()
        full_table_name = f"{database}.{schema}.{table_name}".upper()
        
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            pipe_def = row_dict.get('definition', '')
            pipe_name = row_dict.get('name', '')
            
            # Check if this pipe loads into our target table
            if full_table_name in pipe_def.upper() or table_name.upper() in pipe_def.upper():
                return {
                    'exists': True,
                    'pipe_name': f"{database}.{schema}.{pipe_name}",
                    'definition': pipe_def,
                    'auto_ingest': 'AUTO_INGEST' in pipe_def.upper()
                }
        
        return {'exists': False}
    except Exception as e:
        logger.warning(f"Could not check pipes in {database}.{schema}: {e}")
        return {'exists': False, 'error': str(e)}


def get_external_stages_for_schema(session, database: str, schema: str):
    """
     Get list of external stages available (check PRODUCTION schema for shared stages).
    """
    external_stages = []
    
    # Check same schema
    try:
        result = session.sql(f"SHOW STAGES IN {database}.{schema}").collect()
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            if row_dict.get('type', '').upper() == 'EXTERNAL':
                external_stages.append({
                    'name': row_dict.get('name', ''),
                    'full_name': f"{database}.{schema}.{row_dict.get('name', '')}",
                    'url': row_dict.get('url', '')
                })
    except Exception:
        pass
    
    # Also check PRODUCTION schema for shared external stages
    if schema.upper() != 'PRODUCTION':
        try:
            result = session.sql(f"SHOW STAGES IN {database}.PRODUCTION").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                if row_dict.get('type', '').upper() == 'EXTERNAL':
                    external_stages.append({
                        'name': row_dict.get('name', ''),
                        'full_name': f"{database}.PRODUCTION.{row_dict.get('name', '')}",
                        'url': row_dict.get('url', ''),
                        'shared': True
                    })
        except Exception:
            pass
    
    return external_stages


@app.post("/api/tables/create-bronze")
async def create_bronze_table(
    table_name: str = Form(...),
    table_type: str = Form("bronze_variant"),
    create_pipe: bool = Form(False),
    source_stage: str = Form(None),
    file_pattern: str = Form(".*ami_stream.*\\.json")
):
    """
    Create a bronze table for raw data landing with VARIANT column.
    Advanced Mode: Standard medallion architecture bronze table schema.
    
    Optional: Also create a Snowpipe if user opts in via create_pipe=True.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        # Parse table name
        parts = table_name.upper().split('.')
        if len(parts) == 3:
            db, schema, tbl = parts
        elif len(parts) == 1:
            # Use current context
            ctx = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
            db, schema = ctx[0], ctx[1]
            tbl = parts[0]
        else:
            raise HTTPException(400, "Table name must be fully qualified (DB.SCHEMA.TABLE) or just TABLE name")
        
        full_name = f"{db}.{schema}.{tbl}"
        table_created = False
        table_existed = False
        
        # Check if table exists
        try:
            session.sql(f"DESC TABLE {full_name}").collect()
            table_existed = True
        except Exception:
            pass  # Table doesn't exist, create it
        
        if not table_existed:
            # Create bronze table with standard schema
            create_sql = f"""
            CREATE TABLE {full_name} (
                RAW_DATA VARIANT COMMENT 'Raw JSON/Parquet data as VARIANT',
                FILE_NAME VARCHAR(500) COMMENT 'Source file name from METADATA$FILENAME',
                LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Load timestamp'
            )
            COMMENT = 'Bronze layer table for raw data ingestion via Snowpipe'
            """
            
            session.sql(create_sql).collect()
            logger.info(f"Created bronze table: {full_name}")
            table_created = True
        
        #  Check if a pipe exists for this table
        pipe_info = check_pipe_exists_for_table(session, db, schema, tbl)
        
        #  Get available external stages for pipe creation suggestion
        available_stages = get_external_stages_for_schema(session, db, schema)
        
        #  If user opted in to create pipe and provided source stage
        pipe_created = False
        pipe_details = None
        if create_pipe and source_stage and not pipe_info.get('exists'):
            try:
                # Generate a pipe name based on table name
                pipe_name = f"{tbl}_PIPE"
                full_pipe_name = f"{db}.{schema}.{pipe_name}"
                
                # Normalize stage name
                stage_ref = source_stage if '.' in source_stage else f"{db}.PRODUCTION.{source_stage}"
                
                create_pipe_sql = f"""
                CREATE OR REPLACE PIPE {full_pipe_name}
                  AUTO_INGEST = TRUE
                  COMMENT = 'Auto-created pipe for bronze table {full_name}'
                  AS
                  COPY INTO {full_name} (RAW_DATA, FILE_NAME, LOAD_TS)
                  FROM (
                    SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
                    FROM @{stage_ref}
                  )
                  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
                  PATTERN = '{file_pattern}'
                """
                
                session.sql(create_pipe_sql).collect()
                logger.info(f"Auto-created pipe {full_pipe_name} for table {full_name}")
                
                # PATTERN: Auto-refresh new pipes to catch up on existing files
                files_refreshed = 0
                try:
                    logger.info(f"Auto-refreshing pipe {full_pipe_name} to ingest existing files...")
                    refresh_rows = session.sql(f"ALTER PIPE {full_pipe_name} REFRESH").collect()
                    files_refreshed = len(refresh_rows)
                    logger.info(f"Auto-refresh complete: {files_refreshed} files queued for ingestion")
                except Exception as refresh_err:
                    logger.warning(f"Auto-refresh failed (non-critical): {refresh_err}")
                
                pipe_created = True
                pipe_details = {
                    'pipe_name': full_pipe_name,
                    'source_stage': stage_ref,
                    'pattern': file_pattern,
                    'auto_ingest': True,
                    'files_refreshed': files_refreshed
                }
                
                # Update pipe_info
                pipe_info = {
                    'exists': True,
                    'pipe_name': full_pipe_name,
                    'auto_ingest': True,
                    'just_created': True
                }
                
            except Exception as pipe_err:
                logger.error(f"Failed to auto-create pipe: {pipe_err}")
                pipe_details = {'error': str(pipe_err)}
        
        # Build response with pipe status info
        response = {
            "status": "exists" if table_existed else "created",
            "table_name": full_name,
            "database": db,
            "schema": schema,
            "columns": ["RAW_DATA (VARIANT)", "FILE_NAME (VARCHAR)", "LOAD_TS (TIMESTAMP_NTZ)"],
            #  Include pipe detection info for transparent user feedback
            "pipe_status": {
                "has_pipe": pipe_info.get('exists', False),
                "pipe_name": pipe_info.get('pipe_name'),
                "auto_ingest": pipe_info.get('auto_ingest', False),
                "pipe_created_now": pipe_created,
                "pipe_details": pipe_details
            },
            "available_stages": available_stages,
            #  Provide guidance if no pipe exists
            "requires_pipe": not pipe_info.get('exists', False),
            "pipe_guidance": None if pipe_info.get('exists') else (
                f"No Snowpipe found for {full_name}. To enable auto-ingest from S3, "
                f"create a pipe or re-run with create_pipe=True and specify source_stage."
            )
        }
        
        if table_existed:
            response["message"] = "Table already exists"
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create bronze table {table_name}: {e}")
        raise HTTPException(500, f"Failed to create bronze table: {str(e)}")


@app.get("/api/pipes/check/{database}/{schema}/{table_name}")
async def check_pipe_for_table(database: str, schema: str, table_name: str):
    """
     API endpoint to check if a Snowpipe exists for a given table.
    Used by UI to show pipe status and offer opt-in creation.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        pipe_info = check_pipe_exists_for_table(session, database.upper(), schema.upper(), table_name.upper())
        available_stages = get_external_stages_for_schema(session, database.upper(), schema.upper())
        
        return {
            "table": f"{database}.{schema}.{table_name}".upper(),
            "pipe_exists": pipe_info.get('exists', False),
            "pipe_name": pipe_info.get('pipe_name'),
            "pipe_definition": pipe_info.get('definition'),
            "auto_ingest": pipe_info.get('auto_ingest', False),
            "available_stages": available_stages,
            "can_create_pipe": len(available_stages) > 0,
            "recommendation": (
                "Pipe exists and will auto-ingest data from S3." if pipe_info.get('exists')
                else "No pipe found. Create one to enable automatic S3 → Table ingestion."
            )
        }
    except Exception as e:
        logger.error(f"Failed to check pipe for table: {e}")
        raise HTTPException(500, str(e))


@app.post("/api/pipes/auto-create")
async def auto_create_pipe_for_table(
    database: str = Form(...),
    schema: str = Form(...),
    table_name: str = Form(...),
    source_stage: str = Form(...),
    file_pattern: str = Form(".*ami_stream.*\\.json"),
    auto_refresh: bool = Form(True)
):
    """
     Create a Snowpipe for a bronze table with user consent.
    This is the opt-in endpoint called when user explicitly requests pipe creation.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        db = database.upper()
        sch = schema.upper()
        tbl = table_name.upper()
        full_table = f"{db}.{sch}.{tbl}"
        
        # Verify table exists
        try:
            session.sql(f"DESC TABLE {full_table}").collect()
        except Exception:
            raise HTTPException(400, f"Table {full_table} does not exist. Create the table first.")
        
        # Check if pipe already exists
        existing_pipe = check_pipe_exists_for_table(session, db, sch, tbl)
        if existing_pipe.get('exists'):
            return {
                "status": "exists",
                "message": f"Pipe already exists: {existing_pipe.get('pipe_name')}",
                "pipe_name": existing_pipe.get('pipe_name')
            }
        
        # Generate pipe name
        pipe_name = f"{tbl}_PIPE"
        full_pipe_name = f"{db}.{sch}.{pipe_name}"
        
        # Normalize stage reference
        stage_ref = source_stage.upper()
        if not '.' in stage_ref:
            stage_ref = f"{db}.PRODUCTION.{stage_ref}"
        
        # Create the pipe
        create_pipe_sql = f"""
        CREATE PIPE {full_pipe_name}
          AUTO_INGEST = TRUE
          COMMENT = 'Auto-created pipe for bronze table {full_table} - User opted in via FLUX Data Forge'
          AS
          COPY INTO {full_table} (RAW_DATA, FILE_NAME, LOAD_TS)
          FROM (
            SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
            FROM @{stage_ref}
          )
          FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
          PATTERN = '{file_pattern}'
        """
        
        session.sql(create_pipe_sql).collect()
        logger.info(f"User-requested pipe created: {full_pipe_name}")
        
        # Optionally refresh to pick up existing files
        files_queued = 0
        if auto_refresh:
            try:
                refresh_result = session.sql(f"ALTER PIPE {full_pipe_name} REFRESH").collect()
                files_queued = len(refresh_result)
                logger.info(f"Pipe refreshed, {files_queued} files queued for ingestion")
            except Exception as refresh_err:
                logger.warning(f"Pipe refresh warning: {refresh_err}")
        
        return {
            "status": "created",
            "pipe_name": full_pipe_name,
            "target_table": full_table,
            "source_stage": stage_ref,
            "file_pattern": file_pattern,
            "auto_ingest": True,
            "files_queued": files_queued,
            "message": f"Snowpipe created successfully. {files_queued} existing files queued for ingestion.",
            "ddl": create_pipe_sql
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create pipe: {e}")
        raise HTTPException(500, f"Failed to create pipe: {str(e)}")


@app.get("/api/warehouses")
async def list_warehouses():
    """List all accessible warehouses"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql("SHOW WAREHOUSES").collect()
        warehouses = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result
        ])
        return {"warehouses": warehouses, "count": len(warehouses)}
    except Exception as e:
        logger.warning(f"Failed to list warehouses: {e}")
        return {"warehouses": ["FLUX_WH"], "error": str(e)}


@app.get("/api/stages/{database}/{schema}")
async def list_stages_in_schema(database: str, schema: str):
    """List stages in a specific database and schema"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql(f"SHOW STAGES IN {database}.{schema}").collect()
        stages = {
            'internal': [],
            'external': []
        }
        for r in result:
            row_dict = r.asDict() if hasattr(r, 'asDict') else dict(r)
            stage_info = {
                "name": row_dict.get('name', ''),
                "type": row_dict.get('type', 'INTERNAL'),
                "url": row_dict.get('url', ''),
                "full_name": f"{database}.{schema}.{row_dict.get('name', '')}",
                "comment": row_dict.get('comment', '')
            }
            if stage_info['type'] == 'EXTERNAL':
                stages['external'].append(stage_info)
            else:
                stages['internal'].append(stage_info)
        return {"stages": stages, "database": database, "schema": schema}
    except Exception as e:
        return {"stages": {'internal': [], 'external': []}, "error": str(e)}


@app.get("/api/pipes/{database}/{schema}")
async def list_pipes(database: str, schema: str):
    """List all pipes in a schema"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql(f"SHOW PIPES IN {database}.{schema}").collect()
        pipes = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result
        ])
        return {"pipes": pipes, "database": database, "schema": schema, "count": len(pipes)}
    except Exception as e:
        return {"pipes": [], "error": str(e)}


@app.get("/api/resources")
async def list_all_resources():
    """
    Comprehensive endpoint to fetch all Snowflake resources in one call.
    Returns databases, warehouses, stages (internal & external), and current context.
    Optimized for initial page load.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    resources = {
        "databases": [],
        "warehouses": [],
        "stages": {"internal": [], "external": []},
        "current_context": {},
        "errors": []
    }
    
    # Get current context
    try:
        ctx_result = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()").collect()
        if ctx_result:
            row = ctx_result[0]
            resources["current_context"] = {
                "database": row[0],
                "schema": row[1],
                "warehouse": row[2],
                "role": row[3]
            }
    except Exception as e:
        resources["errors"].append(f"Context: {str(e)}")
    
    # Get databases
    try:
        result = session.sql("SHOW DATABASES").collect()
        resources["databases"] = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result 
            if not (r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']).startswith('SNOWFLAKE')
        ])
    except Exception as e:
        resources["errors"].append(f"Databases: {str(e)}")
        resources["databases"] = [DB]
    
    # Get warehouses
    try:
        result = session.sql("SHOW WAREHOUSES").collect()
        resources["warehouses"] = sorted([
            r.asDict().get('name', r['name']) if hasattr(r, 'asDict') else r['name']
            for r in result
        ])
    except Exception as e:
        resources["errors"].append(f"Warehouses: {str(e)}")
        resources["warehouses"] = ["FLUX_WH"]
    
    # Get stages in account
    try:
        result = session.sql("SHOW STAGES IN ACCOUNT").collect()
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            stage_info = {
                'name': row_dict.get('name', ''),
                'database': row_dict.get('database_name', ''),
                'schema': row_dict.get('schema_name', ''),
                'type': row_dict.get('type', 'INTERNAL'),
                'url': row_dict.get('url', ''),
                'full_name': f"{row_dict.get('database_name', '')}.{row_dict.get('schema_name', '')}.{row_dict.get('name', '')}"
            }
            
            # Add cloud provider info for external stages
            url = stage_info['url'] or ''
            if stage_info['type'] == 'EXTERNAL':
                if 's3://' in url.lower():
                    stage_info['cloud_provider'] = 'AWS S3'
                elif 'azure://' in url.lower():
                    stage_info['cloud_provider'] = 'Azure Blob'
                elif 'gcs://' in url.lower():
                    stage_info['cloud_provider'] = 'GCS'
                else:
                    stage_info['cloud_provider'] = 'External'
                resources["stages"]["external"].append(stage_info)
            else:
                stage_info['cloud_provider'] = 'Snowflake'
                resources["stages"]["internal"].append(stage_info)
        
        # Sort stages
        resources["stages"]["internal"].sort(key=lambda x: x['full_name'])
        resources["stages"]["external"].sort(key=lambda x: x['full_name'])
    except Exception as e:
        resources["errors"].append(f"Stages: {str(e)}")
    
    return resources


@app.get("/api/context")
async def get_current_context():
    """Get current Snowflake session context (database, schema, warehouse, role)"""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql("""
            SELECT 
                CURRENT_DATABASE() as database,
                CURRENT_SCHEMA() as schema,
                CURRENT_WAREHOUSE() as warehouse,
                CURRENT_ROLE() as role,
                CURRENT_USER() as user,
                CURRENT_ACCOUNT() as account
        """).collect()
        if result:
            row = result[0]
            return {
                "database": row['DATABASE'],
                "schema": row['SCHEMA'],
                "warehouse": row['WAREHOUSE'],
                "role": row['ROLE'],
                "user": row['USER'],
                "account": row['ACCOUNT']
            }
        return {"error": "No context available"}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# PRODUCTION DATA MATCHING ENDPOINTS
# ============================================================================

# In-memory cache for production meters (refreshed on demand)
_production_meters_cache = {
    "meters": [],
    "source": None,
    "fetched_at": None,
    "count": 0
}


@app.get("/api/production/sources")
async def get_production_data_sources():
    """Get available production data sources for meter matching"""
    sources = []
    for source_id, cfg in PRODUCTION_DATA_SOURCES.items():
        source_info = {
            "id": source_id,
            "name": cfg['name'],
            "table": cfg.get('table'),
            "description": cfg['desc'],
            "expected_count": cfg.get('count', 0),
        }
        
        # Check actual count if connected and not synthetic
        if snowflake_session and cfg.get('table'):
            try:
                result = snowflake_session.sql(f"SELECT COUNT(*) as cnt FROM {cfg['table']}").collect()
                source_info["actual_count"] = result[0]['CNT'] if result else 0
                source_info["status"] = "available"
            except Exception as e:
                source_info["status"] = "error"
                source_info["error"] = str(e)
        elif cfg.get('table') is None:
            source_info["status"] = "synthetic"
        else:
            source_info["status"] = "unknown"
        
        sources.append(source_info)
    
    return {"sources": sources}


@app.get("/api/production/meters")
async def fetch_production_meters(
    source: str = "METER_INFRASTRUCTURE",
    sample_size: int = 1000,
    segment: str = None,  # Optional: filter by RESIDENTIAL, COMMERCIAL, INDUSTRIAL
    refresh: bool = False
):
    """
    Fetch real meter IDs from production tables for streaming.
    
    This is the core production-matching feature: instead of generating
    synthetic meter IDs, we pull real meters from infrastructure tables
    so streamed data matches actual grid assets.
    
    Args:
        source: Production data source (METER_INFRASTRUCTURE, AMI_METADATA_SEARCH, SYNTHETIC)
        sample_size: Number of meters to fetch (max 100000)
        segment: Optional filter for customer segment
        refresh: Force refresh of cached meters
    
    Returns:
        List of meter objects with IDs, transformer, circuit, segment, and location
    """
    global _production_meters_cache
    
    # Clamp sample size
    sample_size = min(max(sample_size, 10), 100000)
    
    # Return cached if available and not forcing refresh
    if (not refresh and 
        _production_meters_cache["source"] == source and 
        _production_meters_cache["count"] >= sample_size and
        _production_meters_cache["meters"]):
        return {
            "status": "cached",
            "source": source,
            "count": len(_production_meters_cache["meters"][:sample_size]),
            "fetched_at": _production_meters_cache["fetched_at"],
            "meters": _production_meters_cache["meters"][:sample_size]
        }
    
    # Handle synthetic mode
    if source == "SYNTHETIC" or source not in PRODUCTION_DATA_SOURCES:
        meters = generate_synthetic_meters(sample_size, segment)
        return {
            "status": "generated",
            "source": "SYNTHETIC",
            "count": len(meters),
            "meters": meters
        }
    
    # Fetch from production table
    if not snowflake_session:
        raise HTTPException(503, "Snowflake not connected")
    
    cfg = PRODUCTION_DATA_SOURCES[source]
    if not cfg.get('table'):
        raise HTTPException(400, f"Source {source} has no table configured")
    
    try:
        # Build query based on source configuration
        segment_filter = ""
        if segment and cfg.get('segment_col'):
            safe_col = validate_identifier(cfg['segment_col'], allow_qualified=False)
            safe_segment = sanitize_sql_value(segment)
            segment_filter = f"WHERE {safe_col} = '{safe_segment}'"
        
        query = f"""
        SELECT 
            {cfg['meter_col']} as METER_ID,
            {cfg.get('transformer_col', 'NULL')} as TRANSFORMER_ID,
            {cfg.get('circuit_col', 'NULL')} as CIRCUIT_ID,
            {cfg.get('segment_col', "'RESIDENTIAL'")} as CUSTOMER_SEGMENT,
            {cfg.get('lat_col', 'NULL')} as LATITUDE,
            {cfg.get('lon_col', 'NULL')} as LONGITUDE,
            {cfg.get('substation_col', 'NULL')} as SUBSTATION_ID
        FROM {cfg['table']}
        {segment_filter}
        ORDER BY RANDOM()
        LIMIT {sample_size}
        """
        
        result = snowflake_session.sql(query).collect()
        
        meters = []
        for row in result:
            meters.append({
                "meter_id": row['METER_ID'],
                "transformer_id": row['TRANSFORMER_ID'],
                "circuit_id": row['CIRCUIT_ID'],
                "customer_segment": row['CUSTOMER_SEGMENT'] or 'RESIDENTIAL',
                "latitude": float(row['LATITUDE']) if row['LATITUDE'] else None,
                "longitude": float(row['LONGITUDE']) if row['LONGITUDE'] else None,
                "substation_id": row['SUBSTATION_ID'],
            })
        
        # Update cache
        _production_meters_cache = {
            "meters": meters,
            "source": source,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "count": len(meters)
        }
        
        logger.info(f"Fetched {len(meters)} meters from {cfg['table']}")
        
        return {
            "status": "fetched",
            "source": source,
            "table": cfg['table'],
            "count": len(meters),
            "fetched_at": _production_meters_cache["fetched_at"],
            "meters": meters
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch meters from {source}: {e}")
        return JSONResponse(
            {"status": "error", "source": source, "error": str(e)},
            status_code=500
        )


def generate_synthetic_meters(count: int, segment: str = None) -> list:
    """Generate synthetic meter data when production data is not available"""
    import random
    
    segments = ['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']
    if segment:
        segments = [segment]
    
    # Distribution: 70% residential, 20% commercial, 10% industrial
    segment_weights = {'RESIDENTIAL': 0.70, 'COMMERCIAL': 0.20, 'INDUSTRIAL': 0.10}
    
    meters = []
    for i in range(count):
        if segment:
            seg = segment
        else:
            r = random.random()
            if r < 0.70:
                seg = 'RESIDENTIAL'
            elif r < 0.90:
                seg = 'COMMERCIAL'
            else:
                seg = 'INDUSTRIAL'
        
        # Houston metro area coordinates
        lat = 29.7604 + random.uniform(-0.5, 0.5)
        lon = -95.3698 + random.uniform(-0.5, 0.5)
        
        meters.append({
            "meter_id": f"MTR-SYN-{i:06d}",
            "transformer_id": f"XFMR-SYN-{i // 10:05d}",
            "circuit_id": f"CIRCUIT-SYN-{i // 100:04d}",
            "customer_segment": seg,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "substation_id": f"SUB-SYN-{i // 1000:03d}",
        })
    
    return meters


@app.get("/api/production/cache-status")
async def get_meter_cache_status():
    """Get current meter cache status"""
    return {
        "cached": bool(_production_meters_cache["meters"]),
        "source": _production_meters_cache["source"],
        "count": _production_meters_cache["count"],
        "fetched_at": _production_meters_cache["fetched_at"],
    }


@app.get("/api/streaming/emission-patterns")
async def get_emission_patterns():
    """Get available emission patterns for realistic streaming"""
    return {"patterns": EMISSION_PATTERNS}


@app.get("/api/streaming/event-frequencies")
async def get_event_frequencies():
    """Get event frequency ratios for streaming simulation"""
    return {"frequencies": EVENT_FREQUENCIES}


@app.get("/api/streaming/calculate-metrics")
async def calculate_fleet_metrics(
    fleet_size: int = 10000,
    interval_minutes: int = 15,
    emission_pattern: str = "STAGGERED_REALISTIC"
):
    """
    Calculate derived streaming metrics based on fleet size.
    
    This is the core of the configurable fleet size feature:
    - User inputs fleet size
    - System calculates readings/min, events/min, rows/hour, etc.
    
    Args:
        fleet_size: Number of meters in the fleet
        interval_minutes: AMI reading interval (typically 15 min)
        emission_pattern: How meters report (UNIFORM, STAGGERED_REALISTIC, etc.)
    
    Returns:
        Calculated metrics for UI display
    """
    pattern = EMISSION_PATTERNS.get(emission_pattern, EMISSION_PATTERNS['STAGGERED_REALISTIC'])
    report_pct = pattern.get('meter_report_pct', 100) / 100.0
    
    # Meters that actually report each interval
    reporting_meters = int(fleet_size * report_pct)
    
    # Readings per minute (spread across interval)
    readings_per_min = reporting_meters / interval_minutes
    
    # Event calculations based on frequencies per 1000 meters
    scale = fleet_size / 1000.0
    events_per_interval = {
        event_type: int(freq * scale * report_pct)
        for event_type, freq in EVENT_FREQUENCIES.items()
    }
    
    # Total events per minute
    total_events_per_min = sum(events_per_interval.values()) / interval_minutes
    
    # Rows per hour
    rows_per_hour = reporting_meters * (60 / interval_minutes)
    
    # Data volume estimates (assuming ~500 bytes per row)
    bytes_per_row = 500
    mb_per_hour = (rows_per_hour * bytes_per_row) / (1024 * 1024)
    gb_per_day = (mb_per_hour * 24) / 1024
    
    return {
        "fleet_size": fleet_size,
        "interval_minutes": interval_minutes,
        "emission_pattern": emission_pattern,
        "pattern_description": pattern['description'],
        "metrics": {
            "reporting_meters": reporting_meters,
            "report_percentage": pattern['meter_report_pct'],
            "readings_per_min": round(readings_per_min, 1),
            "total_events_per_min": round(total_events_per_min, 1),
            "rows_per_hour": int(rows_per_hour),
            "rows_per_hour_formatted": format_number(rows_per_hour),
            "mb_per_hour": round(mb_per_hour, 2),
            "gb_per_day": round(gb_per_day, 2),
        },
        "events_per_interval": events_per_interval,
        "stagger_seconds": pattern.get('stagger_seconds', 0),
    }


@app.get("/api/streaming/preview")
async def preview_streaming_records(
    production_source: str = "METER_INFRASTRUCTURE",
    fleet_size: int = 100,
    emission_pattern: str = "STAGGERED_REALISTIC",
    segment_filter: str = None,
    data_format: str = "standard",
    sample_size: int = 10
):
    """
    Generate a preview of streaming records to verify production matching.
    
    Returns sample records showing exactly what will be streamed.
    """
    session = get_valid_session()
    if not session:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "error": "Not connected to Snowflake", "records": []}
        )
    
    try:
        # Get emission pattern config
        pattern_cfg = EMISSION_PATTERNS.get(emission_pattern, EMISSION_PATTERNS['STAGGERED_REALISTIC'])
        report_pct = pattern_cfg.get('meter_report_pct', 100)
        stagger_secs = pattern_cfg.get('stagger_seconds', 0)
        
        # Build meter source query
        if production_source == "SYNTHETIC" or production_source not in PRODUCTION_DATA_SOURCES:
            meter_source_sql = f"""
                SELECT 
                    'MTR-SYN-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::VARCHAR, 6, '0') AS METER_ID,
                    'XFMR-SYN-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 10)::VARCHAR, 5, '0') AS TRANSFORMER_ID,
                    'CIRCUIT-SYN-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 100)::VARCHAR, 4, '0') AS CIRCUIT_ID,
                    ROW_NUMBER() OVER (ORDER BY SEQ4()) AS METER_NUM,
                    CASE MOD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 10)
                        WHEN 0 THEN 'COMMERCIAL'
                        WHEN 1 THEN 'INDUSTRIAL'
                        ELSE 'RESIDENTIAL'
                    END AS CUSTOMER_SEGMENT,
                    29.7604 + UNIFORM(-0.5, 0.5, RANDOM()) AS LATITUDE,
                    -95.3698 + UNIFORM(-0.5, 0.5, RANDOM()) AS LONGITUDE,
                    'SUB-SYN-' || LPAD((ROW_NUMBER() OVER (ORDER BY SEQ4()) / 1000)::VARCHAR, 3, '0') AS SUBSTATION_ID
                FROM TABLE(GENERATOR(ROWCOUNT => {sample_size}))
            """
            production_matched = False
        else:
            src_cfg = PRODUCTION_DATA_SOURCES[production_source]
            segment_where = f"WHERE {src_cfg.get('segment_col', 'CUSTOMER_SEGMENT_ID')} = '{segment_filter}'" if segment_filter else ""
            
            meter_source_sql = f"""
                SELECT 
                    m.{src_cfg['meter_col']} AS METER_ID,
                    m.{src_cfg.get('transformer_col', 'TRANSFORMER_ID')} AS TRANSFORMER_ID,
                    m.{src_cfg.get('circuit_col', 'CIRCUIT_ID')} AS CIRCUIT_ID,
                    ROW_NUMBER() OVER (ORDER BY RANDOM()) AS METER_NUM,
                    COALESCE(m.{src_cfg.get('segment_col', "'RESIDENTIAL'")}, 'RESIDENTIAL') AS CUSTOMER_SEGMENT,
                    m.{src_cfg.get('lat_col', 'NULL')} AS LATITUDE,
                    m.{src_cfg.get('lon_col', 'NULL')} AS LONGITUDE,
                    m.{src_cfg.get('substation_col', 'NULL')} AS SUBSTATION_ID
                FROM {src_cfg['table']} m
                {segment_where}
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """
            production_matched = True
        
        # Build the preview query with all calculated columns
        if data_format == 'raw_ami':
            # Include RAW_PAYLOAD VARIANT column
            preview_sql = f"""
            WITH meter_source AS (
                {meter_source_sql}
            )
            SELECT 
                m.METER_ID,
                m.TRANSFORMER_ID,
                m.CIRCUIT_ID,
                m.SUBSTATION_ID,
                DATEADD(SECOND, 
                    UNIFORM(0, {stagger_secs}, RANDOM()),
                    DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP())
                ) AS READING_TIMESTAMP,
                ROUND(CASE 
                    WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 14 AND 19 THEN UNIFORM(1.5, 3.5, RANDOM())
                    WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 6 AND 9 THEN UNIFORM(1.0, 2.5, RANDOM())
                    ELSE UNIFORM(0.3, 1.5, RANDOM())
                END * CASE m.CUSTOMER_SEGMENT 
                    WHEN 'INDUSTRIAL' THEN 15 
                    WHEN 'COMMERCIAL' THEN 5 
                    ELSE 1 
                END, 4) AS USAGE_KWH,
                ROUND(CASE 
                    WHEN UNIFORM(1, 100, RANDOM()) <= 2 THEN UNIFORM(105, 110, RANDOM())
                    WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN UNIFORM(128, 135, RANDOM())
                    ELSE NORMAL(120, 2, RANDOM()) 
                END, 2) AS VOLTAGE,
                ROUND(UNIFORM(0.92, 0.99, RANDOM()), 3) AS POWER_FACTOR,
                m.CUSTOMER_SEGMENT,
                m.LATITUDE,
                m.LONGITUDE,
                CASE 
                    WHEN UNIFORM(1, 100, RANDOM()) <= 1 THEN 'OUTAGE' 
                    WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN 'ANOMALY' 
                    ELSE 'VALID' 
                END AS DATA_QUALITY,
                {str(production_matched).upper()} AS PRODUCTION_MATCHED,
                '{emission_pattern}' AS EMISSION_PATTERN,
                OBJECT_CONSTRUCT(
                    'meter_id', m.METER_ID,
                    'timestamp', CURRENT_TIMESTAMP()::VARCHAR,
                    'usage_kwh', ROUND(UNIFORM(0.5, 3.0, RANDOM()), 4),
                    'voltage', ROUND(NORMAL(120, 2, RANDOM()), 2),
                    'transformer_id', m.TRANSFORMER_ID,
                    'customer_segment', m.CUSTOMER_SEGMENT,
                    'latitude', m.LATITUDE,
                    'longitude', m.LONGITUDE
                ) AS RAW_PAYLOAD
            FROM meter_source m
            WHERE UNIFORM(1, 100, RANDOM()) <= {report_pct}
            """
        else:
            # Standard format without VARIANT
            preview_sql = f"""
            WITH meter_source AS (
                {meter_source_sql}
            )
            SELECT 
                m.METER_ID,
                m.TRANSFORMER_ID,
                m.CIRCUIT_ID,
                m.SUBSTATION_ID,
                DATEADD(SECOND, 
                    UNIFORM(0, {stagger_secs}, RANDOM()),
                    DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP())
                ) AS READING_TIMESTAMP,
                ROUND(CASE 
                    WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 14 AND 19 THEN UNIFORM(1.5, 3.5, RANDOM())
                    WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 6 AND 9 THEN UNIFORM(1.0, 2.5, RANDOM())
                    ELSE UNIFORM(0.3, 1.5, RANDOM())
                END * CASE m.CUSTOMER_SEGMENT 
                    WHEN 'INDUSTRIAL' THEN 15 
                    WHEN 'COMMERCIAL' THEN 5 
                    ELSE 1 
                END, 4) AS USAGE_KWH,
                ROUND(CASE 
                    WHEN UNIFORM(1, 100, RANDOM()) <= 2 THEN UNIFORM(105, 110, RANDOM())
                    WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN UNIFORM(128, 135, RANDOM())
                    ELSE NORMAL(120, 2, RANDOM()) 
                END, 2) AS VOLTAGE,
                ROUND(UNIFORM(0.92, 0.99, RANDOM()), 3) AS POWER_FACTOR,
                m.CUSTOMER_SEGMENT,
                m.LATITUDE,
                m.LONGITUDE,
                CASE 
                    WHEN UNIFORM(1, 100, RANDOM()) <= 1 THEN 'OUTAGE' 
                    WHEN UNIFORM(1, 100, RANDOM()) >= 98 THEN 'ANOMALY' 
                    ELSE 'VALID' 
                END AS DATA_QUALITY,
                {str(production_matched).upper()} AS PRODUCTION_MATCHED,
                '{emission_pattern}' AS EMISSION_PATTERN
            FROM meter_source m
            WHERE UNIFORM(1, 100, RANDOM()) <= {report_pct}
            """
        
        # Execute preview query with timeout protection
        logger.info(f"Executing preview query for {production_source}, format={data_format}")
        result = session.sql(preview_sql).collect()
        
        # Convert to list of dicts for JSON response
        records = []
        for row in result:
            record = {}
            for key in row.asDict().keys():
                val = row[key]
                # Handle special types
                if hasattr(val, 'isoformat'):
                    record[key] = val.isoformat()
                elif isinstance(val, (dict, list)):
                    record[key] = val
                else:
                    record[key] = val
            records.append(record)
        
        return {
            "status": "success",
            "production_source": production_source,
            "production_matched": production_matched,
            "emission_pattern": emission_pattern,
            "data_format": data_format,
            "sample_size": len(records),
            "records": records,
            "schema": list(records[0].keys()) if records else [],
            "notes": [
                f"{'Production-matched' if production_matched else 'Synthetic'} meter IDs from {production_source}",
                f"Emission pattern: {pattern_cfg['name']} ({report_pct}% reporting)",
                f"Stagger: {stagger_secs} seconds spread across interval",
                f"Data format: {DATA_FORMATS.get(data_format, DATA_FORMATS['standard'])['name']}"
            ]
        }
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Preview generation failed: {e}\n{error_details}")
        return JSONResponse(
            status_code=200,  # Return 200 with error in body for JS handling
            content={
                "status": "error",
                "error": f"Preview failed: {str(e)}",
                "records": []
            }
        )


@app.get("/api/monitor/metrics")
async def get_monitor_metrics():
    """
     API endpoint for AJAX-based monitor page refresh.
    Returns current streaming metrics without requiring full page reload.
    Preserves user state (like stage selection) while keeping data fresh.
    """
    active_streams = 0
    total_rows = 0
    throughput = "--"
    task_count = 0
    snowpipe_count = 0
    recent_rows_1h = 0
    
    # Calculate health status
    stream_health = "IDLE"
    health_color = "#64748b"
    health_icon = "pause_circle"
    health_detail = "No active streaming jobs"
    
    try:
        session = get_valid_session()
        if session:
            # Count active tasks
            try:
                result = session.sql(f"""
                    SHOW TASKS LIKE '%AMI_STREAMING%' IN SCHEMA {DB}.{SCHEMA_PRODUCTION}
                """).collect()
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    if row_dict.get('state', '').lower() == 'started':
                        task_count += 1
            except Exception:
                pass
            
            # Count active SDK jobs from memory
            with streaming_lock:
                for jid, jdata in active_streaming_jobs.items():
                    if jdata['status'] in ['RUNNING', 'STARTING']:
                        snowpipe_count += 1
            
            active_streams = task_count + snowpipe_count
            
            # Get data metrics
            for table_name in ['AMI_STREAMING_DATA', 'AMI_STREAMING_READINGS', 'AMI_STREAMING_READINGS_TEXAS_GULF_COAST', 'AMI_STREAMING_READINGS_HOUSTON_METRO']:
                try:
                    result = session.sql(f"""
                        SELECT COUNT(*) as cnt FROM {DB}.{SCHEMA_PRODUCTION}.{table_name}
                        WHERE CREATED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                    """).collect()
                    recent_rows_1h += result[0]['CNT'] if result else 0
                    
                    result = session.sql(f"""
                        SELECT COUNT(*) as cnt FROM {DB}.{SCHEMA_PRODUCTION}.{table_name}
                    """).collect()
                    total_rows += result[0]['CNT'] if result else 0
                except Exception:
                    pass
            
            throughput = f"{recent_rows_1h / 3600:.1f}" if recent_rows_1h > 0 else "--"
            
            # Calculate health status
            if active_streams > 0:
                most_recent_batch = None
                has_active_batching = False
                with streaming_lock:
                    for jid, jdata in active_streaming_jobs.items():
                        if jdata['status'] in ['RUNNING', 'STARTING']:
                            stats = jdata.get('stats', {})
                            last_batch = stats.get('last_batch_time')
                            if last_batch:
                                if most_recent_batch is None or last_batch > most_recent_batch:
                                    most_recent_batch = last_batch
                            if stats.get('total_rows', 0) > 0 or stats.get('batches_sent', 0) > 0:
                                has_active_batching = True
                
                from datetime import datetime
                now = datetime.now()
                
                if most_recent_batch:
                    time_since_batch = (now - most_recent_batch).total_seconds()
                    if time_since_batch < 60:
                        stream_health = "HEALTHY"
                        health_color = "#22c55e"
                        health_icon = "check_circle"
                        health_detail = f"Data flowing ({int(time_since_batch)}s since last batch)"
                    elif time_since_batch < 300:
                        stream_health = "BUFFERING"
                        health_color = "#f59e0b"
                        health_icon = "hourglass_top"
                        health_detail = f"Buffering data ({int(time_since_batch)}s since last batch)"
                    else:
                        stream_health = "STALLED"
                        health_color = "#ef4444"
                        health_icon = "warning"
                        health_detail = f"No data for {int(time_since_batch//60)}+ minutes"
                elif has_active_batching:
                    stream_health = "STARTING"
                    health_color = "#38bdf8"
                    health_icon = "play_circle"
                    health_detail = "Stream starting, building first batch..."
                else:
                    stream_health = "WAITING"
                    health_color = "#a855f7"
                    health_icon = "schedule"
                    health_detail = "Jobs active, awaiting first data"
    
    except Exception as e:
        logger.error(f"Monitor metrics API error: {e}")
    
    # Collect grid topology summary from active jobs
    grid_topology = None
    with streaming_lock:
        for jid, jdata in active_streaming_jobs.items():
            if jdata['status'] in ['RUNNING', 'STARTING']:
                topo = jdata.get('stats', {}).get('grid_topology')
                if topo:
                    grid_topology = topo
                    break  # Use first active job's topology
    
    return {
        "active_streams": active_streams,
        "task_count": task_count,
        "snowpipe_count": snowpipe_count,
        "throughput": throughput,
        "total_rows": total_rows,
        "total_rows_formatted": format_number(total_rows),
        "recent_rows_1h": recent_rows_1h,
        "health": {
            "status": stream_health,
            "color": health_color,
            "icon": health_icon,
            "detail": health_detail
        },
        "grid_topology": grid_topology,
    }


@app.get("/api/external-stage/diagnostics")
async def external_stage_diagnostics():
    """
    PATTERN: Pre-flight diagnostics for external stage streaming.
    
    This endpoint validates the ENTIRE external stage streaming path BEFORE a user
    starts a job, providing clear feedback on what's working and what's broken.
    
    Day 2 Operations Value:
    - No more "silent failures" - know immediately if credentials are invalid
    - Self-documenting - shows exactly what the system is checking
    - Actionable feedback - tells you HOW to fix each problem
    - Demo-ready - customers can verify their setup works before going live
    
    Checks performed:
    1. boto3 SDK availability
    2. AWS credentials present in environment
    3. Role assumption (STS AssumeRole) works
    4. S3 bucket access (HeadBucket)
    5. S3 write permission (test file upload)
    6. External stage metadata readable
    7. Snowpipe configured (if applicable)
    """
    diagnostics = {
        "timestamp": datetime.now().isoformat(),
        "overall_status": "CHECKING",
        "checks": [],
        "summary": "",
        "action_required": None
    }
    
    all_passed = True
    
    # Check 1: boto3 availability
    check1 = {
        "name": "boto3 SDK",
        "description": "Python AWS SDK required for S3 writes",
        "status": "PASS" if BOTO3_AVAILABLE else "FAIL",
        "detail": "boto3 imported successfully" if BOTO3_AVAILABLE else "boto3 not installed in container"
    }
    if not BOTO3_AVAILABLE:
        check1["fix"] = "Add boto3>=1.34.0 to requirements.txt and rebuild container"
        all_passed = False
    diagnostics["checks"].append(check1)
    
    # Check 2: AWS credentials in environment
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    aws_role_arn = os.getenv('AWS_ROLE_ARN', '')
    
    has_creds = bool(aws_access_key and aws_secret_key)
    check2 = {
        "name": "AWS Credentials",
        "description": "IAM credentials available in environment",
        "status": "PASS" if has_creds else "FAIL",
        "detail": f"Access Key: {aws_access_key[:8]}...{aws_access_key[-4:]}" if has_creds else "No AWS credentials found"
    }
    if not has_creds:
        check2["fix"] = "Update Snowflake secret FLUX_DB.PRODUCTION.AWS_S3_CREDENTIALS with valid IAM user credentials"
        all_passed = False
    diagnostics["checks"].append(check2)
    
    # Check 3: Role ARN configured
    check3 = {
        "name": "AWS Role ARN",
        "description": "IAM role ARN for S3 bucket access",
        "status": "PASS" if aws_role_arn else "WARN",
        "detail": f"Role: {aws_role_arn}" if aws_role_arn else "No role ARN - will use direct credentials"
    }
    diagnostics["checks"].append(check3)
    
    # Check 4: STS AssumeRole (if role configured)
    s3_client = None
    if BOTO3_AVAILABLE and has_creds:
        if aws_role_arn:
            try:
                sts_client = boto3.client(
                    'sts',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name='us-west-2'
                )
                assumed_role = sts_client.assume_role(
                    RoleArn=aws_role_arn,
                    RoleSessionName='flux-diagnostics-check'
                )
                creds = assumed_role['Credentials']
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=creds['AccessKeyId'],
                    aws_secret_access_key=creds['SecretAccessKey'],
                    aws_session_token=creds['SessionToken'],
                    region_name='us-west-2'
                )
                check4 = {
                    "name": "Role Assumption",
                    "description": "STS AssumeRole for temporary S3 credentials",
                    "status": "PASS",
                    "detail": f"Successfully assumed role, expires: {creds['Expiration'].isoformat()}"
                }
            except Exception as e:
                check4 = {
                    "name": "Role Assumption",
                    "description": "STS AssumeRole for temporary S3 credentials",
                    "status": "FAIL",
                    "detail": str(e),
                    "fix": f"Verify IAM user can assume role. Check trust policy on {aws_role_arn}"
                }
                all_passed = False
            diagnostics["checks"].append(check4)
        else:
            # Direct credentials without role
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name='us-west-2'
            )
    
    # Check 5: External stage metadata
    stage_name = f"{DB}.{SCHEMA_PRODUCTION}.EXT_RAW_AMI"
    s3_bucket = None
    s3_prefix = ""
    
    try:
        session = get_valid_session()
        if session:
            result = session.sql(f"DESC STAGE {stage_name}").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                prop_name = row_dict.get('property', row_dict.get('PROPERTY', ''))
                prop_val = row_dict.get('property_value', row_dict.get('PROPERTY_VALUE', ''))
                
                if prop_name.upper() == 'URL':
                    # Handle JSON array format: ["s3://bucket/prefix/"]
                    url_value = prop_val
                    if url_value.startswith('[') and url_value.endswith(']'):
                        try:
                            import json
                            urls = json.loads(url_value)
                            if urls and len(urls) > 0:
                                url_value = urls[0]
                        except Exception:
                            # Try simple string extraction
                            url_value = url_value.strip('[]"\'')
                    
                    if url_value.startswith('s3://'):
                        parts = url_value[5:].rstrip('/').split('/', 1)
                        s3_bucket = parts[0]
                        s3_prefix = parts[1] + '/' if len(parts) > 1 else ''
            
            check5 = {
                "name": "External Stage Metadata",
                "description": f"Stage {stage_name} configuration",
                "status": "PASS" if s3_bucket else "FAIL",
                "detail": f"s3://{s3_bucket}/{s3_prefix}" if s3_bucket else "Could not parse S3 URL from stage"
            }
            if not s3_bucket:
                check5["fix"] = f"Verify external stage {stage_name} exists and has valid S3 URL"
                all_passed = False
    except Exception as e:
        check5 = {
            "name": "External Stage Metadata",
            "description": f"Stage {stage_name} configuration",
            "status": "FAIL",
            "detail": str(e),
            "fix": f"Verify external stage {stage_name} exists in Snowflake"
        }
        all_passed = False
    diagnostics["checks"].append(check5)
    
    # Check 6: S3 bucket access
    if s3_client and s3_bucket:
        try:
            s3_client.head_bucket(Bucket=s3_bucket)
            check6 = {
                "name": "S3 Bucket Access",
                "description": f"HeadBucket on {s3_bucket}",
                "status": "PASS",
                "detail": "Bucket exists and is accessible"
            }
        except Exception as e:
            check6 = {
                "name": "S3 Bucket Access",
                "description": f"HeadBucket on {s3_bucket}",
                "status": "FAIL",
                "detail": str(e),
                "fix": f"Verify IAM role has s3:ListBucket permission on {s3_bucket}"
            }
            all_passed = False
        diagnostics["checks"].append(check6)
    
    # Check 7: S3 write permission (test file)
    if s3_client and s3_bucket:
        test_key = f"{s3_prefix}_diagnostics_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        put_succeeded = False
        try:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=test_key,
                Body=b"FLUX Data Forge diagnostics test - safe to delete",
                ContentType='text/plain'
            )
            put_succeeded = True
            # Try to clean up test file (optional - don't fail if delete is denied)
            try:
                s3_client.delete_object(Bucket=s3_bucket, Key=test_key)
                check7 = {
                    "name": "S3 Write Permission",
                    "description": "PutObject and DeleteObject test",
                    "status": "PASS",
                    "detail": f"Successfully wrote and deleted test file at {test_key}"
                }
            except Exception as del_e:
                # Delete failed but write succeeded - that's OK for streaming
                check7 = {
                    "name": "S3 Write Permission",
                    "description": "PutObject test",
                    "status": "PASS",
                    "detail": f"Successfully wrote test file at {test_key} (cleanup skipped - delete permission not available, but not required for streaming)"
                }
        except Exception as e:
            check7 = {
                "name": "S3 Write Permission",
                "description": "PutObject test",
                "status": "FAIL",
                "detail": str(e),
                "fix": f"Verify IAM role has s3:PutObject permission on s3://{s3_bucket}/{s3_prefix}*"
            }
            all_passed = False
        diagnostics["checks"].append(check7)
    
    # Check 8: Snowpipe configuration for this stage
    pipes_for_stage = []
    try:
        session = get_valid_session()
        if session:
            result = session.sql("SHOW PIPES IN ACCOUNT").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                definition = row_dict.get('definition', '')
                # Check if this pipe references our external stage
                if stage_name.upper() in definition.upper() or (s3_bucket and s3_bucket in definition):
                    pipe_full_name = f"{row_dict.get('database_name', '')}.{row_dict.get('schema_name', '')}.{row_dict.get('name', '')}"
                    pipes_for_stage.append({
                        'name': pipe_full_name,
                        'notification_channel': row_dict.get('notification_channel', ''),
                        'owner': row_dict.get('owner', ''),
                        'auto_ingest': 'AUTO_INGEST' in definition.upper()
                    })
            
            if pipes_for_stage:
                check8 = {
                    "name": "Snowpipe Configuration",
                    "description": f"Pipes reading from {stage_name}",
                    "status": "PASS",
                    "detail": f"Found {len(pipes_for_stage)} pipe(s): {', '.join(p['name'] for p in pipes_for_stage)}",
                    "pipes": pipes_for_stage
                }
            else:
                check8 = {
                    "name": "Snowpipe Configuration",
                    "description": f"Pipes reading from {stage_name}",
                    "status": "WARN",
                    "detail": "No Snowpipe found for this stage. Data will land in S3 but won't auto-ingest to Snowflake.",
                    "fix": "Create a Snowpipe to auto-ingest files from this stage into a target table",
                    "pipes": [],
                    "can_create_pipe": True
                }
    except Exception as e:
        check8 = {
            "name": "Snowpipe Configuration",
            "description": "Checking for pipes",
            "status": "WARN",
            "detail": f"Could not check pipes: {str(e)}",
            "pipes": []
        }
    diagnostics["checks"].append(check8)
    
    # Add stage info for pipe creation UI
    diagnostics["stage_info"] = {
        "name": stage_name,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "pipes": pipes_for_stage
    }
    
    # Summary
    passed = sum(1 for c in diagnostics["checks"] if c["status"] == "PASS")
    warned = sum(1 for c in diagnostics["checks"] if c["status"] == "WARN")
    failed = sum(1 for c in diagnostics["checks"] if c["status"] == "FAIL")
    total = len(diagnostics["checks"])
    
    if failed == 0:
        if warned > 0:
            diagnostics["overall_status"] = "READY_WITH_WARNINGS"
            diagnostics["summary"] = f"{passed}/{total} passed, {warned} warning(s). External stage streaming works, but review warnings."
        else:
            diagnostics["overall_status"] = "READY"
            diagnostics["summary"] = f"All {passed}/{total} checks passed. External stage streaming is ready!"
    else:
        diagnostics["overall_status"] = "NOT_READY"
        failed_checks = [c["name"] for c in diagnostics["checks"] if c["status"] == "FAIL"]
        diagnostics["summary"] = f"{passed}/{total} checks passed. Failed: {', '.join(failed_checks)}"
        diagnostics["action_required"] = "Review failed checks above and apply suggested fixes"
    
    return diagnostics


@app.get("/api/stages")
async def list_stages():
    """
    List available stages for streaming destinations.
    Intelligently categorizes internal vs external stages and provides useful metadata.
    Uses preloaded cache for instant response when available.
    """
    global dependency_cache
    
    #  Use cached data if available for instant response
    with dependency_cache['lock']:
        if dependency_cache['stages'] is not None:
            cached_stages = dependency_cache['stages']
            internal_count = len(cached_stages.get('internal', []))
            external_count = len(cached_stages.get('external', []))
            logger.info(f"list_stages: Returning {internal_count} internal, {external_count} external stages from cache (instant)")
            return {
                "stages": cached_stages,
                "total_internal": internal_count,
                "total_external": external_count,
                "total": internal_count + external_count,
                "cached": True
            }
    
    session = get_valid_session()
    if not session:
        logger.error("list_stages: Failed to get valid Snowflake session")
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        # Get stages in current account - use SHOW STAGES for broader visibility
        logger.info("list_stages: Fetching stages from account...")
        result = session.sql("SHOW STAGES IN ACCOUNT").collect()
        logger.info(f"list_stages: Found {len(result)} stages")
        
        stages = {
            'internal': [],
            'external': []
        }
        
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            stage_info = {
                'name': row_dict.get('name', ''),
                'database': row_dict.get('database_name', ''),
                'schema': row_dict.get('schema_name', ''),
                'type': row_dict.get('type', 'INTERNAL'),
                'url': row_dict.get('url', ''),
                'owner': row_dict.get('owner', ''),
                'comment': row_dict.get('comment', ''),
            }
            
            # Build full qualified name
            full_name = f"{stage_info['database']}.{stage_info['schema']}.{stage_info['name']}"
            stage_info['full_name'] = full_name
            
            # Determine cloud provider for external stages
            url = stage_info['url'] or ''
            if stage_info['type'] == 'EXTERNAL':
                if 's3://' in url.lower():
                    stage_info['cloud_provider'] = 'AWS S3'
                elif 'azure://' in url.lower() or 'blob.core.windows.net' in url.lower():
                    stage_info['cloud_provider'] = 'Azure Blob'
                elif 'gcs://' in url.lower() or 'storage.googleapis.com' in url.lower():
                    stage_info['cloud_provider'] = 'Google Cloud Storage'
                else:
                    stage_info['cloud_provider'] = 'External'
                stages['external'].append(stage_info)
            else:
                stage_info['cloud_provider'] = 'Snowflake Internal'
                stages['internal'].append(stage_info)
        
        # Sort by name for consistent ordering
        stages['internal'].sort(key=lambda x: x['full_name'])
        stages['external'].sort(key=lambda x: x['full_name'])
        
        logger.info(f"list_stages: Returning {len(stages['internal'])} internal, {len(stages['external'])} external stages")
        return {
            "stages": stages,
            "total_internal": len(stages['internal']),
            "total_external": len(stages['external']),
            "total": len(stages['internal']) + len(stages['external'])
        }
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"list_stages: Failed to list stages: {e}\n{error_details}")
        return {
            "stages": {'internal': [], 'external': []},
            "total_internal": 0,
            "total_external": 0,
            "total": 0,
            "error": str(e)
        }


@app.get("/api/stage/preview/{stage_name:path}")
async def preview_stage_files(stage_name: str, limit: int = 10):
    """
    Live preview of files landing in a stage - S3 Select-style querying.
     Critical for demonstrating raw data landing before transformation.
    
    This queries staged files directly using Snowflake's SELECT FROM @stage syntax,
    similar to AWS S3 Select functionality.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        # Ensure stage name is properly formatted
        if not stage_name.startswith('@'):
            stage_name = f"@{stage_name}"
        
        # First, list files in the stage to show file metadata
        #  Sort by last_modified DESC to show most recent files first
        files_result = []
        try:
            list_query = f"LIST {stage_name}"
            list_rows = session.sql(list_query).collect()
            
            # Parse and sort by last_modified timestamp (descending = newest first)
            file_list = []
            for row in list_rows:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                file_list.append({
                    'name': row_dict.get('name', ''),
                    'size': row_dict.get('size', 0),
                    'md5': row_dict.get('md5', ''),
                    'last_modified': str(row_dict.get('last_modified', ''))
                })
            
            # Sort by last_modified descending (most recent first)
            file_list.sort(key=lambda x: x['last_modified'], reverse=True)
            files_result = file_list[:20]  # Limit to first 20 most recent files
            
        except Exception as list_err:
            logger.warning(f"Could not list stage files: {list_err}")
        
        # Now query the actual data from staged files (S3 Select equivalent)
        #  Use FILES parameter to query from the most recent files we found
        preview_data = []
        query_method = None
        
        # Get the most recent file name(s) to query from
        recent_files_pattern = None
        if files_result:
            # Take up to 3 most recent files
            recent_file_names = [f['name'].split('/')[-1] for f in files_result[:3]]
            if recent_file_names:
                recent_files_pattern = recent_file_names
        
        # Try JSON format query
        #  Handle BOTH flat schema (old ami_data files) AND nested schema (new ami_stream files)
        try:
            # If we have recent files, use PATTERN to query only those files
            if recent_files_pattern and len(recent_files_pattern) > 0:
                first_recent_file = recent_files_pattern[0]
                # Determine schema based on filename prefix
                if 'ami_stream' in first_recent_file:
                    # New nested schema: {meter: {meter_id}, reading: {usage_kwh, voltage}, quality: {data_quality}}
                    json_query = f"""
                        SELECT 
                            $1:meter:meter_id::VARCHAR AS METER_ID,
                            $1:reading:timestamp::TIMESTAMP AS READING_TIMESTAMP,
                            $1:reading:usage_kwh::FLOAT AS USAGE_KWH,
                            $1:reading:voltage::FLOAT AS VOLTAGE,
                            $1:meter:customer_segment::VARCHAR AS CUSTOMER_SEGMENT,
                            $1:quality:data_quality::VARCHAR AS DATA_QUALITY,
                            $1:meter:service_area::VARCHAR AS SERVICE_AREA,
                            METADATA$FILENAME AS SOURCE_FILE,
                            METADATA$FILE_ROW_NUMBER AS ROW_NUM
                        FROM {stage_name} (FILE_FORMAT => '{DB}.{SCHEMA_PRODUCTION}.FF_JSON', PATTERN => '.*{first_recent_file}.*')
                        LIMIT {limit}
                    """
                else:
                    # Old flat schema: {METER_ID, USAGE_KWH, VOLTAGE, ...}
                    json_query = f"""
                        SELECT 
                            $1:METER_ID::VARCHAR AS METER_ID,
                            $1:READING_TIMESTAMP::TIMESTAMP AS READING_TIMESTAMP,
                            $1:USAGE_KWH::FLOAT AS USAGE_KWH,
                            $1:VOLTAGE::FLOAT AS VOLTAGE,
                            $1:CUSTOMER_SEGMENT::VARCHAR AS CUSTOMER_SEGMENT,
                            $1:DATA_QUALITY::VARCHAR AS DATA_QUALITY,
                            $1:SERVICE_AREA::VARCHAR AS SERVICE_AREA,
                            METADATA$FILENAME AS SOURCE_FILE,
                            METADATA$FILE_ROW_NUMBER AS ROW_NUM
                        FROM {stage_name} (FILE_FORMAT => '{DB}.{SCHEMA_PRODUCTION}.FF_JSON', PATTERN => '.*{first_recent_file}.*')
                        LIMIT {limit}
                    """
            else:
                json_query = f"""
                    SELECT 
                        $1:METER_ID::VARCHAR AS METER_ID,
                        $1:READING_TIMESTAMP::TIMESTAMP AS READING_TIMESTAMP,
                        $1:USAGE_KWH::FLOAT AS USAGE_KWH,
                        $1:VOLTAGE::FLOAT AS VOLTAGE,
                        $1:CUSTOMER_SEGMENT::VARCHAR AS CUSTOMER_SEGMENT,
                        $1:DATA_QUALITY::VARCHAR AS DATA_QUALITY,
                        $1:SERVICE_AREA::VARCHAR AS SERVICE_AREA,
                        METADATA$FILENAME AS SOURCE_FILE,
                        METADATA$FILE_ROW_NUMBER AS ROW_NUM
                    FROM {stage_name} (FILE_FORMAT => '{DB}.{SCHEMA_PRODUCTION}.FF_JSON')
                    LIMIT {limit}
                """
            result = session.sql(json_query).collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                preview_data.append({
                    'meter_id': row_dict.get('METER_ID', '-'),
                    'reading_timestamp': str(row_dict.get('READING_TIMESTAMP', '-'))[:19],
                    'usage_kwh': round(row_dict.get('USAGE_KWH', 0) or 0, 4),
                    'voltage': round(row_dict.get('VOLTAGE', 0) or 0, 2),
                    'customer_segment': row_dict.get('CUSTOMER_SEGMENT', '-'),
                    'data_quality': row_dict.get('DATA_QUALITY', '-'),
                    'service_area': row_dict.get('SERVICE_AREA', '-'),
                    'source_file': row_dict.get('SOURCE_FILE', ''),
                    'row_num': row_dict.get('ROW_NUM', 0)
                })
            query_method = 'JSON'
        except Exception as json_err:
            logger.debug(f"JSON query failed, trying raw: {json_err}")
            
            # Fallback: Try raw variant query
            try:
                raw_query = f"""
                    SELECT 
                        $1 AS RAW_DATA,
                        METADATA$FILENAME AS SOURCE_FILE,
                        METADATA$FILE_ROW_NUMBER AS ROW_NUM
                    FROM {stage_name}
                    LIMIT {limit}
                """
                result = session.sql(raw_query).collect()
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    raw_data = row_dict.get('RAW_DATA', {})
                    if isinstance(raw_data, str):
                        try:
                            import json
                            raw_data = json.loads(raw_data)
                        except Exception:
                            pass
                    
                    preview_data.append({
                        'meter_id': raw_data.get('METER_ID', '-') if isinstance(raw_data, dict) else '-',
                        'reading_timestamp': str(raw_data.get('READING_TIMESTAMP', '-'))[:19] if isinstance(raw_data, dict) else '-',
                        'usage_kwh': round(raw_data.get('USAGE_KWH', 0) or 0, 4) if isinstance(raw_data, dict) else 0,
                        'voltage': round(raw_data.get('VOLTAGE', 0) or 0, 2) if isinstance(raw_data, dict) else 0,
                        'customer_segment': raw_data.get('CUSTOMER_SEGMENT', '-') if isinstance(raw_data, dict) else '-',
                        'data_quality': raw_data.get('DATA_QUALITY', '-') if isinstance(raw_data, dict) else '-',
                        'service_area': raw_data.get('SERVICE_AREA', '-') if isinstance(raw_data, dict) else '-',
                        'source_file': row_dict.get('SOURCE_FILE', ''),
                        'row_num': row_dict.get('ROW_NUM', 0),
                        'raw_preview': str(raw_data)[:200] if not isinstance(raw_data, dict) else None
                    })
                query_method = 'RAW'
            except Exception as raw_err:
                logger.warning(f"Raw query also failed: {raw_err}")
        
        return {
            "stage_name": stage_name,
            "files": files_result,
            "file_count": len(files_result),
            "preview_data": preview_data,
            "record_count": len(preview_data),
            "query_method": query_method,
            "status": "success" if preview_data or files_result else "empty"
        }
        
    except Exception as e:
        logger.error(f"Stage preview failed: {e}")
        return {
            "stage_name": stage_name,
            "files": [],
            "preview_data": [],
            "error": str(e),
            "status": "error"
        }


@app.get("/api/storage-integrations")
async def list_storage_integrations():
    """
    List available storage integrations for external stages.
    These are used to connect Snowflake to cloud storage (S3, Azure, GCS).
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        result = session.sql("SHOW STORAGE INTEGRATIONS").collect()
        
        integrations = []
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            integ_info = {
                'name': row_dict.get('name', ''),
                'type': row_dict.get('type', 'EXTERNAL_STAGE'),
                'category': row_dict.get('category', ''),
                'enabled': row_dict.get('enabled', 'true'),
                'comment': row_dict.get('comment', ''),
            }
            # Only include enabled storage integrations
            if integ_info['enabled'].lower() == 'true':
                integrations.append(integ_info)
        
        # Sort by name
        integrations.sort(key=lambda x: x['name'])
        
        return {
            "integrations": integrations,
            "total": len(integrations)
        }
    except Exception as e:
        logger.warning(f"Failed to list storage integrations: {e}")
        return {
            "integrations": [],
            "total": 0,
            "error": str(e)
        }


@app.get("/api/pipes")
async def list_pipes():
    """
    List available Snowpipe objects for auto-ingestion.
    PATTERN: Check multiple schemas (PRODUCTION, DEV) to ensure all user pipes are visible.
    Uses preloaded cache for instant response when available.
    """
    global dependency_cache
    
    #  Use cached data if available for instant response
    with dependency_cache['lock']:
        if dependency_cache['pipes'] is not None:
            cached_pipes = dependency_cache['pipes']
            logger.info(f"list_pipes: Returning {len(cached_pipes)} pipes from cache (instant)")
            return {
                "pipes": cached_pipes,
                "count": len(cached_pipes),
                "total": len(cached_pipes),
                "cached": True
            }
    
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    try:
        pipes = []
        seen_pipes = set()  # Track full_name to avoid duplicates
        
        #  Check multiple schemas explicitly to ensure DEV pipes are included
        schemas_to_check = [
            f"{DB}.PRODUCTION",
            f"{DB}.DEV"
        ]
        
        for schema_path in schemas_to_check:
            try:
                logger.info(f"list_pipes: Checking schema {schema_path}")
                result = session.sql(f"SHOW PIPES IN SCHEMA {schema_path}").collect()
                logger.info(f"list_pipes: Found {len(result)} pipes in {schema_path}")
                
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    pipe_info = {
                        'name': row_dict.get('name', ''),
                        'database': row_dict.get('database_name', ''),
                        'schema': row_dict.get('schema_name', ''),
                        'definition': row_dict.get('definition', ''),
                        'owner': row_dict.get('owner', ''),
                        'notification_channel': row_dict.get('notification_channel', ''),
                        'comment': row_dict.get('comment', ''),
                    }
                    # Build full qualified name
                    full_name = f"{pipe_info['database']}.{pipe_info['schema']}.{pipe_info['name']}"
                    pipe_info['full_name'] = full_name
                    
                    # Skip duplicates
                    if full_name in seen_pipes:
                        continue
                    seen_pipes.add(full_name)
                    
                    # Determine if it's an external stage pipe
                    definition = pipe_info.get('definition', '').upper()
                    pipe_info['is_external'] = any(x in definition for x in ['S3://', 'AZURE://', 'GCS://'])
                    pipe_info['auto_ingest'] = 'AUTO_INGEST' in definition
                    
                    pipes.append(pipe_info)
            except Exception as schema_err:
                logger.warning(f"list_pipes: Could not list pipes in {schema_path}: {schema_err}")
                continue
        
        # Sort by schema then name
        pipes.sort(key=lambda x: (x['schema'], x['name']))
        
        logger.info(f"list_pipes: Returning {len(pipes)} total pipes from {len(schemas_to_check)} schemas")
        return {
            "pipes": pipes,
            "count": len(pipes),
            "total": len(pipes)
        }
    except Exception as e:
        logger.error(f"Failed to list pipes: {e}")
        # Try account-level as fallback
        try:
            result = session.sql("SHOW PIPES IN ACCOUNT").collect()
            pipes = []
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                pipe_info = {
                    'name': row_dict.get('name', ''),
                    'database': row_dict.get('database_name', ''),
                    'schema': row_dict.get('schema_name', ''),
                    'definition': row_dict.get('definition', ''),
                    'owner': row_dict.get('owner', ''),
                }
                full_name = f"{pipe_info['database']}.{pipe_info['schema']}.{pipe_info['name']}"
                pipe_info['full_name'] = full_name
                pipes.append(pipe_info)
            pipes.sort(key=lambda x: x['name'])
            return {"pipes": pipes, "total": len(pipes), "count": len(pipes)}
        except Exception:
            return {
                "pipes": [],
                "total": 0,
                "error": str(e)
            }


# Helper function for validating Snowflake identifiers
def validate_snowflake_identifier(name: str, identifier_type: str = "identifier") -> str:
    """
    Validate a Snowflake identifier to prevent SQL injection.
    Only allows alphanumeric characters and underscores.
    """
    import re
    # Remove leading @ for stages
    clean_name = name.lstrip('@')
    # Allow database.schema.object format
    parts = clean_name.split('.')
    for part in parts:
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', part):
            raise HTTPException(400, f"Invalid {identifier_type} name: '{part}'. Only alphanumeric characters and underscores allowed.")
    return clean_name


def validate_stage_url(url: str) -> str:
    """
    Validate external stage URL format.
    Only allows S3, Azure Blob, and GCS URLs.
    """
    import re
    url = url.strip()
    
    # Valid cloud storage URL patterns
    valid_patterns = [
        (r'^s3://[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9](/.*)?$', 'S3'),
        (r'^azure://[a-zA-Z0-9][a-zA-Z0-9-]*\.blob\.core\.windows\.net(/.*)?$', 'Azure'),
        (r'^gcs://[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9](/.*)?$', 'GCS'),
    ]
    
    for pattern, cloud in valid_patterns:
        if re.match(pattern, url, re.IGNORECASE):
            return url
    
    raise HTTPException(
        400, 
        "Invalid stage URL. Expected: s3://bucket/path, azure://account.blob.core.windows.net/path, or gcs://bucket/path"
    )


def get_session_context(session) -> tuple:
    """
    Get the current database and schema from the session.
    Returns (database, schema) tuple.
    """
    try:
        result = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
        if result:
            row = result[0]
            database = row[0] or DB
            schema = row[1] or SCHEMA_PRODUCTION
            return (database, schema)
    except Exception as e:
        logger.warning(f"Could not get session context, using defaults: {e}")
    return (DB, SCHEMA_PRODUCTION)


def check_object_exists(session, object_type: str, object_name: str) -> dict:
    """
    Check if a Snowflake object exists and return its metadata.
    
    Args:
        session: Active Snowpark session
        object_type: One of 'STAGE', 'PIPE', 'TABLE'
        object_name: Fully qualified object name (database.schema.name)
    
    Returns:
        dict with 'exists' boolean and type-specific details if found
    """
    try:
        if object_type.upper() == 'STAGE':
            result = session.sql(f"DESC STAGE {object_name}").collect()
            return {"exists": True, "type": "STAGE", "properties": len(result)}
        elif object_type.upper() == 'PIPE':
            result = session.sql(f"DESC PIPE {object_name}").collect()
            details = {}
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                details[row_dict.get('property', '')] = row_dict.get('value', '')
            return {"exists": True, "type": "PIPE", "details": details}
        elif object_type.upper() == 'TABLE':
            result = session.sql(f"DESC TABLE {object_name}").collect()
            return {"exists": True, "type": "TABLE", "columns": len(result)}
    except Exception as e:
        error_msg = str(e).lower()
        if 'does not exist' in error_msg or 'object does not exist' in error_msg:
            return {"exists": False}
        raise
    return {"exists": False}


@app.post("/api/stages/create")
async def create_stage(
    stage_name: str = Form(...),
    stage_url: str = Form(""),
    storage_integration: str = Form(""),
    file_format: str = Form("JSON"),
    force_replace: bool = Form(False)
):
    """
    Create a new Snowflake stage (internal or external).
    
    If stage_url is empty, creates an internal stage.
    If stage_url is provided, creates an external stage requiring storage_integration.
    Uses session context for database/schema instead of hardcoded values.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        # Validate inputs to prevent SQL injection
        validate_snowflake_identifier(stage_name, "stage")
        if storage_integration:
            validate_snowflake_identifier(storage_integration, "storage integration")
        if file_format.upper() not in ('JSON', 'PARQUET', 'CSV', 'AVRO', 'ORC'):
            raise HTTPException(400, f"Invalid file format: {file_format}")
        
        # Validate stage URL if provided
        if stage_url and stage_url.strip():
            validate_stage_url(stage_url)
        
        # Get database/schema from session context (not hardcoded)
        database, schema = get_session_context(session)
        full_stage_name = f"{database}.{schema}.{stage_name.upper()}"
        
        # Check if stage already exists
        existing = check_object_exists(session, 'STAGE', full_stage_name)
        if existing.get('exists') and not force_replace:
            return {
                "status": "exists",
                "stage_name": full_stage_name,
                "message": f"Stage {full_stage_name} already exists. Set force_replace=true to overwrite.",
                "existing_properties": existing.get('properties', 0)
            }
        
        # Build CREATE statement
        if stage_url and stage_url.strip():
            # External stage - requires storage integration
            if not storage_integration:
                raise HTTPException(400, "Storage integration required for external stages")
            
            create_sql = f"""
            CREATE OR REPLACE STAGE {full_stage_name}
                URL = '{stage_url}'
                STORAGE_INTEGRATION = {storage_integration}
                FILE_FORMAT = (TYPE = {file_format})
                COMMENT = 'External stage for AMI data landing'
            """
            stage_type = "EXTERNAL"
        else:
            # Internal stage
            create_sql = f"""
            CREATE OR REPLACE STAGE {full_stage_name}
                FILE_FORMAT = (TYPE = {file_format})
                COMMENT = 'Internal stage for AMI data landing'
            """
            stage_type = "INTERNAL"
        
        session.sql(create_sql).collect()
        
        # Structured audit log for stage creation
        logger.info(f"Stage created: name={full_stage_name}, type={stage_type}, database={database}, schema={schema}, replaced={existing.get('exists', False)}")
        
        return {
            "status": "created" if not existing.get('exists') else "replaced",
            "stage_name": full_stage_name,
            "stage_type": stage_type,
            "database": database,
            "schema": schema,
            "url": stage_url or None,
            "storage_integration": storage_integration or None,
            "was_replaced": existing.get('exists', False)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create stage: {e}")
        raise HTTPException(500, f"Failed to create stage: {str(e)}")


@app.post("/api/pipes/create")
async def create_pipe(
    pipe_name: str = Form(...),
    stage_name: str = Form(...),
    target_table: str = Form(...),
    file_format: str = Form("JSON"),
    auto_ingest: bool = Form(True),
    force_replace: bool = Form(False)
):
    """
    Create a new Snowpipe for auto-ingestion from a stage to a target table.
    
    Creates:
    1. Target bronze table with VARIANT column (if not exists)
    2. Snowpipe with the specified configuration
    
    Uses session context for database/schema.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        # Validate inputs to prevent SQL injection
        validate_snowflake_identifier(pipe_name, "pipe")
        validate_snowflake_identifier(stage_name, "stage")
        validate_snowflake_identifier(target_table, "table")
        if file_format.upper() not in ('JSON', 'PARQUET', 'CSV', 'AVRO', 'ORC'):
            raise HTTPException(400, f"Invalid file format: {file_format}")
        
        # Get database/schema from session context
        database, schema = get_session_context(session)
        
        # Build fully qualified names if not already
        if '.' not in pipe_name:
            full_pipe_name = f"{database}.{schema}.{pipe_name.upper()}"
        else:
            full_pipe_name = pipe_name.upper()
        
        if '.' not in target_table:
            full_table_name = f"{database}.{schema}.{target_table.upper()}"
        else:
            full_table_name = target_table.upper()
        
        # Check if pipe already exists
        existing_pipe = check_object_exists(session, 'PIPE', full_pipe_name)
        if existing_pipe.get('exists') and not force_replace:
            return {
                "status": "exists",
                "pipe_name": full_pipe_name,
                "message": f"Pipe {full_pipe_name} already exists. Set force_replace=true to overwrite.",
                "existing_details": existing_pipe.get('details', {})
            }
        
        results = []
        
        # Normalize stage name - remove @ prefix if present (we'll add it in SQL)
        stage_name_normalized = stage_name.lstrip('@')
        
        # Step 1: Create target bronze table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            RAW_DATA VARIANT,
            FILE_NAME VARCHAR(500),
            LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            ROW_NUMBER NUMBER AUTOINCREMENT
        )
        COMMENT = 'Bronze layer table for raw AMI data from Snowpipe'
        """
        session.sql(create_table_sql).collect()
        results.append(f"Table {full_table_name} ready")
        
        # Step 2: Create or replace the pipe
        # NOTE: The COPY INTO syntax varies by file format
        auto_ingest_clause = "AUTO_INGEST = TRUE" if auto_ingest else "AUTO_INGEST = FALSE"
        
        if file_format.upper() == 'JSON':
            # JSON: $1 returns the entire document as VARIANT
            copy_select = f"""
  SELECT 
    $1 AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @{stage_name_normalized}"""
        elif file_format.upper() == 'PARQUET':
            # PARQUET: Use OBJECT_CONSTRUCT to combine all columns into VARIANT
            copy_select = f"""
  SELECT 
    OBJECT_CONSTRUCT(*) AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @{stage_name_normalized}"""
        else:
            # CSV: For CSV with multiple columns, use OBJECT_CONSTRUCT with positional references
            # This creates a VARIANT object from CSV columns
            # Note: For AMI data, CSV typically has: timestamp, meter_id, reading, unit
            copy_select = f"""
  SELECT 
    OBJECT_CONSTRUCT(
      'col1', $1, 'col2', $2, 'col3', $3, 'col4', $4, 'col5', $5,
      'col6', $6, 'col7', $7, 'col8', $8, 'col9', $9, 'col10', $10
    ) AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME,
    CURRENT_TIMESTAMP() AS LOAD_TS
  FROM @{stage_name_normalized}"""
        
        create_pipe_sql = f"""
        CREATE OR REPLACE PIPE {full_pipe_name}
          {auto_ingest_clause}
          COMMENT = 'Auto-ingests raw AMI data from stage to bronze table'
        AS
        COPY INTO {full_table_name} (RAW_DATA, FILE_NAME, LOAD_TS)
        FROM ({copy_select}
        )
        FILE_FORMAT = (TYPE = {file_format})
        """
        session.sql(create_pipe_sql).collect()
        results.append(f"Pipe {full_pipe_name} created")
        
        # Structured audit log for pipe creation
        logger.info(f"Pipe created: name={full_pipe_name}, table={full_table_name}, stage={stage_name}, format={file_format}, auto_ingest={auto_ingest}, replaced={existing_pipe.get('exists', False)}")
        
        # PATTERN: Auto-refresh new pipes to catch up on existing files
        # When a new pipe is created on an external stage with existing files,
        # it won't automatically ingest them - only new notifications trigger ingestion.
        files_refreshed = 0
        try:
            logger.info(f"Auto-refreshing pipe {full_pipe_name} to ingest existing files...")
            refresh_rows = session.sql(f"ALTER PIPE {full_pipe_name} REFRESH").collect()
            files_refreshed = len(refresh_rows)
            if files_refreshed > 0:
                results.append(f"Auto-refreshed {files_refreshed} existing files for immediate ingestion")
            logger.info(f"Auto-refresh complete: {files_refreshed} files queued for ingestion")
        except Exception as refresh_err:
            logger.warning(f"Auto-refresh failed (non-critical): {refresh_err}")
        
        # Step 3: Get the notification channel for SQS/Event Grid setup
        notification_channel = None
        try:
            desc_result = session.sql(f"DESC PIPE {full_pipe_name}").collect()
            for row in desc_result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                if row_dict.get('property') == 'NOTIFICATION_CHANNEL':
                    notification_channel = row_dict.get('value')
                    break
        except Exception as e:
            logger.warning(f"Could not retrieve notification channel for pipe {full_pipe_name}: {e}")
        
        return {
            "status": "created" if not existing_pipe.get('exists') else "replaced",
            "pipe_name": full_pipe_name,
            "target_table": full_table_name,
            "stage_name": stage_name,
            "database": database,
            "schema": schema,
            "auto_ingest": auto_ingest,
            "notification_channel": notification_channel,
            "results": results,
            "files_refreshed": files_refreshed,
            "was_replaced": existing_pipe.get('exists', False),
            "next_steps": [
                "Configure cloud event notification to send to the notification_channel",
                "For S3: Configure SQS → Snowpipe using the notification channel ARN",
                "For Azure: Configure Event Grid → Snowpipe",
                "For GCS: Configure Pub/Sub → Snowpipe"
            ] if auto_ingest and notification_channel else []
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create pipe: {e}")
        raise HTTPException(500, f"Failed to create pipe: {str(e)}")


@app.post("/api/storage-integrations/create")
async def create_storage_integration(
    integration_name: str = Form(...),
    integration_type: str = Form("S3"),
    storage_allowed_locations: str = Form(...)
):
    """
    Generate the SQL for creating a storage integration.
    Note: Creating storage integrations requires ACCOUNTADMIN role,
    so we return the SQL for manual execution.
    
    Also checks if integration already exists.
    """
    session = get_valid_session()
    
    try:
        # Validate integration name
        validate_snowflake_identifier(integration_name, "integration")
        
        # Validate storage_allowed_locations format based on integration type
        if integration_type == "S3":
            if not storage_allowed_locations.startswith('s3://'):
                raise HTTPException(400, "S3 storage locations must start with 's3://'")
        elif integration_type == "AZURE":
            if not storage_allowed_locations.startswith('azure://'):
                raise HTTPException(400, "Azure storage locations must start with 'azure://'")
        elif integration_type == "GCS":
            if not storage_allowed_locations.startswith('gcs://'):
                raise HTTPException(400, "GCS storage locations must start with 'gcs://'")
        
        # Check if integration already exists
        integration_exists = False
        existing_type = None
        if session:
            try:
                result = session.sql(f"DESC INTEGRATION {integration_name}").collect()
                integration_exists = True
                for row in result:
                    row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                    if row_dict.get('property') == 'TYPE':
                        existing_type = row_dict.get('value')
                        break
            except Exception as e:
                logger.debug(f"Integration {integration_name} does not exist: {e}")
        
        if integration_type == "S3":
            sql = f"""
-- Run this with ACCOUNTADMIN role:
CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<your-iam-role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('{storage_allowed_locations}');

-- After creating, run this to get the trust relationship info:
DESC INTEGRATION {integration_name};

-- Then update your AWS IAM trust policy with STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
"""
        elif integration_type == "AZURE":
            sql = f"""
-- Run this with ACCOUNTADMIN role:
CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('{storage_allowed_locations}');

-- After creating, run this to get the consent URL:
DESC INTEGRATION {integration_name};

-- Then visit AZURE_CONSENT_URL to grant Snowflake access to your storage
"""
        elif integration_type == "GCS":
            sql = f"""
-- Run this with ACCOUNTADMIN role:
CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('{storage_allowed_locations}');

-- After creating, get the service account to grant access:
DESC INTEGRATION {integration_name};

-- Then grant STORAGE_GCP_SERVICE_ACCOUNT the Storage Object Viewer role on your bucket
"""
        else:
            raise HTTPException(400, f"Unsupported integration type: {integration_type}")
        
        # Audit log for integration SQL generation
        logger.info(f"Storage integration SQL generated: name={integration_name}, type={integration_type}, exists={integration_exists}")
        
        return {
            "status": "sql_generated",
            "integration_name": integration_name,
            "integration_type": integration_type,
            "sql": sql,
            "integration_exists": integration_exists,
            "existing_type": existing_type,
            "note": "Storage integrations require ACCOUNTADMIN role. Copy and run this SQL manually.",
            "warning": f"Integration {integration_name} already exists and will be replaced!" if integration_exists else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate storage integration SQL: {e}")
        raise HTTPException(500, f"Failed to generate SQL: {str(e)}")

@app.get("/api/postgres/databases")
async def list_postgres_databases():
    """List Snowflake Managed Postgres databases (via Polaris/Iceberg catalogs or direct connection)"""
    if not snowflake_session:
        raise HTTPException(503, "Not connected")
    try:
        result = snowflake_session.sql("""
            SHOW INTEGRATIONS LIKE '%POSTGRES%'
        """).collect()
        integrations = [r['name'] for r in result]
        
        pg_databases = []
        for integ in integrations:
            pg_databases.append({
                "integration": integ,
                "host": f"{integ.lower()}.postgres.snowflakecomputing.com",
                "status": "available"
            })
        
        if not pg_databases:
            pg_databases = [{
                "integration": "FLUX_OPERATIONS_POSTGRES",
                "host": "flux-ops.postgres.snowflakecomputing.com",
                "status": "example"
            }]
        
        return {"postgres_databases": pg_databases}
    except Exception as e:
        return {
            "postgres_databases": [{
                "integration": "FLUX_OPERATIONS_POSTGRES",
                "host": "flux-ops.postgres.snowflakecomputing.com",
                "status": "example"
            }],
            "error": str(e)
        }


@app.get("/api/postgres/test-connection")
async def test_postgres_connection():
    """Test PostgreSQL connectivity and insert test data"""
    import os
    from datetime import datetime
    
    # Get Postgres config from environment
    host = os.environ.get('POSTGRES_HOST', '')
    database = os.environ.get('POSTGRES_DATABASE', 'flux_ops')
    user = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', '')
    port = int(os.environ.get('POSTGRES_PORT', '5432'))
    
    result = {
        "config": {
            "host": host,
            "database": database,
            "user": user,
            "port": port,
            "password_set": bool(password),
        },
        "status": "pending",
        "tests": {}
    }
    
    if not host:
        result["status"] = "error"
        result["error"] = "POSTGRES_HOST not configured"
        return result
    
    if not password:
        result["status"] = "error"
        result["error"] = "POSTGRES_PASSWORD not set"
        return result
    
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        
        # Test 1: Connection
        result["tests"]["connect"] = "testing..."
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
        conn.autocommit = False
        result["tests"]["connect"] = "success"
        
        # Test 2: Create table
        result["tests"]["create_table"] = "testing..."
        create_sql = """
        CREATE TABLE IF NOT EXISTS public.ami_streaming_test (
            meter_id VARCHAR(50),
            reading_timestamp TIMESTAMP,
            usage_kwh FLOAT,
            voltage FLOAT,
            test_marker VARCHAR(50),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        result["tests"]["create_table"] = "success"
        
        # Test 3: Insert test data
        result["tests"]["insert"] = "testing..."
        timestamp = datetime.now(timezone.utc)
        test_rows = [
            (f'MTR-PG-API-TEST-{i:03d}', timestamp, 1.5 + i * 0.1, 120.0 + i * 0.5, 'api_test')
            for i in range(5)
        ]
        insert_sql = """
        INSERT INTO public.ami_streaming_test
        (meter_id, reading_timestamp, usage_kwh, voltage, test_marker)
        VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, test_rows, page_size=100)
        conn.commit()
        result["tests"]["insert"] = f"success - {len(test_rows)} rows"
        
        # Test 4: Query data
        result["tests"]["query"] = "testing..."
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as total, MAX(ingested_at) as last_insert
                FROM public.ami_streaming_test
                WHERE test_marker = 'api_test'
            """)
            row = cur.fetchone()
            result["tests"]["query"] = f"success - {row[0]} total rows, last insert: {row[1]}"
        
        conn.close()
        result["status"] = "success"
        result["message"] = "PostgreSQL streaming verified - all tests passed"
        
    except ImportError:
        result["status"] = "error"
        result["error"] = "psycopg2 package not installed"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
    
    return result


@app.post("/api/postgres/stream-test-data")
async def stream_test_data_to_postgres(
    num_rows: int = Form(10),
    table_name: str = Form("ami_realtime_readings")
):
    """Stream test AMI data to PostgreSQL"""
    import os
    from datetime import datetime
    import random
    
    host = os.environ.get('POSTGRES_HOST', '')
    database = os.environ.get('POSTGRES_DATABASE', 'flux_ops')
    user = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', '')
    port = int(os.environ.get('POSTGRES_PORT', '5432'))
    
    if not host or not password:
        return {"status": "error", "error": "PostgreSQL not configured"}
    
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password,
            connect_timeout=10
        )
        conn.autocommit = False
        
        # Ensure table exists
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            meter_id VARCHAR(50),
            reading_timestamp TIMESTAMP,
            usage_kwh FLOAT,
            voltage FLOAT,
            customer_segment VARCHAR(50),
            transformer_id VARCHAR(50),
            substation_id VARCHAR(50),
            service_area VARCHAR(50),
            temperature_c FLOAT,
            is_outage BOOLEAN,
            data_quality VARCHAR(20),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        
        # Generate test data
        timestamp = datetime.now(timezone.utc)
        segments = ['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']
        service_areas = ['HOUSTON_METRO', 'DALLAS_METRO', 'AUSTIN']
        
        test_rows = []
        for i in range(num_rows):
            row = (
                f'MTR-PG-{i:06d}',
                timestamp,
                round(random.uniform(0.5, 5.0), 3),
                round(random.uniform(118.0, 122.0), 2),
                random.choice(segments),
                f'TRF-{i // 10:05d}',
                f'SUB-{i // 100:03d}',
                random.choice(service_areas),
                round(random.uniform(15.0, 35.0), 1),
                False,
                'VALID',
            )
            test_rows.append(row)
        
        # Insert
        insert_sql = f"""
        INSERT INTO public.{table_name}
        (meter_id, reading_timestamp, usage_kwh, voltage, customer_segment,
         transformer_id, substation_id, service_area, temperature_c, is_outage, data_quality)
        VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, test_rows, page_size=1000)
        conn.commit()
        
        # Get count
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM public.{table_name}")
            total = cur.fetchone()[0]
        
        conn.close()
        
        return {
            "status": "success",
            "rows_inserted": num_rows,
            "total_rows_in_table": total,
            "table": f"public.{table_name}"
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/sdk-limits")
async def get_sdk_limits():
    return SNOWPIPE_SDK_LIMITS


@app.get("/api/table-columns/{database}/{schema}/{table}")
async def get_table_columns(database: str, schema: str, table: str):
    if not snowflake_session:
        raise HTTPException(503, "Not connected")
    try:
        result = snowflake_session.sql(f"DESCRIBE TABLE {database}.{schema}.{table}").collect()
        columns = [{"name": r['name'], "type": r['type'], "nullable": r.get('null?', 'Y')} for r in result]
        return {"columns": columns}
    except Exception as e:
        return {"columns": [], "error": str(e)}


@app.post("/api/create-table")
async def create_table(
    database: str = Form(...),
    schema: str = Form(...),
    table: str = Form(...),
    include_variant: bool = Form(False)
):
    if not snowflake_session:
        raise HTTPException(503, "Not connected")
    
    variant_col = ",\n                RAW_PAYLOAD VARIANT" if include_variant else ""
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
                ENTITY_ID NUMBER(38,0) AUTOINCREMENT,
                READING_ID VARCHAR(50),
                METER_ID VARCHAR(50),
                CUSTOMER_SEGMENT VARCHAR(50),
                CUSTOMER_SUBTYPE VARCHAR(50),
                TIMESTAMP TIMESTAMP_NTZ(9),
                USAGE_KWH FLOAT,
                VOLTAGE_READING FLOAT,
                TEMPERATURE_CELSIUS FLOAT,
                PEAK_HOURS_FLAG BOOLEAN,
                TRANSFORMER_ID VARCHAR(50),
                SUBSTATION_ID VARCHAR(50),
                CIRCUIT_ID VARCHAR(50),
                SERVICE_AREA VARCHAR(50),
                LOCATION_LAT FLOAT,
                LOCATION_LON FLOAT{variant_col},
                CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
            )
    """
    
    try:
        snowflake_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
        snowflake_session.sql(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}").collect()
        snowflake_session.sql(create_sql).collect()
        return {"status": "success", "table": f"{database}.{schema}.{table}"}
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)


# ============================================================================
# SNOWPIPE STREAMING API ENDPOINTS (Classic + HP)
# ============================================================================

@app.get("/api/streaming/architectures")
async def get_streaming_architectures():
    """Get information about available Snowpipe Streaming architectures"""
    try:
        from snowpipe_streaming_impl import ARCHITECTURE_INFO
        return {"architectures": ARCHITECTURE_INFO}
    except ImportError:
        return {
            "architectures": {
                "classic": {
                    "name": "Snowpipe Classic SDK",
                    "package": "snowflake-ingest",
                    "description": "Direct table channels - simpler setup",
                    "latency": "< 5 seconds",
                    "requires_pipe": False,
                },
                "hp": {
                    "name": "Snowpipe HP SDK",
                    "package": "snowpipe-streaming",
                    "description": "PIPE object based - 10GB/s throughput",
                    "latency": "< 5 seconds",
                    "requires_pipe": True,
                }
            }
        }


@app.post("/api/streaming/generate-ddl")
async def generate_streaming_ddl(
    architecture: str = Form(...),  # 'classic' or 'hp'
    database: str = Form(DB),
    schema: str = Form("PRODUCTION"),
    table_name: str = Form("AMI_STREAMING_READINGS"),
    pipe_name: str = Form("AMI_STREAMING_PIPE"),
    enable_transformation: bool = Form(True),
    enable_clustering: bool = Form(True)
):
    """
    Generate SQL DDL for Snowpipe Streaming setup.
    
    Args:
        architecture: 'classic' (direct table) or 'hp' (PIPE object)
        database: Target database
        schema: Target schema
        table_name: Landing table name
        pipe_name: PIPE object name (HP only)
        enable_transformation: Enable SQL transformation (HP only)
        enable_clustering: Enable cluster at ingest (HP only)
    
    Returns:
        DDL SQL script for the selected architecture
    """
    try:
        from snowpipe_streaming_impl import (
            generate_classic_streaming_ddl,
            generate_hp_streaming_ddl
        )
        
        if architecture.lower() in ('classic', 'snowpipe_classic'):
            ddl = generate_classic_streaming_ddl(
                database=database,
                schema=schema,
                table_name=table_name,
                enable_change_tracking=True
            )
            arch_info = {
                "architecture": "classic",
                "name": "Classic (SQL INSERT via Snowpark)",
                "requires_pipe": False,
                "sdk": "snowflake-snowpark-python",
                "note": "Classic SDK is Java-only; this uses SQL INSERT as the Python equivalent"
            }
        elif architecture.lower() in ('hp', 'snowpipe_hp', 'high_performance'):
            ddl = generate_hp_streaming_ddl(
                database=database,
                schema=schema,
                table_name=table_name,
                pipe_name=pipe_name,
                enable_transformation=enable_transformation,
                enable_clustering=enable_clustering
            )
            arch_info = {
                "architecture": "hp",
                "name": "High-Performance Snowpipe Streaming",
                "requires_pipe": True,
                "sdk": "snowpipe-streaming"
            }
        else:
            return JSONResponse(
                {"status": "error", "message": f"Unknown architecture: {architecture}. Use 'classic' or 'hp'."},
                status_code=400
            )
        
        return {
            "status": "success",
            "architecture": arch_info,
            "target": {
                "database": database,
                "schema": schema,
                "table": table_name,
                "pipe": pipe_name if architecture.lower() in ('hp', 'snowpipe_hp', 'high_performance') else None
            },
            "ddl": ddl
        }
        
    except ImportError as e:
        return JSONResponse(
            {"status": "error", "message": f"Streaming implementation not found: {e}"},
            status_code=500
        )
    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )


@app.post("/api/streaming/deploy-ddl")
async def deploy_streaming_ddl(
    architecture: str = Form(...),
    database: str = Form(DB),
    schema: str = Form("PRODUCTION"),
    table_name: str = Form("AMI_STREAMING_READINGS"),
    pipe_name: str = Form("AMI_STREAMING_PIPE")
):
    """
    Deploy Snowpipe Streaming DDL to Snowflake.
    Creates table and PIPE object (for HP architecture).
    """
    if not snowflake_session:
        raise HTTPException(503, "Snowflake not connected")
    
    logger.info(f"deploy_streaming_ddl: arch={architecture} db={database} schema={schema} table={table_name} pipe={pipe_name}")
    
    # ── Normalize names: strip database.schema prefix if already qualified ──
    # The UI dropdown may send fully-qualified names like "FLUX_DB.DEV.HP_TEST_MAR_1"
    # but the DDL generator also prepends database.schema, causing double-qualification.
    if '.' in table_name:
        table_name = table_name.split('.')[-1]
    if '.' in pipe_name:
        pipe_name = pipe_name.split('.')[-1]
    logger.info(f"deploy_streaming_ddl (normalized): table={table_name} pipe={pipe_name}")
    
    try:
        from snowpipe_streaming_impl import (
            generate_classic_streaming_ddl,
            generate_hp_streaming_ddl
        )
        
        # ── Introspect actual table columns (if the table already exists) ──
        table_columns = None
        try:
            desc_result = snowflake_session.sql(
                f"DESC TABLE {database}.{schema}.{table_name}"
            ).collect()
            table_columns = [
                {"name": r["name"], "type": r["type"]}
                for r in desc_result
            ]
            logger.info(f"Introspected {len(table_columns)} columns from {database}.{schema}.{table_name}")
        except Exception:
            # Table doesn't exist yet — will be created by the DDL generator
            logger.info(f"Table {database}.{schema}.{table_name} does not exist yet; using default schema")

        # Generate DDL
        if architecture.lower() in ('classic', 'snowpipe_classic'):
            ddl = generate_classic_streaming_ddl(database, schema, table_name)
        else:
            ddl = generate_hp_streaming_ddl(
                database, schema, table_name, pipe_name,
                table_columns=table_columns,
            )
        
        # Execute DDL statements
        executed_statements = []
        errors = []
        
        # Split by semicolons (comment stripping happens per-statement below)
        statements = [s.strip() for s in ddl.split(';') if s.strip()]
        
        for stmt in statements:
            # Skip comments and empty lines
            lines = [l for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
            clean_stmt = '\n'.join(lines)
            
            if not clean_stmt.strip():
                continue
                
            # Only execute CREATE, GRANT, ALTER statements
            if any(clean_stmt.upper().strip().startswith(kw) for kw in ['CREATE', 'GRANT', 'ALTER', 'COMMENT']):
                try:
                    snowflake_session.sql(clean_stmt).collect()
                    executed_statements.append(clean_stmt[:100] + '...')
                except Exception as e:
                    import re
                    err_str = str(e)
                    logger.warning(f"DDL statement failed: {clean_stmt[:200]}  |  Error: {err_str[:300]}")
                    # Strip Snowflake query ID noise: "(1304): 01c2bf96-...:002003 (02000): "
                    err_str = re.sub(r'^\(\d+\):\s*[\w-]+:\d+\s*\(\d+\):\s*', '', err_str)
                    err_str = re.sub(r'[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}:\d+', '', err_str).strip()
                    errors.append({"statement": clean_stmt[:200], "error": err_str})
        
        # Classify errors as benign vs real failures
        # GRANT failures are non-critical: the PIPE and table are the essential objects
        benign_keywords = ['already exists', 'already granted', 'object already exists']
        benign_errors = []
        real_errors = []
        for err in errors:
            err_msg = err.get('error', '').lower()
            stmt_upper = err.get('statement', '').upper().strip()
            is_grant = stmt_upper.startswith('GRANT')
            if any(kw in err_msg for kw in benign_keywords):
                benign_errors.append(err)
            elif is_grant:
                # GRANT failures are non-critical — log but treat as warning
                logger.warning(f"Non-critical GRANT failure (treated as warning): {err}")
                benign_errors.append(err)
            else:
                real_errors.append(err)
        
        # If all errors are benign, treat as success
        if errors and not real_errors:
            status = "success"
        elif real_errors:
            status = "partial"
        else:
            status = "success"
        
        return {
            "status": status,
            "architecture": architecture,
            "database": database,
            "schema": schema,
            "table": table_name,
            "pipe": pipe_name if architecture.lower() in ('hp', 'snowpipe_hp') else None,
            "executed": len(executed_statements),
            "errors": real_errors,
            "warnings": benign_errors,
        }
        
    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )


@app.get("/api/streaming/check-prerequisites")
async def check_streaming_prerequisites(architecture: str):
    """
    Check if prerequisites for streaming are met.
    Verifies SDK packages are installed and authentication is configured.
    """
    result = {
        "architecture": architecture,
        "prerequisites": []
    }
    
    # Check for Classic architecture prerequisites
    if architecture.lower() in ('classic', 'snowpipe_classic', 'all'):
        try:
            from snowflake.snowpark import Session as _SnowparkSession
            result["prerequisites"].append({
                "name": "snowflake-snowpark-python",
                "status": "installed",
                "required_for": "Classic Architecture (SQL INSERT path)",
                "note": "Classic SDK is Java-only; Snowpark SQL INSERT is the Python equivalent"
            })
        except ImportError:
            result["prerequisites"].append({
                "name": "snowflake-snowpark-python",
                "status": "missing",
                "install_cmd": "pip install snowflake-snowpark-python",
                "required_for": "Classic Architecture (SQL INSERT path)"
            })
    
    # Check for HP SDK
    if architecture.lower() in ('hp', 'snowpipe_hp', 'high_performance', 'all'):
        try:
            from snowflake.ingest.streaming import StreamingIngestClient as _HPClient
            result["prerequisites"].append({
                "name": "snowpipe-streaming",
                "status": "installed",
                "required_for": "HP Architecture"
            })
        except ImportError:
            result["prerequisites"].append({
                "name": "snowpipe-streaming",
                "status": "missing",
                "install_cmd": "pip install snowpipe-streaming",
                "required_for": "HP Architecture"
            })
    
    # Check for private key
    key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', '/usr/local/creds/secret_string')
    if os.path.exists(key_path):
        result["prerequisites"].append({
            "name": "Private Key",
            "status": "found",
            "path": key_path
        })
    elif os.environ.get('SNOWFLAKE_PRIVATE_KEY'):
        result["prerequisites"].append({
            "name": "Private Key",
            "status": "found",
            "path": "SNOWFLAKE_PRIVATE_KEY env var"
        })
    else:
        result["prerequisites"].append({
            "name": "Private Key",
            "status": "missing",
            "required_for": "All Snowpipe Streaming (key pair auth required)"
        })
    
    # Check for required env vars
    for var in ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER']:
        if os.environ.get(var):
            result["prerequisites"].append({
                "name": var,
                "status": "set"
            })
        else:
            result["prerequisites"].append({
                "name": var,
                "status": "missing",
                "required_for": "All Snowpipe Streaming"
            })
    
    # Overall status
    all_ok = all(p.get('status') in ('installed', 'found', 'set') for p in result["prerequisites"])
    result["ready"] = all_ok
    
    return result


@app.get("/api/streaming/python-client-code")
async def get_python_client_code(
    architecture: str,
    database: str = DB,
    schema: str = SCHEMA_PRODUCTION,
    table_name: str = "AMI_STREAMING_READINGS",
    pipe_name: str = "AMI_STREAMING_PIPE"
):
    """
    Generate Python client code for streaming data using the selected architecture.
    """
    if architecture.lower() in ('classic', 'snowpipe_classic'):
        try:
            from snowpipe_streaming_impl import get_classic_python_client_code
            code = get_classic_python_client_code(database, schema, table_name)
        except ImportError:
            code = f'''"""
Classic Snowpipe Streaming - SQL INSERT via Snowpark

NOTE: The native Classic Snowpipe Streaming SDK is Java-only (snowflake-ingest-sdk).
For Python applications, SQL INSERT via Snowpark session is the equivalent approach.
No PIPE object required - data is written directly to the table.
"""
from snowflake.snowpark import Session

connection_params = {{
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "role": "SYSADMIN",
    "warehouse": "FLUX_WH",
    "database": "{database}",
    "schema": "{schema}",
}}
session = Session.builder.configs(connection_params).create()

rows = [
    {{"METER_ID": "MTR-001", "READING_TIMESTAMP": "2026-01-17 12:00:00", "USAGE_KWH": 1.5}},
    {{"METER_ID": "MTR-002", "READING_TIMESTAMP": "2026-01-17 12:00:00", "USAGE_KWH": 2.1}},
]

columns = list(rows[0].keys())
col_str = ", ".join(columns)
values_parts = []
for row in rows:
    vals = ", ".join(f"\\'{v}\\'" if isinstance(v, str) else str(v) for v in row.values())
    values_parts.append(f"({{vals}})")

insert_sql = f"INSERT INTO {database}.{schema}.{table_name} ({{col_str}}) VALUES {{\\', \\'.join(values_parts)}}"
session.sql(insert_sql).collect()
print(f"Inserted {{len(rows)}} rows via SQL INSERT")

session.close()
'''
        deps = ["snowflake-snowpark-python"]
    else:
        try:
            from snowpipe_streaming_impl import get_hp_python_client_code
            code = get_hp_python_client_code(database, schema, pipe_name)
        except ImportError:
            code = f'''"""
High-Performance Snowpipe Streaming Client

Requires: pip install snowpipe-streaming
Auth: Key pair (JWT/RSA) required
PIPE object must exist before streaming (run DDL first)
"""
from snowflake.ingest.streaming import StreamingIngestClient

ACCOUNT = 'your_account_id'
USER = 'your_username'
DATABASE = '{database}'
SCHEMA = '{schema}'
PIPE_NAME = '{pipe_name}'

with open('rsa_key.p8', 'r') as f:
    private_key = f.read()

properties = {{
    'url': f'https://{{ACCOUNT}}.snowflakecomputing.com',
    'account': ACCOUNT,
    'user': USER,
    'private_key': private_key,
    'role': 'SYSADMIN',
    'authorization_type': 'JWT',
}}

client = StreamingIngestClient(
    client_name='my_streaming_client',
    db_name=DATABASE,
    schema_name=SCHEMA,
    pipe_name=PIPE_NAME,
    properties=properties,
)

channel, status = client.open_channel(channel_name='my_channel')
print(f"Channel opened: {{status.status_code}}")

rows = [
    {{"METER_ID": "MTR-001", "READING_TIMESTAMP": "2026-01-17T12:00:00", "USAGE_KWH": 1.5}},
    {{"METER_ID": "MTR-002", "READING_TIMESTAMP": "2026-01-17T12:00:00", "USAGE_KWH": 2.1}},
]
channel.append_rows(rows=rows, start_offset_token="batch_1", end_offset_token="batch_1")

channel.initiate_flush()
channel.wait_for_flush(timeout_seconds=30)

statuses = client.get_channel_statuses(["my_channel"])
ch_status = statuses["my_channel"]
print(f"Rows inserted: {{ch_status.rows_inserted}}")

channel.close()
client.close()
'''
        deps = ["snowpipe-streaming"]
    
    return {
        "architecture": architecture,
        "language": "python",
        "code": code,
        "dependencies": deps,
        "note": "Classic SDK is Java-only; Classic Python code uses Snowpark SQL INSERT" if architecture.lower() in ('classic', 'snowpipe_classic') else "HP SDK requires key-pair auth and PIPE object"
    }


# =============================================================================
# SNOWPIPE MANAGEMENT APIs
# Note: Primary /api/pipes endpoint is defined earlier in the file (~line 9544)
# =============================================================================


@app.get("/api/pipes/{database}/{schema}")
async def list_pipes_in_schema(database: str, schema: str):
    """List pipes in a specific schema."""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        result = session.sql(f"SHOW PIPES IN SCHEMA {database}.{schema}").collect()
        pipes = []
        
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            pipe_info = {
                'name': row_dict.get('name', ''),
                'database': database,
                'schema': schema,
                'definition': row_dict.get('definition', ''),
                'owner': row_dict.get('owner', ''),
                'notification_channel': row_dict.get('notification_channel', ''),
                'created_on': str(row_dict.get('created_on', '')),
            }
            pipe_info['full_name'] = f"{database}.{schema}.{pipe_info['name']}"
            pipes.append(pipe_info)
        
        return {"pipes": pipes, "count": len(pipes)}
    except Exception as e:
        raise HTTPException(500, f"Failed to list pipes: {str(e)}")


@app.get("/api/pipe/status/{pipe_name:path}")
async def get_pipe_status(pipe_name: str):
    """Get detailed status of a specific Snowpipe."""
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        result = session.sql(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')").collect()
        if result:
            status_json = result[0][0]
            import json
            status = json.loads(status_json)
            return {
                "pipe_name": pipe_name,
                "status": status
            }
        return {"pipe_name": pipe_name, "status": None}
    except Exception as e:
        raise HTTPException(500, f"Failed to get pipe status: {str(e)}")


@app.post("/api/pipes/create")
async def create_pipe(request: Request):
    """
    Create a new Snowpipe on-demand.
    
    PATTERN: Self-service data pipeline provisioning.
    Allows users to create Snowpipes for their specific schema/table combinations
    without needing direct Snowflake access or DDL knowledge.
    
    Parameters (form data):
    - pipe_name: Name for the new pipe
    - target_database: Database containing the target table
    - target_schema: Schema containing the target table  
    - target_table: Table to load data into
    - source_stage: External stage to read from (full qualified name)
    - file_format: JSON, PARQUET, CSV, etc.
    - auto_ingest: Enable auto-ingest with SQS notifications
    - pattern: Optional file pattern to match (e.g., '.*\\.json')
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    form = await request.form()
    
    pipe_name = form.get('pipe_name', '').strip().upper()
    target_database = form.get('target_database', DB).strip().upper()
    target_schema = form.get('target_schema', 'DEV').strip().upper()
    target_table = form.get('target_table', '').strip().upper()
    source_stage = form.get('source_stage', '').strip()
    file_format = form.get('file_format', 'JSON').strip().upper()
    auto_ingest = form.get('auto_ingest', 'false').lower() == 'true'
    file_pattern = form.get('file_pattern', '').strip()
    strip_outer_array = form.get('strip_outer_array', 'true').lower() == 'true'
    
    # Validation
    if not pipe_name:
        raise HTTPException(400, "pipe_name is required")
    if not target_table:
        raise HTTPException(400, "target_table is required")
    if not source_stage:
        raise HTTPException(400, "source_stage is required")
    
    full_pipe_name = f"{target_database}.{target_schema}.{pipe_name}"
    full_table_name = f"{target_database}.{target_schema}.{target_table}"
    
    # Ensure stage has @ prefix
    if not source_stage.startswith('@'):
        source_stage = f"@{source_stage}"
    
    try:
        # First, check if target table exists and get its columns
        try:
            table_result = session.sql(f"DESC TABLE {full_table_name}").collect()
            columns = []
            for row in table_result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                col_name = row_dict.get('name', row_dict.get('NAME', ''))
                if col_name:
                    columns.append(col_name)
        except Exception as table_err:
            raise HTTPException(400, f"Target table {full_table_name} does not exist or is not accessible: {str(table_err)}")
        
        # Build COPY INTO statement based on file format
        if file_format == 'JSON':
            # For JSON with RAW_DATA VARIANT column pattern (common bronze table)
            if 'RAW_DATA' in columns:
                copy_columns = "(RAW_DATA, FILE_NAME"
                select_clause = "$1, METADATA$FILENAME"
                
                # Check for timestamp column
                if 'LOAD_TS' in columns:
                    copy_columns += ", LOAD_TS)"
                    select_clause += ", CURRENT_TIMESTAMP()"
                elif 'LOAD_TIMESTAMP' in columns:
                    copy_columns += ", LOAD_TIMESTAMP)"
                    select_clause += ", CURRENT_TIMESTAMP()"
                else:
                    copy_columns += ")"
                
                copy_stmt = f"""
                    SELECT {select_clause}
                    FROM {source_stage}
                """
                file_format_clause = f"FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = {str(strip_outer_array).upper()})"
            else:
                # Generic JSON load
                copy_columns = ""
                copy_stmt = f"SELECT $1 FROM {source_stage}"
                file_format_clause = f"FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = {str(strip_outer_array).upper()})"
        elif file_format == 'PARQUET':
            copy_columns = ""
            copy_stmt = f"SELECT * FROM {source_stage}"
            file_format_clause = "FILE_FORMAT = (TYPE = 'PARQUET')"
        elif file_format == 'CSV':
            copy_columns = ""
            copy_stmt = f"SELECT $1, $2, $3, $4, $5 FROM {source_stage}"  # Adjust based on actual columns
            file_format_clause = "FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)"
        else:
            copy_columns = ""
            copy_stmt = f"SELECT $1 FROM {source_stage}"
            file_format_clause = f"FILE_FORMAT = (TYPE = '{file_format}')"
        
        # Build the CREATE PIPE statement
        auto_ingest_clause = "AUTO_INGEST = TRUE" if auto_ingest else ""
        pattern_clause = f"PATTERN = '{file_pattern}'" if file_pattern else ""
        
        # Combine optional clauses
        optional_clauses = " ".join(filter(None, [pattern_clause]))
        
        create_pipe_sql = f"""
        CREATE OR REPLACE PIPE {full_pipe_name}
        {auto_ingest_clause}
        AS
        COPY INTO {full_table_name} {copy_columns}
        FROM ({copy_stmt})
        {file_format_clause}
        {optional_clauses}
        """.strip()
        
        logger.info(f"Creating pipe with SQL: {create_pipe_sql}")
        
        # Execute the CREATE PIPE
        session.sql(create_pipe_sql).collect()
        
        # PATTERN: Auto-refresh new pipes to catch up on existing files
        # When a new pipe is created on an external stage with existing files,
        # it won't automatically ingest them - only new S3 notifications trigger ingestion.
        # This auto-refresh ensures users don't have to manually discover this behavior.
        refresh_result = None
        files_refreshed = 0
        try:
            logger.info(f"Auto-refreshing pipe {full_pipe_name} to ingest existing files...")
            refresh_rows = session.sql(f"ALTER PIPE {full_pipe_name} REFRESH").collect()
            files_refreshed = len(refresh_rows)
            refresh_result = {
                'files_queued': files_refreshed,
                'sample_files': [row.asDict().get('File', '') if hasattr(row, 'asDict') else str(row) 
                                for row in refresh_rows[:5]]  # Show first 5 files
            }
            logger.info(f"Auto-refresh complete: {files_refreshed} files queued for ingestion")
        except Exception as refresh_err:
            logger.warning(f"Auto-refresh failed (non-critical): {refresh_err}")
            refresh_result = {'error': str(refresh_err), 'files_queued': 0}
        
        # Get the pipe status after creation
        status_result = session.sql(f"SELECT SYSTEM$PIPE_STATUS('{full_pipe_name}')").collect()
        pipe_status = {}
        notification_channel = None
        if status_result:
            import json
            pipe_status = json.loads(status_result[0][0])
            notification_channel = pipe_status.get('notificationChannelName')
        
        # Return success with details
        return {
            "success": True,
            "pipe_name": full_pipe_name,
            "target_table": full_table_name,
            "source_stage": source_stage,
            "auto_ingest": auto_ingest,
            "notification_channel": notification_channel,
            "status": pipe_status,
            "ddl": create_pipe_sql,
            "auto_refresh": refresh_result,
            "message": f"Pipe {full_pipe_name} created successfully!" + 
                      (f" Auto-refreshed {files_refreshed} existing files." if files_refreshed > 0 else ""),
            "next_steps": [
                "For auto-ingest: Configure S3 event notifications to the SQS queue shown above",
                f"To check status: SELECT SYSTEM$PIPE_STATUS('{full_pipe_name}')",
                f"To manually refresh again: ALTER PIPE {full_pipe_name} REFRESH"
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create pipe: {e}")
        raise HTTPException(500, f"Failed to create pipe: {str(e)}")


@app.post("/api/pipes/refresh/{pipe_name:path}")
async def refresh_pipe(pipe_name: str):
    """
    Manually refresh a pipe to ingest any existing files in the stage.
    This is useful for loading historical data or recovering from issues.
    """
    session = get_valid_session()
    if not session:
        raise HTTPException(503, "Not connected to Snowflake")
    
    try:
        result = session.sql(f"ALTER PIPE {pipe_name} REFRESH").collect()
        
        # Parse the result to count files sent
        files_sent = []
        for row in result:
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            files_sent.append({
                'file': row_dict.get('File', row_dict.get('file', '')),
                'status': row_dict.get('Status', row_dict.get('status', ''))
            })
        
        return {
            "success": True,
            "pipe_name": pipe_name,
            "files_sent": len(files_sent),
            "files": files_sent[:50],  # Limit to first 50 for display
            "message": f"Refresh triggered for {len(files_sent)} files"
        }
    except Exception as e:
        logger.error(f"Failed to refresh pipe: {e}")
        raise HTTPException(500, f"Failed to refresh pipe: {str(e)}")


@app.get("/pipes")
async def pipes_management_page():
    """
    Snowpipe Management UI - Create, monitor, and manage Snowpipes.
    
    PATTERN: Self-service data pipeline management.
    Provides operational visibility and control over data ingestion pipelines.
    """
    
    # Get available schemas for dropdown
    session = get_valid_session()
    schemas_options = ""
    stages_options = ""
    
    if session:
        try:
            # Get schemas
            result = session.sql(f"SHOW SCHEMAS IN DATABASE {DB}").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                schema_name = row_dict.get('name', '')
                if schema_name and schema_name not in ('INFORMATION_SCHEMA',):
                    selected = 'selected' if schema_name == 'DEV' else ''
                    schemas_options += f'<option value="{schema_name}" {selected}>{schema_name}</option>'
            
            # Get external stages
            result = session.sql("SHOW STAGES IN ACCOUNT").collect()
            for row in result:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                if row_dict.get('type', '') == 'EXTERNAL':
                    stage_name = f"{row_dict.get('database_name', '')}.{row_dict.get('schema_name', '')}.{row_dict.get('name', '')}"
                    url = row_dict.get('url', '')
                    stages_options += f'<option value="{stage_name}">{stage_name} ({url})</option>'
        except Exception as e:
            logger.error(f"Error loading schemas/stages: {e}")
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Snowpipe Management - FLUX Data Forge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {get_base_styles()}
        <style>
            .pipe-card {{
                background: rgba(30, 41, 59, 0.8);
                border: 1px solid rgba(56, 189, 248, 0.2);
                border-radius: 12px;
                padding: 20px;
                margin-bottom: 16px;
            }}
            .pipe-card:hover {{
                border-color: rgba(56, 189, 248, 0.5);
            }}
            .pipe-name {{
                font-family: monospace;
                font-size: 1.1rem;
                color: #38bdf8;
                margin-bottom: 8px;
            }}
            .pipe-meta {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 12px;
                margin-top: 12px;
            }}
            .pipe-meta-item {{
                background: rgba(15, 23, 42, 0.5);
                padding: 8px 12px;
                border-radius: 6px;
            }}
            .pipe-meta-label {{
                font-size: 0.75rem;
                color: #64748b;
                text-transform: uppercase;
            }}
            .pipe-meta-value {{
                font-size: 0.9rem;
                color: #e2e8f0;
                margin-top: 2px;
                word-break: break-all;
            }}
            .status-badge {{
                display: inline-block;
                padding: 4px 12px;
                border-radius: 20px;
                font-size: 0.75rem;
                font-weight: 600;
            }}
            .status-running {{
                background: rgba(34, 197, 94, 0.2);
                color: #22c55e;
            }}
            .status-paused {{
                background: rgba(245, 158, 11, 0.2);
                color: #f59e0b;
            }}
            .form-section {{
                background: rgba(30, 41, 59, 0.6);
                border: 1px solid rgba(56, 189, 248, 0.2);
                border-radius: 12px;
                padding: 24px;
                margin-bottom: 24px;
            }}
            .form-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 16px;
            }}
            .form-group {{
                margin-bottom: 16px;
            }}
            .form-group label {{
                display: block;
                color: #94a3b8;
                font-size: 0.85rem;
                margin-bottom: 6px;
            }}
            .form-group input, .form-group select {{
                width: 100%;
                padding: 10px 14px;
                background: rgba(15, 23, 42, 0.8);
                border: 1px solid rgba(100, 116, 139, 0.3);
                border-radius: 8px;
                color: #e2e8f0;
                font-size: 0.95rem;
            }}
            .form-group input:focus, .form-group select:focus {{
                outline: none;
                border-color: #38bdf8;
            }}
            .checkbox-group {{
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            .checkbox-group input[type="checkbox"] {{
                width: 18px;
                height: 18px;
            }}
            .result-panel {{
                background: rgba(15, 23, 42, 0.8);
                border: 1px solid rgba(34, 197, 94, 0.3);
                border-radius: 8px;
                padding: 16px;
                margin-top: 16px;
                display: none;
            }}
            .result-panel.show {{
                display: block;
            }}
            .result-panel.error {{
                border-color: rgba(239, 68, 68, 0.3);
            }}
            .code-block {{
                background: #0f172a;
                border-radius: 6px;
                padding: 12px;
                font-family: monospace;
                font-size: 0.85rem;
                overflow-x: auto;
                white-space: pre-wrap;
                color: #94a3b8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            {get_header_html()}
            {get_status_bar_html()}
            {get_tabs_html('pipes')}
            
            <div class="panel">
                <div class="panel-title">
                    {get_material_icon('plumbing', '24px', '#38bdf8')} Snowpipe Management
                </div>
                <p style="color: #94a3b8; margin-bottom: 24px;">
                    Create and manage Snowpipes for automated data ingestion from external stages (S3/Azure/GCS) into Snowflake tables.
                </p>
                
                <!-- Create New Pipe Section -->
                <div class="form-section">
                    <h3 style="color: #e2e8f0; margin-bottom: 16px;">
                        {get_material_icon('add_circle', '20px', '#22c55e')} Create New Snowpipe
                    </h3>
                    
                    <form id="create-pipe-form" onsubmit="createPipe(event)">
                        <div class="form-grid">
                            <div class="form-group">
                                <label>Pipe Name *</label>
                                <input type="text" name="pipe_name" id="pipe_name" placeholder="AMI_RAW_INGEST_PIPE_DEV" required>
                            </div>
                            
                            <div class="form-group">
                                <label>Target Database</label>
                                <input type="text" name="target_database" id="target_database" value="{DB}">
                            </div>
                            
                            <div class="form-group">
                                <label>Target Schema *</label>
                                <select name="target_schema" id="target_schema" required>
                                    {schemas_options}
                                </select>
                            </div>
                            
                            <div class="form-group">
                                <label>Target Table *</label>
                                <input type="text" name="target_table" id="target_table" placeholder="AMI_BRONZE_RAW_TEST_JAN19" required>
                            </div>
                            
                            <div class="form-group">
                                <label>Source External Stage *</label>
                                <select name="source_stage" id="source_stage" required>
                                    <option value="">Select a stage...</option>
                                    {stages_options}
                                </select>
                            </div>
                            
                            <div class="form-group">
                                <label>File Format</label>
                                <select name="file_format" id="file_format">
                                    <option value="JSON" selected>JSON (NDJSON)</option>
                                    <option value="PARQUET">Parquet</option>
                                    <option value="CSV">CSV</option>
                                    <option value="AVRO">Avro</option>
                                </select>
                            </div>
                            
                            <div class="form-group">
                                <label>File Pattern (optional)</label>
                                <input type="text" name="file_pattern" id="file_pattern" placeholder=".*ami_stream.*\\.json">
                            </div>
                            
                            <div class="form-group">
                                <label>&nbsp;</label>
                                <div class="checkbox-group">
                                    <input type="checkbox" name="auto_ingest" id="auto_ingest" value="true">
                                    <label for="auto_ingest" style="margin: 0; color: #e2e8f0;">Enable Auto-Ingest (requires S3 event notifications)</label>
                                </div>
                            </div>
                            
                            <div class="form-group">
                                <label>&nbsp;</label>
                                <div class="checkbox-group">
                                    <input type="checkbox" name="strip_outer_array" id="strip_outer_array" value="true" checked>
                                    <label for="strip_outer_array" style="margin: 0; color: #e2e8f0;">Strip Outer Array (for NDJSON)</label>
                                </div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 20px;">
                            <button type="submit" class="btn-primary" style="padding: 12px 24px;">
                                {get_material_icon('add', '18px', 'white')} Create Snowpipe
                            </button>
                        </div>
                    </form>
                    
                    <div id="create-result" class="result-panel">
                        <div id="result-content"></div>
                    </div>
                </div>
                
                <!-- Existing Pipes Section -->
                <div style="margin-top: 32px;">
                    <h3 style="color: #e2e8f0; margin-bottom: 16px; display: flex; align-items: center; gap: 8px;">
                        {get_material_icon('list', '20px', '#38bdf8')} Existing Snowpipes
                        <button onclick="loadPipes()" class="btn-secondary" style="margin-left: auto; padding: 6px 12px; font-size: 0.8rem;">
                            {get_material_icon('refresh', '16px', '#94a3b8')} Refresh
                        </button>
                    </h3>
                    <div id="pipes-list">
                        <div style="text-align: center; padding: 40px; color: #64748b;">
                            Loading pipes...
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            // Load pipes on page load
            document.addEventListener('DOMContentLoaded', loadPipes);
            
            async function loadPipes() {{
                const container = document.getElementById('pipes-list');
                container.innerHTML = '<div style="text-align: center; padding: 40px; color: #64748b;">Loading pipes...</div>';
                
                try {{
                    const resp = await fetch('/api/pipes');
                    const data = await resp.json();
                    
                    if (data.pipes && data.pipes.length > 0) {{
                        let html = '';
                        for (const pipe of data.pipes) {{
                            const statusClass = pipe.notification_channel ? 'status-running' : 'status-paused';
                            const statusText = pipe.notification_channel ? 'Auto-Ingest' : 'Manual';
                            
                            html += `
                            <div class="pipe-card">
                                <div style="display: flex; justify-content: space-between; align-items: start;">
                                    <div class="pipe-name">${{pipe.full_name || 'Unknown'}}</div>
                                    <span class="status-badge ${{statusClass}}">${{statusText}}</span>
                                </div>
                                <div class="pipe-meta">
                                    <div class="pipe-meta-item">
                                        <div class="pipe-meta-label">Created</div>
                                        <div class="pipe-meta-value">${{(pipe.created_on || '').substring(0, 19) || 'N/A'}}</div>
                                    </div>
                                    <div class="pipe-meta-item">
                                        <div class="pipe-meta-label">Owner</div>
                                        <div class="pipe-meta-value">${{pipe.owner || 'N/A'}}</div>
                                    </div>
                                    <div class="pipe-meta-item">
                                        <div class="pipe-meta-label">Type</div>
                                        <div class="pipe-meta-value">${{pipe.kind || 'STAGE'}} ${{pipe.is_external ? '(External)' : '(Internal)'}}</div>
                                    </div>
                                </div>
                                ${{pipe.notification_channel ? `
                                <div class="pipe-meta" style="margin-top: 8px;">
                                    <div class="pipe-meta-item" style="grid-column: 1 / -1;">
                                        <div class="pipe-meta-label">SQS Notification Channel</div>
                                        <div class="pipe-meta-value" style="font-size: 0.8rem;">${{pipe.notification_channel}}</div>
                                    </div>
                                </div>
                                ` : ''}}
                                <div style="margin-top: 12px; display: flex; gap: 8px;">
                                    <button onclick="refreshPipe('${{pipe.full_name}}')" class="btn-secondary" style="padding: 6px 12px; font-size: 0.75rem;">
                                        Refresh Files
                                    </button>
                                    <button onclick="getPipeStatus('${{pipe.full_name}}')" class="btn-secondary" style="padding: 6px 12px; font-size: 0.75rem;">
                                        Check Status
                                    </button>
                                </div>
                                <div id="status-${{pipe.full_name.replace(/\\./g, '-')}}" style="margin-top: 8px; display: none;"></div>
                            </div>
                            `;
                        }}
                        container.innerHTML = html;
                    }} else {{
                        container.innerHTML = `
                            <div style="text-align: center; padding: 40px; color: #64748b;">
                                <div style="font-size: 2rem; margin-bottom: 8px;">📭</div>
                                No Snowpipes found. Create one above to start auto-ingesting data from external stages.
                            </div>
                        `;
                    }}
                }} catch (err) {{
                    container.innerHTML = `<div style="color: #ef4444; padding: 20px;">Error loading pipes: ${{err.message}}</div>`;
                }}
            }}
            
            async function createPipe(event) {{
                event.preventDefault();
                
                const form = document.getElementById('create-pipe-form');
                const formData = new FormData(form);
                const resultPanel = document.getElementById('create-result');
                const resultContent = document.getElementById('result-content');
                
                // Handle checkboxes
                formData.set('auto_ingest', document.getElementById('auto_ingest').checked ? 'true' : 'false');
                formData.set('strip_outer_array', document.getElementById('strip_outer_array').checked ? 'true' : 'false');
                
                resultPanel.className = 'result-panel show';
                resultContent.innerHTML = '<div style="color: #38bdf8;">Creating pipe...</div>';
                
                try {{
                    const resp = await fetch('/api/pipes/create', {{
                        method: 'POST',
                        body: formData
                    }});
                    
                    const data = await resp.json();
                    
                    if (resp.ok && data.success) {{
                        resultPanel.className = 'result-panel show';
                        resultContent.innerHTML = `
                            <div style="color: #22c55e; font-weight: 600; margin-bottom: 12px;">
                                ✓ ${{data.message}}
                            </div>
                            <div style="margin-bottom: 12px;">
                                <div style="color: #94a3b8; font-size: 0.85rem;">Pipe Name:</div>
                                <div style="color: #38bdf8; font-family: monospace;">${{data.pipe_name}}</div>
                            </div>
                            <div style="margin-bottom: 12px;">
                                <div style="color: #94a3b8; font-size: 0.85rem;">Target Table:</div>
                                <div style="color: #e2e8f0; font-family: monospace;">${{data.target_table}}</div>
                            </div>
                            ${{data.notification_channel ? `
                            <div style="margin-bottom: 12px;">
                                <div style="color: #94a3b8; font-size: 0.85rem;">SQS Queue (for S3 events):</div>
                                <div style="color: #f59e0b; font-family: monospace; font-size: 0.8rem; word-break: break-all;">${{data.notification_channel}}</div>
                            </div>
                            ` : ''}}
                            <div style="margin-bottom: 12px;">
                                <div style="color: #94a3b8; font-size: 0.85rem;">Generated DDL:</div>
                                <div class="code-block">${{data.ddl}}</div>
                            </div>
                            <div style="margin-top: 16px;">
                                <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 8px;">Next Steps:</div>
                                <ul style="color: #e2e8f0; margin: 0; padding-left: 20px;">
                                    ${{data.next_steps.map(s => `<li style="margin-bottom: 4px;">${{s}}</li>`).join('')}}
                                </ul>
                            </div>
                        `;
                        loadPipes();  // Refresh the list
                    }} else {{
                        throw new Error(data.detail || 'Failed to create pipe');
                    }}
                }} catch (err) {{
                    resultPanel.className = 'result-panel show error';
                    resultContent.innerHTML = `<div style="color: #ef4444;">Error: ${{err.message}}</div>`;
                }}
            }}
            
            async function refreshPipe(pipeName) {{
                try {{
                    const resp = await fetch(`/api/pipes/refresh/${{encodeURIComponent(pipeName)}}`, {{
                        method: 'POST'
                    }});
                    const data = await resp.json();
                    
                    if (data.success) {{
                        alert(`Refresh triggered for ${{data.files_sent}} files!`);
                    }} else {{
                        alert('Failed to refresh: ' + (data.detail || 'Unknown error'));
                    }}
                }} catch (err) {{
                    alert('Error: ' + err.message);
                }}
            }}
            
            async function getPipeStatus(pipeName) {{
                const statusDiv = document.getElementById('status-' + pipeName.replace(/\\./g, '-'));
                statusDiv.style.display = 'block';
                statusDiv.innerHTML = '<div style="color: #38bdf8; font-size: 0.85rem;">Loading status...</div>';
                
                try {{
                    const resp = await fetch(`/api/pipe/status/${{encodeURIComponent(pipeName)}}`);
                    const data = await resp.json();
                    
                    if (data.status) {{
                        const s = data.status;
                        statusDiv.innerHTML = `
                            <div style="background: rgba(15, 23, 42, 0.8); border-radius: 6px; padding: 12px; font-size: 0.85rem;">
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px;">
                                    <div>
                                        <span style="color: #64748b;">State:</span>
                                        <span style="color: ${{s.executionState === 'RUNNING' ? '#22c55e' : '#f59e0b'}};">${{s.executionState}}</span>
                                    </div>
                                    <div>
                                        <span style="color: #64748b;">Pending Files:</span>
                                        <span style="color: #e2e8f0;">${{s.pendingFileCount}}</span>
                                    </div>
                                    <div>
                                        <span style="color: #64748b;">Last Ingested:</span>
                                        <span style="color: #e2e8f0;">${{s.lastIngestedTimestamp || 'N/A'}}</span>
                                    </div>
                                </div>
                                ${{s.lastIngestedFilePath ? `
                                <div style="margin-top: 8px;">
                                    <span style="color: #64748b;">Last File:</span>
                                    <span style="color: #94a3b8; font-family: monospace; font-size: 0.8rem;">${{s.lastIngestedFilePath}}</span>
                                </div>
                                ` : ''}}
                            </div>
                        `;
                    }} else {{
                        statusDiv.innerHTML = '<div style="color: #64748b; font-size: 0.85rem;">No status available</div>';
                    }}
                }} catch (err) {{
                    statusDiv.innerHTML = `<div style="color: #ef4444; font-size: 0.85rem;">Error: ${{err.message}}</div>`;
                }}
            }}
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(html_content)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
