"""
HP Streaming Eval — Snowpipe Streaming HP Evaluation Wizard
Part of the Flux Data Forge suite.

A lightweight, standalone wizard that helps customers evaluate
Snowpipe Streaming High-Performance (HP) architecture on Snowflake.

Usage:
    pip install -r requirements.txt
    python app.py

    # Or with Snowflake CLI credentials:
    cortex source <connection> --map account=SNOWFLAKE_ACCOUNT \
        --map user=SNOWFLAKE_USER --map password=SNOWFLAKE_PASSWORD \
        -- python app.py
"""

import os
import json
import time
import asyncio
import base64
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import sys
import uvicorn
from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

# ---------------------------------------------------------------------------
# Shared Data Generation Library (Step 3B)
# When co-located with the SPCS app, import the rich utility generators.
# Falls back gracefully when deployed standalone.
# ---------------------------------------------------------------------------
_SHARED_GENERATORS_AVAILABLE = False
try:
    # Try sibling directory first (development layout)
    _spcs_path = os.path.join(os.path.dirname(__file__), '..', 'spcs_app')
    if os.path.isdir(_spcs_path) and _spcs_path not in sys.path:
        sys.path.insert(0, os.path.abspath(_spcs_path))
    from data_generators import (
        generate_ami_reading,
        generate_itron_grid_planning_row,
        generate_symphony_iris_row,
        generate_carto_spatial_row,
        generate_siemens_edge_row,
        DATA_FORMATS as SHARED_DATA_FORMATS,
        UTILITY_PROFILES as SHARED_UTILITY_PROFILES,
    )
    _SHARED_GENERATORS_AVAILABLE = True
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("hp_streaming_eval")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="HP Streaming Eval", version="1.0")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
eval_state: Dict[str, Any] = {
    "connection": None,       # Snowflake connector connection object
    "conn_params": {},        # account, user, role etc.
    "private_key_pem": None,  # PEM string for HP SDK
    "target": {},             # database, schema, table, pipe
    "streaming": {
        "active": False,
        "job_id": None,
        "stats": {"rows_inserted": 0, "rows_sec": 0, "elapsed": 0, "errors": 0, "batches": 0},
        "start_time": None,
    },
}
state_lock = threading.Lock()


# ============================================================================
# LOGO (shared with main Flux Data Forge app)
# ============================================================================
FLUX_LOGO_BASE64 = ""  # populated at bottom of file or loaded from env


def _load_logo():
    global FLUX_LOGO_BASE64
    logo_path = os.path.join(os.path.dirname(__file__), "..", "spcs_app", "fastapi_app.py")
    try:
        with open(logo_path) as f:
            for line in f:
                if "FLUX_LOGO_BASE64" in line and "=" in line and "decode" not in line:
                    FLUX_LOGO_BASE64 = line.split('"')[1]
                    break
    except Exception:
        FLUX_LOGO_BASE64 = ""


_load_logo()


# ============================================================================
# HTML RENDERING
# ============================================================================

def _material_icon(name: str, size: str = "18px", color: str = "#94a3b8") -> str:
    return f'<span class="material-symbols-outlined" style="font-size:{size};color:{color};vertical-align:middle;">{name}</span>'


def _page_html() -> str:
    """Render the full single-page wizard."""
    logo_src = f"data:image/png;base64,{FLUX_LOGO_BASE64}" if FLUX_LOGO_BASE64 else ""
    logo_img = f'<img src="{logo_src}" alt="Flux" style="width:44px;height:44px;border-radius:8px;">' if logo_src else ""

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Streaming Ingest Evaluator — Flux Data Forge</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&display=swap" rel="stylesheet">
<style>
/* ── Reset & Base ─────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
:root {{
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-tertiary: #334155;
    --border: #475569;
    --text-primary: #f1f5f9;
    --text-secondary: #cbd5e1;
    --text-muted: #94a3b8;
    --accent: #38bdf8;
    --accent-sec: #6366f1;
    --success: #22c55e;
    --warning: #f59e0b;
    --error: #ef4444;
    --radius: 12px;
    --shadow: 0 4px 24px rgba(0,0,0,0.3), 0 1px 4px rgba(0,0,0,0.2);
    --shadow-glow: 0 0 20px rgba(56,189,248,0.08);
}}
html {{ height: 100%; }}
body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: linear-gradient(135deg, #0f172a 0%, #0c1222 50%, #0f172a 100%);
    background-attachment: fixed;
    color: var(--text-primary);
    min-height: 100vh;
    line-height: 1.5;
}}
a {{ color: var(--accent); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
code {{
    font-family: 'SF Mono','Fira Code','Monaco',monospace;
    background: rgba(56,189,248,0.08);
    padding: 1px 5px; border-radius: 4px;
    font-size: 0.85em; color: var(--warning);
}}

/* ── Layout ───────────────────────────────────────────── */
.container {{ max-width: 860px; margin: 0 auto; padding: 24px 20px 60px; }}

/* ── Header ───────────────────────────────────────────── */
.header {{
    display: flex; align-items: center; gap: 14px;
    margin-bottom: 8px;
}}
.header h1 {{
    font-size: 1.35rem; font-weight: 700;
    background: linear-gradient(135deg, var(--accent), #a78bfa);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}}
.header .subtitle {{ font-size: 0.8rem; color: var(--text-muted); margin-top: 2px; }}
.badge {{
    display: inline-block; font-size: 0.65rem; font-weight: 600;
    padding: 2px 8px; border-radius: 99px;
    background: rgba(56,189,248,0.12); color: var(--accent);
    letter-spacing: 0.04em; text-transform: uppercase;
}}

/* ── Stepper ──────────────────────────────────────────── */
.stepper {{
    display: flex; gap: 4px; margin: 20px 0 24px;
    background: var(--bg-secondary); border-radius: var(--radius);
    padding: 6px; border: 1px solid rgba(71,85,105,0.4);
}}
.step-tab {{
    flex: 1; text-align: center; padding: 10px 8px;
    border-radius: 8px; cursor: pointer;
    font-size: 0.78rem; font-weight: 500;
    color: var(--text-muted); transition: all 0.2s;
    display: flex; align-items: center; justify-content: center; gap: 6px;
}}
.step-tab:hover {{ background: rgba(56,189,248,0.04); }}
.step-tab.active {{
    background: rgba(56,189,248,0.1);
    color: var(--accent); font-weight: 600;
    box-shadow: 0 1px 8px rgba(56,189,248,0.1);
}}
.step-tab.done {{
    color: var(--success);
}}
.step-tab .num {{
    width: 22px; height: 22px; border-radius: 50%;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 0.7rem; font-weight: 700;
    border: 1.5px solid var(--text-muted);
    flex-shrink: 0;
}}
.step-tab.active .num {{ border-color: var(--accent); color: var(--accent); }}
.step-tab.done .num {{
    border-color: var(--success); background: var(--success); color: #0f172a;
}}

/* ── Panels ───────────────────────────────────────────── */
.panel {{
    background: var(--bg-secondary);
    border: 1px solid rgba(71,85,105,0.4);
    border-radius: var(--radius);
    padding: 28px;
    box-shadow: var(--shadow);
    margin-bottom: 16px;
}}
.panel-title {{
    font-size: 1.05rem; font-weight: 600; margin-bottom: 4px;
    display: flex; align-items: center; gap: 8px;
}}
.panel-desc {{
    color: var(--text-muted); font-size: 0.82rem; margin-bottom: 20px;
    line-height: 1.6;
}}
.step-panel {{ display: none; }}
.step-panel.active {{ display: block; }}

/* ── Forms ────────────────────────────────────────────── */
.form-group {{
    margin-bottom: 16px;
}}
.form-group label {{
    display: block; font-size: 0.78rem; font-weight: 500;
    color: var(--text-secondary); margin-bottom: 5px;
    text-transform: uppercase; letter-spacing: 0.04em;
}}
.form-group label .req {{ color: var(--error); }}
.form-row {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 12px;
}}
input[type="text"], input[type="password"], input[type="number"], textarea, select {{
    width: 100%;
    background: var(--bg-primary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    color: var(--text-primary);
    padding: 10px 12px;
    font-size: 0.88rem;
    font-family: inherit;
    transition: border-color 0.15s;
    outline: none;
}}
input:focus, textarea:focus, select:focus {{
    border-color: var(--accent);
    box-shadow: 0 0 0 2px rgba(56,189,248,0.12);
}}
textarea {{
    font-family: 'SF Mono','Fira Code',monospace;
    font-size: 0.8rem;
    resize: vertical;
    min-height: 120px;
}}
select {{ cursor: pointer; }}
.obj-badge {{ display: block; }}

/* ── Buttons ──────────────────────────────────────────── */
.btn {{
    display: inline-flex; align-items: center; gap: 6px;
    padding: 10px 20px; border-radius: 8px;
    font-size: 0.85rem; font-weight: 600;
    cursor: pointer; border: none;
    transition: all 0.15s;
}}
.btn-primary {{
    background: linear-gradient(135deg, var(--accent), #0ea5e9);
    color: #0f172a;
}}
.btn-primary:hover {{ transform: translateY(-1px); box-shadow: 0 4px 16px rgba(56,189,248,0.3); }}
.btn-primary:disabled {{ opacity: 0.5; cursor: not-allowed; transform: none; box-shadow: none; }}
.btn-secondary {{
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border: 1px solid var(--border);
}}
.btn-secondary:hover {{ background: rgba(71,85,105,0.7); }}
.btn-success {{
    background: linear-gradient(135deg, var(--success), #16a34a);
    color: #0f172a;
}}
.btn-danger {{
    background: linear-gradient(135deg, var(--error), #dc2626);
    color: white;
}}
.btn-sm {{ padding: 7px 14px; font-size: 0.78rem; }}

/* ── Status ───────────────────────────────────────────── */
.status-badge {{
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 0.78rem; font-weight: 500;
    padding: 4px 10px; border-radius: 99px;
}}
.status-badge.ok {{ background: rgba(34,197,94,0.12); color: var(--success); }}
.status-badge.warn {{ background: rgba(245,158,11,0.12); color: var(--warning); }}
.status-badge.err {{ background: rgba(239,68,68,0.12); color: var(--error); }}
.status-badge.info {{ background: rgba(56,189,248,0.12); color: var(--accent); }}

/* ── DDL Preview ──────────────────────────────────────── */
.ddl-preview {{
    background: var(--bg-primary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    padding: 14px 16px;
    font-family: 'SF Mono','Fira Code',monospace;
    font-size: 0.78rem;
    color: var(--text-secondary);
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 300px;
    overflow-y: auto;
    line-height: 1.6;
}}

/* ── Live Dashboard ───────────────────────────────────── */
.metrics-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px; margin: 16px 0;
}}
.metric-card {{
    background: var(--bg-primary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 10px;
    padding: 16px;
    text-align: center;
}}
.metric-card .value {{
    font-size: 1.6rem; font-weight: 700;
    background: linear-gradient(135deg, var(--accent), #a78bfa);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}}
.metric-card .label {{
    font-size: 0.7rem; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.05em;
    margin-top: 4px;
}}

/* ── Report ───────────────────────────────────────────── */
.report-section {{
    background: var(--bg-primary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 8px;
    padding: 16px; margin-bottom: 12px;
}}
.report-section h4 {{
    font-size: 0.82rem; font-weight: 600; margin-bottom: 8px;
    display: flex; align-items: center; gap: 6px;
}}
.report-row {{
    display: flex; justify-content: space-between;
    padding: 5px 0; font-size: 0.82rem;
    border-bottom: 1px solid rgba(71,85,105,0.3);
}}
.report-row:last-child {{ border-bottom: none; }}
.report-row .rk {{ color: var(--text-muted); }}
.report-row .rv {{ color: var(--text-primary); font-weight: 500; }}

/* ── Throughput Chart ─────────────────────────────────── */
.throughput-chart-wrap {{
    background: var(--bg-primary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 10px;
    padding: 14px 16px 10px;
    margin-top: 12px;
}}
.throughput-chart-wrap .chart-header {{
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 8px;
}}
.throughput-chart-wrap .chart-title {{
    font-size: 0.72rem; font-weight: 600; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.04em;
}}
.throughput-chart-wrap .chart-legend {{
    display: flex; gap: 12px; font-size: 0.68rem; color: var(--text-muted);
}}
.throughput-chart-wrap .chart-legend .dot {{
    display: inline-block; width: 8px; height: 8px;
    border-radius: 50%; margin-right: 4px; vertical-align: middle;
}}
.throughput-chart-wrap svg {{
    display: block; width: 100%;
}}

/* ── Utility ──────────────────────────────────────────── */
.mt-12 {{ margin-top: 12px; }}
.mt-16 {{ margin-top: 16px; }}
.mt-20 {{ margin-top: 20px; }}
.mb-8 {{ margin-bottom: 8px; }}
.text-muted {{ color: var(--text-muted); }}
.text-sm {{ font-size: 0.8rem; }}
.flex-end {{ display: flex; justify-content: flex-end; gap: 8px; }}
.hidden {{ display: none !important; }}
.spinner {{
    display: inline-block; width: 16px; height: 16px;
    border: 2px solid rgba(56,189,248,0.2);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.6s linear infinite;
    vertical-align: middle;
}}
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
@keyframes shake {{ 
    0%, 100% {{ transform: translateX(0); }}
    25% {{ transform: translateX(-4px); }}
    75% {{ transform: translateX(4px); }}
}}
.doc-link {{
    color: var(--warning); font-size: 0.78rem; font-weight: 500;
}}
.doc-link:hover {{ opacity: 0.85; }}

/* ── DDL Console ─────────────────────────────────────────── */
.ddl-console {{
    background: #0c0c0c;
    border: 1px solid #333;
    border-radius: 6px;
    padding: 12px 14px;
    margin-top: 14px;
    font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Code', monospace;
    font-size: 0.78rem;
    line-height: 1.7;
    max-height: 220px;
    overflow-y: auto;
    display: none;
}}
.ddl-console.visible {{ display: block; }}
.ddl-console .line {{
    display: flex;
    align-items: flex-start;
    gap: 8px;
}}
.ddl-console .line.ok {{ color: #4ade80; }}
.ddl-console .line.err {{ color: #f87171; }}
.ddl-console .line.warn {{ color: #fbbf24; }}
.ddl-console .line.pending {{ color: #94a3b8; }}
.ddl-console .icon {{ flex-shrink: 0; width: 16px; text-align: center; }}
.ddl-console .label {{ color: #38bdf8; min-width: 140px; }}
.ddl-console .sql {{ color: #64748b; font-size: 0.72rem; margin-left: 4px; }}
.ddl-console .errmsg {{ color: #fbbf24; font-size: 0.72rem; margin-left: 24px; display: block; }}
.ddl-console .errmsg.warn {{ color: #fbbf24; }}

/* ── Decision Guide Cards ──────────────────────────────── */
.guide-card {{
    background: var(--bg-secondary);
    border: 1px solid var(--bg-tertiary);
    border-radius: 10px;
    padding: 14px;
    cursor: pointer;
    transition: all 0.15s ease;
}}
.guide-card:hover {{
    border-color: rgba(56,189,248,0.3);
    background: rgba(56,189,248,0.04);
}}
.guide-card.selected {{
    border-color: var(--accent);
    background: rgba(56,189,248,0.08);
    box-shadow: 0 0 0 1px var(--accent);
}}
.guide-card-icon {{ margin-bottom: 6px; }}
.guide-card-title {{ font-weight: 600; font-size: 0.85rem; color: var(--text-primary); }}
.guide-card-desc {{ font-size: 0.72rem; color: var(--text-muted); margin-top: 4px; line-height: 1.4; }}
.guide-rec {{
    background: var(--bg-secondary);
    border: 2px solid rgba(34,197,94,0.4);
    border-radius: 12px;
    padding: 20px;
}}
.guide-rec-title {{
    font-weight: 700; font-size: 1rem; color: #22c55e; margin-bottom: 8px;
    display: flex; align-items: center; gap: 8px;
}}
.guide-rec-body {{ font-size: 0.85rem; color: var(--text-secondary); line-height: 1.6; }}
.guide-rec-alt {{
    margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--bg-tertiary);
    font-size: 0.78rem; color: var(--text-muted);
}}

@media (max-width: 640px) {{
    .form-row {{ grid-template-columns: 1fr; }}
    .stepper {{ flex-wrap: wrap; }}
    .step-tab {{ font-size: 0.7rem; }}
    .metrics-grid {{ grid-template-columns: 1fr 1fr; }}
}}
</style>
</head>
<body>
<div class="container">
    <!-- Header -->
    <div class="header">
        {logo_img}
        <div>
            <h1>Streaming Ingest Evaluator</h1>
            <div class="subtitle">Snowpipe Streaming Performance Testing &mdash; <span class="badge">Flux Data Forge</span></div>
        </div>
    </div>

    <!-- Stepper -->
    <div class="stepper" id="stepper">
        <div class="step-tab active" data-step="0" onclick="goToStep(0)">
            <span class="num">0</span> Guide
        </div>
        <div class="step-tab" data-step="1" onclick="goToStep(1)">
            <span class="num">1</span> Connect
        </div>
        <div class="step-tab" data-step="2" onclick="goToStep(2)">
            <span class="num">2</span> Configure
        </div>
        <div class="step-tab" data-step="3" onclick="goToStep(3)">
            <span class="num">3</span> Stream
        </div>
        <div class="step-tab" data-step="4" onclick="goToStep(4)">
            <span class="num">4</span> Verify
        </div>
    </div>

    <!-- ================= STEP 0: DECISION GUIDE ================= -->
    <div class="step-panel active" id="step0">
        <div class="panel">
            <div class="panel-title">
                {_material_icon('assistant_navigation', '22px', '#38bdf8')} Streaming Decision Guide
            </div>
            <div class="panel-desc">
                Answer two questions to get a recommendation on which Snowflake streaming
                approach fits your workload. Or <a href="#" onclick="skipGuide(); return false;" style="color: var(--accent);">skip to Connect &rarr;</a>
            </div>

            <!-- Question 1: Source System -->
            <div style="margin-top: 20px;">
                <label class="label">What is your primary data source?</label>
                <div id="guide_sources" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; margin-top: 8px;">
                    <div class="guide-card" data-src="ami" onclick="selectGuideSource(this, 'ami')">
                        <div class="guide-card-icon">{_material_icon('electric_meter', '28px', '#38bdf8')}</div>
                        <div class="guide-card-title">AMI / Smart Meters</div>
                        <div class="guide-card-desc">Itron, Landis+Gyr, Honeywell &mdash; interval reads (15-min)</div>
                    </div>
                    <div class="guide-card" data-src="scada" onclick="selectGuideSource(this, 'scada')">
                        <div class="guide-card-icon">{_material_icon('sensors', '28px', '#22c55e')}</div>
                        <div class="guide-card-title">SCADA / OT</div>
                        <div class="guide-card-desc">OSIsoft PI, Aveva &mdash; sub-second telemetry, voltage, frequency</div>
                    </div>
                    <div class="guide-card" data-src="kafka" onclick="selectGuideSource(this, 'kafka')">
                        <div class="guide-card-icon">{_material_icon('swap_horiz', '28px', '#a855f7')}</div>
                        <div class="guide-card-title">Kafka / Event Bus</div>
                        <div class="guide-card-desc">Confluent, MSK, EventHub &mdash; existing event streaming infra</div>
                    </div>
                    <div class="guide-card" data-src="cloud" onclick="selectGuideSource(this, 'cloud')">
                        <div class="guide-card-icon">{_material_icon('cloud_queue', '28px', '#f59e0b')}</div>
                        <div class="guide-card-title">Cloud Events</div>
                        <div class="guide-card-desc">IoT Hub, PubSub, Kinesis &mdash; managed cloud event services</div>
                    </div>
                    <div class="guide-card" data-src="batch" onclick="selectGuideSource(this, 'batch')">
                        <div class="guide-card-icon">{_material_icon('folder_open', '28px', '#64748b')}</div>
                        <div class="guide-card-title">Batch / Files</div>
                        <div class="guide-card-desc">CSV, Parquet, JSON drops &mdash; periodic file-based ingestion</div>
                    </div>
                    <div class="guide-card" data-src="cdc" onclick="selectGuideSource(this, 'cdc')">
                        <div class="guide-card-icon">{_material_icon('storage', '28px', '#ef4444')}</div>
                        <div class="guide-card-title">Database CDC</div>
                        <div class="guide-card-desc">MySQL, PostgreSQL, Oracle &mdash; change data capture replication</div>
                    </div>
                </div>
            </div>

            <!-- Question 2: Volume -->
            <div id="guide_volume_section" style="margin-top: 20px; display: none;">
                <label class="label">Expected data volume?</label>
                <div id="guide_volumes" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; margin-top: 8px;">
                    <div class="guide-card" data-vol="low" onclick="selectGuideVolume(this, 'low')">
                        <div class="guide-card-title">Low</div>
                        <div class="guide-card-desc">&lt; 1,000 events/sec</div>
                    </div>
                    <div class="guide-card" data-vol="medium" onclick="selectGuideVolume(this, 'medium')">
                        <div class="guide-card-title">Medium</div>
                        <div class="guide-card-desc">1K &ndash; 50K events/sec</div>
                    </div>
                    <div class="guide-card" data-vol="high" onclick="selectGuideVolume(this, 'high')">
                        <div class="guide-card-title">High</div>
                        <div class="guide-card-desc">&gt; 50K events/sec</div>
                    </div>
                </div>
            </div>

            <!-- Recommendation -->
            <div id="guide_recommendation" style="margin-top: 24px; display: none;">
            </div>

            <div class="flex-end mt-20">
                <button class="btn btn-secondary btn-sm" onclick="skipGuide()">
                    Skip Guide
                </button>
                <button class="btn btn-primary" id="btn_to_step1" disabled onclick="completeGuide()">
                    Continue to Connect {_material_icon('arrow_forward', '16px', '#0f172a')}
                </button>
            </div>
        </div>
    </div>

    <!-- ================= STEP 1: CONNECT ================= -->
    <div class="step-panel" id="step1">
        <div class="panel">
            <div class="panel-title">
                {_material_icon('cloud_sync', '22px', '#38bdf8')} Connect to Snowflake
            </div>
            <div class="panel-desc">
                Provide your Snowflake credentials. Choose password auth for quick testing
                or key-pair (RSA) for production-style HP streaming.
                <a class="doc-link" href="https://docs.snowflake.com/en/user-guide/key-pair-auth" target="_blank">Key-pair auth docs &nearr;</a>
            </div>

            <div class="form-row">
                <div class="form-group">
                    <label>Account Identifier <span class="req">*</span></label>
                    <input type="text" id="c_account" placeholder="xy12345 or org-account">
                </div>
                <div class="form-group">
                    <label>Username <span class="req">*</span></label>
                    <input type="text" id="c_user" placeholder="MY_USER">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Role</label>
                    <input type="text" id="c_role" value="SYSADMIN" placeholder="SYSADMIN">
                </div>
                <div class="form-group">
                    <label>Warehouse (for DDL only)</label>
                    <input type="text" id="c_warehouse" placeholder="COMPUTE_WH">
                </div>
            </div>

            <!-- Auth mode toggle -->
            <div class="form-group" style="margin-top:4px;">
                <label>Authentication</label>
                <div style="display:flex;gap:8px;margin-top:4px;">
                    <button type="button" class="btn btn-sm" id="auth_mode_pw"
                            style="background:rgba(56,189,248,0.15);color:var(--accent);border:1px solid var(--accent);"
                            onclick="setAuthMode('password')">
                        {_material_icon('password', '14px', '#38bdf8')} Password
                    </button>
                    <button type="button" class="btn btn-sm" id="auth_mode_kp"
                            style="background:var(--bg-tertiary);color:var(--text-muted);border:1px solid var(--border);"
                            onclick="setAuthMode('keypair')">
                        {_material_icon('key', '14px', '#94a3b8')} Key Pair (RSA)
                    </button>
                </div>
            </div>

            <!-- Password auth fields -->
            <div id="auth_password_fields">
                <div class="form-group">
                    <label>Password <span class="req">*</span></label>
                    <input type="password" id="c_password" placeholder="Enter password">
                </div>
            </div>

            <!-- Key-pair auth fields (hidden by default) -->
            <div id="auth_keypair_fields" style="display:none;">
                <div class="form-group">
                    <label>RSA Private Key (PEM) <span class="req">*</span>
                        &nbsp;<a class="doc-link" href="https://docs.snowflake.com/en/user-guide/key-pair-auth#generate-the-private-key" target="_blank">How to generate &nearr;</a>
                    </label>
                    <textarea id="c_private_key" placeholder="-----BEGIN PRIVATE KEY-----&#10;MIIEvQIBADANBg...&#10;-----END PRIVATE KEY-----"></textarea>
                </div>

                <div class="form-group">
                    <label style="text-transform:none;font-size:0.82rem;">Or upload <code>.p8</code> file</label>
                    <input type="file" id="c_key_file" accept=".p8,.pem" style="font-size:0.8rem;color:var(--text-muted);"
                           onchange="handleKeyUpload(this)">
                </div>
            </div>

            <div style="display:flex;align-items:center;gap:12px;margin-top:20px;">
                <button class="btn btn-primary" onclick="testConnection()">
                    {_material_icon('bolt', '18px', '#0f172a')} Test Connection
                </button>
                <span id="conn_status"></span>
            </div>

            <div class="flex-end mt-20">
                <button class="btn btn-primary" id="btn_to_step2" disabled onclick="goToStep(2)">
                    Configure Target {_material_icon('arrow_forward', '16px', '#0f172a')}
                </button>
            </div>
        </div>
    </div>

    <!-- ================= STEP 2: CONFIGURE ================= -->
    <div class="step-panel" id="step2">
        <div class="panel">
            <div class="panel-title">
                {_material_icon('database', '22px', '#38bdf8')} Configure Target
            </div>
            <div class="panel-desc">
                Choose or create the Snowflake objects for your HP streaming test.
                <a class="doc-link" href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview" target="_blank">HP Architecture Overview &nearr;</a>
            </div>

            <div class="form-row">
                <div class="form-group">
                    <label>Database <span class="req">*</span></label>
                    <div style="position:relative;">
                        <select id="t_database" onchange="onDatabaseChange()" style="width:100%;">
                            <option value="">-- Select or type below --</option>
                        </select>
                        <input type="text" id="t_database_custom" placeholder="Or type new DB name"
                               style="margin-top:4px;font-size:0.75rem;" oninput="onDatabaseCustom()">
                    </div>
                </div>
                <div class="form-group">
                    <label>Schema <span class="req">*</span></label>
                    <div style="position:relative;">
                        <select id="t_schema" onchange="onSchemaChange()" style="width:100%;">
                            <option value="">-- Select database first --</option>
                        </select>
                        <input type="text" id="t_schema_custom" placeholder="Or type new schema"
                               style="margin-top:4px;font-size:0.75rem;" oninput="onSchemaCustom()">
                    </div>
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Table Name <span class="req">*</span></label>
                    <div style="position:relative;">
                        <select id="t_table_select" onchange="onTableSelect()" style="width:100%;">
                            <option value="">-- Select schema first --</option>
                        </select>
                        <input type="text" id="t_table" value="HP_EVAL_READINGS" placeholder="HP_EVAL_READINGS"
                               style="margin-top:4px;font-size:0.75rem;" oninput="updateDdlPreview()">
                        <span class="obj-badge" id="table_badge" style="display:none;font-size:0.65rem;margin-top:2px;"></span>
                    </div>
                </div>
                <div class="form-group">
                    <label>Pipe Name <span class="req">*</span>
                        &nbsp;<a class="doc-link" href="https://docs.snowflake.com/en/sql-reference/sql/create-pipe" target="_blank">CREATE PIPE &nearr;</a>
                    </label>
                    <div style="position:relative;">
                        <select id="t_pipe_select" onchange="onPipeSelect()" style="width:100%;">
                            <option value="">-- Select schema first --</option>
                        </select>
                        <input type="text" id="t_pipe" value="HP_EVAL_PIPE" placeholder="HP_EVAL_PIPE"
                               style="margin-top:4px;font-size:0.75rem;" oninput="updateDdlPreview()">
                        <span class="obj-badge" id="pipe_badge" style="display:none;font-size:0.65rem;margin-top:2px;"></span>
                    </div>
                </div>
            </div>

            <div class="form-group mt-12">
                <label style="text-transform:none;">DDL Preview</label>
                <div class="ddl-preview" id="ddl_preview">-- Fill in fields above to preview DDL</div>
            </div>

            <div style="display:flex;align-items:center;gap:12px;margin-top:12px;">
                <button class="btn btn-success btn-sm" onclick="deployDdl()">
                    {_material_icon('rocket_launch', '16px', '#0f172a')} Deploy DDL
                </button>
                <span id="ddl_status"></span>
            </div>

            <div id="ddl_console" class="ddl-console"></div>

            <div style="display:flex;justify-content:space-between;margin-top:24px;">
                <button class="btn btn-secondary btn-sm" onclick="goToStep(1)">
                    {_material_icon('arrow_back', '14px')} Back
                </button>
                <button class="btn btn-primary" id="btn_to_step3" disabled onclick="goToStep(3)">
                    Stream Data {_material_icon('arrow_forward', '16px', '#0f172a')}
                </button>
            </div>
        </div>
    </div>

    <!-- ================= STEP 3: STREAM ================= -->
    <div class="step-panel" id="step3">
        <div class="panel">
            <div class="panel-title">
                {_material_icon('stream', '22px', '#38bdf8')} Stream Data
            </div>
            <div class="panel-desc">
                Choose a data source and stream through HP Streaming with live metrics.
                <a class="doc-link" href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-python-sdk-reference" target="_blank">Python SDK Reference &nearr;</a>
                &nbsp;&middot;&nbsp;
                <a class="doc-link" href="https://pypi.org/project/snowpipe-streaming/" target="_blank">PyPI: snowpipe-streaming &nearr;</a>
            </div>

            <!-- Source Selector -->
            <div style="margin-bottom:18px;">
                <label style="font-size:0.78rem;color:var(--text-secondary);margin-bottom:6px;display:block;">Data Source</label>
                <div style="display:flex;gap:8px;">
                    <button class="btn btn-sm" id="src_test" onclick="setSource('test')"
                        style="background:rgba(56,189,248,0.15);color:var(--accent);border:1px solid var(--accent);flex:1;">
                        {_material_icon('science', '16px', '#38bdf8')} Test Data
                    </button>
                    <button class="btn btn-sm" id="src_eventhub" onclick="setSource('eventhub')"
                        style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);flex:1;">
                        {_material_icon('cloud_queue', '16px')} Azure EventHub
                    </button>
                    <button class="btn btn-sm" id="src_pubsub" onclick="setSource('pubsub')"
                        style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);flex:1;">
                        {_material_icon('cloud', '16px')} Google PubSub
                    </button>
                </div>
                <div style="font-size:0.68rem;color:var(--text-muted);margin-top:6px;padding:6px 8px;background:rgba(148,163,184,0.06);border-radius:6px;border:1px solid rgba(148,163,184,0.1);">
                    {_material_icon('info', '12px', '#94a3b8')} <b>Snowflake Openflow</b> natively supports Kafka, Kinesis, EventHub, PubSub, MySQL, PostgreSQL, SQL Server, and Oracle connectors.
                    For no-code ingestion, consider Openflow first. This eval benchmarks the HP SDK for maximum throughput control.
                </div>
            </div>

            <!-- EventHub Config (hidden by default) -->
            <div id="eventhub_config" style="display:none;padding:14px;background:var(--bg-tertiary);border-radius:10px;margin-bottom:14px;border:1px solid var(--border);">
                <div style="font-size:0.82rem;font-weight:600;color:var(--accent);margin-bottom:10px;">
                    {_material_icon('cloud_queue', '16px', '#38bdf8')} Azure EventHub Configuration
                </div>
                <div class="form-row">
                    <div class="form-group" style="flex:2;">
                        <label>Connection String</label>
                        <input type="password" id="eh_conn_str" placeholder="Endpoint=sb://...">
                    </div>
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>Event Hub Name</label>
                        <input type="text" id="eh_name" placeholder="my-event-hub">
                    </div>
                    <div class="form-group">
                        <label>Consumer Group</label>
                        <input type="text" id="eh_consumer_group" value="$Default">
                    </div>
                </div>
                <div style="font-size:0.72rem;color:var(--text-muted);margin-top:6px;">
                    Requires <code style="color:var(--accent);">pip install azure-eventhub</code>
                    &nbsp;&middot;&nbsp; Events are consumed and streamed to Snowflake via the SDK.
                </div>
            </div>

            <!-- PubSub Config (hidden by default) -->
            <div id="pubsub_config" style="display:none;padding:14px;background:var(--bg-tertiary);border-radius:10px;margin-bottom:14px;border:1px solid var(--border);">
                <div style="font-size:0.82rem;font-weight:600;color:var(--accent);margin-bottom:10px;">
                    {_material_icon('cloud', '16px', '#38bdf8')} Google PubSub Configuration
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>GCP Project ID</label>
                        <input type="text" id="ps_project" placeholder="my-gcp-project">
                    </div>
                    <div class="form-group">
                        <label>Subscription Name</label>
                        <input type="text" id="ps_subscription" placeholder="my-subscription">
                    </div>
                </div>
                <div class="form-row">
                    <div class="form-group" style="flex:2;">
                        <label>Service Account JSON (paste or upload)</label>
                        <textarea id="ps_credentials" rows="3" placeholder='{{"type":"service_account",...}}' style="font-family:monospace;font-size:0.75rem;resize:vertical;"></textarea>
                    </div>
                </div>
                <div style="font-size:0.72rem;color:var(--text-muted);margin-top:6px;">
                    Requires <code style="color:var(--accent);">pip install google-cloud-pubsub</code>
                    &nbsp;&middot;&nbsp; Messages are pulled and streamed to Snowflake via the SDK.
                </div>
            </div>

            <!-- Test Data Config (shown by default) -->
            <div id="testdata_config">
                <div class="form-row">
                    <div class="form-group">
                        <label>Rows per Second</label>
                        <input type="number" id="s_rows_sec" value="100" min="1" max="50000">
                    </div>
                    <div class="form-group">
                        <label>Batch Size</label>
                        <input type="number" id="s_batch" value="100" min="1" max="10000">
                    </div>
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>Channels</label>
                        <select id="s_channels">
                            <option value="1">1 channel</option>
                            <option value="2">2 channels</option>
                            <option value="4">4 channels</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Data Profile</label>
                        <select id="s_profile">
                            <option value="ami">AMI Smart Meter (energy)</option>
                            <option value="iot">IoT Sensor (temperature)</option>
                            <option value="clickstream">Clickstream (web events)</option>
                            <option value="financial">Financial (transactions)</option>
                            <option value="counter">Simple Counter</option>
                            {'<optgroup label="Energy Solutions (Shared Library)">' +
                             '<option value="utility:standard">Utility AMI (full schema)</option>' +
                             '<option value="utility:itron_grid_planning">Itron Grid Planning (8,760-hr)</option>' +
                             '<option value="utility:symphony_iris">SymphonyAI IRIS Foundry</option>' +
                             '<option value="utility:carto_spatial">CARTO Spatial Analytics</option>' +
                             '<option value="utility:siemens_edge">Siemens Industrial Edge</option>' +
                             '</optgroup>' if _SHARED_GENERATORS_AVAILABLE else ''}
                        </select>
                    </div>
                </div>
            </div>

            <!-- Shared: Batch Size for EventHub/PubSub -->
            <div id="source_batch_config" style="display:none;">
                <div class="form-row">
                    <div class="form-group">
                        <label>Batch Size (rows per flush)</label>
                        <input type="number" id="src_batch" value="100" min="1" max="10000">
                    </div>
                    <div class="form-group">
                        <label>Channels</label>
                        <select id="src_channels">
                            <option value="1">1 channel</option>
                            <option value="2">2 channels</option>
                            <option value="4">4 channels</option>
                        </select>
                    </div>
                </div>
            </div>

            <!-- V1 vs V2 Comparison Toggle (test data + key-pair only) -->
            <div id="compare_toggle" style="display:none;margin-bottom:14px;padding:12px;background:rgba(167,139,250,0.08);border:1px solid rgba(167,139,250,0.3);border-radius:10px;">
                <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:0.82rem;color:var(--text-primary);">
                    <input type="checkbox" id="compare_mode" style="width:16px;height:16px;accent-color:#a78bfa;">
                    {_material_icon('compare_arrows', '18px', '#a78bfa')}
                    <span><b>HP SDK vs SQL INSERT Comparison Mode</b> &mdash; Run HP SDK and SQL INSERT simultaneously on identical data, then compare throughput side-by-side.</span>
                </label>
                <div style="font-size:0.72rem;color:var(--text-muted);margin-top:6px;margin-left:42px;">
                    Requires key-pair auth. Both modes stream the same generated rows to separate tables for fair comparison.
                </div>
            </div>

            <div style="display:flex;align-items:center;gap:12px;margin-top:16px;">
                <button class="btn btn-success" id="btn_start" onclick="startStreaming()">
                    {_material_icon('play_arrow', '18px', '#0f172a')} Start Streaming
                </button>
                <button class="btn btn-danger hidden" id="btn_stop" onclick="stopStreaming()">
                    {_material_icon('stop', '18px', 'white')} Stop
                </button>
                <span id="stream_status"></span>
            </div>

            <!-- Live Metrics -->
            <div class="metrics-grid mt-16" id="live_metrics" style="display:none;">
                <div class="metric-card">
                    <div class="value" id="m_rows">0</div>
                    <div class="label">Rows Inserted</div>
                </div>
                <div class="metric-card">
                    <div class="value" id="m_rps">0</div>
                    <div class="label">Rows / sec</div>
                </div>
                <div class="metric-card">
                    <div class="value" id="m_elapsed">0s</div>
                    <div class="label">Elapsed</div>
                </div>
                <div class="metric-card">
                    <div class="value" id="m_batches">0</div>
                    <div class="label">Batches</div>
                </div>
                <div class="metric-card">
                    <div class="value" id="m_errors">0</div>
                    <div class="label">Errors</div>
                </div>
            </div>

            <!-- Live Throughput Chart -->
            <div class="throughput-chart-wrap" id="throughput_chart_wrap" style="display:none;">
                <div class="chart-header">
                    <span class="chart-title">{_material_icon('show_chart', '14px', '#94a3b8')} Throughput Over Time</span>
                    <div class="chart-legend" id="chart_legend">
                        <span><span class="dot" style="background:#38bdf8;"></span>rows/sec</span>
                    </div>
                </div>
                <div id="throughput_chart_svg" style="height:120px;"></div>
            </div>

            <!-- Live HP SDK vs SQL INSERT Comparison (shown during comparison mode) -->
            <div id="live_compare" style="display:none;margin-top:12px;">
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
                    <div style="padding:10px;background:rgba(56,189,248,0.06);border:1px solid rgba(56,189,248,0.2);border-radius:8px;text-align:center;">
                        <div style="font-size:0.72rem;color:var(--accent);font-weight:600;margin-bottom:4px;">{_material_icon('bolt', '14px', '#38bdf8')} HP SDK</div>
                        <div style="font-size:1.2rem;font-weight:700;color:var(--text-primary);" id="lc_v2_rps">0</div>
                        <div style="font-size:0.68rem;color:var(--text-muted);">rows/sec &middot; <span id="lc_v2_rows">0</span> total</div>
                    </div>
                    <div style="padding:10px;background:rgba(249,115,22,0.06);border:1px solid rgba(249,115,22,0.2);border-radius:8px;text-align:center;">
                        <div style="font-size:0.72rem;color:#f97316;font-weight:600;margin-bottom:4px;">{_material_icon('storage', '14px', '#f97316')} SQL INSERT</div>
                        <div style="font-size:1.2rem;font-weight:700;color:var(--text-primary);" id="lc_v1_rps">0</div>
                        <div style="font-size:0.68rem;color:var(--text-muted);">rows/sec &middot; <span id="lc_v1_rows">0</span> total</div>
                    </div>
                </div>
            </div>

            <div style="display:flex;justify-content:space-between;margin-top:24px;">
                <button class="btn btn-secondary btn-sm" onclick="goToStep(2)">
                    {_material_icon('arrow_back', '14px')} Back
                </button>
                <button class="btn btn-primary" id="btn_to_step4" disabled onclick="goToStep(4)">
                    Verify Results {_material_icon('arrow_forward', '16px', '#0f172a')}
                </button>
            </div>
        </div>
    </div>

    <!-- ================= STEP 4: VERIFY ================= -->
    <div class="step-panel" id="step4">
        <div class="panel">
            <div class="panel-title">
                {_material_icon('verified', '22px', '#22c55e')} Verify &amp; Report
            </div>
            <div class="panel-desc">
                Query the target table and review the evaluation summary.
            </div>

            <div style="display:flex;align-items:center;gap:12px;">
                <button class="btn btn-primary btn-sm" onclick="runVerification()">
                    {_material_icon('query_stats', '16px', '#0f172a')} Run Verification
                </button>
                <span id="verify_status"></span>
            </div>

            <div id="verify_results" class="mt-16" style="display:none;">
                <div class="report-section">
                    <h4>{_material_icon('table_chart', '16px', '#38bdf8')} Table Query</h4>
                    <div id="verify_table_rows"></div>
                </div>
                <div class="report-section">
                    <h4>{_material_icon('analytics', '16px', '#38bdf8')} Channel Status</h4>
                    <div id="verify_channel"></div>
                </div>
                <div class="report-section">
                    <h4>{_material_icon('speed', '16px', '#38bdf8')} Throughput Summary</h4>
                    <div id="verify_throughput"></div>
                </div>
                <div class="report-section" id="verify_perf_section" style="display:none;">
                    <h4>{_material_icon('timer', '16px', '#22c55e')} Batch Latency (ms)</h4>
                    <div id="verify_perf"></div>
                </div>
                <div class="report-section" id="verify_cost_section" style="display:none;">
                    <h4>{_material_icon('payments', '16px', '#f59e0b')} Production Cost Projection</h4>
                    <div style="font-size:0.72rem;color:var(--text-muted);margin-bottom:10px;">
                        Estimated monthly costs extrapolated from your eval throughput. HP SDK uses throughput-based billing ($0.0037/GB ingested). SQL INSERT uses warehouse credits.
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;" id="cost_cards">
                        <div style="padding:12px;background:rgba(56,189,248,0.04);border:1px solid rgba(56,189,248,0.2);border-radius:8px;">
                            <div style="font-size:0.72rem;font-weight:600;color:var(--accent);margin-bottom:6px;">
                                {_material_icon('bolt', '14px', '#38bdf8')} HP SDK (Throughput-Based)
                            </div>
                            <div id="cost_hp"></div>
                        </div>
                        <div style="padding:12px;background:rgba(249,115,22,0.04);border:1px solid rgba(249,115,22,0.2);border-radius:8px;">
                            <div style="font-size:0.72rem;font-weight:600;color:#f97316;margin-bottom:6px;">
                                {_material_icon('storage', '14px', '#f97316')} SQL INSERT (Warehouse Credits)
                            </div>
                            <div id="cost_sql"></div>
                        </div>
                    </div>
                    <div style="font-size:0.68rem;color:var(--text-muted);margin-top:8px;padding:6px 8px;background:rgba(148,163,184,0.04);border-radius:4px;">
                        * Estimates use standard pricing. Actual costs depend on contract, edition, and cloud region.
                        HP SDK: $0.0037/GB ingested. Warehouse credits: $2-4/credit depending on edition.
                    </div>
                </div>
                <div class="report-section" id="verify_compare_section" style="display:none;">
                    <h4>{_material_icon('compare_arrows', '16px', '#a78bfa')} HP SDK vs SQL INSERT Comparison</h4>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;" id="verify_compare">
                        <div style="padding:12px;background:rgba(56,189,248,0.06);border:1px solid rgba(56,189,248,0.2);border-radius:8px;">
                            <div style="font-size:0.78rem;font-weight:600;color:var(--accent);margin-bottom:8px;">
                                {_material_icon('bolt', '16px', '#38bdf8')} HP SDK
                            </div>
                            <div id="cmp_v2_results"></div>
                        </div>
                        <div style="padding:12px;background:rgba(249,115,22,0.06);border:1px solid rgba(249,115,22,0.2);border-radius:8px;">
                            <div style="font-size:0.78rem;font-weight:600;color:#f97316;margin-bottom:8px;">
                                {_material_icon('storage', '16px', '#f97316')} SQL INSERT
                            </div>
                            <div id="cmp_v1_results"></div>
                        </div>
                    </div>
                    <div id="cmp_winner" style="margin-top:10px;padding:8px 12px;border-radius:8px;font-size:0.82rem;font-weight:600;text-align:center;display:none;"></div>
                </div>
            </div>

            <div style="display:flex;justify-content:space-between;margin-top:24px;">
                <button class="btn btn-secondary btn-sm" onclick="goToStep(3)">
                    {_material_icon('arrow_back', '14px')} Back
                </button>
                <div id="btn_export" style="display:none;">
                    <button class="btn btn-primary btn-sm" onclick="exportReportHTML()">
                        {_material_icon('description', '16px', '#0f172a')} Export HTML Report
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="exportReportJSON()" style="margin-left:6px;">
                        {_material_icon('data_object', '14px')} JSON
                    </button>
                </div>
            </div>
        </div>

        <!-- Code Snippets & Deployment Guide -->
        <div class="panel mt-12" id="code_snippets_panel" style="display:none;">
            <div class="panel-title">
                {_material_icon('code', '22px', '#a78bfa')} Code Snippets &amp; Deployment
            </div>
            <div class="panel-desc">
                Production-ready code for your EventHub &rarr; Snowflake pipeline. Click any tab, then copy.
            </div>

            <!-- Snippet Tabs -->
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px;" id="snippet_tabs">
                <button class="btn btn-sm snippet-tab active" data-tab="hp_eventhub" onclick="showSnippet('hp_eventhub')"
                    style="background:rgba(56,189,248,0.15);color:var(--accent);border:1px solid var(--accent);font-size:0.72rem;">
                    HP SDK + EventHub
                </button>
                <button class="btn btn-sm snippet-tab" data-tab="v1_eventhub" onclick="showSnippet('v1_eventhub')"
                    style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);font-size:0.72rem;">
                    SQL INSERT + EventHub
                </button>
                <button class="btn btn-sm snippet-tab" data-tab="ddl_sql" onclick="showSnippet('ddl_sql')"
                    style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);font-size:0.72rem;">
                    SQL DDL
                </button>
                <button class="btn btn-sm snippet-tab" data-tab="docker_deploy" onclick="showSnippet('docker_deploy')"
                    style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);font-size:0.72rem;">
                    Docker Deploy
                </button>
                <button class="btn btn-sm snippet-tab" data-tab="spcs_deploy" onclick="showSnippet('spcs_deploy')"
                    style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);font-size:0.72rem;">
                    SPCS Deploy
                </button>
            </div>

            <!-- Copy Button -->
            <div style="display:flex;justify-content:flex-end;margin-bottom:6px;">
                <button class="btn btn-sm" onclick="copySnippet()" style="background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border);font-size:0.72rem;">
                    {_material_icon('content_copy', '14px')} Copy to Clipboard
                </button>
            </div>

            <!-- HP SDK + EventHub -->
            <pre class="snippet-code" id="snip_hp_eventhub" style="background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:14px;font-size:0.75rem;color:var(--text-primary);overflow-x:auto;max-height:420px;overflow-y:auto;white-space:pre;font-family:'JetBrains Mono',monospace;line-height:1.6;">#!/usr/bin/env python3
\"\"\"EventHub &rarr; Snowflake via Snowpipe Streaming HP SDK.

Requires:
    pip install snowpipe-streaming azure-eventhub cryptography
\"\"\"
import json, time, uuid, threading
from datetime import datetime, timezone
from azure.eventhub import EventHubConsumerClient
from snowflake.ingest.streaming import StreamingIngestClient

# ── Configuration ────────────────────────────────────
SNOWFLAKE_ACCOUNT  = "&lt;your-account&gt;"
SNOWFLAKE_USER     = "&lt;your-user&gt;"
PRIVATE_KEY_PATH   = "rsa_key.p8"       # PKCS8 unencrypted PEM
ROLE               = "SYSADMIN"
DATABASE           = "MY_DB"
SCHEMA             = "PUBLIC"
PIPE               = "MY_STREAMING_PIPE" # must exist (DATA_SOURCE TYPE=STREAMING)

EVENTHUB_CONN_STR  = "&lt;Endpoint=sb://...&gt;"
EVENTHUB_NAME      = "&lt;hub-name&gt;"
CONSUMER_GROUP     = "$Default"

BATCH_SIZE = 500
NUM_CHANNELS = 2

# ── Load RSA key ─────────────────────────────────────
with open(PRIVATE_KEY_PATH, "rb") as f:
    private_key_pem = f.read().decode("utf-8")

# ── HP SDK client &amp; channels ─────────────────────────
props = {{
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "private_key": private_key_pem,
    "role": ROLE,
    "database": DATABASE,
    "schema": SCHEMA,
}}
client = StreamingIngestClient(
    client_name="eventhub_ingest",
    db_name=DATABASE,
    schema_name=SCHEMA,
    pipe_name=PIPE,
    properties=props,
)
channels = []
for i in range(NUM_CHANNELS):
    ch = client.open_channel(
        channel_name=f"eventhub_ch_{{i}}",
    )
    channels.append(ch)
print(f"Opened {{len(channels)}} HP channels against pipe {{PIPE}}")

# ── Batching state ───────────────────────────────────
buffer = []
buffer_lock = threading.Lock()
ch_idx = 0
stats = {{"rows": 0, "batches": 0, "errors": 0}}

def flush():
    global ch_idx
    with buffer_lock:
        if not buffer:
            return
        batch = buffer[:]
        buffer.clear()
    channel = channels[ch_idx % len(channels)]
    ch_idx += 1
    try:
        channel.append_rows(batch)
        stats["rows"] += len(batch)
        stats["batches"] += 1
    except Exception as e:
        stats["errors"] += 1
        print(f"Flush error: {{e}}")

# ── EventHub consumer callback ───────────────────────
def on_event(partition_context, event):
    if event is None:
        return
    try:
        body = json.loads(event.body_as_str())
    except Exception:
        body = {{"raw": event.body_as_str()}}

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    row = {{
        "record_id":     f"eh-{{uuid.uuid4().hex[:12]}}",
        "meter_id":      str(body.get("meter_id", body.get("device_id", "UNKNOWN"))),
        "reading_ts":    str(body.get("reading_ts", body.get("timestamp", ts))),
        "reading_value": float(body.get("reading_value", body.get("value", 0))),
        "unit":          str(body.get("unit", "kWh")),
        "quality":       str(body.get("quality", "VALID")),
        "source":        "eventhub",
    }}
    with buffer_lock:
        buffer.append(row)
        if len(buffer) &gt;= BATCH_SIZE:
            flush()
    partition_context.update_checkpoint()

# ── Main loop ────────────────────────────────────────
consumer = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONN_STR,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENTHUB_NAME,
)
print("Listening to EventHub... (Ctrl+C to stop)")
try:
    consumer.receive(on_event=on_event, starting_position="-1")
except KeyboardInterrupt:
    flush()  # final flush
    print(f"\\nDone. Rows={{stats['rows']}}, Batches={{stats['batches']}}, Errors={{stats['errors']}}")
finally:
    consumer.close()
    for ch in channels:
        ch.close(drop=False, wait_for_flush=True, timeout_seconds=30)
    client.close(wait_for_flush=True, timeout_seconds=30)
</pre>

            <!-- V1 (SQL INSERT) + EventHub -->
            <pre class="snippet-code" id="snip_v1_eventhub" style="display:none;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:14px;font-size:0.75rem;color:var(--text-primary);overflow-x:auto;max-height:420px;overflow-y:auto;white-space:pre;font-family:'JetBrains Mono',monospace;line-height:1.6;">#!/usr/bin/env python3
\"\"\"EventHub &rarr; Snowflake via SQL INSERT (Snowpark).

Requires:
    pip install snowflake-connector-python azure-eventhub
\"\"\"
import json, time, uuid, threading
from datetime import datetime, timezone
from azure.eventhub import EventHubConsumerClient
import snowflake.connector

# ── Configuration ────────────────────────────────────
SNOWFLAKE_ACCOUNT  = "&lt;your-account&gt;"
SNOWFLAKE_USER     = "&lt;your-user&gt;"
SNOWFLAKE_PASSWORD = "&lt;your-password&gt;"
ROLE               = "SYSADMIN"
WAREHOUSE          = "MY_WH"
DATABASE           = "MY_DB"
SCHEMA             = "PUBLIC"
TABLE              = "MY_TABLE"

EVENTHUB_CONN_STR  = "&lt;Endpoint=sb://...&gt;"
EVENTHUB_NAME      = "&lt;hub-name&gt;"
CONSUMER_GROUP     = "$Default"

BATCH_SIZE = 500

# ── Snowflake connection ─────────────────────────────
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
)
fqt = f"{{DATABASE}}.{{SCHEMA}}.{{TABLE}}"
print(f"Connected to Snowflake. Target: {{fqt}}")

# ── Batching state ───────────────────────────────────
buffer = []
buffer_lock = threading.Lock()
stats = {{"rows": 0, "batches": 0, "errors": 0}}

def flush():
    with buffer_lock:
        if not buffer:
            return
        batch = buffer[:]
        buffer.clear()
    try:
        values = ", ".join(
            f"('{{r[\"record_id\"]}}','{{r[\"meter_id\"]}}',\
'{{r[\"reading_ts\"]}}',{{r[\"reading_value\"]}},\
'{{r[\"unit\"]}}','{{r[\"quality\"]}}','{{r[\"source\"]}}')"
            for r in batch
        )
        sql = f\"\"\"INSERT INTO {{fqt}}
            (record_id,meter_id,reading_ts,reading_value,unit,quality,source)
            VALUES {{values}}\"\"\"
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        stats["rows"] += len(batch)
        stats["batches"] += 1
    except Exception as e:
        stats["errors"] += 1
        print(f"Flush error: {{e}}")

# ── EventHub consumer callback ───────────────────────
def on_event(partition_context, event):
    if event is None:
        return
    try:
        body = json.loads(event.body_as_str())
    except Exception:
        body = {{"raw": event.body_as_str()}}

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    row = {{
        "record_id":     f"eh-{{uuid.uuid4().hex[:12]}}",
        "meter_id":      str(body.get("meter_id", body.get("device_id", "UNKNOWN"))),
        "reading_ts":    str(body.get("reading_ts", body.get("timestamp", ts))),
        "reading_value": float(body.get("reading_value", body.get("value", 0))),
        "unit":          str(body.get("unit", "kWh")),
        "quality":       str(body.get("quality", "VALID")),
        "source":        "eventhub",
    }}
    with buffer_lock:
        buffer.append(row)
        if len(buffer) &gt;= BATCH_SIZE:
            flush()
    partition_context.update_checkpoint()

# ── Main loop ────────────────────────────────────────
consumer = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONN_STR,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENTHUB_NAME,
)
print("Listening to EventHub... (Ctrl+C to stop)")
try:
    consumer.receive(on_event=on_event, starting_position="-1")
except KeyboardInterrupt:
    flush()
    print(f"\\nDone. Rows={{stats['rows']}}, Batches={{stats['batches']}}, Errors={{stats['errors']}}")
finally:
    consumer.close()
    conn.close()
</pre>

            <!-- SQL DDL -->
            <pre class="snippet-code" id="snip_ddl_sql" style="display:none;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:14px;font-size:0.75rem;color:var(--text-primary);overflow-x:auto;max-height:420px;overflow-y:auto;white-space:pre;font-family:'JetBrains Mono',monospace;line-height:1.6;">-- ============================================================
-- Snowpipe Streaming DDL (HP SDK + SQL INSERT)
-- ============================================================

-- 1) Target table
CREATE TABLE IF NOT EXISTS MY_DB.PUBLIC.STREAMING_DATA (
    record_id       STRING,
    meter_id        STRING,
    reading_ts      TIMESTAMP_NTZ,
    reading_value   FLOAT,
    unit            STRING,
    quality         STRING,
    source          STRING,
    inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2) Enable schema evolution (auto-add new columns)
ALTER TABLE MY_DB.PUBLIC.STREAMING_DATA
  SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- 3) HP Streaming PIPE (V2 only — not needed for V1 SQL INSERT)
CREATE OR REPLACE PIPE MY_DB.PUBLIC.STREAMING_PIPE
  AS COPY INTO MY_DB.PUBLIC.STREAMING_DATA (
    record_id, meter_id, reading_ts,
    reading_value, unit, quality, source, inserted_at
  )
  FROM (
    SELECT
      $1:record_id::STRING,
      $1:meter_id::STRING,
      $1:reading_ts::TIMESTAMP_NTZ,
      $1:reading_value::FLOAT,
      $1:unit::STRING,
      COALESCE($1:quality::STRING, 'VALID'),
      $1:source::STRING,
      CURRENT_TIMESTAMP()
    FROM TABLE(DATA_SOURCE(TYPE =&gt; 'STREAMING'))
  );

-- 4) Grants
GRANT SELECT, INSERT ON TABLE MY_DB.PUBLIC.STREAMING_DATA TO ROLE SYSADMIN;
GRANT OPERATE ON PIPE MY_DB.PUBLIC.STREAMING_PIPE TO ROLE SYSADMIN;

-- 5) RSA key-pair auth (required for HP SDK)
-- Generate: openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8
-- Extract:  openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
ALTER USER MY_USER SET RSA_PUBLIC_KEY='&lt;paste-public-key-here&gt;';

-- 6) Verify setup
DESCRIBE PIPE MY_DB.PUBLIC.STREAMING_PIPE;
SELECT SYSTEM$PIPE_STATUS('MY_DB.PUBLIC.STREAMING_PIPE');
</pre>

            <!-- Docker Deploy -->
            <pre class="snippet-code" id="snip_docker_deploy" style="display:none;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:14px;font-size:0.75rem;color:var(--text-primary);overflow-x:auto;max-height:420px;overflow-y:auto;white-space:pre;font-family:'JetBrains Mono',monospace;line-height:1.6;"># ============================================================
# Dockerfile — EventHub &rarr; Snowflake Streaming Bridge
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY eventhub_to_snowflake.py .
COPY rsa_key.p8 .

# Environment variables (override at runtime)
ENV SNOWFLAKE_ACCOUNT=""
ENV SNOWFLAKE_USER=""
ENV PRIVATE_KEY_PATH="rsa_key.p8"
ENV ROLE="SYSADMIN"
ENV DATABASE="MY_DB"
ENV SCHEMA="PUBLIC"
ENV PIPE="MY_STREAMING_PIPE"
ENV EVENTHUB_CONN_STR=""
ENV EVENTHUB_NAME=""
ENV CONSUMER_GROUP="$Default"
ENV BATCH_SIZE="500"
ENV NUM_CHANNELS="2"

CMD ["python", "eventhub_to_snowflake.py"]

# ============================================================
# requirements.txt
# ============================================================
# snowpipe-streaming&gt;=1.0.0
# azure-eventhub&gt;=5.11.0
# cryptography&gt;=41.0.0

# ============================================================
# Build &amp; Run
# ============================================================
# docker build -t eh-snowflake-bridge .
# docker run -d --name eh-bridge \
#   -e SNOWFLAKE_ACCOUNT="myorg-myaccount" \
#   -e SNOWFLAKE_USER="MY_USER" \
#   -e EVENTHUB_CONN_STR="Endpoint=sb://..." \
#   -e EVENTHUB_NAME="my-hub" \
#   eh-snowflake-bridge
</pre>

            <!-- SPCS Deploy -->
            <pre class="snippet-code" id="snip_spcs_deploy" style="display:none;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:14px;font-size:0.75rem;color:var(--text-primary);overflow-x:auto;max-height:420px;overflow-y:auto;white-space:pre;font-family:'JetBrains Mono',monospace;line-height:1.6;">-- ============================================================
-- SPCS Deployment — EventHub &rarr; Snowflake Bridge
-- ============================================================

-- 1) Create image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS MY_DB.PUBLIC.IMAGES;

-- 2) Push Docker image (from terminal):
--    docker tag eh-snowflake-bridge:latest \
--      &lt;account&gt;.registry.snowflakecomputing.com/MY_DB/PUBLIC/IMAGES/eh-bridge:latest
--    docker push &lt;account&gt;.registry.snowflakecomputing.com/MY_DB/PUBLIC/IMAGES/eh-bridge:latest

-- 3) Create compute pool
CREATE COMPUTE POOL IF NOT EXISTS EH_BRIDGE_POOL
  MIN_NODES = 1  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_SUSPEND_SECS = 3600;

-- 4) External access (EventHub endpoint)
CREATE OR REPLACE NETWORK RULE EH_EGRESS_RULE
  TYPE = HOST_PORT  MODE = EGRESS
  VALUE_LIST = ('&lt;your-namespace&gt;.servicebus.windows.net:5671');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION EH_ACCESS
  ALLOWED_NETWORK_RULES = (EH_EGRESS_RULE)
  ENABLED = TRUE;

-- 5) Service spec (inline)
CREATE SERVICE MY_DB.PUBLIC.EH_BRIDGE_SERVICE
  IN COMPUTE POOL EH_BRIDGE_POOL
  EXTERNAL_ACCESS_INTEGRATIONS = (EH_ACCESS)
  SPEC = $$
  spec:
    containers:
    - name: bridge
      image: /MY_DB/PUBLIC/IMAGES/eh-bridge:latest
      env:
        SNOWFLAKE_ACCOUNT: "&lt;account&gt;"
        SNOWFLAKE_USER: "&lt;user&gt;"
        EVENTHUB_CONN_STR: "&lt;conn-str&gt;"
        EVENTHUB_NAME: "&lt;hub-name&gt;"
        DATABASE: "MY_DB"
        SCHEMA: "PUBLIC"
        PIPE: "MY_STREAMING_PIPE"
        BATCH_SIZE: "500"
        NUM_CHANNELS: "2"
      resources:
        requests:
          cpu: 0.5
          memory: 512M
        limits:
          cpu: 2
          memory: 2G
      volumeMounts:
      - name: keys
        mountPath: /keys
    volumes:
    - name: keys
      source: "@MY_DB.PUBLIC.KEY_STAGE"
  $$;

-- 6) Verify
SHOW SERVICES IN SCHEMA MY_DB.PUBLIC;
SELECT SYSTEM$GET_SERVICE_STATUS('MY_DB.PUBLIC.EH_BRIDGE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('MY_DB.PUBLIC.EH_BRIDGE_SERVICE', 0, 'bridge', 50);
</pre>
        </div>

        <!-- Next Steps -->
        <div class="panel mt-12" id="next_steps" style="display:none;">
            <div class="panel-title">
                {_material_icon('lightbulb', '22px', '#f59e0b')} Next Steps
            </div>
            <div style="color:var(--text-secondary);font-size:0.85rem;line-height:1.8;">
                <b>1.</b> <a href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started" target="_blank">Complete the Getting Started tutorial</a><br>
                <b>2.</b> <a href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview#billing-and-pricing" target="_blank">Review billing &amp; pricing</a><br>
                <b>3.</b> Integrate HP Streaming into your production pipeline using the <a href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-python-sdk-reference" target="_blank">Python SDK</a><br>
                <b>4.</b> Scale with multiple channels for higher throughput (up to 10 GB/s per table)<br>
            </div>
        </div>
    </div>
</div>

<script>
// ── State ──────────────────────────────────────────────
let currentStep = 0;
const completedSteps = new Set();
let pollTimer = null;

// ── LocalStorage Persistence (Connection Form) ────────
const STORAGE_KEY = 'flux_hp_eval_conn';
const PERSIST_FIELDS = ['c_account', 'c_user', 'c_role', 'c_warehouse'];

function saveConnectionForm() {{
    const data = {{}};
    PERSIST_FIELDS.forEach(id => {{
        const el = document.getElementById(id);
        if (el && el.value) data[id] = el.value;
    }});
    try {{ localStorage.setItem(STORAGE_KEY, JSON.stringify(data)); }} catch(e) {{}}
}}

function loadConnectionForm() {{
    try {{
        const data = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}');
        PERSIST_FIELDS.forEach(id => {{
            const el = document.getElementById(id);
            if (el && data[id]) el.value = data[id];
        }});
    }} catch(e) {{}}
}}

// Attach listeners after DOM ready
setTimeout(() => {{
    PERSIST_FIELDS.forEach(id => {{
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', saveConnectionForm);
    }});
    loadConnectionForm();
}}, 0);

// ── Throughput Chart State ────────────────────────────
const chartData = {{ rps: [], v2_rps: [], v1_rps: [] }};
const CHART_MAX_POINTS = 120;  // 2 minutes at 1s poll

function resetChart() {{
    chartData.rps = [];
    chartData.v2_rps = [];
    chartData.v1_rps = [];
    document.getElementById('throughput_chart_svg').innerHTML = '';
}}

function pushChartPoint(rps, v2Rps, v1Rps) {{
    chartData.rps.push(rps || 0);
    if (v2Rps !== undefined) chartData.v2_rps.push(v2Rps || 0);
    if (v1Rps !== undefined) chartData.v1_rps.push(v1Rps || 0);
    // Trim to max window
    if (chartData.rps.length > CHART_MAX_POINTS) chartData.rps.shift();
    if (chartData.v2_rps.length > CHART_MAX_POINTS) chartData.v2_rps.shift();
    if (chartData.v1_rps.length > CHART_MAX_POINTS) chartData.v1_rps.shift();
    renderChart();
}}

function renderChart() {{
    const container = document.getElementById('throughput_chart_svg');
    const wrap = document.getElementById('throughput_chart_wrap');
    const isCompare = chartData.v2_rps.length > 0;
    const series = isCompare ? [chartData.v2_rps, chartData.v1_rps] : [chartData.rps];
    const colors = isCompare ? ['#38bdf8', '#f97316'] : ['#38bdf8'];

    // Update legend
    const legend = document.getElementById('chart_legend');
    if (isCompare) {{
        legend.innerHTML = '<span><span class="dot" style="background:#38bdf8;"></span>HP SDK</span>' +
                           '<span><span class="dot" style="background:#f97316;"></span>SQL INSERT</span>';
    }} else {{
        legend.innerHTML = '<span><span class="dot" style="background:#38bdf8;"></span>rows/sec</span>';
    }}

    // Dimensions
    const W = container.clientWidth || 600;
    const H = 120;
    const padL = 44, padR = 8, padT = 6, padB = 20;
    const plotW = W - padL - padR;
    const plotH = H - padT - padB;

    // Find global max across all series
    let globalMax = 0;
    series.forEach(s => {{ s.forEach(v => {{ if (v > globalMax) globalMax = v; }}); }});
    if (globalMax === 0) globalMax = 1;
    // Round up to a nice number
    const magnitude = Math.pow(10, Math.floor(Math.log10(globalMax)));
    const niceMax = Math.ceil(globalMax / magnitude) * magnitude || 1;

    // Build SVG
    let svg = `<svg width="${{W}}" height="${{H}}" xmlns="http://www.w3.org/2000/svg">`;

    // Grid lines + Y-axis labels
    const gridSteps = 4;
    for (let i = 0; i <= gridSteps; i++) {{
        const y = padT + (plotH * i / gridSteps);
        const val = niceMax * (1 - i / gridSteps);
        const label = val >= 1000 ? (val / 1000).toFixed(val >= 10000 ? 0 : 1) + 'k' : Math.round(val);
        svg += `<line x1="${{padL}}" y1="${{y}}" x2="${{W - padR}}" y2="${{y}}" stroke="rgba(71,85,105,0.3)" stroke-width="1"/>`;
        svg += `<text x="${{padL - 6}}" y="${{y + 3}}" fill="#94a3b8" font-size="9" font-family="Inter,sans-serif" text-anchor="end">${{label}}</text>`;
    }}

    // X-axis labels (every 15s)
    const maxPts = Math.max(...series.map(s => s.length));
    for (let i = 0; i < maxPts; i += 15) {{
        const x = padL + (i / Math.max(maxPts - 1, 1)) * plotW;
        svg += `<text x="${{x}}" y="${{H - 2}}" fill="#64748b" font-size="8" font-family="Inter,sans-serif" text-anchor="middle">${{i}}s</text>`;
    }}

    // Draw series
    series.forEach((pts, si) => {{
        if (pts.length < 2) return;
        // Gradient area fill
        const areaId = 'area' + si;
        const color = colors[si];
        const opacity = isCompare ? '0.08' : '0.12';
        svg += `<defs><linearGradient id="${{areaId}}" x1="0" y1="0" x2="0" y2="1">`;
        svg += `<stop offset="0%" stop-color="${{color}}" stop-opacity="${{opacity}}"/>`;
        svg += `<stop offset="100%" stop-color="${{color}}" stop-opacity="0"/>`;
        svg += `</linearGradient></defs>`;

        let polyline = '';
        let area = `M ${{padL}},${{padT + plotH}}`;
        pts.forEach((v, i) => {{
            const x = padL + (i / (pts.length - 1)) * plotW;
            const y = padT + plotH - (v / niceMax) * plotH;
            polyline += (i === 0 ? 'M' : 'L') + ` ${{x.toFixed(1)}},${{y.toFixed(1)}} `;
            area += ` L ${{x.toFixed(1)}},${{y.toFixed(1)}}`;
        }});
        area += ` L ${{padL + ((pts.length - 1) / (pts.length - 1)) * plotW}},${{padT + plotH}} Z`;

        svg += `<path d="${{area}}" fill="url(#${{areaId}})"/>`;
        svg += `<polyline points="" fill="none" stroke="${{color}}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" d="${{polyline}}"/>`;

        // Current value dot
        const lastX = padL + plotW;
        const lastY = padT + plotH - (pts[pts.length - 1] / niceMax) * plotH;
        svg += `<circle cx="${{lastX.toFixed(1)}}" cy="${{lastY.toFixed(1)}}" r="3" fill="${{color}}"/>`;
    }});

    svg += '</svg>';
    container.innerHTML = svg;
    wrap.style.display = '';
}}

// ── Decision Guide (Step 0) ──────────────────────────────
let guideSource = null;
let guideVolume = null;

const GUIDE_RECS = {{
    ami:   {{ rec: 'HP SDK', alt: 'SQL INSERT', icon: 'rocket_launch',
             body: 'AMI head-end systems generate high-volume, steady-state meter reads. The <b>HP SDK</b> provides offset tracking so no readings are lost during network hiccups, and throughput-based pricing ($0.0037/GB) is far cheaper than running a warehouse 24/7.',
             openflow: false }},
    scada: {{ rec: 'HP SDK', alt: 'Dual Write (HP + Stage)', icon: 'rocket_launch',
             body: 'SCADA feeds require sustained high throughput with low latency. The <b>HP SDK</b> handles sub-second telemetry at scale. For archival, consider dual-write to both Snowflake and an S3 stage.',
             openflow: false }},
    kafka: {{ rec: 'Openflow (Kafka Connector)', alt: 'HP SDK', icon: 'swap_horiz',
             body: '<b>Snowflake Openflow</b> provides a native, managed Kafka connector (GA on AWS). Zero custom code &mdash; just point it at your Kafka cluster. If Openflow is not available in your region, the HP SDK is the next best option.',
             openflow: true }},
    cloud: {{ rec: 'Openflow (Cloud Connector)', alt: 'HP SDK', icon: 'cloud_sync',
             body: '<b>Snowflake Openflow</b> has native connectors for EventHub (ConsumeAzureEventHub), PubSub (ConsumeGCPubSub), and Kinesis. Managed, no code. Fall back to HP SDK if you need custom transforms in-flight.',
             openflow: true }},
    batch: {{ rec: 'Snowpipe + Stage', alt: 'Snowflake Task', icon: 'folder_open',
             body: 'File-based workloads are best served by <b>Snowpipe auto-ingest</b> (files land in a stage, Snowpipe loads them). For periodic batch loads, a Snowflake Task with COPY INTO is simpler. HP SDK is not needed here.',
             openflow: false }},
    cdc:   {{ rec: 'Openflow (DB Connector)', alt: 'HP SDK', icon: 'storage',
             body: '<b>Snowflake Openflow</b> has native CDC connectors for MySQL, PostgreSQL, SQL Server, and Oracle. Managed change capture with no custom code. If Openflow is not available, use Debezium → HP SDK.',
             openflow: true }},
}};

function selectGuideSource(el, src) {{
    guideSource = src;
    document.querySelectorAll('#guide_sources .guide-card').forEach(c => c.classList.remove('selected'));
    el.classList.add('selected');
    document.getElementById('guide_volume_section').style.display = '';
    guideVolume = null;
    document.querySelectorAll('#guide_volumes .guide-card').forEach(c => c.classList.remove('selected'));
    document.getElementById('guide_recommendation').style.display = 'none';
    document.getElementById('btn_to_step1').disabled = true;
}}

function selectGuideVolume(el, vol) {{
    guideVolume = vol;
    document.querySelectorAll('#guide_volumes .guide-card').forEach(c => c.classList.remove('selected'));
    el.classList.add('selected');
    showGuideRecommendation();
}}

function showGuideRecommendation() {{
    const info = GUIDE_RECS[guideSource];
    if (!info) return;

    // Adjust recommendation for low volume
    let rec = info.rec;
    let body = info.body;
    if (guideVolume === 'low' && (rec === 'HP SDK' || rec.startsWith('Openflow'))) {{
        body += '<br><br><em>At low volume (&lt;1K events/sec), <b>SQL INSERT</b> via Snowpark is also a viable option with simpler setup &mdash; no PIPE objects or key-pair auth required.</em>';
    }}
    if (guideVolume === 'high') {{
        body += '<br><br><em>At high volume (&gt;50K events/sec), HP SDK supports up to 10 GB/s per table using multiple channels. Make sure to size your channel count accordingly.</em>';
    }}

    const openflowBadge = info.openflow
        ? '<span style="display:inline-block;background:rgba(56,189,248,0.15);color:#38bdf8;padding:2px 8px;border-radius:4px;font-size:0.7rem;font-weight:600;margin-left:8px;">OPENFLOW</span>'
        : '';

    const recDiv = document.getElementById('guide_recommendation');
    recDiv.innerHTML = `
        <div class="guide-rec">
            <div class="guide-rec-title">
                <span class="material-symbols-outlined" style="font-size:20px;vertical-align:middle;">${{info.icon}}</span>
                Recommended: ${{rec}} ${{openflowBadge}}
            </div>
            <div class="guide-rec-body">${{body}}</div>
            <div class="guide-rec-alt">
                <b>Alternative:</b> ${{info.alt}}
            </div>
        </div>
    `;
    recDiv.style.display = '';
    document.getElementById('btn_to_step1').disabled = false;
}}

function skipGuide() {{
    markStepDone(0);
    goToStep(1);
}}

function completeGuide() {{
    markStepDone(0);
    goToStep(1);
}}

// ── Navigation ─────────────────────────────────────────
function goToStep(n) {{
    // Only allow forward if previous step is completed
    if (n > currentStep && !completedSteps.has(currentStep)) {{
        // Visual feedback when step is blocked
        const tab = document.querySelector(`.step-tab[data-step="${{n}}"]`);
        if (tab) {{
            tab.style.animation = 'shake 0.3s ease';
            setTimeout(() => tab.style.animation = '', 300);
        }}
        return;
    }}
    currentStep = n;
    document.querySelectorAll('.step-panel').forEach(p => p.classList.remove('active'));
    document.getElementById('step' + n).classList.add('active');
    document.querySelectorAll('.step-tab').forEach(t => {{
        const s = parseInt(t.dataset.step);
        t.classList.toggle('active', s === n);
        t.classList.toggle('done', completedSteps.has(s));
    }});
    // Scroll to top when changing steps
    window.scrollTo({{ top: 0, behavior: 'smooth' }});
}}

function markStepDone(n) {{
    completedSteps.add(n);
    document.querySelectorAll('.step-tab').forEach(t => {{
        if (parseInt(t.dataset.step) === n) t.classList.add('done');
    }});
}}

// ── Step 1: Connect ────────────────────────────────────
let authMode = 'password';

function setAuthMode(mode) {{
    authMode = mode;
    const pwFields = document.getElementById('auth_password_fields');
    const kpFields = document.getElementById('auth_keypair_fields');
    const pwBtn = document.getElementById('auth_mode_pw');
    const kpBtn = document.getElementById('auth_mode_kp');
    if (mode === 'password') {{
        pwFields.style.display = '';
        kpFields.style.display = 'none';
        pwBtn.style.background = 'rgba(56,189,248,0.15)';
        pwBtn.style.color = 'var(--accent)';
        pwBtn.style.borderColor = 'var(--accent)';
        kpBtn.style.background = 'var(--bg-tertiary)';
        kpBtn.style.color = 'var(--text-muted)';
        kpBtn.style.borderColor = 'var(--border)';
    }} else {{
        pwFields.style.display = 'none';
        kpFields.style.display = '';
        kpBtn.style.background = 'rgba(56,189,248,0.15)';
        kpBtn.style.color = 'var(--accent)';
        kpBtn.style.borderColor = 'var(--accent)';
        pwBtn.style.background = 'var(--bg-tertiary)';
        pwBtn.style.color = 'var(--text-muted)';
        pwBtn.style.borderColor = 'var(--border)';
    }}
}}

function handleKeyUpload(input) {{
    const file = input.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = e => {{
        document.getElementById('c_private_key').value = e.target.result;
    }};
    reader.readAsText(file);
}}

async function testConnection() {{
    const statusEl = document.getElementById('conn_status');
    statusEl.innerHTML = '<span class="spinner"></span> Testing...';

    const body = new URLSearchParams();
    body.set('account', document.getElementById('c_account').value.trim());
    body.set('user', document.getElementById('c_user').value.trim());
    body.set('role', document.getElementById('c_role').value.trim());
    body.set('warehouse', document.getElementById('c_warehouse').value.trim());
    body.set('auth_mode', authMode);
    if (authMode === 'password') {{
        body.set('password', document.getElementById('c_password').value);
    }} else {{
        body.set('private_key', document.getElementById('c_private_key').value.trim());
    }}

    try {{
        const resp = await fetch('/api/connect', {{ method: 'POST', body }});
        const data = await resp.json();
        if (data.status === 'ok') {{
            statusEl.innerHTML = '<span class="status-badge ok">&#x2713; Connected &mdash; ' + (data.version || '') + '</span>';
            document.getElementById('btn_to_step2').disabled = false;
            markStepDone(1);
            // Auto-fetch databases for Step 2 cascading dropdowns
            fetchDatabases();
        }} else {{
            statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Connection failed') + '</span>';
        }}
    }} catch (err) {{
        statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
    }}
}}

// ── Step 2: Configure ──────────────────────────────────
// Helper: get effective value from select+custom-input combo
function getDbVal() {{
    const sel = document.getElementById('t_database').value;
    const custom = document.getElementById('t_database_custom').value.trim();
    return custom || sel;
}}
function getSchemaVal() {{
    const sel = document.getElementById('t_schema').value;
    const custom = document.getElementById('t_schema_custom').value.trim();
    return custom || sel;
}}
function getTableVal() {{
    return document.getElementById('t_table').value.trim();
}}
function getPipeVal() {{
    return document.getElementById('t_pipe').value.trim();
}}

// ── Cascading dropdown fetchers ──────────────────────────
async function fetchDatabases() {{
    const sel = document.getElementById('t_database');
    sel.innerHTML = '<option value="">Loading...</option>';
    try {{
        const resp = await fetch('/api/databases');
        const data = await resp.json();
        if (data.status === 'ok') {{
            sel.innerHTML = '<option value="">-- Select database --</option>';
            (data.databases || []).forEach(db => {{
                const opt = document.createElement('option');
                opt.value = db;
                opt.textContent = db;
                sel.appendChild(opt);
            }});
        }} else {{
            sel.innerHTML = '<option value="">-- Error loading --</option>';
        }}
    }} catch (e) {{
        sel.innerHTML = '<option value="">-- Fetch failed --</option>';
    }}
}}

function onDatabaseChange() {{
    const db = document.getElementById('t_database').value;
    document.getElementById('t_database_custom').value = '';
    if (db) fetchSchemas(db);
    else resetSchemas();
    updateDdlPreview();
}}

function onDatabaseCustom() {{
    // User is typing a custom DB — clear the select
    document.getElementById('t_database').value = '';
    resetSchemas();
    updateDdlPreview();
}}

function resetSchemas() {{
    document.getElementById('t_schema').innerHTML = '<option value="">-- Select database first --</option>';
    document.getElementById('t_schema_custom').value = '';
    resetTablesAndPipes();
}}

async function fetchSchemas(db) {{
    const sel = document.getElementById('t_schema');
    sel.innerHTML = '<option value="">Loading...</option>';
    try {{
        const resp = await fetch('/api/schemas/' + encodeURIComponent(db));
        const data = await resp.json();
        if (data.status === 'ok') {{
            sel.innerHTML = '<option value="">-- Select schema --</option>';
            (data.schemas || []).forEach(s => {{
                const opt = document.createElement('option');
                opt.value = s;
                opt.textContent = s;
                sel.appendChild(opt);
            }});
        }} else {{
            sel.innerHTML = '<option value="">-- Error --</option>';
        }}
    }} catch (e) {{
        sel.innerHTML = '<option value="">-- Fetch failed --</option>';
    }}
}}

function onSchemaChange() {{
    const db = getDbVal();
    const schema = document.getElementById('t_schema').value;
    document.getElementById('t_schema_custom').value = '';
    if (db && schema) fetchTablesAndPipes(db, schema);
    else resetTablesAndPipes();
    updateDdlPreview();
}}

function onSchemaCustom() {{
    document.getElementById('t_schema').value = '';
    resetTablesAndPipes();
    updateDdlPreview();
}}

function resetTablesAndPipes() {{
    document.getElementById('t_table_select').innerHTML = '<option value="">-- Select schema first --</option>';
    document.getElementById('t_pipe_select').innerHTML = '<option value="">-- Select schema first --</option>';
    document.getElementById('table_badge').style.display = 'none';
    document.getElementById('pipe_badge').style.display = 'none';
}}

async function fetchTablesAndPipes(db, schema) {{
    const tSel = document.getElementById('t_table_select');
    const pSel = document.getElementById('t_pipe_select');
    tSel.innerHTML = '<option value="">Loading...</option>';
    pSel.innerHTML = '<option value="">Loading...</option>';
    try {{
        const [tResp, pResp] = await Promise.all([
            fetch('/api/tables/' + encodeURIComponent(db) + '/' + encodeURIComponent(schema)),
            fetch('/api/pipes/' + encodeURIComponent(db) + '/' + encodeURIComponent(schema)),
        ]);
        const tData = await tResp.json();
        const pData = await pResp.json();
        tSel.innerHTML = '<option value="">-- New table (type below) --</option>';
        if (tData.status === 'ok') {{
            (tData.tables || []).forEach(t => {{
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                tSel.appendChild(opt);
            }});
        }}
        pSel.innerHTML = '<option value="">-- New pipe (type below) --</option>';
        if (pData.status === 'ok') {{
            (pData.pipes || []).forEach(p => {{
                const opt = document.createElement('option');
                opt.value = p;
                opt.textContent = p;
                pSel.appendChild(opt);
            }});
        }}
    }} catch (e) {{
        tSel.innerHTML = '<option value="">-- Fetch failed --</option>';
        pSel.innerHTML = '<option value="">-- Fetch failed --</option>';
    }}
}}

function onTableSelect() {{
    const v = document.getElementById('t_table_select').value;
    if (v) {{
        document.getElementById('t_table').value = v;
        document.getElementById('table_badge').style.display = '';
        document.getElementById('table_badge').innerHTML = '<span style="color:#22c55e;">&#x2713; Existing table</span>';
    }} else {{
        document.getElementById('table_badge').style.display = 'none';
    }}
    updateDdlPreview();
}}

function onPipeSelect() {{
    const v = document.getElementById('t_pipe_select').value;
    if (v) {{
        document.getElementById('t_pipe').value = v;
        document.getElementById('pipe_badge').style.display = '';
        document.getElementById('pipe_badge').innerHTML = '<span style="color:#22c55e;">&#x2713; Existing pipe</span>';
    }} else {{
        document.getElementById('pipe_badge').style.display = 'none';
    }}
    updateDdlPreview();
}}

function updateDdlPreview() {{
    const db = getDbVal() || 'MY_DB';
    const schema = getSchemaVal() || 'PUBLIC';
    const table = getTableVal() || 'HP_EVAL_READINGS';
    const pipe = getPipeVal() || 'HP_EVAL_PIPE';
    const fqt = db + '.' + schema + '.' + table;
    const fqp = db + '.' + schema + '.' + pipe;

    const ddl = `-- 1) Create target table
CREATE TABLE IF NOT EXISTS ${{fqt}} (
    record_id       STRING,
    meter_id        STRING,
    reading_ts      TIMESTAMP_NTZ,
    reading_value   FLOAT,
    unit            STRING,
    quality         STRING,
    source          STRING,
    inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3) Create streaming PIPE
CREATE OR REPLACE PIPE ${{fqp}}
  AS COPY INTO ${{fqt}} (
    record_id, meter_id, reading_ts,
    reading_value, unit, quality, source, inserted_at
  )
  FROM (
    SELECT
      $1:record_id::STRING,
      $1:meter_id::STRING,
      $1:reading_ts::TIMESTAMP_NTZ,
      $1:reading_value::FLOAT,
      $1:unit::STRING,
      COALESCE($1:quality::STRING, 'VALID'),
      $1:source::STRING,
      CURRENT_TIMESTAMP()
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
  );

-- 3) Grant permissions
GRANT SELECT, INSERT ON TABLE ${{fqt}} TO ROLE SYSADMIN;
GRANT OPERATE ON PIPE ${{fqp}} TO ROLE SYSADMIN;
ALTER TABLE ${{fqt}} SET ENABLE_SCHEMA_EVOLUTION = TRUE;`;

    document.getElementById('ddl_preview').textContent = ddl;
}}

async function deployDdl() {{
    const statusEl = document.getElementById('ddl_status');
    const consoleEl = document.getElementById('ddl_console');
    
    // Show console and clear previous output
    consoleEl.innerHTML = '';
    consoleEl.classList.add('visible');
    statusEl.innerHTML = '<span class="spinner"></span> Deploying...';

    const body = new URLSearchParams();
    body.set('database', getDbVal());
    body.set('schema', getSchemaVal());
    body.set('table', getTableVal());
    body.set('pipe', getPipeVal());

    try {{
        const resp = await fetch('/api/deploy-ddl', {{ method: 'POST', body }});
        const data = await resp.json();
        
        // Render each statement result in console
        if (data.results && Array.isArray(data.results)) {{
            data.results.forEach(r => {{
                const icon = r.status === 'ok' ? '✓' : r.status === 'warn' ? '⚠' : '✗';
                const cls = r.status === 'ok' ? 'ok' : r.status === 'warn' ? 'warn' : 'err';
                const sqlPreview = r.sql ? r.sql.replace(/</g, '&lt;').substring(0, 80) + (r.sql.length > 80 ? '...' : '') : '';
                let html = `<div class="line ${{cls}}"><span class="icon">${{icon}}</span><span class="label">${{r.label}}</span><span class="sql">${{sqlPreview}}</span></div>`;
                if ((r.status === 'error' || r.status === 'warn') && r.message) {{
                    html += `<span class="errmsg ${{cls}}">↳ ${{r.message.replace(/</g, '&lt;')}}</span>`;
                }}
                consoleEl.innerHTML += html;
            }});
        }}
        
        if (data.status === 'ok') {{
            statusEl.innerHTML = '<span class="status-badge ok">&#x2713; ' + (data.message || 'Deployed') + '</span>';
            document.getElementById('btn_to_step3').disabled = false;
            markStepDone(2);
        }} else {{
            statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Deploy failed') + '</span>';
        }}
    }} catch (err) {{
        consoleEl.innerHTML = `<div class="line err"><span class="icon">✗</span><span class="label">Network Error</span></div><span class="errmsg">↳ ${{err.message}}</span>`;
        statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
    }}
}}

// ── Step 3: Source & Stream ────────────────────────────
let sourceMode = 'test';

function setSource(mode) {{
    sourceMode = mode;
    // Toggle config panels
    document.getElementById('testdata_config').style.display = mode === 'test' ? '' : 'none';
    document.getElementById('eventhub_config').style.display = mode === 'eventhub' ? '' : 'none';
    document.getElementById('pubsub_config').style.display = mode === 'pubsub' ? '' : 'none';
    document.getElementById('source_batch_config').style.display = (mode === 'eventhub' || mode === 'pubsub') ? '' : 'none';
    // Comparison toggle only visible for test data
    document.getElementById('compare_toggle').style.display = mode === 'test' ? '' : 'none';
    if (mode !== 'test') document.getElementById('compare_mode').checked = false;
    // Toggle button styles
    ['test', 'eventhub', 'pubsub'].forEach(m => {{
        const btn = document.getElementById('src_' + m);
        if (m === mode) {{
            btn.style.background = 'rgba(56,189,248,0.15)';
            btn.style.color = 'var(--accent)';
            btn.style.borderColor = 'var(--accent)';
        }} else {{
            btn.style.background = 'var(--bg-tertiary)';
            btn.style.color = 'var(--text-secondary)';
            btn.style.borderColor = 'var(--border)';
        }}
    }});
}}

async function startStreaming() {{
    document.getElementById('btn_start').classList.add('hidden');
    document.getElementById('btn_stop').classList.remove('hidden');
    document.getElementById('live_metrics').style.display = '';
    document.getElementById('stream_status').innerHTML = '<span class="status-badge info"><span class="spinner"></span> Streaming...</span>';
    resetChart();
    // Show live comparison if compare mode is on
    const isCompare = sourceMode === 'test' && document.getElementById('compare_mode').checked;
    document.getElementById('live_compare').style.display = isCompare ? '' : 'none';

    const body = new URLSearchParams();
    body.set('source', sourceMode);

    if (sourceMode === 'test') {{
        body.set('rows_per_sec', document.getElementById('s_rows_sec').value);
        body.set('batch_size', document.getElementById('s_batch').value);
        body.set('channels', document.getElementById('s_channels').value);
        body.set('profile', document.getElementById('s_profile').value);
        body.set('compare_mode', document.getElementById('compare_mode').checked ? '1' : '0');
    }} else if (sourceMode === 'eventhub') {{
        body.set('eh_conn_str', document.getElementById('eh_conn_str').value);
        body.set('eh_name', document.getElementById('eh_name').value);
        body.set('eh_consumer_group', document.getElementById('eh_consumer_group').value);
        body.set('batch_size', document.getElementById('src_batch').value);
        body.set('channels', document.getElementById('src_channels').value);
    }} else if (sourceMode === 'pubsub') {{
        body.set('ps_project', document.getElementById('ps_project').value);
        body.set('ps_subscription', document.getElementById('ps_subscription').value);
        body.set('ps_credentials', document.getElementById('ps_credentials').value);
        body.set('batch_size', document.getElementById('src_batch').value);
        body.set('channels', document.getElementById('src_channels').value);
    }}

    try {{
        const resp = await fetch('/api/stream/start', {{ method: 'POST', body }});
        const data = await resp.json();
        if (data.status === 'ok') {{
            pollTimer = setInterval(pollMetrics, 1000);
        }} else {{
            document.getElementById('stream_status').innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Failed') + '</span>';
            document.getElementById('btn_start').classList.remove('hidden');
            document.getElementById('btn_stop').classList.add('hidden');
        }}
    }} catch (err) {{
        document.getElementById('stream_status').innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
        document.getElementById('btn_start').classList.remove('hidden');
        document.getElementById('btn_stop').classList.add('hidden');
    }}
}}

async function stopStreaming() {{
    if (pollTimer) {{ clearInterval(pollTimer); pollTimer = null; }}
    document.getElementById('stream_status').innerHTML = '<span class="spinner"></span> Stopping...';

    try {{
        const resp = await fetch('/api/stream/stop', {{ method: 'POST' }});
        const data = await resp.json();
        document.getElementById('stream_status').innerHTML = '<span class="status-badge ok">&#x2713; Stopped</span>';
    }} catch (err) {{
        document.getElementById('stream_status').innerHTML = '<span class="status-badge warn">&#x26A0; ' + err.message + '</span>';
    }}

    document.getElementById('btn_start').classList.remove('hidden');
    document.getElementById('btn_stop').classList.add('hidden');
    document.getElementById('btn_to_step4').disabled = false;
    markStepDone(3);
}}

async function pollMetrics() {{
    try {{
        const resp = await fetch('/api/stream/metrics');
        const data = await resp.json();
        if (data.active === false) {{
            // Streaming stopped externally
            clearInterval(pollTimer); pollTimer = null;
            document.getElementById('btn_start').classList.remove('hidden');
            document.getElementById('btn_stop').classList.add('hidden');
            document.getElementById('stream_status').innerHTML = '<span class="status-badge ok">&#x2713; Complete</span>';
            document.getElementById('btn_to_step4').disabled = false;
            markStepDone(3);
        }}
        document.getElementById('m_rows').textContent = (data.rows_inserted || 0).toLocaleString();
        document.getElementById('m_rps').textContent = (data.rows_sec || 0).toLocaleString();
        const el = data.elapsed || 0;
        document.getElementById('m_elapsed').textContent = el >= 60 ? Math.floor(el/60) + 'm ' + (el%60) + 's' : el + 's';
        document.getElementById('m_batches').textContent = (data.batches || 0).toLocaleString();
        document.getElementById('m_errors').textContent = (data.errors || 0).toLocaleString();
        // Update throughput chart
        if (data.compare) {{
            pushChartPoint(null, data.compare.v2.rps || 0, data.compare.v1.rps || 0);
        }} else {{
            pushChartPoint(data.rows_sec || 0);
        }}
        // Live comparison stats
        if (data.compare) {{
            document.getElementById('lc_v2_rps').textContent = (data.compare.v2.rps || 0).toLocaleString();
            document.getElementById('lc_v2_rows').textContent = (data.compare.v2.rows || 0).toLocaleString();
            document.getElementById('lc_v1_rps').textContent = (data.compare.v1.rps || 0).toLocaleString();
            document.getElementById('lc_v1_rows').textContent = (data.compare.v1.rows || 0).toLocaleString();
        }}
    }} catch (e) {{ /* ignore transient */ }}
}}

// ── Step 4: Verify ─────────────────────────────────────
async function runVerification() {{
    const statusEl = document.getElementById('verify_status');
    statusEl.innerHTML = '<span class="spinner"></span> Querying...';

    try {{
        const resp = await fetch('/api/verify');
        const data = await resp.json();
        if (data.status === 'ok') {{
            statusEl.innerHTML = '<span class="status-badge ok">&#x2713; Verified</span>';
            document.getElementById('verify_results').style.display = '';
            document.getElementById('btn_export').style.display = '';
            document.getElementById('code_snippets_panel').style.display = '';
            document.getElementById('next_steps').style.display = '';

            // Table rows
            const tr = data.table || {{}};
            document.getElementById('verify_table_rows').innerHTML =
                reportRow('Total Rows', (tr.count || 0).toLocaleString()) +
                reportRow('Min Timestamp', tr.min_ts || '—') +
                reportRow('Max Timestamp', tr.max_ts || '—') +
                reportRow('Distinct Meters', tr.distinct_meters || '—');

            // Channel status
            const ch = data.channel || {{}};
            document.getElementById('verify_channel').innerHTML =
                reportRow('Rows Inserted', (ch.rows_inserted_count || 0).toLocaleString()) +
                reportRow('Rows Parsed', (ch.rows_parsed_count || 0).toLocaleString()) +
                reportRow('Rows Errored', (ch.rows_error_count || 0).toLocaleString()) +
                reportRow('Latest Offset', ch.latest_committed_offset_token || '—');

            // Throughput
            const tp = data.throughput || {{}};
            document.getElementById('verify_throughput').innerHTML =
                reportRow('Avg Rows/sec', (tp.avg_rps || 0).toLocaleString()) +
                reportRow('Total Duration', (tp.duration || '—')) +
                reportRow('Total Batches', (tp.batches || 0).toLocaleString()) +
                reportRow('Peak Rows/sec', (tp.peak_rps || 0).toLocaleString());

            // Performance / Latency
            const pf = data.perf || {{}};
            if (pf.p50_ms !== undefined) {{
                document.getElementById('verify_perf_section').style.display = '';
                document.getElementById('verify_perf').innerHTML =
                    reportRow('P50 (median)', pf.p50_ms + ' ms') +
                    reportRow('P95', pf.p95_ms + ' ms') +
                    reportRow('P99', pf.p99_ms + ' ms') +
                    reportRow('Min', pf.min_ms + ' ms') +
                    reportRow('Max', pf.max_ms + ' ms') +
                    reportRow('Avg', pf.avg_ms + ' ms') +
                    reportRow('Samples', (pf.samples || 0).toLocaleString());
            }}

            // Cost Projection
            const avgRps = tp.avg_rps || 0;
            if (avgRps > 0) {{
                document.getElementById('verify_cost_section').style.display = '';
                // Assumptions: avg row ~250 bytes, 24/7 operation
                const rowBytes = 250;
                const gbPerHour = (avgRps * 3600 * rowBytes) / (1024 * 1024 * 1024);
                const gbPerMonth = gbPerHour * 24 * 30;
                // HP SDK: $0.0037 per GB ingested
                const hpCostMonth = gbPerMonth * 0.0037;
                // SQL INSERT: warehouse credits. XL ~$4/credit/hr, ~25 MB/s throughput
                // At observed rps: MB/s = avgRps * rowBytes / 1M
                const mbPerSec = (avgRps * rowBytes) / (1024 * 1024);
                // Need warehouse size to sustain this throughput
                const whSizes = [
                    {{ name: 'X-Small', mbps: 2, creditHr: 1 }},
                    {{ name: 'Small', mbps: 5, creditHr: 2 }},
                    {{ name: 'Medium', mbps: 10, creditHr: 4 }},
                    {{ name: 'Large', mbps: 15, creditHr: 8 }},
                    {{ name: 'X-Large', mbps: 25, creditHr: 16 }},
                    {{ name: '2X-Large', mbps: 40, creditHr: 32 }},
                ];
                let wh = whSizes[whSizes.length - 1];
                for (const w of whSizes) {{ if (w.mbps >= mbPerSec) {{ wh = w; break; }} }}
                const creditPrice = 3.0;  // standard edition avg
                const sqlCostMonth = wh.creditHr * creditPrice * 24 * 30;

                document.getElementById('cost_hp').innerHTML =
                    reportRow('Data Volume', gbPerMonth.toFixed(1) + ' GB/month') +
                    reportRow('Rate', '$0.0037/GB') +
                    reportRow('Monthly Cost', '<b style="color:var(--accent);">$' + hpCostMonth.toFixed(2) + '</b>') +
                    reportRow('Annual Cost', '$' + (hpCostMonth * 12).toFixed(0));

                document.getElementById('cost_sql').innerHTML =
                    reportRow('Throughput', mbPerSec.toFixed(2) + ' MB/s') +
                    reportRow('Warehouse Size', wh.name + ' (' + wh.creditHr + ' credits/hr)') +
                    reportRow('Monthly Cost', '<b style="color:#f97316;">$' + sqlCostMonth.toLocaleString() + '</b>') +
                    reportRow('Annual Cost', '$' + (sqlCostMonth * 12).toLocaleString());
            }}

            markStepDone(4);
            // Store for export
            window._evalReport = data;

            // Comparison results
            if (data.compare) {{
                document.getElementById('verify_compare_section').style.display = '';
                const v2 = data.compare.v2 || {{}};
                const v1 = data.compare.v1 || {{}};
                document.getElementById('cmp_v2_results').innerHTML =
                    reportRow('Rows Inserted', (v2.rows || 0).toLocaleString()) +
                    reportRow('Avg Rows/sec', (v2.rps || 0).toLocaleString()) +
                    reportRow('Peak Rows/sec', (v2.peak_rps || 0).toLocaleString()) +
                    reportRow('Batches', (v2.batches || 0).toLocaleString()) +
                    reportRow('Errors', (v2.errors || 0).toLocaleString());
                document.getElementById('cmp_v1_results').innerHTML =
                    reportRow('Rows Inserted', (v1.rows || 0).toLocaleString()) +
                    reportRow('Avg Rows/sec', (v1.rps || 0).toLocaleString()) +
                    reportRow('Peak Rows/sec', (v1.peak_rps || 0).toLocaleString()) +
                    reportRow('Batches', (v1.batches || 0).toLocaleString()) +
                    reportRow('Errors', (v1.errors || 0).toLocaleString());
                // Winner banner
                const winnerEl = document.getElementById('cmp_winner');
                const v2rps = v2.rps || 0;
                const v1rps = v1.rps || 0;
                if (v2rps > 0 || v1rps > 0) {{
                    winnerEl.style.display = '';
                    if (v2rps > v1rps) {{
                        const speedup = v1rps > 0 ? (v2rps / v1rps).toFixed(1) : '&#x221e;';
                        winnerEl.style.background = 'rgba(56,189,248,0.1)';
                        winnerEl.style.color = 'var(--accent)';
                        winnerEl.style.border = '1px solid rgba(56,189,248,0.3)';
                        winnerEl.innerHTML = '&#x26A1; HP SDK is ' + speedup + 'x faster than SQL INSERT';
                    }} else if (v1rps > v2rps) {{
                        const speedup = v2rps > 0 ? (v1rps / v2rps).toFixed(1) : '&#x221e;';
                        winnerEl.style.background = 'rgba(249,115,22,0.1)';
                        winnerEl.style.color = '#f97316';
                        winnerEl.style.border = '1px solid rgba(249,115,22,0.3)';
                        winnerEl.innerHTML = 'SQL INSERT is ' + speedup + 'x faster &mdash; unusual, check configuration';
                    }} else {{
                        winnerEl.style.background = 'rgba(148,163,184,0.1)';
                        winnerEl.style.color = 'var(--text-secondary)';
                        winnerEl.style.border = '1px solid var(--border)';
                        winnerEl.innerHTML = 'Both modes performed equally';
                    }}
                }}
            }}
        }} else {{
            statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Failed') + '</span>';
        }}
    }} catch (err) {{
        statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
    }}
}}

function reportRow(key, val) {{
    return '<div class="report-row"><span class="rk">' + key + '</span><span class="rv">' + val + '</span></div>';
}}

function exportReportJSON() {{
    if (!window._evalReport) return;
    const blob = new Blob([JSON.stringify(window._evalReport, null, 2)], {{ type: 'application/json' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'hp_streaming_eval_report.json';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
}}

function exportReportHTML() {{
    const d = window._evalReport;
    if (!d) return;

    const ts = d.timestamp ? new Date(d.timestamp).toLocaleString() : new Date().toLocaleString();
    const tgt = d.target || {{}};
    const tbl = d.table || {{}};
    const ch  = d.channel || {{}};
    const tp  = d.throughput || {{}};
    const pf  = d.perf || {{}};
    const cmp = d.compare || null;

    // ── Executive Summary Logic ──
    const avgRps = tp.avg_rps || 0;
    const peakRps = tp.peak_rps || 0;
    const totalRows = tbl.count || 0;
    const errorRate = ch.rows_error_count && ch.rows_inserted_count
        ? ((ch.rows_error_count / (ch.rows_inserted_count + ch.rows_error_count)) * 100).toFixed(2)
        : '0.00';
    const p50 = pf.p50_ms || '—';
    const p99 = pf.p99_ms || '—';

    // Cost calc (mirrors in-app logic)
    const rowBytes = 250;
    const gbPerHour = (avgRps * 3600 * rowBytes) / (1024 * 1024 * 1024);
    const gbPerMonth = gbPerHour * 24 * 30;
    const hpCostMonth = gbPerMonth * 0.0037;
    const mbPerSec = (avgRps * rowBytes) / (1024 * 1024);
    const whSizes = [
        {{ name: 'X-Small', mbps: 2, creditHr: 1 }},
        {{ name: 'Small', mbps: 5, creditHr: 2 }},
        {{ name: 'Medium', mbps: 10, creditHr: 4 }},
        {{ name: 'Large', mbps: 15, creditHr: 8 }},
        {{ name: 'X-Large', mbps: 25, creditHr: 16 }},
        {{ name: '2X-Large', mbps: 40, creditHr: 32 }},
    ];
    let wh = whSizes[whSizes.length - 1];
    for (const w of whSizes) {{ if (w.mbps >= mbPerSec) {{ wh = w; break; }} }}
    const sqlCostMonth = wh.creditHr * 3.0 * 24 * 30;
    const savings = sqlCostMonth > 0 ? Math.round((1 - hpCostMonth / sqlCostMonth) * 100) : 0;

    // Comparison summary
    let cmpHTML = '';
    if (cmp) {{
        const v2 = cmp.v2 || {{}};
        const v1 = cmp.v1 || {{}};
        const v2rps = v2.rps || 0;
        const v1rps = v1.rps || 0;
        let speedup = '—';
        let winner = 'HP SDK';
        if (v2rps > 0 && v1rps > 0) {{
            speedup = v2rps > v1rps ? (v2rps / v1rps).toFixed(1) + 'x' : (v1rps / v2rps).toFixed(1) + 'x';
            winner = v2rps >= v1rps ? 'HP SDK' : 'SQL INSERT';
        }}
        cmpHTML = `
        <div class="section">
            <h2>Head-to-Head Comparison</h2>
            <table>
                <thead><tr><th>Metric</th><th>HP SDK (Snowpipe Streaming)</th><th>SQL INSERT (Snowpark)</th></tr></thead>
                <tbody>
                    <tr><td>Rows Inserted</td><td>${{(v2.rows || 0).toLocaleString()}}</td><td>${{(v1.rows || 0).toLocaleString()}}</td></tr>
                    <tr><td>Avg Rows/sec</td><td>${{(v2.rps || 0).toLocaleString()}}</td><td>${{(v1.rps || 0).toLocaleString()}}</td></tr>
                    <tr><td>Peak Rows/sec</td><td>${{(v2.peak_rps || 0).toLocaleString()}}</td><td>${{(v1.peak_rps || 0).toLocaleString()}}</td></tr>
                    <tr><td>Batches</td><td>${{(v2.batches || 0).toLocaleString()}}</td><td>${{(v1.batches || 0).toLocaleString()}}</td></tr>
                    <tr><td>Errors</td><td>${{v2.errors || 0}}</td><td>${{v1.errors || 0}}</td></tr>
                </tbody>
            </table>
            <div class="highlight" style="margin-top:12px;">Winner: ${{winner}} (${{speedup}} faster)</div>
        </div>`;
    }}

    // Build recommendation
    let recommendation = 'HP SDK (Snowpipe Streaming) is recommended for this workload.';
    if (avgRps > 5000) {{
        recommendation += ' High throughput sustained at ' + avgRps.toLocaleString() + ' rows/sec confirms HP SDK is the optimal choice for production.';
    }} else if (avgRps > 0) {{
        recommendation += ' Observed throughput of ' + avgRps.toLocaleString() + ' rows/sec is well within HP SDK capability.';
    }}
    if (savings > 50) {{
        recommendation += ' Cost savings of ' + savings + '% vs SQL INSERT make this a clear financial win.';
    }}

    const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HP Streaming Evaluation Report</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; line-height: 1.6; padding: 0; }}
    .container {{ max-width: 820px; margin: 0 auto; padding: 40px 32px; }}
    .header {{ text-align: center; margin-bottom: 36px; padding-bottom: 28px; border-bottom: 1px solid #1e293b; }}
    .header h1 {{ font-size: 1.5rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.01em; }}
    .header .subtitle {{ font-size: 0.85rem; color: #64748b; margin-top: 6px; }}
    .header .badge {{ display: inline-block; margin-top: 12px; padding: 4px 14px; border-radius: 20px; font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }}
    .badge-pass {{ background: rgba(34,197,94,0.15); color: #22c55e; border: 1px solid rgba(34,197,94,0.3); }}
    .badge-fail {{ background: rgba(239,68,68,0.15); color: #ef4444; border: 1px solid rgba(239,68,68,0.3); }}

    .exec-summary {{ background: linear-gradient(135deg, rgba(56,189,248,0.08), rgba(139,92,246,0.06)); border: 1px solid rgba(56,189,248,0.2); border-radius: 12px; padding: 24px; margin-bottom: 28px; }}
    .exec-summary h2 {{ font-size: 0.95rem; font-weight: 700; color: #38bdf8; margin-bottom: 12px; }}
    .exec-summary .rec {{ font-size: 0.85rem; color: #cbd5e1; line-height: 1.7; }}
    .kpi-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 16px; }}
    .kpi {{ background: rgba(15,23,42,0.5); border: 1px solid #1e293b; border-radius: 8px; padding: 12px; text-align: center; }}
    .kpi .val {{ font-size: 1.15rem; font-weight: 700; color: #f8fafc; }}
    .kpi .lbl {{ font-size: 0.68rem; color: #64748b; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.04em; }}

    .section {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 20px; margin-bottom: 20px; }}
    .section h2 {{ font-size: 0.9rem; font-weight: 700; color: #f1f5f9; margin-bottom: 14px; padding-bottom: 8px; border-bottom: 1px solid #334155; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
    th {{ text-align: left; color: #94a3b8; font-weight: 600; padding: 6px 10px; border-bottom: 1px solid #334155; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.03em; }}
    td {{ padding: 7px 10px; border-bottom: 1px solid rgba(51,65,85,0.5); color: #cbd5e1; }}
    td:last-child {{ color: #f1f5f9; font-weight: 500; }}

    .cost-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .cost-card {{ background: rgba(15,23,42,0.5); border: 1px solid #334155; border-radius: 8px; padding: 16px; }}
    .cost-card h3 {{ font-size: 0.8rem; font-weight: 700; margin-bottom: 10px; }}
    .cost-card.hp h3 {{ color: #38bdf8; }}
    .cost-card.sql h3 {{ color: #f97316; }}
    .cost-line {{ display: flex; justify-content: space-between; font-size: 0.78rem; padding: 3px 0; }}
    .cost-line .ck {{ color: #94a3b8; }}
    .cost-line .cv {{ color: #e2e8f0; }}
    .savings {{ text-align: center; margin-top: 14px; padding: 8px 14px; border-radius: 8px; font-size: 0.82rem; font-weight: 600; }}
    .savings.positive {{ background: rgba(34,197,94,0.12); color: #22c55e; border: 1px solid rgba(34,197,94,0.25); }}

    .highlight {{ background: rgba(56,189,248,0.1); border: 1px solid rgba(56,189,248,0.2); border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; font-weight: 600; color: #38bdf8; text-align: center; }}
    .footer {{ text-align: center; margin-top: 32px; padding-top: 20px; border-top: 1px solid #1e293b; font-size: 0.72rem; color: #475569; }}
    @media print {{
        body {{ background: #fff; color: #1e293b; }}
        .container {{ padding: 20px; }}
        .exec-summary {{ background: #f0f9ff; border-color: #bae6fd; }}
        .section {{ background: #f8fafc; border-color: #e2e8f0; }}
        .kpi {{ background: #f8fafc; border-color: #e2e8f0; }}
        .kpi .val {{ color: #0f172a; }}
        th {{ color: #475569; }}
        td {{ color: #1e293b; }}
        td:last-child {{ color: #0f172a; }}
        .cost-card {{ background: #f8fafc; border-color: #e2e8f0; }}
        .cost-line .ck {{ color: #475569; }}
        .cost-line .cv {{ color: #0f172a; }}
    }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>Snowpipe Streaming &mdash; HP SDK Evaluation Report</h1>
        <div class="subtitle">Generated ${{ts}} &bull; Target: ${{tgt.fqt || '—'}}</div>
        <div class="badge ${{parseInt(errorRate) < 1 ? 'badge-pass' : 'badge-fail'}}">
            ${{parseInt(errorRate) < 1 ? 'PASS — All checks passed' : 'REVIEW — Error rate ' + errorRate + '%'}}
        </div>
    </div>

    <div class="exec-summary">
        <h2>Executive Summary</h2>
        <div class="rec">${{recommendation}}</div>
        <div class="kpi-row">
            <div class="kpi"><div class="val">${{avgRps.toLocaleString()}}</div><div class="lbl">Avg Rows/sec</div></div>
            <div class="kpi"><div class="val">${{peakRps.toLocaleString()}}</div><div class="lbl">Peak Rows/sec</div></div>
            <div class="kpi"><div class="val">${{typeof p50 === 'number' ? p50 + 'ms' : p50}}</div><div class="lbl">P50 Latency</div></div>
            <div class="kpi"><div class="val">${{errorRate}}%</div><div class="lbl">Error Rate</div></div>
        </div>
    </div>

    <div class="section">
        <h2>Connection &amp; Target</h2>
        <table>
            <tbody>
                <tr><td>Account</td><td>${{tgt.account || '—'}}</td></tr>
                <tr><td>Database</td><td>${{tgt.database || '—'}}</td></tr>
                <tr><td>Schema</td><td>${{tgt.schema || '—'}}</td></tr>
                <tr><td>Table</td><td>${{tgt.table || '—'}}</td></tr>
                <tr><td>Fully Qualified</td><td>${{tgt.fqt || '—'}}</td></tr>
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>Data Verification</h2>
        <table>
            <tbody>
                <tr><td>Total Rows Landed</td><td>${{(totalRows).toLocaleString()}}</td></tr>
                <tr><td>Distinct Meters</td><td>${{tbl.distinct_meters || '—'}}</td></tr>
                <tr><td>Time Range</td><td>${{tbl.min_ts || '—'}} &rarr; ${{tbl.max_ts || '—'}}</td></tr>
                <tr><td>Rows Inserted (Channel)</td><td>${{(ch.rows_inserted_count || 0).toLocaleString()}}</td></tr>
                <tr><td>Rows Errored</td><td>${{ch.rows_error_count || 0}}</td></tr>
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>Throughput &amp; Performance</h2>
        <table>
            <thead><tr><th>Metric</th><th>Value</th></tr></thead>
            <tbody>
                <tr><td>Avg Rows/sec</td><td>${{avgRps.toLocaleString()}}</td></tr>
                <tr><td>Peak Rows/sec</td><td>${{peakRps.toLocaleString()}}</td></tr>
                <tr><td>Duration</td><td>${{tp.duration || '—'}}</td></tr>
                <tr><td>Batches</td><td>${{(tp.batches || 0).toLocaleString()}}</td></tr>
                ${{pf.p50_ms !== undefined ? `
                <tr><td>P50 Latency</td><td>${{pf.p50_ms}} ms</td></tr>
                <tr><td>P95 Latency</td><td>${{pf.p95_ms}} ms</td></tr>
                <tr><td>P99 Latency</td><td>${{pf.p99_ms}} ms</td></tr>
                <tr><td>Min / Max</td><td>${{pf.min_ms}} ms / ${{pf.max_ms}} ms</td></tr>
                <tr><td>Avg Latency</td><td>${{pf.avg_ms}} ms</td></tr>
                ` : ''}}
            </tbody>
        </table>
    </div>

    ${{avgRps > 0 ? `
    <div class="section">
        <h2>Cost Projection (24/7 Operation)</h2>
        <div class="cost-grid">
            <div class="cost-card hp">
                <h3>HP SDK (Snowpipe Streaming)</h3>
                <div class="cost-line"><span class="ck">Data Volume</span><span class="cv">${{gbPerMonth.toFixed(1)}} GB/month</span></div>
                <div class="cost-line"><span class="ck">Rate</span><span class="cv">$0.0037/GB</span></div>
                <div class="cost-line"><span class="ck">Monthly Cost</span><span class="cv" style="font-weight:700;color:#38bdf8;">$${{hpCostMonth.toFixed(2)}}</span></div>
                <div class="cost-line"><span class="ck">Annual Cost</span><span class="cv">$${{(hpCostMonth * 12).toFixed(0)}}</span></div>
            </div>
            <div class="cost-card sql">
                <h3>SQL INSERT (Warehouse)</h3>
                <div class="cost-line"><span class="ck">Throughput</span><span class="cv">${{mbPerSec.toFixed(2)}} MB/s</span></div>
                <div class="cost-line"><span class="ck">Warehouse Size</span><span class="cv">${{wh.name}} (${{wh.creditHr}} cr/hr)</span></div>
                <div class="cost-line"><span class="ck">Monthly Cost</span><span class="cv" style="font-weight:700;color:#f97316;">$${{sqlCostMonth.toLocaleString()}}</span></div>
                <div class="cost-line"><span class="ck">Annual Cost</span><span class="cv">$${{(sqlCostMonth * 12).toLocaleString()}}</span></div>
            </div>
        </div>
        ${{savings > 0 ? `<div class="savings positive">HP SDK saves ${{savings}}% vs SQL INSERT &mdash; $${{(sqlCostMonth - hpCostMonth).toFixed(0)}}/month</div>` : ''}}
    </div>
    ` : ''}}

    ${{cmpHTML}}

    <div class="footer">
        Snowflake HP Streaming Evaluation &bull; Flux Data Forge &bull; Report generated ${{ts}}<br>
        Raw JSON data embedded below for programmatic access.
    </div>
</div>
<script type="application/json" id="raw-data">
${{JSON.stringify(d, null, 2)}}
<\/script>
</body>
</html>`;

    const blob = new Blob([html], {{ type: 'text/html' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'hp_streaming_eval_report.html';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
}}

// ── Code Snippets ───────────────────────────────────────
let activeSnippet = 'hp_eventhub';

function showSnippet(tab) {{
    activeSnippet = tab;
    document.querySelectorAll('.snippet-code').forEach(el => el.style.display = 'none');
    const target = document.getElementById('snip_' + tab);
    if (target) target.style.display = '';
    document.querySelectorAll('.snippet-tab').forEach(btn => {{
        if (btn.dataset.tab === tab) {{
            btn.style.background = 'rgba(56,189,248,0.15)';
            btn.style.color = 'var(--accent)';
            btn.style.borderColor = 'var(--accent)';
        }} else {{
            btn.style.background = 'var(--bg-tertiary)';
            btn.style.color = 'var(--text-secondary)';
            btn.style.borderColor = 'var(--border)';
        }}
    }});
}}

function copySnippet() {{
    const el = document.getElementById('snip_' + activeSnippet);
    if (!el) return;
    const text = el.textContent || el.innerText;
    navigator.clipboard.writeText(text).then(() => {{
        const btn = event.target.closest('button');
        const orig = btn.innerHTML;
        btn.innerHTML = '&#x2713; Copied!';
        btn.style.color = '#22c55e';
        setTimeout(() => {{ btn.innerHTML = orig; btn.style.color = 'var(--text-secondary)'; }}, 1500);
    }});
}}

// ── Init ───────────────────────────────────────────────
updateDdlPreview();
</script>
</body>
</html>'''


# ============================================================================
# CONNECTION RESILIENCE
# ============================================================================

def _get_connection():
    """Return a live Snowflake connection, reconnecting if the session expired."""
    with state_lock:
        conn = eval_state.get("connection")
        params = eval_state.get("conn_params", {})

    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return conn
        except Exception:
            logger.info("Connection stale, reconnecting...")
            try:
                conn.close()
            except Exception:
                pass

    # Reconnect using stored params
    if not params.get("account"):
        return None

    reconnect_params = {
        "account": params["account"],
        "user": params["user"],
        "role": params.get("role", "SYSADMIN"),
    }
    if params.get("warehouse"):
        reconnect_params["warehouse"] = params["warehouse"]

    if params.get("auth_mode") == "keypair":
        pem = eval_state.get("private_key_pem", "")
        if pem:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            pkb = load_pem_private_key(pem.encode(), password=None)
            reconnect_params["private_key"] = pkb
            reconnect_params["authenticator"] = "snowflake_jwt"
        else:
            return None
    else:
        pwd = params.get("password", "")
        if pwd:
            reconnect_params["password"] = pwd
        else:
            return None

    try:
        new_conn = snowflake.connector.connect(**reconnect_params)
        with state_lock:
            eval_state["connection"] = new_conn
        logger.info("Reconnected to Snowflake")
        return new_conn
    except Exception:
        logger.exception("Reconnection failed")
        return None


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    return _page_html()


@app.post("/api/connect")
async def api_connect(
    account: str = Form(""),
    user: str = Form(""),
    role: str = Form("SYSADMIN"),
    warehouse: str = Form(""),
    auth_mode: str = Form("password"),
    password: str = Form(""),
    private_key: str = Form(""),
):
    """Test Snowflake connection and store credentials."""
    if not account or not user:
        return JSONResponse({"status": "error", "message": "Account and user are required"})

    if auth_mode == "keypair" and not private_key:
        return JSONResponse({"status": "error", "message": "Private key is required for key-pair auth"})
    if auth_mode == "password" and not password:
        return JSONResponse({"status": "error", "message": "Password is required"})

    try:
        import snowflake.connector

        if auth_mode == "keypair":
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            pem_bytes = private_key.strip().encode("utf-8")
            p_key = serialization.load_pem_private_key(pem_bytes, password=None, backend=default_backend())
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            conn_params = {
                "account": account,
                "user": user,
                "role": role,
                "private_key": pkb,
                "authenticator": "snowflake_jwt",
            }
        else:
            conn_params = {
                "account": account,
                "user": user,
                "role": role,
                "password": password,
            }

        if warehouse:
            conn_params["warehouse"] = warehouse

        conn = snowflake.connector.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()

        with state_lock:
            eval_state["connection"] = conn
            eval_state["conn_params"] = {
                "account": account,
                "user": user,
                "role": role,
                "warehouse": warehouse,
                "auth_mode": auth_mode,
                "password": password if auth_mode == "password" else "",
            }
            eval_state["private_key_pem"] = private_key.strip() if auth_mode == "keypair" else None

        return JSONResponse({"status": "ok", "version": version})

    except Exception as e:
        logger.exception("Connection failed")
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/databases")
async def api_databases():
    """Return list of databases visible to the connected user."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected"})
    try:
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        rows = cur.fetchall()
        # name is column index 1
        dbs = [r[1] for r in rows]
        cur.close()
        return JSONResponse({"status": "ok", "databases": sorted(dbs)})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/schemas/{database}")
async def api_schemas(database: str):
    """Return list of schemas in a database."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected"})
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS IN DATABASE \"{database}\"")
        rows = cur.fetchall()
        schemas = [r[1] for r in rows if r[1] != "INFORMATION_SCHEMA"]
        cur.close()
        return JSONResponse({"status": "ok", "schemas": sorted(schemas)})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/tables/{database}/{schema}")
async def api_tables(database: str, schema: str):
    """Return list of tables in a schema."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected"})
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN \"{database}\".\"{schema}\"")
        rows = cur.fetchall()
        tables = [r[1] for r in rows]
        cur.close()
        return JSONResponse({"status": "ok", "tables": sorted(tables)})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/pipes/{database}/{schema}")
async def api_pipes(database: str, schema: str):
    """Return list of pipes in a schema."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected"})
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW PIPES IN \"{database}\".\"{schema}\"")
        rows = cur.fetchall()
        pipes = [r[1] for r in rows]
        cur.close()
        return JSONResponse({"status": "ok", "pipes": sorted(pipes)})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.get("/api/warehouses")
async def api_warehouses():
    """Return list of warehouses visible to the connected user."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected"})
    try:
        cur = conn.cursor()
        cur.execute("SHOW WAREHOUSES")
        rows = cur.fetchall()
        whs = [r[0] for r in rows]
        cur.close()
        return JSONResponse({"status": "ok", "warehouses": sorted(whs)})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)[:300]})


@app.post("/api/deploy-ddl")
async def api_deploy_ddl(
    database: str = Form(""),
    schema: str = Form(""),
    table: str = Form(""),
    pipe: str = Form(""),
):
    """Deploy table + pipe DDL."""
    conn = _get_connection()
    if not conn:
        return JSONResponse({"status": "error", "message": "Not connected. Complete Step 1 first."})
    if not database or not schema or not table or not pipe:
        return JSONResponse({"status": "error", "message": "All fields are required"})

    fqt = f"{database}.{schema}.{table}"
    fqp = f"{database}.{schema}.{pipe}"

    ddl_statements = [
        ("CREATE SCHEMA", f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}"),
        ("CREATE TABLE", f"""CREATE TABLE IF NOT EXISTS {fqt} (
            record_id       STRING,
            meter_id        STRING,
            reading_ts      TIMESTAMP_NTZ,
            reading_value   FLOAT,
            unit            STRING,
            quality         STRING,
            source          STRING,
            inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),
        ("CREATE PIPE", f"""CREATE PIPE IF NOT EXISTS {fqp}
            AS COPY INTO {fqt} (
                record_id, meter_id, reading_ts,
                reading_value, unit, quality, source, inserted_at
            )
            FROM (
                SELECT
                    $1:record_id::STRING,
                    $1:meter_id::STRING,
                    $1:reading_ts::TIMESTAMP_NTZ,
                    $1:reading_value::FLOAT,
                    $1:unit::STRING,
                    COALESCE($1:quality::STRING, 'VALID'),
                    $1:source::STRING,
                    CURRENT_TIMESTAMP()
                FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
            )"""),
        ("GRANT TABLE", f"GRANT SELECT, INSERT ON TABLE {fqt} TO ROLE SYSADMIN"),
        ("GRANT PIPE", f"GRANT OPERATE ON PIPE {fqp} TO ROLE SYSADMIN"),
        ("SCHEMA EVOLUTION", f"ALTER TABLE {fqt} SET ENABLE_SCHEMA_EVOLUTION = TRUE"),
    ]

    # Labels that are non-blocking (soft-fail) - may fail due to permissions or object exists
    soft_fail_labels = {"CREATE SCHEMA", "GRANT TABLE", "GRANT PIPE"}
    
    results = []
    has_blocking_error = False
    cur = conn.cursor()
    try:
        for label, stmt in ddl_statements:
            try:
                cur.execute(stmt)
                results.append({"label": label, "status": "ok", "sql": stmt.strip()[:120]})
            except Exception as e:
                is_soft = label in soft_fail_labels
                if not is_soft:
                    has_blocking_error = True
                results.append({
                    "label": label,
                    "status": "warn" if is_soft else "error",
                    "sql": stmt.strip()[:120],
                    "message": str(e)[:200]
                })
    finally:
        cur.close()

    with state_lock:
        eval_state["target"] = {
            "database": database,
            "schema": schema,
            "table": table,
            "pipe": pipe,
            "fqt": fqt,
            "fqp": fqp,
        }

    # Check if any warnings (soft-fails) occurred
    has_warnings = any(r.get("status") == "warn" for r in results)
    
    if not has_blocking_error:
        msg = f"Table + PIPE created: {fqp}"
        if has_warnings:
            msg += " (with warnings)"
        return JSONResponse({"status": "ok", "message": msg, "results": results})
    else:
        return JSONResponse({"status": "error", "message": "One or more statements failed", "results": results})


@app.post("/api/stream/start")
async def api_stream_start(
    source: str = Form("test"),
    rows_per_sec: int = Form(100),
    batch_size: int = Form(100),
    channels: int = Form(1),
    profile: str = Form("ami"),
    # EventHub params
    eh_conn_str: str = Form(""),
    eh_name: str = Form(""),
    eh_consumer_group: str = Form("$Default"),
    # PubSub params
    ps_project: str = Form(""),
    ps_subscription: str = Form(""),
    ps_credentials: str = Form(""),
    # Comparison mode
    compare_mode: str = Form("0"),
):
    """Start streaming worker thread (test data, EventHub, or PubSub source)."""
    with state_lock:
        if eval_state["streaming"]["active"]:
            return JSONResponse({"status": "error", "message": "Already streaming"})
        pem = eval_state.get("private_key_pem")
        conn_params = eval_state.get("conn_params", {})
        target = eval_state.get("target", {})

    if not target.get("pipe"):
        return JSONResponse({"status": "error", "message": "Complete Steps 1 & 2 first"})

    # Validate source-specific params
    if source == "eventhub":
        if not eh_conn_str or not eh_name:
            return JSONResponse({"status": "error", "message": "EventHub connection string and name are required"})
        try:
            from azure.eventhub import EventHubConsumerClient  # noqa: F401
        except ImportError:
            return JSONResponse({"status": "error", "message": "azure-eventhub not installed. Run: pip install azure-eventhub"})
    elif source == "pubsub":
        if not ps_project or not ps_subscription:
            return JSONResponse({"status": "error", "message": "GCP Project ID and Subscription name are required"})
        try:
            from google.cloud import pubsub_v1  # noqa: F401
        except ImportError:
            return JSONResponse({"status": "error", "message": "google-cloud-pubsub not installed. Run: pip install google-cloud-pubsub"})

    # Determine streaming mode: HP SDK (key-pair) or SQL INSERT (password)
    use_hp_sdk = bool(pem)
    is_compare = compare_mode == "1" and source == "test" and use_hp_sdk

    job_id = str(uuid.uuid4())[:8]
    with state_lock:
        eval_state["streaming"] = {
            "active": True,
            "job_id": job_id,
            "stats": {"rows_inserted": 0, "rows_sec": 0, "elapsed": 0, "errors": 0, "batches": 0, "peak_rps": 0, "batch_latencies": []},
            "start_time": time.time(),
            "stop_requested": False,
            "mode": "comparison" if is_compare else ("hp_sdk" if use_hp_sdk else "sql_insert"),
            "source": source,
        }
        if is_compare:
            eval_state["streaming"]["compare"] = {
                "v2": {"rows": 0, "rps": 0, "batches": 0, "errors": 0, "peak_rps": 0},
                "v1": {"rows": 0, "rps": 0, "batches": 0, "errors": 0, "peak_rps": 0},
            }

    if is_compare:
        t = threading.Thread(
            target=_comparison_worker,
            args=(job_id, conn_params, target, pem, rows_per_sec, batch_size, channels, profile),
            daemon=True,
        )
    elif source == "eventhub":
        t = threading.Thread(
            target=_eventhub_worker,
            args=(job_id, conn_params, target, pem, eh_conn_str, eh_name, eh_consumer_group, batch_size, channels),
            daemon=True,
        )
    elif source == "pubsub":
        t = threading.Thread(
            target=_pubsub_worker,
            args=(job_id, conn_params, target, pem, ps_project, ps_subscription, ps_credentials, batch_size, channels),
            daemon=True,
        )
    elif use_hp_sdk:
        t = threading.Thread(
            target=_streaming_worker,
            args=(job_id, conn_params, target, pem, rows_per_sec, batch_size, channels, profile),
            daemon=True,
        )
    else:
        t = threading.Thread(
            target=_sql_insert_worker,
            args=(job_id, conn_params, target, rows_per_sec, batch_size, profile),
            daemon=True,
        )
    t.start()

    return JSONResponse({"status": "ok", "job_id": job_id, "source": source})


@app.post("/api/stream/stop")
async def api_stream_stop():
    """Signal the streaming worker to stop."""
    with state_lock:
        eval_state["streaming"]["stop_requested"] = True
    # Give worker time to flush
    time.sleep(1)
    return JSONResponse({"status": "ok"})


@app.get("/api/stream/metrics")
async def api_stream_metrics():
    """Return current streaming metrics."""
    with state_lock:
        s = eval_state["streaming"]
        stats = dict(s.get("stats", {}))
        stats["active"] = s.get("active", False)
        stats["mode"] = s.get("mode", "")
        compare = s.get("compare")
        if compare:
            stats["compare"] = compare
    return JSONResponse(stats)


@app.get("/api/verify")
async def api_verify():
    """Run verification queries and return report."""
    with state_lock:
        conn = eval_state.get("connection")
        target = eval_state.get("target", {})
        stream_stats = dict(eval_state["streaming"].get("stats", {}))
        start_time = eval_state["streaming"].get("start_time")

    if not conn or not target.get("fqt"):
        return JSONResponse({"status": "error", "message": "No target configured"})

    fqt = target["fqt"]
    table_info = {}
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*), MIN(reading_ts), MAX(reading_ts), COUNT(DISTINCT meter_id) FROM {fqt}")
        row = cur.fetchone()
        table_info = {
            "count": row[0],
            "min_ts": str(row[1]) if row[1] else None,
            "max_ts": str(row[2]) if row[2] else None,
            "distinct_meters": row[3],
        }
        cur.close()
    except Exception as e:
        logger.exception("Verify query failed")
        return JSONResponse({"status": "error", "message": str(e)[:300]})

    # Build throughput summary
    elapsed = stream_stats.get("elapsed", 0)
    duration_str = f"{elapsed // 60}m {elapsed % 60}s" if elapsed >= 60 else f"{elapsed}s"
    throughput = {
        "avg_rps": stream_stats.get("rows_sec", 0),
        "duration": duration_str,
        "batches": stream_stats.get("batches", 0),
        "peak_rps": stream_stats.get("peak_rps", 0),
    }

    # Compute latency percentiles from batch_latencies
    latencies = sorted(stream_stats.get("batch_latencies", []))
    perf = {}
    if latencies:
        n = len(latencies)
        perf = {
            "p50_ms": round(latencies[int(n * 0.50)], 1),
            "p95_ms": round(latencies[min(int(n * 0.95), n - 1)], 1),
            "p99_ms": round(latencies[min(int(n * 0.99), n - 1)], 1),
            "min_ms": round(latencies[0], 1),
            "max_ms": round(latencies[-1], 1),
            "avg_ms": round(sum(latencies) / n, 1),
            "samples": n,
        }

    # Channel status (best-effort from SDK)
    channel_info = {
        "rows_inserted_count": stream_stats.get("rows_inserted", 0),
        "rows_parsed_count": stream_stats.get("rows_inserted", 0) + stream_stats.get("errors", 0),
        "rows_error_count": stream_stats.get("errors", 0),
        "latest_committed_offset_token": stream_stats.get("batches", 0),
    }

    report = {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "target": target,
        "table": table_info,
        "channel": channel_info,
        "throughput": throughput,
        "perf": perf,
    }

    # Include comparison data if available
    with state_lock:
        compare = eval_state["streaming"].get("compare")
        mode = eval_state["streaming"].get("mode", "")
    if compare and mode == "comparison":
        report["compare"] = compare

    return JSONResponse(report)


# ============================================================================
# STREAMING WORKER
# ============================================================================

def _generate_row(profile: str, seq: int) -> dict:
    """Generate a single test row based on profile."""
    import random
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rid = f"eval-{uuid.uuid4().hex[:12]}"

    if profile == "ami":
        return {
            "record_id": rid,
            "meter_id": f"MTR-{random.randint(1000, 9999)}",
            "reading_ts": ts,
            "reading_value": round(random.uniform(0.1, 45.0), 4),
            "unit": "kWh",
            "quality": "VALID",
            "source": "hp_eval",
        }
    elif profile == "iot":
        return {
            "record_id": rid,
            "meter_id": f"SENSOR-{random.randint(100, 999)}",
            "reading_ts": ts,
            "reading_value": round(random.uniform(-10.0, 50.0), 2),
            "unit": "celsius",
            "quality": "VALID",
            "source": "hp_eval",
        }
    elif profile == "clickstream":
        pages = ["/home", "/products", "/cart", "/checkout", "/account", "/search", "/api/v1/data", "/dashboard"]
        actions = ["page_view", "click", "scroll", "form_submit", "add_to_cart", "search"]
        return {
            "record_id": rid,
            "meter_id": f"SESSION-{random.randint(10000, 99999)}",
            "reading_ts": ts,
            "reading_value": round(random.uniform(0.1, 30.0), 3),  # duration_sec
            "unit": random.choice(actions),
            "quality": random.choice(pages),
            "source": f"web-{random.choice(['desktop', 'mobile', 'tablet'])}",
        }
    elif profile == "financial":
        txn_types = ["purchase", "refund", "transfer", "withdrawal", "deposit", "payment"]
        currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
        return {
            "record_id": rid,
            "meter_id": f"ACCT-{random.randint(100000, 999999)}",
            "reading_ts": ts,
            "reading_value": round(random.uniform(1.0, 5000.0), 2),  # amount
            "unit": random.choice(currencies),
            "quality": random.choice(txn_types),
            "source": f"channel-{random.choice(['pos', 'online', 'atm', 'mobile', 'wire'])}",
        }
    else:  # counter
        return {
            "record_id": rid,
            "meter_id": "COUNTER-1",
            "reading_ts": ts,
            "reading_value": float(seq),
            "unit": "count",
            "quality": "VALID",
            "source": "hp_eval",
        }


# Utility-profile generator: uses shared data_generators when available.
# Returns rows in the native format of the selected utility data format,
# suitable for creating format-specific tables rather than the generic eval schema.
_UTILITY_METER_CACHE: list = []

def _generate_utility_row(data_format: str, seq: int) -> dict:
    """Generate a row using the shared Flux Data Forge generators.

    Returns the native column set for the selected format (e.g., Itron, IRIS,
    CARTO, Siemens Edge, or standard AMI).  Only available when co-located
    with the SPCS app's data_generators module.
    """
    import random
    if not _SHARED_GENERATORS_AVAILABLE:
        return _generate_row("ami", seq)

    # Build a synthetic meter_info if cache is empty
    if not _UTILITY_METER_CACHE:
        for i in range(100):
            _UTILITY_METER_CACHE.append({
                'meter_id': f'EVAL-MTR-{i:04d}',
                'transformer_id': f'TX-{i % 20:03d}',
                'circuit_id': f'CKT-{i % 5:02d}',
                'substation_id': f'SUB-{i % 3:02d}',
                'customer_segment': random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']),
                'latitude': 29.76 + random.uniform(-0.1, 0.1),
                'longitude': -95.37 + random.uniform(-0.1, 0.1),
            })

    meter = random.choice(_UTILITY_METER_CACHE)

    if data_format == 'itron_grid_planning':
        return generate_itron_grid_planning_row(meter, 'TEXAS_GULF_COAST')
    elif data_format == 'symphony_iris':
        return generate_symphony_iris_row(meter, 'TEXAS_GULF_COAST')
    elif data_format == 'carto_spatial':
        return generate_carto_spatial_row(meter, 'TEXAS_GULF_COAST')
    elif data_format == 'siemens_edge':
        return generate_siemens_edge_row(meter, 'TEXAS_GULF_COAST')
    else:
        return generate_ami_reading(meter, 'TEXAS_GULF_COAST', 'UNIFORM')


def _dispatch_row(profile: str, seq: int) -> dict:
    """Route row generation: 'utility:FORMAT' profiles use the shared library,
    all others use the built-in eval profiles."""
    if profile.startswith('utility:'):
        data_format = profile.split(':', 1)[1]
        return _generate_utility_row(data_format, seq)
    return _generate_row(profile, seq)


def _streaming_worker(
    job_id: str,
    conn_params: dict,
    target: dict,
    private_key_pem: str,
    rows_per_sec: int,
    batch_size: int,
    num_channels: int,
    profile: str,
):
    """Background worker that streams data via HP SDK."""
    logger.info(f"[{job_id}] Starting HP streaming worker: {rows_per_sec} rows/s, batch={batch_size}, channels={num_channels}")

    client = None
    channels = []
    try:
        from snowflake.ingest.streaming import StreamingIngestClient

        properties = {
            "url": f"https://{conn_params['account']}.snowflakecomputing.com",
            "account": conn_params["account"],
            "user": conn_params["user"],
            "private_key": private_key_pem,
            "role": conn_params.get("role", "SYSADMIN"),
            "authorization_type": "JWT",
        }

        client = StreamingIngestClient(
            f"hp_eval_{job_id}",
            target["database"],
            target["schema"],
            target["pipe"],
            properties,
        )

        for i in range(num_channels):
            ch_name = f"eval_ch_{job_id}_{i}"
            channel, status = client.open_channel(ch_name)
            channels.append((ch_name, channel))
            logger.info(f"[{job_id}] Opened channel {ch_name}")

        seq = 0
        total_inserted = 0
        total_batches = 0
        total_errors = 0
        peak_rps = 0
        interval = 1.0 / max(rows_per_sec / batch_size, 0.1)

        while True:
            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
                    break

            batch_start = time.time()

            for ch_idx, (ch_name, channel) in enumerate(channels):
                rows = []
                for _ in range(batch_size):
                    seq += 1
                    rows.append(_dispatch_row(profile, seq))

                try:
                    channel.append_rows(
                        rows,
                        start_offset_token=str(total_batches),
                        end_offset_token=str(total_batches + 1),
                    )
                    batch_latency_ms = (time.time() - batch_start) * 1000
                    total_inserted += len(rows)
                    total_batches += 1
                except Exception as e:
                    total_errors += 1
                    logger.error(f"[{job_id}] append_rows error: {e}")

            # Flush periodically
            if total_batches % 10 == 0:
                for _, channel in channels:
                    try:
                        channel.initiate_flush()
                    except Exception:
                        pass

            elapsed = time.time() - eval_state["streaming"]["start_time"]
            current_rps = int(total_inserted / max(elapsed, 1))
            peak_rps = max(peak_rps, current_rps)

            with state_lock:
                lats = eval_state["streaming"]["stats"].get("batch_latencies", [])
                lats.append(batch_latency_ms)
                # Keep last 1000 latencies to avoid memory bloat
                if len(lats) > 1000:
                    lats = lats[-1000:]
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_inserted,
                    "rows_sec": current_rps,
                    "elapsed": int(elapsed),
                    "errors": total_errors,
                    "batches": total_batches,
                    "peak_rps": peak_rps,
                    "batch_latencies": lats,
                }

            # Pace to target rate
            batch_elapsed = time.time() - batch_start
            sleep_time = interval - batch_elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except Exception as e:
        logger.exception(f"[{job_id}] Worker error")
        with state_lock:
            eval_state["streaming"]["stats"]["errors"] += 1

    finally:
        # Cleanup
        for ch_name, channel in channels:
            try:
                channel.close(drop=False, wait_for_flush=True, timeout_seconds=30)
                logger.info(f"[{job_id}] Closed channel {ch_name}")
            except Exception as e:
                logger.error(f"[{job_id}] Error closing channel {ch_name}: {e}")

        if client:
            try:
                client.close(wait_for_flush=True, timeout_seconds=30)
            except Exception:
                pass

        with state_lock:
            eval_state["streaming"]["active"] = False

        logger.info(f"[{job_id}] Worker finished")


def _sql_insert_worker(
    job_id: str,
    conn_params: dict,
    target: dict,
    rows_per_sec: int,
    batch_size: int,
    profile: str,
):
    """Background worker that streams data via SQL INSERT (password auth fallback)."""
    logger.info(f"[{job_id}] Starting SQL INSERT worker: {rows_per_sec} rows/s, batch={batch_size}")

    conn = None
    try:
        import snowflake.connector

        sf_params = {
            "account": conn_params["account"],
            "user": conn_params["user"],
            "role": conn_params.get("role", "SYSADMIN"),
        }
        if conn_params.get("password"):
            sf_params["password"] = conn_params["password"]
        if conn_params.get("warehouse"):
            sf_params["warehouse"] = conn_params["warehouse"]

        conn = snowflake.connector.connect(**sf_params)
        fqt = target["fqt"]

        seq = 0
        total_inserted = 0
        total_batches = 0
        total_errors = 0
        peak_rps = 0
        interval = 1.0 / max(rows_per_sec / batch_size, 0.1)

        while True:
            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
                    break

            batch_start = time.time()
            rows = []
            for _ in range(batch_size):
                seq += 1
                rows.append(_dispatch_row(profile, seq))

            try:
                cur = conn.cursor()
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(rows))
                values = []
                for r in rows:
                    values.extend([
                        r["record_id"], r["meter_id"], r["reading_ts"],
                        r["reading_value"], r["unit"], r["quality"], r["source"],
                    ])
                cur.execute(
                    f"INSERT INTO {fqt} (record_id, meter_id, reading_ts, reading_value, unit, quality, source) "
                    f"VALUES {placeholders}",
                    values,
                )
                cur.close()
                batch_latency_ms = (time.time() - batch_start) * 1000
                total_inserted += len(rows)
                total_batches += 1
            except Exception as e:
                total_errors += 1
                logger.error(f"[{job_id}] SQL INSERT error: {e}")

            elapsed = time.time() - eval_state["streaming"]["start_time"]
            current_rps = int(total_inserted / max(elapsed, 1))
            peak_rps = max(peak_rps, current_rps)

            with state_lock:
                lats = eval_state["streaming"]["stats"].get("batch_latencies", [])
                lats.append(batch_latency_ms)
                if len(lats) > 1000:
                    lats = lats[-1000:]
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_inserted,
                    "rows_sec": current_rps,
                    "elapsed": int(elapsed),
                    "errors": total_errors,
                    "batches": total_batches,
                    "peak_rps": peak_rps,
                    "batch_latencies": lats,
                }

            batch_elapsed = time.time() - batch_start
            sleep_time = interval - batch_elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except Exception as e:
        logger.exception(f"[{job_id}] SQL INSERT worker error")
        with state_lock:
            eval_state["streaming"]["stats"]["errors"] += 1

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        with state_lock:
            eval_state["streaming"]["active"] = False
        logger.info(f"[{job_id}] SQL INSERT worker finished")


# ============================================================================
# V1 vs V2 COMPARISON WORKER
# ============================================================================

def _comparison_worker(
    job_id: str,
    conn_params: dict,
    target: dict,
    private_key_pem: str,
    rows_per_sec: int,
    batch_size: int,
    num_channels: int,
    profile: str,
):
    """Run HP SDK (V2) and SQL INSERT (V1) in parallel on identical data, track separate stats."""
    logger.info(f"[{job_id}] Starting V1 vs V2 comparison: {rows_per_sec} rows/s, batch={batch_size}")

    # Shared state for both sub-workers
    v2_stats = {"rows": 0, "batches": 0, "errors": 0, "peak_rps": 0}
    v1_stats = {"rows": 0, "batches": 0, "errors": 0, "peak_rps": 0}

    def v2_worker():
        """HP SDK (V2) sub-worker."""
        client = None
        channels = []
        try:
            from snowflake.ingest.streaming import StreamingIngestClient

            properties = {
                "url": f"https://{conn_params['account']}.snowflakecomputing.com",
                "account": conn_params["account"],
                "user": conn_params["user"],
                "private_key": private_key_pem,
                "role": conn_params.get("role", "SYSADMIN"),
                "authorization_type": "JWT",
            }
            client = StreamingIngestClient(
                f"cmp_v2_{job_id}",
                target["database"],
                target["schema"],
                target["pipe"],
                properties,
            )
            for i in range(num_channels):
                ch_name = f"cmp_v2_{job_id}_{i}"
                channel, status = client.open_channel(ch_name)
                channels.append((ch_name, channel))

            seq = 0
            interval = 1.0 / max(rows_per_sec / batch_size, 0.1)

            while True:
                with state_lock:
                    if eval_state["streaming"].get("stop_requested"):
                        break
                batch_start = time.time()
                for ch_idx, (ch_name, channel) in enumerate(channels):
                    rows = []
                    for _ in range(batch_size):
                        seq += 1
                        rows.append(_dispatch_row(profile, seq))
                    try:
                        channel.append_rows(rows, start_offset_token=str(v2_stats["batches"]), end_offset_token=str(v2_stats["batches"] + 1))
                        v2_stats["rows"] += len(rows)
                        v2_stats["batches"] += 1
                    except Exception as e:
                        v2_stats["errors"] += 1
                        logger.error(f"[{job_id}] V2 append error: {e}")

                if v2_stats["batches"] % 10 == 0:
                    for _, channel in channels:
                        try:
                            channel.initiate_flush()
                        except Exception:
                            pass

                elapsed = time.time() - eval_state["streaming"]["start_time"]
                current_rps = int(v2_stats["rows"] / max(elapsed, 1))
                v2_stats["peak_rps"] = max(v2_stats["peak_rps"], current_rps)

                batch_elapsed = time.time() - batch_start
                sleep_time = interval - batch_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except Exception as e:
            logger.exception(f"[{job_id}] V2 comparison worker error")
            v2_stats["errors"] += 1
        finally:
            for ch_name, channel in channels:
                try:
                    channel.close(drop=False, wait_for_flush=True, timeout_seconds=30)
                except Exception:
                    pass
            if client:
                try:
                    client.close(wait_for_flush=True, timeout_seconds=30)
                except Exception:
                    pass

    def v1_worker():
        """SQL INSERT (V1) sub-worker."""
        conn = None
        try:
            import snowflake.connector
            sf_params = {
                "account": conn_params["account"],
                "user": conn_params["user"],
                "role": conn_params.get("role", "SYSADMIN"),
            }
            if conn_params.get("password"):
                sf_params["password"] = conn_params["password"]
            else:
                # Use key-pair for V1 too if no password
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                pkb = load_pem_private_key(private_key_pem.encode(), password=None)
                from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
                sf_params["private_key"] = pkb.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
                sf_params["authenticator"] = "snowflake_jwt"
            if conn_params.get("warehouse"):
                sf_params["warehouse"] = conn_params["warehouse"]

            conn = snowflake.connector.connect(**sf_params)
            # Create a separate V1 table for fair comparison
            fqt_v1 = target["fqt"] + "_V1_CMP"
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {fqt_v1} LIKE {target['fqt']}")
            cur.close()

            seq = 0
            interval = 1.0 / max(rows_per_sec / batch_size, 0.1)

            while True:
                with state_lock:
                    if eval_state["streaming"].get("stop_requested"):
                        break
                batch_start = time.time()
                rows = []
                for _ in range(batch_size):
                    seq += 1
                    rows.append(_dispatch_row(profile, seq))
                try:
                    cur = conn.cursor()
                    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(rows))
                    values = []
                    for r in rows:
                        values.extend([r["record_id"], r["meter_id"], r["reading_ts"], r["reading_value"], r["unit"], r["quality"], r["source"]])
                    cur.execute(
                        f"INSERT INTO {fqt_v1} (record_id, meter_id, reading_ts, reading_value, unit, quality, source) VALUES {placeholders}",
                        values,
                    )
                    cur.close()
                    v1_stats["rows"] += len(rows)
                    v1_stats["batches"] += 1
                except Exception as e:
                    v1_stats["errors"] += 1
                    logger.error(f"[{job_id}] V1 INSERT error: {e}")

                elapsed = time.time() - eval_state["streaming"]["start_time"]
                current_rps = int(v1_stats["rows"] / max(elapsed, 1))
                v1_stats["peak_rps"] = max(v1_stats["peak_rps"], current_rps)

                batch_elapsed = time.time() - batch_start
                sleep_time = interval - batch_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except Exception as e:
            logger.exception(f"[{job_id}] V1 comparison worker error")
            v1_stats["errors"] += 1
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # Launch both workers in parallel
    t_v2 = threading.Thread(target=v2_worker, daemon=True)
    t_v1 = threading.Thread(target=v1_worker, daemon=True)
    t_v2.start()
    t_v1.start()

    # Monitor loop: update combined stats
    try:
        while t_v2.is_alive() or t_v1.is_alive():
            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
                    break
            elapsed = time.time() - eval_state["streaming"]["start_time"]
            total_rows = v2_stats["rows"] + v1_stats["rows"]
            combined_rps = int(total_rows / max(elapsed, 1))

            with state_lock:
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_rows,
                    "rows_sec": combined_rps,
                    "elapsed": int(elapsed),
                    "errors": v2_stats["errors"] + v1_stats["errors"],
                    "batches": v2_stats["batches"] + v1_stats["batches"],
                    "peak_rps": max(v2_stats["peak_rps"], v1_stats["peak_rps"]),
                }
                v2_rps = int(v2_stats["rows"] / max(elapsed, 1))
                v1_rps = int(v1_stats["rows"] / max(elapsed, 1))
                eval_state["streaming"]["compare"] = {
                    "v2": {"rows": v2_stats["rows"], "rps": v2_rps, "batches": v2_stats["batches"], "errors": v2_stats["errors"], "peak_rps": v2_stats["peak_rps"]},
                    "v1": {"rows": v1_stats["rows"], "rps": v1_rps, "batches": v1_stats["batches"], "errors": v1_stats["errors"], "peak_rps": v1_stats["peak_rps"]},
                }
            time.sleep(1)
    except Exception:
        pass

    # Wait for sub-workers to finish
    t_v2.join(timeout=60)
    t_v1.join(timeout=60)

    with state_lock:
        eval_state["streaming"]["active"] = False
    logger.info(f"[{job_id}] Comparison worker finished. V2: {v2_stats['rows']} rows, V1: {v1_stats['rows']} rows")


# ============================================================================
# EVENTHUB → SNOWFLAKE STREAMING WORKER
# ============================================================================

def _eventhub_worker(
    job_id: str,
    conn_params: dict,
    target: dict,
    private_key_pem: str,
    eh_conn_str: str,
    eh_name: str,
    eh_consumer_group: str,
    batch_size: int,
    num_channels: int,
):
    """Background worker: consume from Azure EventHub → stream to Snowflake via HP SDK or SQL INSERT."""
    logger.info(f"[{job_id}] Starting EventHub worker: hub={eh_name}, batch={batch_size}")

    consumer = None
    client = None
    channels = []
    sf_conn = None

    try:
        from azure.eventhub import EventHubConsumerClient

        use_hp_sdk = bool(private_key_pem)

        # Set up Snowflake sink (HP SDK or SQL INSERT)
        if use_hp_sdk:
            from snowflake.ingest.streaming import StreamingIngestClient

            properties = {
                "url": f"https://{conn_params['account']}.snowflakecomputing.com",
                "account": conn_params["account"],
                "user": conn_params["user"],
                "private_key": private_key_pem,
                "role": conn_params.get("role", "SYSADMIN"),
                "authorization_type": "JWT",
            }
            client = StreamingIngestClient(
                f"eh_{job_id}",
                target["database"],
                target["schema"],
                target["pipe"],
                properties,
            )
            for i in range(num_channels):
                ch_name = f"eh_ch_{job_id}_{i}"
                channel, status = client.open_channel(ch_name)
                channels.append((ch_name, channel))
                logger.info(f"[{job_id}] Opened HP channel {ch_name}")
        else:
            import snowflake.connector
            sf_params = {
                "account": conn_params["account"],
                "user": conn_params["user"],
                "role": conn_params.get("role", "SYSADMIN"),
            }
            if conn_params.get("password"):
                sf_params["password"] = conn_params["password"]
            if conn_params.get("warehouse"):
                sf_params["warehouse"] = conn_params["warehouse"]
            sf_conn = snowflake.connector.connect(**sf_params)

        total_inserted = 0
        total_batches = 0
        total_errors = 0
        peak_rps = 0
        batch_buffer = []
        ch_idx = 0

        def _flush_batch(rows):
            """Flush a batch of rows to Snowflake."""
            nonlocal total_inserted, total_batches, total_errors, peak_rps, ch_idx

            if not rows:
                return

            try:
                if use_hp_sdk and channels:
                    _, channel = channels[ch_idx % len(channels)]
                    ch_idx += 1
                    channel.append_rows(
                        rows,
                        start_offset_token=str(total_batches),
                        end_offset_token=str(total_batches + 1),
                    )
                    if total_batches % 10 == 0:
                        channel.initiate_flush()
                elif sf_conn:
                    fqt = target["fqt"]
                    columns = list(rows[0].keys())
                    col_str = ", ".join(columns)
                    ph = "(" + ", ".join(["%s"] * len(columns)) + ")"
                    placeholders = ", ".join([ph] * len(rows))
                    values = []
                    for r in rows:
                        values.extend([r.get(c) for c in columns])
                    cur = sf_conn.cursor()
                    cur.execute(
                        f"INSERT INTO {fqt} ({col_str}) VALUES {placeholders}",
                        values,
                    )
                    cur.close()

                total_inserted += len(rows)
                total_batches += 1
            except Exception as e:
                total_errors += 1
                logger.error(f"[{job_id}] Flush error: {e}")

            elapsed = time.time() - eval_state["streaming"]["start_time"]
            current_rps = int(total_inserted / max(elapsed, 1))
            peak_rps = max(peak_rps, current_rps)

            with state_lock:
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_inserted,
                    "rows_sec": current_rps,
                    "elapsed": int(elapsed),
                    "errors": total_errors,
                    "batches": total_batches,
                    "peak_rps": peak_rps,
                }

        def on_event(partition_context, event):
            """Callback for each EventHub event."""
            nonlocal batch_buffer

            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
                    raise StopIteration("Stop requested")

            if event is None:
                return

            # Parse event body as JSON, or wrap raw text
            try:
                body = event.body_as_json()
            except Exception:
                body_str = event.body_as_str()
                body = {"raw_data": body_str}

            # Map to our table schema
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
                _flush_batch(batch_buffer[:])
                batch_buffer.clear()

        # Create EventHub consumer
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=eh_conn_str,
            consumer_group=eh_consumer_group,
            eventhub_name=eh_name,
        )

        logger.info(f"[{job_id}] Connected to EventHub: {eh_name}")

        # Receive in a blocking loop (runs until StopIteration or close)
        try:
            consumer.receive(
                on_event=on_event,
                starting_position="-1",  # From beginning of partition
            )
        except StopIteration:
            logger.info(f"[{job_id}] Stop requested, flushing remaining buffer")

        # Flush remaining
        if batch_buffer:
            _flush_batch(batch_buffer[:])
            batch_buffer.clear()

    except Exception as e:
        logger.exception(f"[{job_id}] EventHub worker error")
        with state_lock:
            eval_state["streaming"]["stats"]["errors"] = eval_state["streaming"]["stats"].get("errors", 0) + 1

    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        for ch_name, channel in channels:
            try:
                channel.close(drop=False, wait_for_flush=True, timeout_seconds=30)
            except Exception:
                pass
        if client:
            try:
                client.close(wait_for_flush=True, timeout_seconds=30)
            except Exception:
                pass
        if sf_conn:
            try:
                sf_conn.close()
            except Exception:
                pass
        with state_lock:
            eval_state["streaming"]["active"] = False
        logger.info(f"[{job_id}] EventHub worker finished")


# ============================================================================
# PUBSUB → SNOWFLAKE STREAMING WORKER
# ============================================================================

def _pubsub_worker(
    job_id: str,
    conn_params: dict,
    target: dict,
    private_key_pem: str,
    ps_project: str,
    ps_subscription: str,
    ps_credentials_json: str,
    batch_size: int,
    num_channels: int,
):
    """Background worker: consume from Google PubSub → stream to Snowflake via HP SDK or SQL INSERT."""
    logger.info(f"[{job_id}] Starting PubSub worker: project={ps_project}, sub={ps_subscription}")

    subscriber = None
    streaming_pull_future = None
    client = None
    channels = []
    sf_conn = None

    try:
        from google.cloud import pubsub_v1
        from google.oauth2 import service_account
        import tempfile

        use_hp_sdk = bool(private_key_pem)

        # Set up Snowflake sink
        if use_hp_sdk:
            from snowflake.ingest.streaming import StreamingIngestClient

            properties = {
                "url": f"https://{conn_params['account']}.snowflakecomputing.com",
                "account": conn_params["account"],
                "user": conn_params["user"],
                "private_key": private_key_pem,
                "role": conn_params.get("role", "SYSADMIN"),
                "authorization_type": "JWT",
            }
            client = StreamingIngestClient(
                f"ps_{job_id}",
                target["database"],
                target["schema"],
                target["pipe"],
                properties,
            )
            for i in range(num_channels):
                ch_name = f"ps_ch_{job_id}_{i}"
                channel, status = client.open_channel(ch_name)
                channels.append((ch_name, channel))
                logger.info(f"[{job_id}] Opened HP channel {ch_name}")
        else:
            import snowflake.connector
            sf_params = {
                "account": conn_params["account"],
                "user": conn_params["user"],
                "role": conn_params.get("role", "SYSADMIN"),
            }
            if conn_params.get("password"):
                sf_params["password"] = conn_params["password"]
            if conn_params.get("warehouse"):
                sf_params["warehouse"] = conn_params["warehouse"]
            sf_conn = snowflake.connector.connect(**sf_params)

        total_inserted = 0
        total_batches = 0
        total_errors = 0
        peak_rps = 0
        batch_buffer = []
        buffer_lock = threading.Lock()
        ch_idx = 0

        def _flush_batch(rows):
            nonlocal total_inserted, total_batches, total_errors, peak_rps, ch_idx
            if not rows:
                return
            try:
                if use_hp_sdk and channels:
                    _, channel = channels[ch_idx % len(channels)]
                    ch_idx += 1
                    channel.append_rows(
                        rows,
                        start_offset_token=str(total_batches),
                        end_offset_token=str(total_batches + 1),
                    )
                    if total_batches % 10 == 0:
                        channel.initiate_flush()
                elif sf_conn:
                    fqt = target["fqt"]
                    columns = list(rows[0].keys())
                    col_str = ", ".join(columns)
                    ph = "(" + ", ".join(["%s"] * len(columns)) + ")"
                    placeholders = ", ".join([ph] * len(rows))
                    values = []
                    for r in rows:
                        values.extend([r.get(c) for c in columns])
                    cur = sf_conn.cursor()
                    cur.execute(
                        f"INSERT INTO {fqt} ({col_str}) VALUES {placeholders}",
                        values,
                    )
                    cur.close()
                total_inserted += len(rows)
                total_batches += 1
            except Exception as e:
                total_errors += 1
                logger.error(f"[{job_id}] Flush error: {e}")

            elapsed = time.time() - eval_state["streaming"]["start_time"]
            current_rps = int(total_inserted / max(elapsed, 1))
            peak_rps = max(peak_rps, current_rps)

            with state_lock:
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_inserted,
                    "rows_sec": current_rps,
                    "elapsed": int(elapsed),
                    "errors": total_errors,
                    "batches": total_batches,
                    "peak_rps": peak_rps,
                }

        def callback(message):
            nonlocal batch_buffer
            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
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
                    _flush_batch(batch_buffer[:])
                    batch_buffer.clear()

        # Create PubSub subscriber
        if ps_credentials_json.strip():
            creds_dict = json.loads(ps_credentials_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        else:
            subscriber = pubsub_v1.SubscriberClient()

        subscription_path = subscriber.subscription_path(ps_project, ps_subscription)
        logger.info(f"[{job_id}] Subscribing to: {subscription_path}")

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

        # Block until stop requested
        while True:
            with state_lock:
                if eval_state["streaming"].get("stop_requested"):
                    break
            time.sleep(1)

        # Flush remaining
        with buffer_lock:
            if batch_buffer:
                _flush_batch(batch_buffer[:])
                batch_buffer.clear()

    except Exception as e:
        logger.exception(f"[{job_id}] PubSub worker error")
        with state_lock:
            eval_state["streaming"]["stats"]["errors"] = eval_state["streaming"]["stats"].get("errors", 0) + 1

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
        for ch_name, channel in channels:
            try:
                channel.close(drop=False, wait_for_flush=True, timeout_seconds=30)
            except Exception:
                pass
        if client:
            try:
                client.close(wait_for_flush=True, timeout_seconds=30)
            except Exception:
                pass
        if sf_conn:
            try:
                sf_conn.close()
            except Exception:
                pass
        with state_lock:
            eval_state["streaming"]["active"] = False
        logger.info(f"[{job_id}] PubSub worker finished")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8090"))
    logger.info(f"Starting HP Streaming Eval on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
