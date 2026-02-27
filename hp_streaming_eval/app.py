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

import uvicorn
from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

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
<title>HP Streaming Eval — Flux Data Forge</title>
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
.doc-link {{
    color: var(--warning); font-size: 0.78rem; font-weight: 500;
}}
.doc-link:hover {{ opacity: 0.85; }}

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
            <h1>HP Streaming Eval</h1>
            <div class="subtitle">Snowpipe Streaming High-Performance Evaluation &mdash; <span class="badge">Flux Data Forge</span></div>
        </div>
    </div>

    <!-- Stepper -->
    <div class="stepper" id="stepper">
        <div class="step-tab active" data-step="1" onclick="goToStep(1)">
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

    <!-- ================= STEP 1: CONNECT ================= -->
    <div class="step-panel active" id="step1">
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
                    <input type="text" id="t_database" placeholder="MY_DB">
                </div>
                <div class="form-group">
                    <label>Schema <span class="req">*</span></label>
                    <input type="text" id="t_schema" placeholder="PUBLIC">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Table Name <span class="req">*</span></label>
                    <input type="text" id="t_table" value="HP_EVAL_READINGS" placeholder="HP_EVAL_READINGS"
                           oninput="updateDdlPreview()">
                </div>
                <div class="form-group">
                    <label>Pipe Name <span class="req">*</span>
                        &nbsp;<a class="doc-link" href="https://docs.snowflake.com/en/sql-reference/sql/create-pipe" target="_blank">CREATE PIPE &nearr;</a>
                    </label>
                    <input type="text" id="t_pipe" value="HP_EVAL_PIPE" placeholder="HP_EVAL_PIPE"
                           oninput="updateDdlPreview()">
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
                Send test data through HP Streaming and watch the live metrics.
                <a class="doc-link" href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-python-sdk-reference" target="_blank">Python SDK Reference &nearr;</a>
                &nbsp;&middot;&nbsp;
                <a class="doc-link" href="https://pypi.org/project/snowpipe-streaming/" target="_blank">PyPI: snowpipe-streaming &nearr;</a>
            </div>

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
                        <option value="counter">Simple Counter</option>
                    </select>
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
            </div>

            <div style="display:flex;justify-content:space-between;margin-top:24px;">
                <button class="btn btn-secondary btn-sm" onclick="goToStep(3)">
                    {_material_icon('arrow_back', '14px')} Back
                </button>
                <button class="btn btn-primary btn-sm" id="btn_export" style="display:none;" onclick="exportReport()">
                    {_material_icon('download', '16px', '#0f172a')} Export Report
                </button>
            </div>
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
let currentStep = 1;
const completedSteps = new Set();
let pollTimer = null;

// ── Navigation ─────────────────────────────────────────
function goToStep(n) {{
    // Only allow forward if previous step is completed
    if (n > currentStep && !completedSteps.has(currentStep)) return;
    currentStep = n;
    document.querySelectorAll('.step-panel').forEach(p => p.classList.remove('active'));
    document.getElementById('step' + n).classList.add('active');
    document.querySelectorAll('.step-tab').forEach(t => {{
        const s = parseInt(t.dataset.step);
        t.classList.toggle('active', s === n);
        t.classList.toggle('done', completedSteps.has(s));
    }});
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
        }} else {{
            statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Connection failed') + '</span>';
        }}
    }} catch (err) {{
        statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
    }}
}}

// ── Step 2: Configure ──────────────────────────────────
function updateDdlPreview() {{
    const db = document.getElementById('t_database').value.trim() || 'MY_DB';
    const schema = document.getElementById('t_schema').value.trim() || 'PUBLIC';
    const table = document.getElementById('t_table').value.trim() || 'HP_EVAL_READINGS';
    const pipe = document.getElementById('t_pipe').value.trim() || 'HP_EVAL_PIPE';
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
    statusEl.innerHTML = '<span class="spinner"></span> Deploying...';

    const body = new URLSearchParams();
    body.set('database', document.getElementById('t_database').value.trim());
    body.set('schema', document.getElementById('t_schema').value.trim());
    body.set('table', document.getElementById('t_table').value.trim());
    body.set('pipe', document.getElementById('t_pipe').value.trim());

    try {{
        const resp = await fetch('/api/deploy-ddl', {{ method: 'POST', body }});
        const data = await resp.json();
        if (data.status === 'ok') {{
            statusEl.innerHTML = '<span class="status-badge ok">&#x2713; ' + (data.message || 'Deployed') + '</span>';
            document.getElementById('btn_to_step3').disabled = false;
            markStepDone(2);
        }} else {{
            statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + (data.message || 'Deploy failed') + '</span>';
        }}
    }} catch (err) {{
        statusEl.innerHTML = '<span class="status-badge err">&#x2717; ' + err.message + '</span>';
    }}
}}

// ── Step 3: Stream ─────────────────────────────────────
async function startStreaming() {{
    document.getElementById('btn_start').classList.add('hidden');
    document.getElementById('btn_stop').classList.remove('hidden');
    document.getElementById('live_metrics').style.display = '';
    document.getElementById('stream_status').innerHTML = '<span class="status-badge info"><span class="spinner"></span> Streaming...</span>';

    const body = new URLSearchParams();
    body.set('rows_per_sec', document.getElementById('s_rows_sec').value);
    body.set('batch_size', document.getElementById('s_batch').value);
    body.set('channels', document.getElementById('s_channels').value);
    body.set('profile', document.getElementById('s_profile').value);

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

            markStepDone(4);
            // Store for export
            window._evalReport = data;
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

function exportReport() {{
    if (!window._evalReport) return;
    const blob = new Blob([JSON.stringify(window._evalReport, null, 2)], {{ type: 'application/json' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'hp_streaming_eval_report.json';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
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
        f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}",
        f"USE DATABASE {database}",
        f"USE SCHEMA {schema}",
        f"""CREATE TABLE IF NOT EXISTS {fqt} (
            record_id       STRING,
            meter_id        STRING,
            reading_ts      TIMESTAMP_NTZ,
            reading_value   FLOAT,
            unit            STRING,
            quality         STRING,
            source          STRING,
            inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )""",
        f"""CREATE OR REPLACE PIPE {fqp}
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
            )""",
        f"GRANT SELECT, INSERT ON TABLE {fqt} TO ROLE SYSADMIN",
        f"GRANT OPERATE ON PIPE {fqp} TO ROLE SYSADMIN",
    ]

    errors = []
    cur = conn.cursor()
    try:
        for stmt in ddl_statements:
            try:
                cur.execute(stmt)
            except Exception as e:
                errors.append(str(e)[:200])
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

    if not errors:
        return JSONResponse({"status": "ok", "message": f"Table + PIPE created: {fqp}"})
    else:
        return JSONResponse({"status": "error", "message": "; ".join(errors)})


@app.post("/api/stream/start")
async def api_stream_start(
    rows_per_sec: int = Form(100),
    batch_size: int = Form(100),
    channels: int = Form(1),
    profile: str = Form("ami"),
):
    """Start HP streaming worker thread."""
    with state_lock:
        if eval_state["streaming"]["active"]:
            return JSONResponse({"status": "error", "message": "Already streaming"})
        pem = eval_state.get("private_key_pem")
        conn_params = eval_state.get("conn_params", {})
        target = eval_state.get("target", {})

    if not target.get("pipe"):
        return JSONResponse({"status": "error", "message": "Complete Steps 1 & 2 first"})

    # Determine streaming mode: HP SDK (key-pair) or SQL INSERT (password)
    use_hp_sdk = bool(pem)

    job_id = str(uuid.uuid4())[:8]
    with state_lock:
        eval_state["streaming"] = {
            "active": True,
            "job_id": job_id,
            "stats": {"rows_inserted": 0, "rows_sec": 0, "elapsed": 0, "errors": 0, "batches": 0, "peak_rps": 0},
            "start_time": time.time(),
            "stop_requested": False,
            "mode": "hp_sdk" if use_hp_sdk else "sql_insert",
        }

    if use_hp_sdk:
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

    return JSONResponse({"status": "ok", "job_id": job_id})


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
    }
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
                    rows.append(_generate_row(profile, seq))

                try:
                    channel.append_rows(
                        rows,
                        start_offset_token=str(total_batches),
                        end_offset_token=str(total_batches + 1),
                    )
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
                eval_state["streaming"]["stats"] = {
                    "rows_inserted": total_inserted,
                    "rows_sec": current_rps,
                    "elapsed": int(elapsed),
                    "errors": total_errors,
                    "batches": total_batches,
                    "peak_rps": peak_rps,
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
                rows.append(_generate_row(profile, seq))

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
                total_inserted += len(rows)
                total_batches += 1
            except Exception as e:
                total_errors += 1
                logger.error(f"[{job_id}] SQL INSERT error: {e}")

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
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8090"))
    logger.info(f"Starting HP Streaming Eval on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
