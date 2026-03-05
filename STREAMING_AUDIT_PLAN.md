# Streaming Architecture Audit & Fix Plan

**Context:** CenterPoint Energy (Vinay Suneja) POC evaluation of Snowflake streaming ingestion.
This plan addresses gaps between what was communicated in the 2/27 working session and what the code actually does.

**Reference files:**
- Meeting transcript: `/Users/abannerjee/Downloads/Snowflake and CenterPoint Energy Working Session 2_27 .md`
- Main app: `spcs_app/fastapi_app.py` (~13,891 lines)
- Standalone eval wizard: `hp_streaming_eval/app.py` (~3,421 lines)
- Streaming impl: `spcs_app/snowpipe_streaming_impl.py` (~842 lines)
- Tests: `tests/test_e2e_streaming.py`, `tests/test_unit.py`

---

## Architecture Truth Table

| What we told Vinay | Ground truth | What our code does | Gap? |
|---|---|---|---|
| Classic V1 vs HP V2 comparison | Classic SDK = Java-only; HP SDK = Python available | "Classic" path uses SQL INSERT via Snowpark, not Classic SDK | YES — mislabeled |
| Openflow for CDC | Openflow has MySQL, PostgreSQL, SQL Server, Oracle CDC connectors | Not in our codebase (out of scope for POC) | OK — just need to reference |
| Openflow for streaming | Openflow has Kafka + Kinesis connectors; NO EventHub or PubSub | Our code uses custom Python SDK consumers for EventHub/PubSub | OK — correct approach |
| PubSub needs a compute consumer | True — no native Snowflake connector for PubSub | We built it in Python | OK |
| EventHub connector coming to Openflow | Jon confirmed "in development" | Our custom consumer is the interim path | OK |
| HP SDK Python import | `from snowflake.ingest.streaming import StreamingIngestClient` | UI code snippet shows wrong import path | YES — wrong snippet |
| HP SDK throughput-based pricing | 0.0037 credits/uncompressed GB | Code implements HP SDK correctly | OK |

---

## Tasks

### Task 1: Terminology Fix (CRITICAL)

**Problem:** All user-facing text says "V1" / "Classic" / "V2" implying we run both Snowpipe Streaming SDK versions. In reality:
- "V2" / "HP SDK" = genuine `snowpipe-streaming` Python SDK (correct)
- "V1" / "Classic" = SQL INSERT via `session.sql()` (mislabeled — the real Classic SDK is Java-only)

**Files to modify (OWNED by this task — no other task touches these lines):**

| File | What to change |
|---|---|
| `hp_streaming_eval/app.py` | ~25 occurrences: HTML labels ("V1", "V2"), JS variables, comparison headers, code snippet text, worker comments |
| `spcs_app/snowpipe_streaming_impl.py` | Class name `ClassicStreamingClient`, `ARCHITECTURE_INFO['classic']` metadata, docstrings |

**Renaming rules:**
- "V1" / "Classic" / "Snowpipe Streaming Classic" → **"SQL INSERT (Baseline)"** or **"Warehouse INSERT"**
- "V2" / "HP" / "Snowpipe Streaming HP" → **"HP SDK (Snowpipe Streaming)"**
- Class `ClassicStreamingClient` → `SqlInsertClient`
- Any `ARCHITECTURE_INFO['classic']` → `ARCHITECTURE_INFO['sql_insert']`
- Comments must clarify: "This is NOT the Java-only Classic SDK; it uses Snowpark session.sql() for direct INSERT"

**Success criteria:**
- `grep -ri "v1\|v2\|classic" hp_streaming_eval/app.py` returns zero hits for mislabeled streaming references (note: "classic" in non-streaming contexts like CSS class names is OK)
- `grep -ri "ClassicStreamingClient" spcs_app/` returns zero hits
- `python -m py_compile hp_streaming_eval/app.py` passes
- `python -m py_compile spcs_app/snowpipe_streaming_impl.py` passes

**Dependencies:** None — can run in parallel with Tasks 2-5.

---

### Task 2: Code Snippet Import Fix

**Problem:** The UI shows a code snippet with the wrong import path:
```python
# WRONG (shown in UI):
from snowpipe_streaming.client import SnowpipeStreamingClient
# CORRECT:
from snowflake.ingest.streaming import StreamingIngestClient
```

**Files to modify (OWNED):**

| File | What to change |
|---|---|
| `hp_streaming_eval/app.py` | Line ~910 area: the embedded code snippet in the architecture info section. Update the import path and class name. |

**Success criteria:**
- The code snippet in the UI shows `from snowflake.ingest.streaming import StreamingIngestClient`
- `python -m py_compile hp_streaming_eval/app.py` passes

**Dependencies:** None — can run in parallel. Note: Task 1 also edits `app.py` but at different lines. If running in parallel, coordinate via non-overlapping line ranges.

**IMPORTANT — Parallel editing safety with Task 1:**
- Task 1 owns: HTML label strings, JS variables, comparison headers, worker function comments
- Task 2 owns: ONLY the code snippet block near line ~910
- These do not overlap, but if both tasks touch `app.py`, the second to finish must re-read the file before editing to avoid stale `old_string` matches.

---

### Task 3: Schema Mismatch Fix

**Problem:** EventHub and PubSub workers write 7-column rows. DDL generators create 12-column tables with PIPE COPY transforms expecting all 12 columns. This causes failures when streaming from EventHub/PubSub into DDL-generated tables.

**Files to modify (OWNED):**

| File | What to change |
|---|---|
| `spcs_app/fastapi_app.py` | EventHub worker (`eventhub_streaming_worker`, line ~696) and PubSub worker (`pubsub_streaming_worker`, line ~886): expand the row dict to include all 12 columns with sensible defaults for missing fields |
| `hp_streaming_eval/app.py` | EventHub worker (lines ~2974-3187) and PubSub worker (lines ~3194-3410): same 7→12 column expansion |

**The 12 columns (from DDL generators):**
1. METER_ID
2. READING_TS
3. READING_VALUE
4. VOLTAGE
5. TEMPERATURE_C
6. SERVICE_AREA
7. CUSTOMER_SEGMENT
8. TRANSFORMER_ID
9. SUBSTATION_ID
10. IS_OUTAGE
11. DATA_QUALITY
12. INGESTION_TS (metadata, added by PIPE transform)

**The 7 columns (current EventHub/PubSub workers):**
1. record_id
2. meter_id
3. reading_ts
4. reading_value
5. unit
6. quality
7. source

**Mapping strategy:** Map existing fields where possible, add defaults for missing fields:
- `METER_ID` ← `meter_id`
- `READING_TS` ← `reading_ts`
- `READING_VALUE` ← `reading_value`
- `VOLTAGE` ← default `120.0` or extract from message if present
- `TEMPERATURE_C` ← default `null`
- `SERVICE_AREA` ← default `"UNKNOWN"` or extract from message
- `CUSTOMER_SEGMENT` ← default `"RESIDENTIAL"`
- `TRANSFORMER_ID` ← default `null`
- `SUBSTATION_ID` ← default `null`
- `IS_OUTAGE` ← default `false`
- `DATA_QUALITY` ← `quality` field
- INGESTION_TS is added by the PIPE transform, not by the client

**Success criteria:**
- EventHub and PubSub workers produce rows with all 12 expected columns
- `python -m py_compile spcs_app/fastapi_app.py` passes
- `python -m py_compile hp_streaming_eval/app.py` passes

**Dependencies:** None — different line ranges from Tasks 1 and 2.

---

### Task 4: Architecture Landscape Note

**Problem:** The streaming UI doesn't explain the full Snowflake streaming landscape. Vinay might think our custom Python consumer is the only option. In reality:
- **Openflow** handles Kafka, Kinesis, EventHub, PubSub, and CDC databases natively (zero-code)
- **Custom SDK consumers** (what we built) give maximum throughput control but Openflow is the no-code alternative
- **This eval** benchmarks the HP SDK directly for performance characterization

**Files to modify (OWNED):**

| File | What to change |
|---|---|
| `spcs_app/fastapi_app.py` | Add an architecture info panel/tooltip in the streaming page HTML that explains the landscape. Target: the streaming page HTML template section (after line ~9000 in the streaming form area). |

**Content for the panel:**
```
Snowflake Real-Time Ingestion Landscape:

Source Type          | Recommended Approach           | Status
---------------------|-------------------------------|--------
Kafka / Kinesis      | Openflow connector (managed)   | GA
MySQL/PG/SQL Server  | Openflow CDC connector         | GA
Oracle               | Openflow CDC connector         | GA
Azure EventHub       | Openflow connector (managed)   | GA (ConsumeAzureEventHub)
Google PubSub        | Openflow connector (managed)   | GA (ConsumeGCPubSub)
AWS SQS/SNS          | Custom SDK consumer             | Interim

All streaming paths use Snowpipe Streaming HP SDK for sub-5-second ingestion latency.
This eval benchmarks the HP SDK directly for maximum throughput control.
```

**Success criteria:**
- The streaming page shows the landscape context
- `python -m py_compile spcs_app/fastapi_app.py` passes

**Dependencies:** None for the logic. But since this modifies `fastapi_app.py` HTML (different section than Tasks 3 and 5), it can run in parallel.

---

### Task 5: SQL Parameterization Fix

**Problem:** Main app's EventHub and PubSub workers use string interpolation for SQL INSERT values. Should use parameterized queries.

**Files to modify (OWNED):**

| File | Lines | What to change |
|---|---|---|
| `spcs_app/fastapi_app.py` | EventHub worker `_flush` logic (~line 778) | Replace f-string SQL with parameterized `session.sql("INSERT INTO ... VALUES (?, ?, ...)", params=[...])` or use Snowpark `session.write_pandas()` |
| `spcs_app/fastapi_app.py` | PubSub worker `_flush` logic (~line 970) | Same parameterization |

**Success criteria:**
- No f-string or `.format()` SQL construction in EventHub/PubSub workers
- `python -m py_compile spcs_app/fastapi_app.py` passes

**Dependencies:** None — different code sections from other tasks.

---

### Task 6: Winner Metric Fix

**Problem:** Comparison mode declares a "winner" using `peak_rps`, which can be a momentary spike. Average throughput is more representative for a customer demo.

**Files to modify (OWNED):**

| File | What to change |
|---|---|
| `spcs_app/fastapi_app.py` | `comparison_streaming_worker` function (~line 1089): change winner calculation from `peak_rps` to average rows/sec; show peak as secondary |
| `hp_streaming_eval/app.py` | Comparison result display: same change if applicable |

**Success criteria:**
- Winner banner uses average throughput, not peak
- Peak is still displayed as a secondary metric
- `python -m py_compile` passes for both files

**Dependencies:** Task 1 must complete first (it renames the stat keys this task references).

---

### Task 7: Test Coverage

**Problem:** No tests for EventHub/PubSub workers. No mocked unit tests for streaming clients. Classic streaming E2E test is permanently deferred.

**Files to create/modify (OWNED):**

| File | What to do |
|---|---|
| `tests/test_streaming_workers.py` (NEW) | Mock-based unit tests for: (1) EventHub worker row mapping, (2) PubSub worker row mapping, (3) dispatch logic in `/api/stream`, (4) comparison stats aggregation |
| `tests/test_streaming_clients.py` (NEW) | Mock-based unit tests for: (1) `HPStreamingClient` init/open_channel/append/flush/close lifecycle, (2) `SqlInsertClient` (formerly ClassicStreamingClient) SQL generation |
| `hp_streaming_eval/requirements.txt` | Uncomment `azure-eventhub` and `google-cloud-pubsub` |

**Success criteria:**
- `pytest tests/test_streaming_workers.py -v` passes (all mocked, no real connections needed)
- `pytest tests/test_streaming_clients.py -v` passes
- Dependencies in requirements.txt are uncommented
- All tests use `unittest.mock` to avoid real EventHub/PubSub/Snowflake connections

**Dependencies:** Tasks 1 and 3 must complete first (Task 1 renames classes, Task 3 changes row schemas that tests validate).

---

## Dependency Graph

```
Task 1 (Terminology)  ─────────────────────────┐
Task 2 (Import fix)   ──── parallel ────────────┤
Task 3 (Schema fix)   ──── parallel ────────────┤
Task 4 (Arch note)    ──── parallel ────────────┤
Task 5 (SQL param)    ──── parallel ────────────┤
                                                │
                                    ┌───────────┤
                                    ▼           ▼
                              Task 6         Task 7
                           (Winner fix)   (Test coverage)
                           needs Task 1   needs Tasks 1+3
```

**Wave 1 (parallel):** Tasks 1, 2, 3, 4, 5
**Wave 2 (after Wave 1):** Tasks 6, 7

---

## File Ownership Matrix (Conflict Prevention)

| File | Task 1 | Task 2 | Task 3 | Task 4 | Task 5 | Task 6 | Task 7 |
|---|---|---|---|---|---|---|---|
| `hp_streaming_eval/app.py` | HTML labels, JS vars, comparison headers, worker comments | Code snippet (~line 910) | EventHub/PubSub worker row dicts | — | — | Comparison result display | — |
| `spcs_app/fastapi_app.py` | — | — | EventHub/PubSub worker row dicts (lines ~696-1088) | Streaming page HTML (~line 9000+) | EventHub/PubSub flush SQL (lines ~778, ~970) | comparison_streaming_worker (~line 1089+) | — |
| `spcs_app/snowpipe_streaming_impl.py` | Class names, docstrings, ARCHITECTURE_INFO | — | — | — | — | — | — |
| `tests/test_streaming_workers.py` | — | — | — | — | — | — | NEW file |
| `tests/test_streaming_clients.py` | — | — | — | — | — | — | NEW file |
| `hp_streaming_eval/requirements.txt` | — | — | — | — | — | — | Uncomment deps |

---

## Verification Protocol

After all tasks complete:
1. `python -m py_compile spcs_app/fastapi_app.py`
2. `python -m py_compile hp_streaming_eval/app.py`
3. `python -m py_compile spcs_app/snowpipe_streaming_impl.py`
4. `pytest tests/ -v`
5. `grep -ri "ClassicStreamingClient\|snowpipe_streaming\.client\|from snowpipe_streaming" spcs_app/ hp_streaming_eval/` → should return zero hits
6. `grep -rn "V1\|V2" hp_streaming_eval/app.py` → review: zero streaming-related V1/V2 labels remain (CSS class names like "v1" are OK)
