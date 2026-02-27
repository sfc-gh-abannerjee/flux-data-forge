# Flux Data Forge — Architecture Refactoring Plan

## Current State

`spcs_app/fastapi_app.py` is a **12,274-line monolith** containing:
- 39 route handlers (GET/POST endpoints)
- 4 streaming worker functions (~800 lines each)
- ~800 lines of inline CSS (`get_base_styles()`)
- ~2,000 lines of inline HTML templates (per-page)
- ~500 lines of inline JavaScript
- Session management, data generation, monitoring — all in one file

This makes the codebase difficult to navigate, test, and maintain.

## Target Module Structure

```
spcs_app/
  fastapi_app.py              # Entry point: app creation, lifespan, includes routers
  config.py                   # Centralized configuration (existing)
  utils/
    __init__.py               # (existing)
    sanitize.py               # SQL injection prevention (existing)
  workers/
    __init__.py               # Worker exports
    base.py                   # Shared worker utilities (stats, status, error handling)
    snowpipe_streaming.py     # snowpipe_streaming_worker
    s3_streaming.py           # raw_json_s3_streaming_worker
    internal_stage.py         # internal_stage_streaming_worker
    external_stage.py         # external_stage_streaming_worker
  routes/
    __init__.py               # Router registration
    health.py                 # /health, /readiness, /api/cache/status
    pages.py                  # /, /generate, /monitor, /validate, /history (HTML pages)
    streaming.py              # /api/stream, /api/streaming/*
    generation.py             # /api/generate, /api/streaming/preview, calculate-metrics
    admin.py                  # /api/task/*, /api/databases, /api/schemas, /api/tables, /api/validate
    pipes.py                  # /api/pipes/*
    production.py             # /api/production/*
    monitoring.py             # /api/monitor/metrics, /api/resources, /api/context
    stages.py                 # /api/stages, /api/external-stage/*
  templates/                  # Future: Jinja2 templates (replace inline HTML)
    base.html
    generate.html
    monitor.html
    stream.html
    validate.html
    history.html
  static/                     # Future: extracted CSS/JS
    styles.css
    app.js
```

## Extraction Priority

### Priority 1: Workers (Lowest risk, highest clarity)
The 4 streaming workers are self-contained functions with clear boundaries.
They share common patterns (stats init, status management, batch loop, error backoff).

Steps:
1. Create `workers/base.py` with shared `StreamingWorkerBase` class
2. Move `generate_ami_reading()` to `workers/base.py`
3. Extract each worker to its own file, inheriting from base
4. Update `fastapi_app.py` to import from workers module
5. Verify streaming still works end-to-end

### Priority 2: API Routes (Medium risk, high value)
Route handlers are mostly independent — each handles one endpoint.
Shared state (global `snowflake_session`, `active_streaming_jobs`) needs to be passed via dependency injection or a shared state module.

Steps:
1. Create a `state.py` module for shared globals (session, jobs dict, locks)
2. Move route handlers to router files, one group at a time
3. Register routers in `fastapi_app.py` via `app.include_router()`
4. Test each group after extraction

### Priority 3: HTML Pages (Medium risk, medium value)
The 5 full HTML pages (~2000 lines each) use inline f-string templates.
Moving to Jinja2 templates improves maintainability.

Steps:
1. Add `jinja2` to requirements.txt
2. Extract `get_base_styles()` CSS to `static/styles.css`
3. Create Jinja2 base template with common layout
4. Convert each page endpoint to render a template
5. Move inline JavaScript to `static/app.js`

### Priority 4: Template Extraction (Lower priority)
Extract the CSS (~800 lines) and JavaScript (~500 lines) to static files.
This depends on Priority 3 being done first.

## Migration Strategy

### Incremental Approach
1. **Never break the running SPCS service** — all changes must be backward-compatible
2. **One module at a time** — extract, test, deploy, then move to the next
3. **Import bridges** — after extracting a function, leave an import in the original location so existing references work
4. **Feature flags** — if needed, use environment variables to toggle between old/new code paths

### Testing Protocol
Before each extraction:
- Run `pytest tests/ -v` (all tests must pass)
- Run `python tests/smoke_test.py` (all smoke tests must pass)
- Build Docker image locally: `cd spcs_app && docker build -t flux-test .`
- If possible, test against a dev Snowflake environment

### Deployment
After extracting modules:
- Update `Dockerfile` COPY commands if new directories are added
- Update `.dockerignore` if needed
- Tag a release, build, push to registry, restart SPCS service

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking shared state (session, jobs) | High — streaming stops | Use dependency injection; test with concurrent jobs |
| Missing imports after extraction | Medium — startup crash | Run smoke tests + Docker build before deploy |
| Template rendering differences | Low — UI glitch | Visual diff testing before/after |
| Worker extraction breaks batch logic | High — data loss | Run small streaming test after each worker extraction |
| SPCS token handling changes | High — auth failure | Keep `get_valid_session()` in a shared module, test in SPCS env |

## Completed Work (This Review)

- [x] Created `spcs_app/utils/sanitize.py` — SQL injection prevention
- [x] Created `spcs_app/routes/` — Route stub files with APIRouter instances
- [x] Created `spcs_app/workers/` — Worker base module with shared patterns
- [x] Fixed all SQL injection, XSS, bare excepts, datetime.utcnow()
- [x] Added non-root Docker user, .dockerignore
- [x] Added 31 security regression tests
