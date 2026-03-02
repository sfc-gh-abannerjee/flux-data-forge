"""
Integration Tests for Flux Data Forge — HTTP-level testing via TestClient.

Tests both apps as running HTTP services:
  1. SPCS App (fastapi_app.py) — requires mocked Snowflake session
  2. Eval Wizard (hp_streaming_eval/app.py) — no mocks needed

Run with: pytest tests/test_integration.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Path setup — both apps need their directories on sys.path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(__file__)
_SPCS_DIR = os.path.join(_HERE, '..', 'spcs_app')
_EVAL_DIR = os.path.join(_HERE, '..', 'hp_streaming_eval')

if os.path.abspath(_SPCS_DIR) not in sys.path:
    sys.path.insert(0, os.path.abspath(_SPCS_DIR))
if os.path.abspath(_EVAL_DIR) not in sys.path:
    sys.path.insert(0, os.path.abspath(_EVAL_DIR))


# ============================================================================
# Group 1: SPCS App — TestClient with mocked Snowflake session
# ============================================================================

# ---------------------------------------------------------------------------
# Pre-import: inject mock snowflake modules so fastapi_app can load without
# snowflake-snowpark-python installed locally.
# ---------------------------------------------------------------------------
_SNOWFLAKE_MODS = [
    'snowflake', 'snowflake.connector', 'snowflake.snowpark',
    'snowflake.connector.pandas_tools',
]
for _mod_name in _SNOWFLAKE_MODS:
    if _mod_name not in sys.modules:
        _mock_mod = MagicMock()
        if _mod_name == 'snowflake.snowpark':
            _mock_mod.Session = MagicMock()
        sys.modules[_mod_name] = _mock_mod

import fastapi_app  # noqa: E402 — must come after mock injection


class TestSPCSApp:
    """SPCS app integration tests. Mocks create_snowflake_session() so
    the lifespan handler doesn't need real Snowflake credentials."""

    @pytest.fixture(autouse=True)
    def spcs_client(self):
        """Create a TestClient for the SPCS app with mocked session."""
        mock_session = MagicMock()
        # The lifespan reconciles stale jobs — mock .sql().collect()
        mock_session.sql.return_value.collect.return_value = []

        with patch.object(fastapi_app, 'create_snowflake_session', return_value=mock_session):
            from starlette.testclient import TestClient
            with TestClient(fastapi_app.app) as client:
                self.client = client
                self.mock_session = mock_session
                yield

    # -- Page routes --

    def test_root_redirects_to_generate(self):
        """GET / should redirect to /generate."""
        resp = self.client.get("/", follow_redirects=False)
        assert resp.status_code in (301, 302, 307)
        assert "/generate" in resp.headers.get("location", "")

    def test_health_endpoint(self):
        """GET /health returns healthy status."""
        resp = self.client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["connected"] is True

    def test_generate_page_renders(self):
        """GET /generate returns HTML with FLUX Data Forge branding."""
        resp = self.client.get("/generate")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        body = resp.text
        assert "FLUX Data Forge" in body

    def test_generate_page_has_data_formats(self):
        """In streaming mode, the generate page shows all 5 data format options."""
        resp = self.client.get("/generate?mode=streaming&data_flow=streaming_insert")
        body = resp.text
        assert "standard" in body.lower() or "Standard AMI" in body
        assert "itron_grid_planning" in body
        assert "symphony_iris" in body
        assert "carto_spatial" in body
        assert "siemens_edge" in body

    def test_generate_page_has_utility_profiles(self):
        """The generate page should reference utility service areas."""
        resp = self.client.get("/generate")
        body = resp.text
        assert "TEXAS_GULF_COAST" in body

    def test_cache_status_endpoint(self):
        """GET /api/cache/status returns JSON."""
        resp = self.client.get("/api/cache/status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_monitor_page_renders(self):
        """GET /monitor returns HTML."""
        resp = self.client.get("/monitor")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_validate_page_renders(self):
        """GET /validate returns HTML."""
        resp = self.client.get("/validate")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_history_page_renders(self):
        """GET /history returns HTML."""
        resp = self.client.get("/history")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    # -- API endpoints that need Snowflake (mock returns empty) --

    def test_streaming_status_returns_json(self):
        """GET /api/streaming/status returns JSON about active jobs."""
        resp = self.client.get("/api/streaming/status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


# ============================================================================
# Group 2: Eval Wizard — TestClient, no mocks needed
# ============================================================================

class TestEvalWizard:
    """HP Streaming Eval wizard integration tests. No Snowflake mocks needed
    because the wizard only connects interactively via /api/connect."""

    @pytest.fixture(autouse=True)
    def eval_client(self):
        """Create a TestClient for the eval wizard."""
        from app import app as eval_app
        from starlette.testclient import TestClient
        with TestClient(eval_app) as client:
            self.client = client
            yield

    # -- Page routes --

    def test_root_returns_html(self):
        """GET / returns the wizard HTML page."""
        resp = self.client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_page_has_title(self):
        """The page should contain the HP Streaming Eval title."""
        resp = self.client.get("/")
        body = resp.text
        assert "HP Streaming Eval" in body

    def test_page_has_builtin_profiles(self):
        """The wizard should show built-in data profiles."""
        resp = self.client.get("/")
        body = resp.text
        assert "AMI Smart Meter" in body or "ami" in body.lower()
        assert "IoT Sensor" in body or "iot" in body.lower()
        assert "Clickstream" in body or "clickstream" in body.lower()
        assert "Financial" in body or "financial" in body.lower()

    def test_page_has_shared_library_profiles(self):
        """When co-located with spcs_app, the Energy Solutions shared library
        optgroup should appear with all 5 utility generator profiles."""
        resp = self.client.get("/")
        body = resp.text
        assert "Energy Solutions (Shared Library)" in body
        assert "utility:standard" in body or "Utility AMI" in body
        assert "utility:itron_grid_planning" in body or "Itron Grid Planning" in body
        assert "utility:symphony_iris" in body or "SymphonyAI IRIS" in body
        assert "utility:carto_spatial" in body or "CARTO Spatial" in body
        assert "utility:siemens_edge" in body or "Siemens Industrial Edge" in body

    def test_page_has_connection_form(self):
        """The wizard should have fields for Snowflake connection."""
        resp = self.client.get("/")
        body = resp.text
        # Should have account and user fields
        assert "account" in body.lower()
        assert "user" in body.lower()

    # -- API endpoints --

    def test_connect_rejects_empty_account(self):
        """POST /api/connect with missing account returns error."""
        resp = self.client.post("/api/connect", data={"account": "", "user": "", "auth_mode": "password", "password": "x"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert "required" in data["message"].lower() or "account" in data["message"].lower()

    def test_connect_rejects_missing_password(self):
        """POST /api/connect with password auth but no password returns error."""
        resp = self.client.post("/api/connect", data={
            "account": "testaccount",
            "user": "testuser",
            "auth_mode": "password",
            "password": "",
        })
        data = resp.json()
        assert data["status"] == "error"

    def test_stream_metrics_without_active_stream(self):
        """GET /api/stream/metrics before starting a stream returns baseline."""
        resp = self.client.get("/api/stream/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_databases_without_connection(self):
        """GET /api/databases without an active connection returns error or empty."""
        resp = self.client.get("/api/databases")
        assert resp.status_code == 200
        data = resp.json()
        # Should indicate no connection
        assert "error" in data.get("status", "").lower() or data.get("databases") == []


# ============================================================================
# Group 3: Live server smoke test — eval wizard via subprocess
# ============================================================================

class TestEvalWizardLiveServer:
    """Start the eval wizard as a real subprocess and make HTTP requests.
    This catches issues that TestClient might not (e.g., import-time errors
    when run as __main__, port binding, uvicorn startup)."""

    @pytest.fixture(autouse=True)
    def live_server(self):
        """Start eval wizard on a random port, wait for it, then tear down."""
        import subprocess
        import time
        import socket
        import signal

        # Find a free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            port = s.getsockname()[1]

        env = os.environ.copy()
        env["PORT"] = str(port)

        proc = subprocess.Popen(
            [sys.executable, "app.py"],
            cwd=os.path.abspath(_EVAL_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for server to be ready (up to 10s)
        self.base_url = f"http://127.0.0.1:{port}"
        ready = False
        for _ in range(40):
            try:
                import urllib.request
                urllib.request.urlopen(f"{self.base_url}/", timeout=1)
                ready = True
                break
            except Exception:
                time.sleep(0.25)

        if not ready:
            proc.kill()
            stdout, stderr = proc.communicate(timeout=5)
            pytest.fail(
                f"Eval wizard failed to start on port {port}.\n"
                f"stdout: {stdout.decode()[-500:]}\n"
                f"stderr: {stderr.decode()[-500:]}"
            )

        self.proc = proc
        yield

        # Teardown
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def test_live_root_returns_html(self):
        """Real HTTP GET / returns HTML."""
        import urllib.request
        resp = urllib.request.urlopen(f"{self.base_url}/")
        body = resp.read().decode()
        assert resp.status == 200
        assert "HP Streaming Eval" in body

    def test_live_has_shared_library(self):
        """Real server should have shared library profiles loaded."""
        import urllib.request
        resp = urllib.request.urlopen(f"{self.base_url}/")
        body = resp.read().decode()
        assert "Energy Solutions (Shared Library)" in body

    def test_live_api_connect_rejects_empty(self):
        """Real HTTP POST /api/connect with empty fields returns error."""
        import urllib.request
        import urllib.parse
        data = urllib.parse.urlencode({
            "account": "", "user": "", "auth_mode": "password", "password": "x"
        }).encode()
        req = urllib.request.Request(f"{self.base_url}/api/connect", data=data, method="POST")
        resp = urllib.request.urlopen(req)
        import json
        result = json.loads(resp.read().decode())
        assert result["status"] == "error"
