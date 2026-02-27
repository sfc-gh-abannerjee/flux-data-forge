"""
Shared base utilities for streaming workers.

All 4 streaming workers (snowpipe, S3, internal stage, external stage)
share common patterns: stats tracking, status management, batch loop,
error backoff, and AMI reading generation. This module captures those
shared patterns to eliminate duplication.

Usage:
    from workers.base import StreamingWorkerBase, generate_ami_reading

    class SnowpipeWorker(StreamingWorkerBase):
        def process_batch(self, batch):
            # Worker-specific batch processing
            ...
"""

import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    """Tracks streaming worker statistics."""
    total_rows: int = 0
    batches_sent: int = 0
    errors: int = 0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_batch_time: Optional[datetime] = None
    last_file_time: Optional[datetime] = None
    files_written: int = 0

    def record_batch(self, row_count: int):
        self.total_rows += row_count
        self.batches_sent += 1
        self.last_batch_time = datetime.now(timezone.utc)

    def record_file(self, row_count: int):
        self.total_rows += row_count
        self.files_written += 1
        self.last_file_time = datetime.now(timezone.utc)

    def record_error(self):
        self.errors += 1

    def to_dict(self) -> dict:
        return {
            "total_rows": self.total_rows,
            "batches_sent": self.batches_sent,
            "errors": self.errors,
            "files_written": self.files_written,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "last_batch_time": self.last_batch_time.isoformat() if self.last_batch_time else None,
        }


# Valid worker statuses
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_STOPPING = "STOPPING"
STATUS_STOPPED = "STOPPED"
STATUS_FAILED = "FAILED"
STATUS_COMPLETED = "COMPLETED"


class StreamingWorkerBase:
    """
    Base class for streaming workers.

    Provides the common run loop pattern:
    1. Initialize stats and set status to RUNNING
    2. Loop: check if still running, generate batch, process batch, sleep
    3. Handle errors with backoff
    4. Set final status on exit

    Subclasses implement:
        - setup() — worker-specific initialization
        - process_batch(batch) — worker-specific batch handling
        - cleanup() — worker-specific teardown
    """

    def __init__(self, job_id: str, config: dict, jobs_dict: dict, lock):
        self.job_id = job_id
        self.config = config
        self.jobs_dict = jobs_dict
        self.lock = lock
        self.stats = WorkerStats()

    def is_running(self) -> bool:
        """Check if the worker should continue running."""
        with self.lock:
            job = self.jobs_dict.get(self.job_id)
            if not job:
                return False
            return job.get("status") in (STATUS_RUNNING, STATUS_PENDING)

    def update_status(self, status: str):
        """Update the worker's status in the shared jobs dict."""
        with self.lock:
            if self.job_id in self.jobs_dict:
                self.jobs_dict[self.job_id]["status"] = status

    def update_stats(self):
        """Sync local stats to the shared jobs dict."""
        with self.lock:
            if self.job_id in self.jobs_dict:
                self.jobs_dict[self.job_id]["stats"] = self.stats.to_dict()

    def setup(self):
        """Override for worker-specific initialization."""
        pass

    def process_batch(self, batch: list) -> None:
        """Override to handle a batch of AMI readings."""
        raise NotImplementedError

    def cleanup(self):
        """Override for worker-specific teardown."""
        pass

    def run(self):
        """Main worker loop. Call this from a background thread."""
        logger.info(f"Starting worker {self.__class__.__name__} for job {self.job_id}")

        try:
            self.setup()
            self.update_status(STATUS_RUNNING)

            meters = self.config.get("meters", 1000)
            rows_per_batch = self.config.get("rows_per_batch", 100)
            batch_interval = self.config.get("batch_interval_sec", 10)

            while self.is_running():
                try:
                    # Generate a batch of AMI readings
                    batch = [
                        generate_ami_reading(
                            meter_index=i,
                            total_meters=meters,
                            config=self.config,
                        )
                        for i in range(min(rows_per_batch, meters))
                    ]

                    self.process_batch(batch)
                    self.stats.record_batch(len(batch))
                    self.update_stats()

                    time.sleep(max(batch_interval, 0.1))

                except Exception as e:
                    logger.error(f"Worker error for job {self.job_id}: {e}")
                    self.stats.record_error()
                    self.update_stats()
                    time.sleep(1)  # Back off on error

        except Exception as e:
            logger.error(f"Fatal worker error for job {self.job_id}: {e}")
            self.update_status(STATUS_FAILED)
        else:
            self.update_status(STATUS_COMPLETED)
        finally:
            self.cleanup()
            logger.info(f"Worker {self.__class__.__name__} for job {self.job_id} finished")


def generate_ami_reading(
    meter_index: int = 0,
    total_meters: int = 1000,
    config: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Generate a single synthetic AMI (smart meter) reading.

    Produces realistic energy usage patterns:
    - Time-of-day variation (peak 2-7pm, morning 6-9am, off-peak overnight)
    - Segment multipliers (industrial 15x, commercial 5x, residential 1x)
    - Random voltage fluctuation (118-122V nominal)
    - Occasional outages (~1%) and anomalies (~2%)

    Args:
        meter_index: Index for deterministic meter ID generation
        total_meters: Total fleet size (for prefix calculation)
        config: Optional config dict with service_area, meter_prefix, etc.

    Returns:
        Dict with all AMI reading fields
    """
    config = config or {}

    meter_prefix = config.get("meter_prefix", "MTR")
    service_area = config.get("service_area", "TEXAS_GULF_COAST")

    now = datetime.now(timezone.utc)
    hour = now.hour

    # Time-of-day usage pattern
    if 14 <= hour <= 19:
        base_usage = random.uniform(1.5, 3.5)  # Peak
    elif 6 <= hour <= 9:
        base_usage = random.uniform(1.0, 2.5)  # Morning
    else:
        base_usage = random.uniform(0.3, 1.5)  # Off-peak

    # Segment selection and multiplier
    segment = random.choice(["RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL"])
    multipliers = {"RESIDENTIAL": 1, "COMMERCIAL": 5, "INDUSTRIAL": 15}
    usage = round(base_usage * multipliers[segment], 4)

    # Outage and quality
    roll = random.randint(1, 100)
    is_outage = roll <= 1
    if is_outage:
        data_quality = "OUTAGE"
        usage = 0.0
    elif roll >= 98:
        data_quality = "ANOMALY"
    else:
        data_quality = "VALID"

    meter_id = f"{meter_prefix}-{meter_index:06d}"

    return {
        "METER_ID": meter_id,
        "READING_TIMESTAMP": now,
        "USAGE_KWH": usage,
        "VOLTAGE": round(random.uniform(118, 122), 2),
        "POWER_FACTOR": round(random.uniform(0.85, 1.0), 3),
        "TEMPERATURE_C": round(random.uniform(15, 40), 1),
        "TRANSFORMER_ID": f"TRF-{random.randint(1, 500):04d}",
        "CIRCUIT_ID": f"CKT-{random.randint(1, 100):03d}",
        "SUBSTATION_ID": f"SUB-{random.randint(1, 20):02d}",
        "SERVICE_AREA": service_area,
        "CUSTOMER_SEGMENT": segment,
        "LATITUDE": round(29.7604 + random.uniform(-0.5, 0.5), 6),
        "LONGITUDE": round(-95.3698 + random.uniform(-0.5, 0.5), 6),
        "IS_OUTAGE": is_outage,
        "DATA_QUALITY": data_quality,
    }
