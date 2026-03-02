"""
Flux Data Forge — Data Generation Engine

Extracted from the monolithic fastapi_app.py to support reuse across:
  - SPCS main application (streaming workers)
  - HP Streaming Eval wizard
  - Standalone data generation scripts
  - Future shared library (Step 3B)

Contains:
  - Grid topology state (correlated outages & voltage cascading)
  - AMI reading generator (standard, raw, minimal formats)
  - Itron Grid Planning 8,760-hr format generator
  - SymphonyAI IRIS Foundry asset health generator
  - CARTO Spatial Analytics geospatial generator
  - Siemens Industrial Edge SCADA telemetry generator
  - All supporting constants and configuration
"""

import json
import math
import random
import threading
import uuid
from datetime import datetime, timezone


# ============================================================================
# EMISSION PATTERNS — AMI infrastructure behavior models
# ============================================================================

EMISSION_PATTERNS = {
    'UNIFORM': {
        'name': 'Uniform (All meters report)',
        'description': 'All meters emit at each interval - useful for testing max throughput',
        'meter_report_pct': 100,
        'stagger_seconds': 0,
    },
    'STAGGERED_REALISTIC': {
        'name': 'Staggered (Realistic)',
        'description': 'Meters report across the 15-min window - mimics real AMI behavior',
        'meter_report_pct': 100,
        'stagger_seconds': 900,
    },
    'PARTIAL_REPORTING': {
        'name': 'Partial (98% reporting)',
        'description': '2% communication failures - realistic data quality',
        'meter_report_pct': 98,
        'stagger_seconds': 600,
    },
    'DEGRADED_NETWORK': {
        'name': 'Degraded (85% reporting)',
        'description': 'Simulates network issues or storm conditions',
        'meter_report_pct': 85,
        'stagger_seconds': 900,
    },
}

# Event frequency ratios (per 1000 meters per interval)
EVENT_FREQUENCIES = {
    'readings': 1000,
    'voltage_anomalies': 20,
    'power_quality': 10,
    'outages': 5,
    'tamper_alerts': 1,
    'reverse_flow': 8,
}


# ============================================================================
# GRID TOPOLOGY STATE — Correlated outages & voltage cascading
# ============================================================================

class GridTopologyState:
    """
    Tracks outage and voltage state at substation -> circuit -> transformer level.

    When an outage triggers on a transformer, ALL meters on that transformer
    report outages. Voltage sag cascades from substation -> circuit -> transformer
    with realistic attenuation. State transitions happen probabilistically each
    tick (called once per batch cycle in the streaming worker).

    Thread-safe: uses its own lock for state mutations.
    """

    P_SUBSTATION_EVENT = 0.0005
    P_CIRCUIT_EVENT = 0.002
    P_TRANSFORMER_OUTAGE = 0.001
    P_RECOVER = 0.15

    SAG_SUBSTATION = (-6.0, -2.0)
    SAG_CIRCUIT = (-4.0, -1.0)
    SAG_TRANSFORMER = (-8.0, -3.0)

    def __init__(self):
        self._lock = threading.Lock()
        self._substations = {}
        self._circuits = {}
        self._transformers = {}

    def tick(self):
        """Advance one time step: probabilistically trigger/recover events."""
        with self._lock:
            for store in (self._substations, self._circuits, self._transformers):
                for eid, state in list(store.items()):
                    if state['active'] and random.random() < self.P_RECOVER:
                        state['active'] = False
                        state['sag_v'] = 0.0

            for sid, state in self._substations.items():
                if not state['active'] and random.random() < self.P_SUBSTATION_EVENT:
                    state['active'] = True
                    state['sag_v'] = random.uniform(*self.SAG_SUBSTATION)
                    state['since'] = datetime.now()

            for cid, state in self._circuits.items():
                if not state['active'] and random.random() < self.P_CIRCUIT_EVENT:
                    state['active'] = True
                    state['sag_v'] = random.uniform(*self.SAG_CIRCUIT)
                    state['since'] = datetime.now()

            for tid, state in self._transformers.items():
                if not state['active'] and random.random() < self.P_TRANSFORMER_OUTAGE:
                    state['active'] = True
                    state['sag_v'] = random.uniform(*self.SAG_TRANSFORMER)
                    state['since'] = datetime.now()

    def register_meter(self, meter_info: dict):
        """Register topology IDs so they participate in state transitions."""
        with self._lock:
            sid = meter_info.get('substation_id')
            cid = meter_info.get('circuit_id')
            tid = meter_info.get('transformer_id')
            if sid and sid not in self._substations:
                self._substations[sid] = {'active': False, 'sag_v': 0.0, 'since': None}
            if cid and cid not in self._circuits:
                self._circuits[cid] = {'active': False, 'sag_v': 0.0, 'since': None}
            if tid and tid not in self._transformers:
                self._transformers[tid] = {'active': False, 'sag_v': 0.0, 'since': None}

    def get_meter_state(self, meter_info: dict) -> dict:
        """Return effective outage/voltage state for a meter."""
        with self._lock:
            tid = meter_info.get('transformer_id')
            cid = meter_info.get('circuit_id')
            sid = meter_info.get('substation_id')

            total_sag = 0.0
            is_outage = False
            outage_source = None

            if tid and tid in self._transformers and self._transformers[tid]['active']:
                is_outage = True
                total_sag += self._transformers[tid]['sag_v']
                outage_source = f'TRANSFORMER:{tid}'

            if cid and cid in self._circuits and self._circuits[cid]['active']:
                total_sag += self._circuits[cid]['sag_v']
                if not outage_source:
                    outage_source = f'CIRCUIT:{cid}'

            if sid and sid in self._substations and self._substations[sid]['active']:
                total_sag += self._substations[sid]['sag_v']
                if not outage_source:
                    outage_source = f'SUBSTATION:{sid}'

            if is_outage:
                data_quality = 'OUTAGE'
            elif total_sag < -3.0:
                data_quality = 'ANOMALY'
            else:
                data_quality = 'VALID'

            return {
                'is_outage': is_outage,
                'voltage_offset': total_sag,
                'data_quality': data_quality,
                'outage_source': outage_source,
            }

    def summary(self) -> dict:
        """Return a snapshot of active events for monitoring."""
        with self._lock:
            active_subs = sum(1 for s in self._substations.values() if s['active'])
            active_circuits = sum(1 for s in self._circuits.values() if s['active'])
            active_xfmrs = sum(1 for s in self._transformers.values() if s['active'])
            return {
                'substations_total': len(self._substations),
                'substations_active': active_subs,
                'circuits_total': len(self._circuits),
                'circuits_active': active_circuits,
                'transformers_total': len(self._transformers),
                'transformers_active': active_xfmrs,
            }


# ============================================================================
# DATA FORMAT DEFINITIONS
# ============================================================================

DATA_FORMATS = {
    'standard': {
        'name': 'Standard AMI',
        'description': 'Structured columns optimized for analytics',
        'columns': ['METER_ID', 'TRANSFORMER_ID', 'CIRCUIT_ID', 'READING_TIMESTAMP',
                     'USAGE_KWH', 'VOLTAGE', 'CUSTOMER_SEGMENT'],
    },
    'raw_ami': {
        'name': 'Raw AMI (with VARIANT)',
        'description': 'Includes RAW_PAYLOAD VARIANT column with full JSON for semi-structured analytics',
        'columns': ['METER_ID', 'READING_TIMESTAMP', 'USAGE_KWH', 'RAW_PAYLOAD'],
    },
    'minimal': {
        'name': 'Minimal',
        'description': 'Essential columns only - fastest ingestion',
        'columns': ['METER_ID', 'READING_TIMESTAMP', 'USAGE_KWH'],
    },
    'itron_grid_planning': {
        'name': 'Itron Grid Planning (8,760-hr)',
        'description': 'Hourly load profile format compatible with Itron Grid Planning power flow simulation',
        'columns': [
            'NODE_ID', 'TRANSFORMER_ID', 'FEEDER_ID', 'SUBSTATION_ID',
            'HOUR_OF_YEAR', 'TIMESTAMP_UTC',
            'LOAD_KW', 'LOAD_KVAR', 'POWER_FACTOR',
            'VOLTAGE_PU', 'PHASE',
            'CUSTOMER_COUNT', 'CUSTOMER_CLASS',
            'LATITUDE', 'LONGITUDE',
        ],
    },
    'symphony_iris': {
        'name': 'SymphonyAI IRIS Foundry',
        'description': 'Asset health & predictive maintenance format for SymphonyAI IRIS Foundry analytics',
        'columns': [
            'ASSET_ID', 'ASSET_TYPE', 'SUBSTATION_ID',
            'TIMESTAMP_UTC', 'HEALTH_INDEX',
            'TEMPERATURE_C', 'VIBRATION_MM_S', 'OIL_QUALITY_PPM',
            'LOAD_PERCENT', 'RISK_SCORE', 'MAINTENANCE_FLAG',
            'REMAINING_LIFE_DAYS', 'FAILURE_MODE',
            'LATITUDE', 'LONGITUDE',
        ],
    },
    'carto_spatial': {
        'name': 'CARTO Spatial Analytics',
        'description': 'Geospatial grid analytics format for CARTO spatial intelligence platform',
        'columns': [
            'GRID_CELL_ID', 'H3_INDEX', 'SUBSTATION_ID', 'FEEDER_ID',
            'TIMESTAMP_UTC', 'LOAD_DENSITY_KW_KM2',
            'CUSTOMER_DENSITY', 'OUTAGE_COUNT',
            'VEGETATION_RISK_SCORE', 'WEATHER_SEVERITY',
            'LATITUDE', 'LONGITUDE',
            'GEOJSON',
        ],
    },
    'siemens_edge': {
        'name': 'Siemens Industrial Edge',
        'description': 'SCADA/industrial telemetry format for Siemens Industrial Edge integration',
        'columns': [
            'DEVICE_ID', 'DEVICE_TYPE', 'SUBSTATION_ID',
            'TIMESTAMP_UTC', 'TAG_NAME', 'TAG_VALUE',
            'UNIT', 'QUALITY_CODE',
            'ALARM_ACTIVE', 'ALARM_PRIORITY',
            'PROTOCOL', 'SAMPLE_RATE_HZ',
            'LATITUDE', 'LONGITUDE',
        ],
    },
}


# ============================================================================
# UTILITY SERVICE AREA PROFILES
# ============================================================================

UTILITY_PROFILES = {
    'TEXAS_GULF_COAST': {
        'name': 'Texas Gulf Coast (ERCOT)',
        'segment_dist': {'RESIDENTIAL': 70, 'COMMERCIAL': 20, 'INDUSTRIAL': 10},
        'description': 'Hot humid subtropical - high summer AC load',
        'center_lat': 29.7604, 'center_lon': -95.3698,
    },
    'CALIFORNIA_COASTAL': {
        'name': 'California Coastal (CAISO)',
        'segment_dist': {'RESIDENTIAL': 65, 'COMMERCIAL': 28, 'INDUSTRIAL': 7},
        'description': 'Mediterranean climate - mild temps, evening peaks',
        'center_lat': 34.0522, 'center_lon': -118.2437,
    },
    'NORTHEAST_CORRIDOR': {
        'name': 'Northeast Corridor (NYISO)',
        'segment_dist': {'RESIDENTIAL': 55, 'COMMERCIAL': 38, 'INDUSTRIAL': 7},
        'description': 'Humid continental - cold winters, hot summers',
        'center_lat': 40.7128, 'center_lon': -74.0060,
    },
    'MIDWEST_GREAT_LAKES': {
        'name': 'Midwest/Great Lakes (MISO)',
        'segment_dist': {'RESIDENTIAL': 60, 'COMMERCIAL': 30, 'INDUSTRIAL': 10},
        'description': 'Continental climate - extreme cold winters',
        'center_lat': 41.8781, 'center_lon': -87.6298,
    },
    'SOUTHEAST_SUNBELT': {
        'name': 'Southeast Sunbelt (SERC)',
        'segment_dist': {'RESIDENTIAL': 68, 'COMMERCIAL': 24, 'INDUSTRIAL': 8},
        'description': 'Humid subtropical - high AC load, mild winters',
        'center_lat': 33.7490, 'center_lon': -84.3880,
    },
    'PACIFIC_NORTHWEST': {
        'name': 'Pacific Northwest (BPA)',
        'segment_dist': {'RESIDENTIAL': 62, 'COMMERCIAL': 28, 'INDUSTRIAL': 10},
        'description': 'Marine climate - mild temps, high winter heating',
        'center_lat': 47.6062, 'center_lon': -122.3321,
    },
}

SNOWPIPE_SDK_LIMITS = {
    'max_throughput_gb_s': 10,
    'max_batch_size_mb': 16,
    'optimal_batch_size_mb': {'min': 10, 'max': 16},
    'max_client_lag_seconds': {'min': 1, 'max': 600, 'default': 1, 'iceberg_default': 30},
    'row_size_estimate_bytes': 500,
    'channel_inactive_days': 30,
}


# ============================================================================
# GENERATOR: Standard AMI Readings
# ============================================================================

def generate_ami_reading(meter_info: dict, service_area: str, emission_pattern: str,
                         grid_state: GridTopologyState = None) -> dict:
    """Generate a single realistic AMI reading.

    When *grid_state* is provided, outage and voltage values are derived from
    the topology hierarchy (substation -> circuit -> transformer) instead of
    independent random rolls.
    """
    hour = datetime.now().hour

    if 14 <= hour <= 19:
        base_usage = random.uniform(1.5, 3.5)
    elif 6 <= hour <= 9:
        base_usage = random.uniform(1.0, 2.5)
    else:
        base_usage = random.uniform(0.3, 1.5)

    segment = meter_info.get('customer_segment', 'RESIDENTIAL')
    if segment == 'INDUSTRIAL':
        usage_multiplier = 15
    elif segment == 'COMMERCIAL':
        usage_multiplier = 5
    else:
        usage_multiplier = 1

    if grid_state is not None:
        topo = grid_state.get_meter_state(meter_info)
        is_outage = topo['is_outage']
        data_quality = topo['data_quality']
        voltage = 120.0 + topo['voltage_offset'] + random.uniform(-0.5, 0.5)
    else:
        quality_roll = random.randint(1, 100)
        if quality_roll <= 1:
            data_quality = 'OUTAGE'
            is_outage = True
        elif quality_roll >= 98:
            data_quality = 'ANOMALY'
            is_outage = False
        else:
            data_quality = 'VALID'
            is_outage = False
        voltage = random.uniform(118, 122)

    if is_outage:
        base_usage = 0.0
        voltage = 0.0
        power_factor = 0.0
    else:
        power_factor = round(random.uniform(0.92, 0.99), 3)

    return {
        'METER_ID': meter_info.get('meter_id', f'MTR-{uuid.uuid4().hex[:8].upper()}'),
        'TRANSFORMER_ID': meter_info.get('transformer_id'),
        'CIRCUIT_ID': meter_info.get('circuit_id'),
        'SUBSTATION_ID': meter_info.get('substation_id'),
        'READING_TIMESTAMP': datetime.now(),
        'USAGE_KWH': round(base_usage * usage_multiplier, 4),
        'VOLTAGE': round(voltage, 2),
        'POWER_FACTOR': power_factor,
        'TEMPERATURE_C': round(random.uniform(15, 35), 1),
        'SERVICE_AREA': service_area,
        'CUSTOMER_SEGMENT': segment,
        'LATITUDE': meter_info.get('latitude'),
        'LONGITUDE': meter_info.get('longitude'),
        'IS_OUTAGE': is_outage,
        'DATA_QUALITY': data_quality,
        'PRODUCTION_MATCHED': meter_info.get('production_matched', False),
        'EMISSION_PATTERN': emission_pattern,
        'CREATED_AT': datetime.now(),
    }


# ============================================================================
# GENERATOR: Itron Grid Planning (8,760-hr Power Flow)
# ============================================================================

_LOAD_SHAPES = {
    'RESIDENTIAL': [
        0.35, 0.30, 0.28, 0.27, 0.28, 0.32,
        0.42, 0.55, 0.50, 0.42, 0.40, 0.42,
        0.45, 0.50, 0.65, 0.80, 0.95, 1.00,
        0.90, 0.75, 0.60, 0.50, 0.45, 0.38,
    ],
    'COMMERCIAL': [
        0.25, 0.22, 0.20, 0.20, 0.22, 0.28,
        0.45, 0.70, 0.85, 0.92, 0.95, 0.98,
        1.00, 0.98, 0.95, 0.90, 0.82, 0.65,
        0.45, 0.35, 0.30, 0.28, 0.26, 0.25,
    ],
    'INDUSTRIAL': [
        0.70, 0.70, 0.70, 0.70, 0.72, 0.78,
        0.90, 0.95, 1.00, 1.00, 1.00, 0.98,
        0.95, 1.00, 1.00, 1.00, 0.98, 0.90,
        0.80, 0.75, 0.72, 0.70, 0.70, 0.70,
    ],
}


def generate_itron_grid_planning_row(
    meter_info: dict,
    service_area: str,
    grid_state: 'GridTopologyState | None' = None,
) -> dict:
    """Generate one row in Itron Grid Planning 8,760-hr power flow format."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    day_of_year = now.timetuple().tm_yday
    hour_of_year = (day_of_year - 1) * 24 + hour

    segment = meter_info.get('customer_segment', 'RESIDENTIAL')
    shape = _LOAD_SHAPES.get(segment, _LOAD_SHAPES['RESIDENTIAL'])
    load_factor = shape[hour]

    if segment == 'INDUSTRIAL':
        base_kw = random.uniform(200, 800)
        customer_count = random.randint(1, 5)
    elif segment == 'COMMERCIAL':
        base_kw = random.uniform(50, 250)
        customer_count = random.randint(2, 20)
    else:
        base_kw = random.uniform(8, 40)
        customer_count = random.randint(5, 15)

    load_kw = base_kw * load_factor * random.uniform(0.90, 1.10)

    pf_base = {'RESIDENTIAL': 0.95, 'COMMERCIAL': 0.92, 'INDUSTRIAL': 0.88}.get(segment, 0.95)
    power_factor = round(pf_base + random.uniform(-0.03, 0.02), 3)
    power_factor = max(0.80, min(1.0, power_factor))

    load_kvar = load_kw * math.tan(math.acos(power_factor))

    if grid_state is not None:
        topo = grid_state.get_meter_state(meter_info)
        if topo['is_outage']:
            voltage_pu = 0.0
            load_kw = 0.0
            load_kvar = 0.0
        else:
            voltage_pu = 1.0 + (topo['voltage_offset'] / 120.0) + random.uniform(-0.002, 0.002)
    else:
        voltage_pu = 1.0 + random.uniform(-0.02, 0.02)

    phase = random.choice(['A', 'B', 'C'])

    return {
        'NODE_ID': meter_info.get('transformer_id', f'NODE-{uuid.uuid4().hex[:8].upper()}'),
        'TRANSFORMER_ID': meter_info.get('transformer_id'),
        'FEEDER_ID': meter_info.get('circuit_id'),
        'SUBSTATION_ID': meter_info.get('substation_id'),
        'HOUR_OF_YEAR': hour_of_year,
        'TIMESTAMP_UTC': now,
        'LOAD_KW': round(load_kw, 2),
        'LOAD_KVAR': round(load_kvar, 2),
        'POWER_FACTOR': power_factor,
        'VOLTAGE_PU': round(voltage_pu, 4),
        'PHASE': phase,
        'CUSTOMER_COUNT': customer_count,
        'CUSTOMER_CLASS': segment,
        'LATITUDE': meter_info.get('latitude'),
        'LONGITUDE': meter_info.get('longitude'),
    }


# ============================================================================
# GENERATOR: SymphonyAI IRIS Foundry (Asset Health / Predictive Maintenance)
# ============================================================================

_ASSET_TYPES = ['TRANSFORMER', 'RECLOSER', 'CAPACITOR_BANK', 'REGULATOR', 'SWITCHGEAR']
_FAILURE_MODES = [
    'WINDING_DEGRADATION', 'BUSHING_FAILURE', 'OIL_CONTAMINATION',
    'THERMAL_OVERLOAD', 'INSULATION_BREAKDOWN', 'CONTACT_WEAR',
    'CORROSION', 'NONE',
]


def generate_symphony_iris_row(
    meter_info: dict,
    service_area: str,
    grid_state: 'GridTopologyState | None' = None,
) -> dict:
    """Generate one row in SymphonyAI IRIS Foundry asset health format."""
    now = datetime.now(timezone.utc)
    asset_type = random.choice(_ASSET_TYPES)
    asset_id = f"{asset_type[:3]}-{meter_info.get('transformer_id', uuid.uuid4().hex[:8]).upper()}"

    health_index = round(random.gauss(78, 12), 1)
    health_index = max(0, min(100, health_index))

    temp_c = round(random.gauss(55, 15), 1)
    vibration = round(random.uniform(0.5, 8.0), 2)
    oil_ppm = round(random.uniform(5, 120), 1)

    load_pct = round(random.uniform(20, 95), 1)

    risk_base = (100 - health_index) / 100
    if temp_c > 80:
        risk_base += 0.15
    if oil_ppm > 80:
        risk_base += 0.10
    risk_score = round(min(1.0, max(0, risk_base + random.uniform(-0.05, 0.05))), 3)

    maintenance_flag = risk_score > 0.6
    remaining_life = max(0, int((1 - risk_score) * random.uniform(800, 3000)))
    failure_mode = 'NONE' if risk_score < 0.4 else random.choice(_FAILURE_MODES[:-1])

    if grid_state is not None:
        topo = grid_state.get_meter_state(meter_info)
        if topo['is_outage']:
            risk_score = min(1.0, risk_score + 0.3)
            maintenance_flag = True
            health_index = max(0, health_index - 20)

    return {
        'ASSET_ID': asset_id,
        'ASSET_TYPE': asset_type,
        'SUBSTATION_ID': meter_info.get('substation_id'),
        'TIMESTAMP_UTC': now,
        'HEALTH_INDEX': health_index,
        'TEMPERATURE_C': temp_c,
        'VIBRATION_MM_S': vibration,
        'OIL_QUALITY_PPM': oil_ppm,
        'LOAD_PERCENT': load_pct,
        'RISK_SCORE': risk_score,
        'MAINTENANCE_FLAG': maintenance_flag,
        'REMAINING_LIFE_DAYS': remaining_life,
        'FAILURE_MODE': failure_mode,
        'LATITUDE': meter_info.get('latitude'),
        'LONGITUDE': meter_info.get('longitude'),
    }


# ============================================================================
# GENERATOR: CARTO Spatial Analytics (Geospatial Grid Intelligence)
# ============================================================================

def generate_carto_spatial_row(
    meter_info: dict,
    service_area: str,
    grid_state: 'GridTopologyState | None' = None,
) -> dict:
    """Generate one row in CARTO spatial analytics format."""
    now = datetime.now(timezone.utc)
    lat = meter_info.get('latitude', 29.76)
    lon = meter_info.get('longitude', -95.37)

    h3_index = f"89{abs(hash((lat, lon))) % 10**12:012d}"
    grid_cell_id = f"CELL-{h3_index[-8:]}"

    load_density = round(random.uniform(50, 2000), 1)
    customer_density = random.randint(10, 500)

    outage_count = 0
    if grid_state is not None:
        topo = grid_state.get_meter_state(meter_info)
        if topo['is_outage']:
            outage_count = random.randint(1, 20)
            load_density *= 0.3
            load_density = round(load_density, 1)

    vegetation_risk = round(random.betavariate(2, 5), 3)
    weather_severity = round(random.betavariate(2, 8) * 10, 1)

    geojson = json.dumps({
        "type": "Point",
        "coordinates": [round(lon, 6), round(lat, 6)],
    })

    return {
        'GRID_CELL_ID': grid_cell_id,
        'H3_INDEX': h3_index,
        'SUBSTATION_ID': meter_info.get('substation_id'),
        'FEEDER_ID': meter_info.get('circuit_id'),
        'TIMESTAMP_UTC': now,
        'LOAD_DENSITY_KW_KM2': load_density,
        'CUSTOMER_DENSITY': customer_density,
        'OUTAGE_COUNT': outage_count,
        'VEGETATION_RISK_SCORE': vegetation_risk,
        'WEATHER_SEVERITY': weather_severity,
        'LATITUDE': lat,
        'LONGITUDE': lon,
        'GEOJSON': geojson,
    }


# ============================================================================
# GENERATOR: Siemens Industrial Edge (SCADA / Industrial Telemetry)
# ============================================================================

_SCADA_DEVICES = ['RTU', 'IED', 'PLC', 'RELAY', 'METER_GATEWAY']
_SCADA_TAGS = {
    'RTU':           [('BUS_VOLTAGE_KV', 'kV'), ('FREQUENCY_HZ', 'Hz'), ('BREAKER_STATUS', 'bool')],
    'IED':           [('CURRENT_A', 'A'), ('FAULT_CURRENT_KA', 'kA'), ('TRIP_COUNT', 'count')],
    'PLC':           [('TAP_POSITION', 'step'), ('REACTIVE_POWER_KVAR', 'kVAR'), ('ACTIVE_POWER_KW', 'kW')],
    'RELAY':         [('PICKUP_CURRENT_A', 'A'), ('OPERATE_TIME_MS', 'ms'), ('RECLOSE_COUNT', 'count')],
    'METER_GATEWAY': [('THROUGHPUT_MSGS_S', 'msg/s'), ('QUEUE_DEPTH', 'count'), ('LATENCY_MS', 'ms')],
}
_PROTOCOLS = ['DNP3', 'IEC61850', 'MODBUS_TCP', 'OPC_UA']


def generate_siemens_edge_row(
    meter_info: dict,
    service_area: str,
    grid_state: 'GridTopologyState | None' = None,
) -> dict:
    """Generate one row in Siemens Industrial Edge SCADA telemetry format."""
    now = datetime.now(timezone.utc)
    device_type = random.choice(_SCADA_DEVICES)
    device_id = f"{device_type}-{meter_info.get('substation_id', uuid.uuid4().hex[:6]).upper()}"

    tag_name, unit = random.choice(_SCADA_TAGS[device_type])

    if unit == 'kV':
        tag_value = round(random.gauss(13.2, 0.5), 3)
    elif unit == 'Hz':
        tag_value = round(random.gauss(60.0, 0.02), 4)
    elif unit == 'bool':
        tag_value = float(random.choice([0, 1]))
    elif unit == 'A':
        tag_value = round(random.uniform(10, 600), 1)
    elif unit == 'kA':
        tag_value = round(random.uniform(0.5, 25), 2)
    elif unit == 'count':
        tag_value = float(random.randint(0, 50))
    elif unit == 'step':
        tag_value = float(random.randint(-8, 8))
    elif unit in ('kVAR', 'kW'):
        tag_value = round(random.uniform(100, 5000), 1)
    elif unit == 'ms':
        tag_value = round(random.uniform(1, 200), 1)
    elif unit == 'msg/s':
        tag_value = round(random.uniform(50, 5000), 0)
    else:
        tag_value = round(random.uniform(0, 100), 2)

    quality_code = 0
    alarm_active = False
    alarm_priority = 0

    if grid_state is not None:
        topo = grid_state.get_meter_state(meter_info)
        if topo['is_outage']:
            quality_code = 2
            alarm_active = True
            alarm_priority = random.choice([1, 2])
            if unit in ('kV', 'kW', 'kVAR', 'A'):
                tag_value = 0.0
        elif topo.get('voltage_offset', 0) < -2:
            quality_code = 1
            alarm_active = True
            alarm_priority = 3

    protocol = random.choice(_PROTOCOLS)
    sample_rate = random.choice([1, 2, 4, 10, 30, 60])

    return {
        'DEVICE_ID': device_id,
        'DEVICE_TYPE': device_type,
        'SUBSTATION_ID': meter_info.get('substation_id'),
        'TIMESTAMP_UTC': now,
        'TAG_NAME': tag_name,
        'TAG_VALUE': tag_value,
        'UNIT': unit,
        'QUALITY_CODE': quality_code,
        'ALARM_ACTIVE': alarm_active,
        'ALARM_PRIORITY': alarm_priority,
        'PROTOCOL': protocol,
        'SAMPLE_RATE_HZ': sample_rate,
        'LATITUDE': meter_info.get('latitude'),
        'LONGITUDE': meter_info.get('longitude'),
    }
