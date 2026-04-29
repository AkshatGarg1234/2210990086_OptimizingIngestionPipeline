"""
ETL Module - Shared Transformation Logic
=========================================
Reusable ETL utilities used by both the consumer service
and the Airflow DAG plugin.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def normalize_timestamp(ts: Any) -> Optional[str]:
    """
    Normalize various timestamp formats to ISO 8601.
    Accepts: string, epoch int, epoch float.
    """
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    if isinstance(ts, str):
        return ts  # Already ISO format from producer
    return None


def sanitize_string(value: Any, max_len: int = 255) -> Optional[str]:
    """Strip and truncate string fields to prevent DB errors."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned[:max_len]


def generate_stable_id(data: Dict) -> str:
    """Generate a deterministic ID from event data for idempotency."""
    serialized = json.dumps(data, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def validate_event_schema(event: Dict, required_fields: list) -> bool:
    """Validate that an event has all required top-level fields."""
    for field in required_fields:
        if field not in event or event[field] is None:
            return False
    return True


def compute_anomaly_score(value: float, mean: float, std: float) -> float:
    """Simple z-score based anomaly detection."""
    if std == 0:
        return 0.0
    z = abs((value - mean) / std)
    return min(z / 5.0, 1.0)  # Normalize to [0, 1]
