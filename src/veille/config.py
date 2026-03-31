from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
SITE_DIR = ROOT_DIR / "site"
TESTS_DIR = ROOT_DIR / "tests"


DEFAULT_SETTINGS: dict[str, Any] = {
    "freshness_hours": 72,
    "max_articles_per_day": 20,
    "top_n": 5,
    "min_score": 45,
    "timeout_seconds": 20,
    "history_file": "data/seen_articles.jsonl",
    "state_file": "data/state.json",
    "latest_json": "site/data/latest.json",
    "rss_output": "site/feed.xml",
    "site_dir": "site",
    "timezone": "UTC",
}


def load_sources_config(path: Path | None = None) -> dict[str, Any]:
    path = path or CONFIG_DIR / "sources.yml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    settings = dict(DEFAULT_SETTINGS)
    settings.update(payload.get("settings", {}))
    payload["settings"] = settings
    payload.setdefault("sources", [])
    return payload
