from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

from .config import DATA_DIR, SITE_DIR, load_sources_config
from .dedupe import HistoryStore, dedupe_selected_batch, remove_exact_duplicates, remove_history_duplicates
from .editorial import filter_by_freshness, select_best_articles
from .fetchers import enrich_missing_summaries, fetch_all_sources
from .normalize import normalize_candidates
from .scoring import limit_topic_repetition, score_articles
from .site import build_site
from .utils import append_jsonl, load_json, save_json, utc_now


LOGGER = logging.getLogger("veille.pipeline")


def run_pipeline(config_path: Path | None = None) -> dict[str, Any]:
    config = load_sources_config(config_path)
    settings = config["settings"]
    history_path = Path(settings["history_file"])
    state_path = Path(settings["state_file"])
    site_dir = Path(settings["site_dir"])
    history = HistoryStore(history_path)
    now = utc_now()
    run_date = now.date().isoformat()

    raw_candidates = fetch_all_sources(config)
    normalized = normalize_candidates(raw_candidates)
    normalized = remove_exact_duplicates(normalized)
    normalized = filter_by_freshness(normalized, settings["freshness_hours"])
    normalized = enrich_missing_summaries(
        normalized,
        timeout_seconds=settings["timeout_seconds"],
        limit=min(12, max(4, settings["max_articles_per_day"] * 2)),
    )
    normalized = remove_exact_duplicates(normalized)

    unseen, dropped_history = remove_history_duplicates(normalized, history)
    scored = score_articles(unseen)
    scored = dedupe_selected_batch(scored)
    scored = limit_topic_repetition(scored, max_per_signature=1)
    selected = select_best_articles(
        scored,
        min_score=settings["min_score"],
        max_articles_per_day=settings["max_articles_per_day"],
        top_n=settings["top_n"],
    )

    article_payloads = [article.to_json() for article in selected]
    build_site(run_date, article_payloads, site_dir=site_dir, base_url=str(settings.get("base_url", "")))

    new_records = history.new_records(run_date, selected)
    append_jsonl(history_path, [record.to_json() for record in new_records])

    state = load_json(state_path, default={})
    state.update(
        {
            "last_run_at": now.isoformat(),
            "last_run_date": run_date,
            "fetched_candidates": len(raw_candidates),
            "normalized_candidates": len(normalized),
            "dropped_as_already_seen": len(dropped_history),
            "selected_count": len(selected),
            "history_count": len(history) + len(new_records),
            "source_count": len([s for s in config["sources"] if s.get("enabled", True)]),
        }
    )
    save_json(state_path, state)

    LOGGER.info(
        "Run complete | fetched=%s normalized=%s dropped_seen=%s selected=%s history=%s",
        len(raw_candidates),
        len(normalized),
        len(dropped_history),
        len(selected),
        len(history) + len(new_records),
    )

    return {
        "run_date": run_date,
        "selected": article_payloads,
        "state": state,
        "dropped_history": [
            {"title": article.clean_title, "reason": reason, "url": article.canonical_url}
            for article, reason in dropped_history[:100]
        ],
    }
