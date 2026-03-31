from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

from rapidfuzz import fuzz

from .models import NormalizedArticle, SeenRecord, SelectedArticle
from .utils import read_jsonl


LOGGER = logging.getLogger("veille.dedupe")


@dataclass(slots=True)
class DuplicateCheckResult:
    is_duplicate: bool
    reason: str = ""
    matched_title: str = ""


def _signature_overlap(left: str, right: str) -> int:
    return len(set(left.split()) & set(right.split()))


class HistoryStore:
    def __init__(self, history_path: Path) -> None:
        self.history_path = history_path
        self.rows = [SeenRecord(**row) for row in read_jsonl(history_path)]
        self.canonical_urls = {row.canonical_url for row in self.rows if row.canonical_url}
        self.normalized_titles = {row.normalized_title for row in self.rows if row.normalized_title}
        self.content_fingerprints = {row.content_fingerprint for row in self.rows if row.content_fingerprint}
        self.signature_index: dict[str, list[SeenRecord]] = defaultdict(list)
        self.prefix_index: dict[str, list[SeenRecord]] = defaultdict(list)
        for row in self.rows:
            if row.title_signature:
                for token in row.title_signature.split():
                    self.signature_index[token].append(row)
            if row.normalized_title:
                self.prefix_index[row.normalized_title[:18]].append(row)

    def __len__(self) -> int:
        return len(self.rows)

    def has_seen(self, article: NormalizedArticle) -> DuplicateCheckResult:
        if article.canonical_url and article.canonical_url in self.canonical_urls:
            return DuplicateCheckResult(True, "exact_canonical_url")
        if article.content_fingerprint and article.content_fingerprint in self.content_fingerprints:
            return DuplicateCheckResult(True, "exact_content_fingerprint")
        if article.normalized_title and article.normalized_title in self.normalized_titles:
            return DuplicateCheckResult(True, "exact_normalized_title")

        for candidate in self._history_candidates(article):
            ratio = fuzz.token_set_ratio(article.normalized_title, candidate.normalized_title)
            partial = fuzz.partial_ratio(article.normalized_title, candidate.normalized_title)
            overlap = _signature_overlap(article.title_signature, candidate.title_signature)
            if ratio >= 96:
                return DuplicateCheckResult(True, f"fuzzy_title_ratio_{ratio}", candidate.title)
            if ratio >= 92 and overlap >= 4:
                return DuplicateCheckResult(True, f"fuzzy_title_signature_{ratio}", candidate.title)
            if ratio >= 88 and partial >= 90 and overlap >= 4:
                return DuplicateCheckResult(True, f"fuzzy_title_overlap_{ratio}", candidate.title)
        return DuplicateCheckResult(False)

    def _history_candidates(self, article: NormalizedArticle) -> list[SeenRecord]:
        candidates: list[SeenRecord] = []
        seen_ids: set[tuple[str, str]] = set()
        for token in article.title_signature.split():
            for row in self.signature_index.get(token, []):
                key = (row.canonical_url, row.normalized_title)
                if key not in seen_ids:
                    candidates.append(row)
                    seen_ids.add(key)
        for row in self.prefix_index.get(article.normalized_title[:18], []):
            key = (row.canonical_url, row.normalized_title)
            if key not in seen_ids:
                candidates.append(row)
                seen_ids.add(key)
        return candidates

    def new_records(self, run_date: str, articles: Sequence[SelectedArticle]) -> list[SeenRecord]:
        records: list[SeenRecord] = []
        for article in articles:
            records.append(
                SeenRecord(
                    first_seen_on=run_date,
                    source_id=article.source_id,
                    source_name=article.source_name,
                    title=article.clean_title,
                    normalized_title=article.normalized_title,
                    title_signature=article.title_signature,
                    canonical_url=article.canonical_url,
                    content_fingerprint=article.content_fingerprint,
                    published_date=article.published_date,
                    final_url_domain=article.domain,
                )
            )
        return records


def remove_history_duplicates(
    articles: Iterable[NormalizedArticle], history: HistoryStore
) -> tuple[list[NormalizedArticle], list[tuple[NormalizedArticle, str]]]:
    kept: list[NormalizedArticle] = []
    dropped: list[tuple[NormalizedArticle, str]] = []
    for article in articles:
        match = history.has_seen(article)
        if match.is_duplicate:
            dropped.append((article, match.reason))
        else:
            kept.append(article)
    return kept, dropped


def remove_exact_duplicates(articles: Iterable[NormalizedArticle]) -> list[NormalizedArticle]:
    kept: list[NormalizedArticle] = []
    seen_urls: set[str] = set()
    seen_fingerprints: set[str] = set()
    seen_titles: set[str] = set()
    for article in articles:
        if article.canonical_url in seen_urls:
            continue
        if article.content_fingerprint in seen_fingerprints:
            continue
        if article.normalized_title in seen_titles:
            continue
        seen_urls.add(article.canonical_url)
        seen_fingerprints.add(article.content_fingerprint)
        seen_titles.add(article.normalized_title)
        kept.append(article)
    return kept


def dedupe_selected_batch(articles: Sequence[SelectedArticle]) -> list[SelectedArticle]:
    winners: list[SelectedArticle] = []
    for article in sorted(articles, key=lambda a: (a.score, len(a.summary)), reverse=True):
        duplicate_of: Optional[SelectedArticle] = None
        for winner in winners:
            if article.canonical_url == winner.canonical_url:
                duplicate_of = winner
                break
            if article.content_fingerprint == winner.content_fingerprint:
                duplicate_of = winner
                break
            ratio = fuzz.token_set_ratio(article.normalized_title, winner.normalized_title)
            partial = fuzz.partial_ratio(article.normalized_title, winner.normalized_title)
            overlap = _signature_overlap(article.title_signature, winner.title_signature)
            if ratio >= 96 or (ratio >= 92 and overlap >= 4) or (ratio >= 88 and partial >= 90 and overlap >= 4):
                duplicate_of = winner
                break
        if duplicate_of is None:
            winners.append(article)
        else:
            LOGGER.info(
                "Dropped near-duplicate in batch: '%s' ~ '%s'",
                article.clean_title,
                duplicate_of.clean_title,
            )
    return sorted(winners, key=lambda a: a.score, reverse=True)
