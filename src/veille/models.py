from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(slots=True)
class CandidateArticle:
    source_id: str
    source_name: str
    source_type: str
    source_url: str
    source_category: str
    source_quality: int
    title: str
    url: str
    published_at: Optional[datetime]
    summary: str = ""
    author: str = ""
    language: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedArticle:
    source_id: str
    source_name: str
    source_type: str
    source_url: str
    source_category: str
    source_quality: int
    title: str
    clean_title: str
    normalized_title: str
    title_signature: str
    url: str
    canonical_url: str
    domain: str
    published_at: Optional[datetime]
    published_date: Optional[str]
    summary: str
    summary_fingerprint: str
    content_fingerprint: str
    author: str = ""
    language: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SelectedArticle:
    source_id: str
    source_name: str
    source_category: str
    title: str
    clean_title: str
    url: str
    canonical_url: str
    domain: str
    published_at: Optional[datetime]
    published_date: Optional[str]
    summary: str
    category: str
    score: int
    score_breakdown: Dict[str, int]
    why_selected: str
    impacts: str
    title_signature: str
    content_fingerprint: str
    normalized_title: str

    def to_json(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["published_at"] = self.published_at.isoformat() if self.published_at else None
        return payload


@dataclass(slots=True)
class SeenRecord:
    first_seen_on: str
    source_id: str
    source_name: str
    title: str
    normalized_title: str
    title_signature: str
    canonical_url: str
    content_fingerprint: str
    published_date: Optional[str]
    final_url_domain: str

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)
