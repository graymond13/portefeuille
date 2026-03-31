from __future__ import annotations

import logging
from collections.abc import Iterable
from urllib.parse import urlparse

from .models import CandidateArticle, NormalizedArticle
from .utils import clean_text, normalize_title_for_match, normalize_url, stable_hash, title_signature


LOGGER = logging.getLogger("veille.normalize")


def normalize_candidates(candidates: Iterable[CandidateArticle]) -> list[NormalizedArticle]:
    normalized: list[NormalizedArticle] = []
    for item in candidates:
        canonical_url = normalize_url(item.url, base_url=item.source_url)
        domain = urlparse(canonical_url).netloc
        clean_title = clean_text(item.title)
        normalized_title = normalize_title_for_match(clean_title)
        signature = title_signature(clean_title)
        summary = clean_text(item.summary)
        summary_fingerprint = stable_hash(summary[:400]) if summary else ""
        content_fingerprint = stable_hash(normalized_title, signature, summary[:400])
        published_date = item.published_at.date().isoformat() if item.published_at else None

        if not clean_title or not canonical_url:
            LOGGER.debug("Dropping malformed candidate: %s", item.url)
            continue

        normalized.append(
            NormalizedArticle(
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                source_url=item.source_url,
                source_category=item.source_category,
                source_quality=item.source_quality,
                title=item.title,
                clean_title=clean_title,
                normalized_title=normalized_title,
                title_signature=signature,
                url=item.url,
                canonical_url=canonical_url,
                domain=domain,
                published_at=item.published_at,
                published_date=published_date,
                summary=summary,
                summary_fingerprint=summary_fingerprint,
                content_fingerprint=content_fingerprint,
                author=item.author,
                language=item.language,
                raw=item.raw,
            )
        )
    return normalized
