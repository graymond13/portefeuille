from __future__ import annotations

import logging
from datetime import timedelta
from typing import Iterable

from .models import NormalizedArticle, SelectedArticle
from .utils import utc_now


LOGGER = logging.getLogger("veille.editorial")


def filter_by_freshness(articles: Iterable[NormalizedArticle], max_age_hours: int) -> list[NormalizedArticle]:
    now = utc_now()
    kept: list[NormalizedArticle] = []
    for article in articles:
        if article.published_at is None:
            kept.append(article)
            continue
        age = now - article.published_at
        if age <= timedelta(hours=max_age_hours):
            kept.append(article)
    return kept


def select_best_articles(
    articles: list[SelectedArticle], min_score: int, max_articles_per_day: int, top_n: int
) -> list[SelectedArticle]:
    filtered = [article for article in articles if article.score >= min_score]
    if not filtered:
        LOGGER.warning("No article reached the minimum score (%s)", min_score)
        return []

    top_bucket = filtered[:top_n]
    rest = filtered[top_n:]

    selected = top_bucket + rest[: max(0, max_articles_per_day - len(top_bucket))]
    return selected[:max_articles_per_day]
