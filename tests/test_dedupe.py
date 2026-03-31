from datetime import datetime, timezone
from pathlib import Path

from src.veille.dedupe import HistoryStore, remove_history_duplicates
from src.veille.models import NormalizedArticle
from src.veille.utils import append_jsonl


def make_article(
    title: str,
    url: str,
    fp: str = "abc123",
    normalized_title: str | None = None,
    signature: str = "capital requirement",
) -> NormalizedArticle:
    dt = datetime(2026, 3, 31, 8, 0, tzinfo=timezone.utc)
    return NormalizedArticle(
        source_id="test",
        source_name="Test",
        source_type="rss",
        source_url="https://example.com/feed",
        source_category="finance_markets",
        source_quality=80,
        title=title,
        clean_title=title,
        normalized_title=normalized_title or title.lower(),
        title_signature=signature,
        url=url,
        canonical_url=url,
        domain="example.com",
        published_at=dt,
        published_date=dt.date().isoformat(),
        summary="Summary",
        summary_fingerprint="sum",
        content_fingerprint=fp,
        author="",
        language="en",
        raw={},
    )


def test_history_duplicate_by_canonical_url(tmp_path: Path):
    history_path = tmp_path / "seen_articles.jsonl"
    append_jsonl(
        history_path,
        [
            {
                "first_seen_on": "2026-03-30",
                "source_id": "eba",
                "source_name": "EBA",
                "title": "Capital requirement update",
                "normalized_title": "capital requirement update",
                "title_signature": "capital requirement",
                "canonical_url": "https://example.com/article-1",
                "content_fingerprint": "fp-1",
                "published_date": "2026-03-30",
                "final_url_domain": "example.com",
            }
        ],
    )
    history = HistoryStore(history_path)
    article = make_article("Capital requirement update", "https://example.com/article-1", fp="new")
    kept, dropped = remove_history_duplicates([article], history)
    assert kept == []
    assert dropped[0][1] == "exact_canonical_url"


def test_history_duplicate_by_fuzzy_title(tmp_path: Path):
    history_path = tmp_path / "seen_articles.jsonl"
    append_jsonl(
        history_path,
        [
            {
                "first_seen_on": "2026-03-30",
                "source_id": "eba",
                "source_name": "EBA",
                "title": "EU capital requirement reforms for banks",
                "normalized_title": "eu capital requirement reforms for banks",
                "title_signature": "banks capital eu reforms requirement",
                "canonical_url": "https://example.com/article-2",
                "content_fingerprint": "fp-2",
                "published_date": "2026-03-30",
                "final_url_domain": "example.com",
            }
        ],
    )
    history = HistoryStore(history_path)
    article = make_article(
        "Banks face EU capital requirement reform",
        "https://another.example.org/story",
        fp="fp-3",
        normalized_title="banks face eu capital requirement reform",
        signature="banks capital eu reform requirement",
    )
    kept, dropped = remove_history_duplicates([article], history)
    assert kept == []
    assert dropped[0][1].startswith("fuzzy_title")
