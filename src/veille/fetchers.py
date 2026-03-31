from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Iterable, Optional
from urllib.parse import urljoin

import feedparser
import requests
from requests import RequestException
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import CandidateArticle, NormalizedArticle
from .utils import (
    DEFAULT_USER_AGENT,
    NonRetryableError,
    RetryableError,
    call_with_retry,
    clean_text,
    parse_datetime,
)


LOGGER = logging.getLogger("veille.fetchers")


def build_session(timeout_seconds: int = 20) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
        }
    )
    session.request_timeout = timeout_seconds  # type: ignore[attr-defined]
    return session


def fetch_all_sources(config: dict[str, Any]) -> list[CandidateArticle]:
    settings = config["settings"]
    session = build_session(timeout_seconds=settings["timeout_seconds"])
    all_items: list[CandidateArticle] = []

    for source in config["sources"]:
        if not source.get("enabled", True):
            LOGGER.info("Source disabled: %s", source.get("id"))
            continue

        source_id = source["id"]
        try:
            if source["kind"] == "rss":
                items = call_with_retry(lambda: fetch_rss_source(source, session))
            elif source["kind"] == "scrape":
                items = call_with_retry(lambda: fetch_scrape_source(source, session))
            else:
                raise NonRetryableError(f"Unsupported source kind: {source['kind']}")
            LOGGER.info("Fetched %s items from %s", len(items), source_id)
            all_items.extend(items)
        except Exception as exc:
            LOGGER.error("Source failed: %s | %s", source_id, exc)

    return all_items


def _http_get(session: requests.Session, url: str) -> requests.Response:
    timeout = getattr(session, "request_timeout", 20)
    try:
        response = session.get(url, timeout=timeout)
    except RequestException as exc:
        raise RetryableError(f"Network error for {url}: {exc}") from exc
    if response.status_code >= 500:
        raise RetryableError(f"HTTP {response.status_code} for {url}")
    if response.status_code >= 400:
        raise NonRetryableError(f"HTTP {response.status_code} for {url}")
    return response


def fetch_rss_source(source: dict[str, Any], session: requests.Session) -> list[CandidateArticle]:
    response = _http_get(session, source["url"])
    parsed = feedparser.parse(response.content)
    if parsed.bozo and not parsed.entries:
        raise RetryableError(f"Invalid RSS/Atom feed: {source['url']}")

    entries: list[CandidateArticle] = []
    for entry in parsed.entries:
        title = clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "")
        summary = clean_text(
            getattr(entry, "summary", "")
            or getattr(entry, "description", "")
            or getattr(entry, "subtitle", "")
        )
        published_at = parse_datetime(
            getattr(entry, "published", None)
            or getattr(entry, "updated", None)
            or getattr(entry, "created", None)
        )
        author = clean_text(getattr(entry, "author", ""))
        language = clean_text(getattr(entry, "language", ""))

        if not title or not link:
            continue

        entries.append(
            CandidateArticle(
                source_id=source["id"],
                source_name=source["name"],
                source_type="rss",
                source_url=source["url"],
                source_category=source["category"],
                source_quality=int(source.get("quality", 50)),
                title=title,
                url=link,
                published_at=published_at,
                summary=summary,
                author=author,
                language=language,
                raw={"entry": dict(entry)},
            )
        )
    return entries


def _select_text(node: Any, selectors: list[str]) -> str:
    for selector in selectors:
        found = node.select_one(selector)
        if found:
            return clean_text(found.get_text(" ", strip=True))
    return ""


def _select_attr(node: Any, selectors: list[str], attr: str) -> str:
    for selector in selectors:
        found = node.select_one(selector)
        if found and found.get(attr):
            return clean_text(found.get(attr))
    return ""


def _iter_scraped_items(soup: BeautifulSoup, source: dict[str, Any]) -> Iterable[Any]:
    selectors = source.get("item_selectors") or []
    for selector in selectors:
        nodes = soup.select(selector)
        if nodes:
            return nodes
    return []


def fetch_scrape_source(source: dict[str, Any], session: requests.Session) -> list[CandidateArticle]:
    response = _http_get(session, source["url"])
    soup = BeautifulSoup(response.text, "html.parser")

    items: list[CandidateArticle] = []
    for node in _iter_scraped_items(soup, source):
        title = _select_text(node, source.get("title_selectors", []))
        link = _select_attr(node, source.get("link_selectors", []), source.get("link_attr", "href"))
        summary = _select_text(node, source.get("summary_selectors", []))
        published_raw = _select_text(node, source.get("date_selectors", []))
        published_at = parse_datetime(published_raw)

        if not title:
            link_text = _select_text(node, source.get("link_selectors", []))
            title = link_text
        if not title or not link:
            continue

        absolute_link = urljoin(source["url"], link)

        required_substrings = source.get("require_url_contains") or []
        if required_substrings and not any(fragment in absolute_link for fragment in required_substrings):
            continue

        items.append(
            CandidateArticle(
                source_id=source["id"],
                source_name=source["name"],
                source_type="scrape",
                source_url=source["url"],
                source_category=source["category"],
                source_quality=int(source.get("quality", 50)),
                title=title,
                url=absolute_link,
                published_at=published_at,
                summary=summary,
                author="",
                language=str(source.get("language", "")),
                raw={"published_raw": published_raw},
            )
        )

    if not items:
        LOGGER.warning("No items extracted from scrape source %s", source["id"])
    return items


def enrich_missing_summaries(articles: list[NormalizedArticle], timeout_seconds: int = 20, limit: int = 12) -> list[NormalizedArticle]:
    session = build_session(timeout_seconds=timeout_seconds)
    enriched: list[NormalizedArticle] = []
    count = 0
    for article in articles:
        if article.summary and len(article.summary.split()) >= 12:
            enriched.append(article)
            continue
        if count >= limit:
            enriched.append(article)
            continue
        count += 1
        try:
            response = _http_get(session, article.canonical_url or article.url)
            soup = BeautifulSoup(response.text, "html.parser")
            description = ""
            for selector, attr in [
                ('meta[name="description"]', 'content'),
                ('meta[property="og:description"]', 'content'),
                ('meta[name="twitter:description"]', 'content'),
            ]:
                node = soup.select_one(selector)
                if node and node.get(attr):
                    description = clean_text(node.get(attr))
                    break
            if not description:
                for paragraph in soup.select('article p, main p, .content p')[:3]:
                    text = clean_text(paragraph.get_text(' ', strip=True))
                    if len(text.split()) >= 12:
                        description = text
                        break
            if description:
                enriched.append(replace(article, summary=description))
            else:
                enriched.append(article)
        except Exception as exc:
            LOGGER.debug('Summary enrichment failed for %s: %s', article.canonical_url, exc)
            enriched.append(article)
    return enriched
