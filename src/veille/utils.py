from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, TypeVar
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

from dateutil import parser as date_parser


LOGGER = logging.getLogger("veille")
T = TypeVar("T")

DEFAULT_USER_AGENT = (
    "VeilleBot/1.0 (+https://github.com/your-org/your-repo; contact: change-me@example.com)"
)
TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_name",
    "utm_cid",
    "utm_reader",
    "utm_viz_id",
    "utm_pubreferrer",
    "utm_swu",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ocid",
    "cmpid",
    "cmp",
    "ref",
    "refsrc",
    "xtor",
    "s",
    "guccounter",
    "guce_referrer",
    "guce_referrer_sig",
    "taid",
    "rss",
    "output",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "au",
    "aux",
    "avec",
    "bank",
    "banking",
    "banque",
    "dans",
    "de",
    "des",
    "du",
    "en",
    "et",
    "finance",
    "financial",
    "for",
    "from",
    "in",
    "insurance",
    "insurer",
    "insurers",
    "l",
    "la",
    "le",
    "les",
    "markets",
    "of",
    "on",
    "pour",
    "reuters",
    "sur",
    "the",
    "to",
    "with",
}


class RetryableError(RuntimeError):
    """Raised when a source fetch may succeed on retry."""


class NonRetryableError(RuntimeError):
    """Raised when retrying is pointless."""


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = date_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = unicodedata.normalize("NFKC", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def slugify(value: str) -> str:
    value = clean_text(value).lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "item"


def stable_hash(*parts: str, length: int = 16) -> str:
    h = hashlib.sha1()
    for part in parts:
        h.update((part or "").encode("utf-8", "ignore"))
        h.update(b"\x1f")
    return h.hexdigest()[:length]


def split_sentences(text: str) -> list[str]:
    text = clean_text(text)
    if not text:
        return []
    parts = re.split(r"(?<=[\.!?])\s+(?=[A-ZÀ-ÿ0-9])", text)
    return [part.strip() for part in parts if part.strip()]


def truncate_words(text: str, limit: int) -> str:
    words = clean_text(text).split()
    if len(words) <= limit:
        return " ".join(words)
    return " ".join(words[:limit]).rstrip(" ,;:-") + "…"


def summarize_text(text: str, min_sentences: int = 2, max_sentences: int = 3, max_words: int = 80) -> str:
    sentences = split_sentences(text)
    if not sentences:
        return "Résumé indisponible : métadonnées source trop pauvres."
    picked = sentences[:max(min_sentences, min(max_sentences, len(sentences)))]
    return truncate_words(" ".join(picked), max_words)


def title_signature(title: str) -> str:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9]+", clean_text(title).lower())
        if len(token) > 2 and token not in STOPWORDS
    ]
    if not tokens:
        return ""
    unique_tokens = sorted(dict.fromkeys(tokens))
    return " ".join(unique_tokens[:10])


def normalize_title_for_match(title: str) -> str:
    title = clean_text(title).lower()
    title = re.sub(r"\s+[\-|–—]\s+(reuters|ap|associated press|bloomberg|afp)$", "", title)
    title = re.sub(r"\s*\([^)]*updated[^)]*\)", "", title)
    title = re.sub(r"\b(live|analysis|opinion|podcast|video)\b", "", title)
    title = re.sub(r"[^a-z0-9à-ÿ]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def normalize_url(url: str, base_url: str | None = None) -> str:
    if not url:
        return ""
    url = html.unescape(url.strip())
    if base_url:
        url = urljoin(base_url, url)
    parsed = urlparse(url)

    # Common aggregator patterns
    if "news.google.com" in parsed.netloc:
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "url" in query:
            url = unquote(query["url"])
            parsed = urlparse(url)
        elif parsed.path.startswith("/rss/articles/"):
            pass

    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = parsed.path or "/"
    path = re.sub(r"/+", "/", path)
    if path != "/":
        path = path.rstrip("/")

    filtered_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in TRACKING_PARAMS:
            continue
        filtered_query.append((key, value))
    filtered_query.sort()
    query = urlencode(filtered_query, doseq=True)

    return urlunparse((scheme, netloc, path, "", query, ""))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def call_with_retry(
    func: Callable[[], T],
    retries: int = 2,
    delay_seconds: float = 1.0,
    retry_exceptions: tuple[type[BaseException], ...] = (RetryableError,),
) -> T:
    last_error: Optional[BaseException] = None
    for attempt in range(retries + 1):
        try:
            return func()
        except retry_exceptions as exc:
            last_error = exc
            if attempt >= retries:
                raise
            sleep_for = delay_seconds * (attempt + 1)
            LOGGER.warning("Retry after recoverable error: %s (sleep %.1fs)", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Unexpected retry exhaustion: {last_error}")
