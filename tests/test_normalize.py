from src.veille.utils import normalize_title_for_match, normalize_url, title_signature


def test_normalize_url_removes_tracking_params():
    url = "https://www.example.com/article?id=42&utm_source=newsletter&fbclid=abc"
    assert normalize_url(url) == "https://example.com/article?id=42"


def test_normalize_url_resolves_google_news_redirect_like_links():
    url = "https://news.google.com/rss/articles/CBMiTGh0dHBzOi8vd3d3LmV4YW1wbGUuY29tL25ld3M_dXRtX3NvdXJjZT1yc3MmdXRtX2NhbXBhaWduPXRlc3TaAQA?url=https%3A%2F%2Fwww.example.com%2Fnews%3Futm_source%3Drss"
    assert normalize_url(url) == "https://example.com/news"


def test_normalized_title_and_signature_are_stable():
    title = "Banking Regulation Tightens – Reuters"
    assert normalize_title_for_match(title) == "banking regulation tightens"
    assert title_signature(title) == "regulation tightens"
