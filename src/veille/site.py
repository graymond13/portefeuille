from __future__ import annotations

import html
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape

from .utils import ensure_dir


LOGGER = logging.getLogger("veille.site")


def build_site(run_date: str, articles: list[dict], site_dir: Path, base_url: str = "") -> None:
    ensure_dir(site_dir)
    ensure_dir(site_dir / "archive")
    ensure_dir(site_dir / "data")
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    latest_json_path = site_dir / "data" / "latest.json"
    latest_json_path.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8")

    archive_path = site_dir / "archive" / f"{run_date}.html"
    archive_path.write_text(render_day_page(run_date, articles, title=f"Veille du {run_date}", home=False), encoding="utf-8")

    by_date = _load_existing_latest(site_dir / "data" / "archives_index.json")
    by_date[run_date] = {
        "date": run_date,
        "count": len(articles),
        "top_titles": [item["clean_title"] for item in articles[:5]],
        "path": f"archive/{run_date}.html",
    }
    (site_dir / "data" / "archives_index.json").write_text(
        json.dumps(by_date, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    ordered_dates = sorted(by_date.keys(), reverse=True)
    archive_entries = [by_date[d] for d in ordered_dates]

    (site_dir / "index.html").write_text(
        render_day_page(run_date, articles, title=f"Veille métier — {run_date}", home=True),
        encoding="utf-8",
    )
    (site_dir / "archives.html").write_text(render_archives_page(archive_entries), encoding="utf-8")
    (site_dir / "feed.xml").write_text(render_rss(run_date, articles, base_url=base_url), encoding="utf-8")


def _load_existing_latest(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def render_day_page(run_date: str, articles: list[dict], title: str, home: bool) -> str:
    top5 = articles[:5]
    cards = "\n".join(render_article_card(article, rank=i + 1) for i, article in enumerate(articles))
    top_cards = "\n".join(render_top_card(article, rank=i + 1) for i, article in enumerate(top5))
    nav = '<a href="index.html">Accueil</a> · <a href="archives.html">Archives</a>' if home else '<a href="../index.html">Accueil</a> · <a href="../archives.html">Archives</a>'
    return f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{base_css()}</style>
</head>
<body>
  <main class="container">
    <header class="hero">
      <p class="eyebrow">Veille quotidienne assurance / finance / régulation</p>
      <h1>{html.escape(title)}</h1>
      <p class="lede">Sélection éditoriale resserrée, sans republication des articles déjà publiés.</p>
      <p class="nav">{nav}</p>
    </header>

    <section class="top-section">
      <h2>Top 5 du jour</h2>
      <div class="grid top-grid">{top_cards or '<p>Aucun article sélectionné aujourd’hui.</p>'}</div>
    </section>

    <section>
      <h2>Toute la sélection du {html.escape(run_date)}</h2>
      <div class="stack">{cards or '<p>Aucun article retenu.</p>'}</div>
    </section>
  </main>
</body>
</html>"""


def render_top_card(article: dict, rank: int) -> str:
    return f"""
    <article class="top-card">
      <div class="rank">#{rank}</div>
      <span class="badge">{html.escape(article['category'])}</span>
      <h3><a href="{html.escape(article['url'])}" target="_blank" rel="noopener noreferrer">{html.escape(article['clean_title'])}</a></h3>
      <p class="meta">{html.escape(article['source_name'])} · score {article['score']}</p>
      <p>{html.escape(article['why_selected'])}</p>
    </article>
    """


def render_article_card(article: dict, rank: int) -> str:
    published = article.get("published_at") or article.get("published_date") or "date inconnue"
    return f"""
    <article class="card">
      <div class="card-head">
        <div>
          <span class="badge">{html.escape(article['category'])}</span>
          <span class="badge subtle">{html.escape(article['source_name'])}</span>
        </div>
        <strong class="score">Score {article['score']}</strong>
      </div>
      <h3>{rank}. <a href="{html.escape(article['url'])}" target="_blank" rel="noopener noreferrer">{html.escape(article['clean_title'])}</a></h3>
      <p class="meta">Publié : {html.escape(str(published))} · Domaine : {html.escape(article['domain'])}</p>
      <p>{html.escape(article['summary'])}</p>
      <p><strong>Pourquoi retenu :</strong> {html.escape(article['why_selected'])}</p>
      <p><strong>Impact possible :</strong> {html.escape(article['impacts'])}</p>
    </article>
    """


def render_archives_page(entries: Iterable[dict]) -> str:
    rows = []
    for entry in entries:
        top_titles = " ; ".join(entry.get("top_titles", [])[:3])
        rows.append(
            f"<tr><td><a href='{html.escape(entry['path'])}'>{html.escape(entry['date'])}</a></td>"
            f"<td>{entry['count']}</td><td>{html.escape(top_titles)}</td></tr>"
        )
    table = "\n".join(rows) or "<tr><td colspan='3'>Aucune archive</td></tr>"
    return f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Archives de veille</title>
  <style>{base_css()}</style>
</head>
<body>
  <main class="container">
    <header class="hero">
      <p class="eyebrow">Archives</p>
      <h1>Historique des veilles publiées</h1>
      <p class="nav"><a href="index.html">Accueil</a></p>
    </header>
    <table>
      <thead><tr><th>Date</th><th>Articles</th><th>Aperçu</th></tr></thead>
      <tbody>{table}</tbody>
    </table>
  </main>
</body>
</html>"""


def render_rss(run_date: str, articles: list[dict], base_url: str = "") -> str:
    base = base_url.rstrip("/")
    items = []
    for article in articles[:20]:
        link = article["url"]
        guid = article["canonical_url"]
        description = f"{article['summary']} — {article['why_selected']}"
        pub_date = article.get("published_at") or datetime.utcnow().isoformat()
        items.append(
            f"<item><title>{escape(article['clean_title'])}</title>"
            f"<link>{escape(link)}</link><guid>{escape(guid)}</guid>"
            f"<description>{escape(description)}</description>"
            f"<pubDate>{escape(str(pub_date))}</pubDate></item>"
        )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>Veille métier du {escape(run_date)}</title>
  <link>{escape(base + '/index.html' if base else '')}</link>
  <description>Veille quotidienne assurance, finance, banque et régulation.</description>
  {''.join(items)}
</channel>
</rss>"""


def base_css() -> str:
    return """
:root {
  --bg: #f5f7fb;
  --card: #ffffff;
  --ink: #152033;
  --muted: #526079;
  --line: #d8dfeb;
  --accent: #204ecf;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg);
  color: var(--ink);
  line-height: 1.55;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.container { max-width: 1080px; margin: 0 auto; padding: 32px 20px 72px; }
.hero {
  background: linear-gradient(135deg, #ffffff, #eef3ff);
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 28px;
  margin-bottom: 24px;
}
.eyebrow { text-transform: uppercase; letter-spacing: .08em; color: var(--muted); font-size: .8rem; }
.lede, .nav, .meta { color: var(--muted); }
.grid { display: grid; gap: 16px; }
.top-grid { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
.top-card, .card {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 18px;
  box-shadow: 0 8px 28px rgba(13, 29, 61, 0.05);
}
.stack { display: grid; gap: 16px; }
.card-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
.badge {
  display: inline-block;
  padding: 4px 10px;
  margin-right: 6px;
  border-radius: 999px;
  background: #e8efff;
  color: #24449a;
  font-size: .82rem;
  font-weight: 600;
}
.badge.subtle { background: #f0f2f7; color: #4a5972; }
.rank {
  display: inline-block;
  font-weight: 700;
  margin-bottom: 8px;
}
.score { color: var(--accent); }
table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border: 1px solid var(--line);
  border-radius: 14px;
  overflow: hidden;
}
th, td { border-bottom: 1px solid var(--line); padding: 14px; text-align: left; vertical-align: top; }
@media (max-width: 720px) {
  .container { padding: 18px 14px 50px; }
  .hero { padding: 18px; }
  .card-head { flex-direction: column; align-items: flex-start; }
}
"""
