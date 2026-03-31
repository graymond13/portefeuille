from __future__ import annotations

import logging
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from typing import Iterable

from .models import NormalizedArticle, SelectedArticle
from .utils import summarize_text, utc_now


LOGGER = logging.getLogger("veille.scoring")

THEME_KEYWORDS = {
    "Assurance": {
        "insurance": 12,
        "assurance": 12,
        "insurer": 11,
        "insurtech": 10,
        "reinsurance": 11,
        "broker": 6,
        "claims": 6,
        "solvency ii": 12,
        "underwriting": 8,
        "pensions": 5,
    },
    "Finance": {
        "finance": 8,
        "financial": 8,
        "bank": 10,
        "banking": 10,
        "banque": 10,
        "asset management": 12,
        "gestion d actifs": 12,
        "épargne": 10,
        "epargne": 10,
        "fund": 7,
        "market": 6,
        "markets": 6,
        "capital": 5,
        "liquidity": 6,
        "payments": 8,
        "fintech": 10,
        "mifid": 8,
        "micar": 8,
        "mica": 8,
        "psd": 8,
    },
    "Réglementation": {
        "regulation": 12,
        "regulatory": 12,
        "compliance": 10,
        "fraud": 10,
        "cyber": 10,
        "cybersecurity": 10,
        "aml": 11,
        "anti money laundering": 11,
        "sanctions": 9,
        "supervision": 10,
        "consultation": 7,
        "guidelines": 7,
        "directive": 8,
        "dora": 10,
        "crr": 8,
        "crd": 8,
        "esg": 6,
    },
    "Géopolitique": {
        "geopolitical": 8,
        "gulf": 8,
        "hormuz": 12,
        "iran": 8,
        "ukraine": 8,
        "russia": 8,
        "china": 5,
        "tariff": 8,
        "trade": 7,
        "energy": 10,
        "oil": 8,
        "gas": 8,
        "country risk": 10,
    },
}

IMPACT_KEYWORDS = {
    "impact réglementaire": ["guideline", "regulation", "directive", "consultation", "opinion", "guidelines"],
    "impact marché": ["markets", "market", "bond", "stock", "liquidity", "yield", "fund", "asset management"],
    "impact assurance": ["insurance", "insurer", "reinsurance", "solvency ii", "claims"],
    "impact risques/cyber": ["fraud", "cyber", "cybersecurity", "ict", "operational resilience", "dora"],
    "impact énergie / commerce": ["oil", "gas", "energy", "sanctions", "tariff", "trade", "shipping", "hormuz"],
}

LOW_SIGNAL_TERMS = {
    "podcast",
    "video",
    "webinar",
    "sponsored",
    "advertisement",
    "advertorial",
    "career",
    "jobs",
}

HIGH_SIGNAL_SOURCES = {
    "ecb": 10,
    "eba": 11,
    "eiopa": 11,
    "esma": 11,
    "imf": 9,
}


def score_articles(articles: Iterable[NormalizedArticle]) -> list[SelectedArticle]:
    scored: list[SelectedArticle] = []
    now = utc_now()
    for article in articles:
        text = f"{article.clean_title} {article.summary}".lower()
        breakdown: dict[str, int] = {}

        theme_scores = {
            theme: sum(weight for key, weight in keywords.items() if key in text)
            for theme, keywords in THEME_KEYWORDS.items()
        }
        theme = max(theme_scores, key=theme_scores.get)
        theme_score = theme_scores[theme]
        breakdown["proximite_thematique"] = min(theme_score, 30)

        breakdown["qualite_source"] = min(article.source_quality // 4, 20)
        breakdown["nature_source"] = HIGH_SIGNAL_SOURCES.get(article.source_id, 0)

        published_at = article.published_at or now
        age_hours = max((now - published_at).total_seconds() / 3600, 0)
        if age_hours <= 12:
            freshness = 16
        elif age_hours <= 24:
            freshness = 12
        elif age_hours <= 48:
            freshness = 8
        else:
            freshness = 3
        breakdown["fraicheur"] = freshness

        impact = 0
        impact_reasons: list[str] = []
        for label, keywords in IMPACT_KEYWORDS.items():
            if any(keyword in text for keyword in keywords):
                impact += 8
                impact_reasons.append(label)
        breakdown["impact_metier"] = min(impact, 24)

        if article.source_category == "geopolitique_major" and theme != "Géopolitique":
            breakdown["source_geo_sans_impact"] = -8
        elif article.source_category == "geopolitique_major" and not impact_reasons:
            breakdown["source_geo_sans_impact"] = -10
        else:
            breakdown["source_geo_sans_impact"] = 0

        if any(term in text for term in LOW_SIGNAL_TERMS):
            breakdown["bruit_marketing"] = -18
        else:
            breakdown["bruit_marketing"] = 0

        summary = article.summary or summarize_text(article.clean_title, min_sentences=1, max_sentences=1, max_words=20)
        if len(summary.split()) >= 12:
            breakdown["densite_information"] = 6
        else:
            breakdown["densite_information"] = 1

        total_score = sum(breakdown.values())
        why_selected = _build_why_selected(theme, impact_reasons, article)
        impacts = _build_impacts(impact_reasons, article)

        scored.append(
            SelectedArticle(
                source_id=article.source_id,
                source_name=article.source_name,
                source_category=article.source_category,
                title=article.title,
                clean_title=article.clean_title,
                url=article.url,
                canonical_url=article.canonical_url,
                domain=article.domain,
                published_at=article.published_at,
                published_date=article.published_date,
                summary=_smart_summary(article.clean_title, summary),
                category=theme,
                score=max(total_score, 0),
                score_breakdown=breakdown,
                why_selected=why_selected,
                impacts=impacts,
                title_signature=article.title_signature,
                content_fingerprint=article.content_fingerprint,
                normalized_title=article.normalized_title,
            )
        )
    return sorted(scored, key=lambda item: item.score, reverse=True)


def limit_topic_repetition(articles: list[SelectedArticle], max_per_signature: int = 1) -> list[SelectedArticle]:
    counter: Counter[str] = Counter()
    kept: list[SelectedArticle] = []
    for article in articles:
        key = article.title_signature or article.normalized_title[:24]
        if counter[key] >= max_per_signature:
            continue
        counter[key] += 1
        kept.append(article)
    return kept


def _build_why_selected(theme: str, impact_reasons: list[str], article: NormalizedArticle) -> str:
    reasons = [f"thématique {theme.lower()}", f"source jugée fiable ({article.source_name})"]
    if impact_reasons:
        reasons.append(impact_reasons[0])
    if article.published_at and utc_now() - article.published_at <= timedelta(hours=24):
        reasons.append("information très récente")
    return " ; ".join(reasons[:3]).capitalize() + "."


def _build_impacts(impact_reasons: list[str], article: NormalizedArticle) -> str:
    if impact_reasons:
        mapping = {
            "impact réglementaire": "Peut modifier la conformité, les reportings ou la gouvernance des acteurs financiers.",
            "impact marché": "Peut affecter les conditions de marché, les valorisations ou l’allocation d’actifs.",
            "impact assurance": "Peut toucher la souscription, la solvabilité, la distribution ou le provisionnement des assureurs.",
            "impact risques/cyber": "Peut modifier l’exposition opérationnelle, fraude ou cyber des établissements.",
            "impact énergie / commerce": "Peut se transmettre aux prix, au risque pays, aux sinistres ou au coût du capital.",
        }
        return mapping[impact_reasons[0]]
    if article.source_category == "geopolitique_major":
        return "Signal géopolitique surveillé uniquement car un effet indirect sur marchés, énergie ou risque pays est plausible."
    return "Impact métier modéré mais utile pour maintenir une veille sectorielle structurée."


def _smart_summary(title: str, summary: str) -> str:
    summary = summarize_text(summary, min_sentences=2, max_sentences=3, max_words=80)
    if title.lower() in summary.lower():
        return summary
    return summary
