# Veille quotidienne assurance / finance / régulation

Projet Python de veille métier qui :
- collecte des contenus récents depuis des flux RSS et quelques pages publiques,
- normalise les métadonnées,
- élimine strictement les doublons et quasi-doublons,
- score la pertinence métier,
- publie un site statique sur GitHub Pages,
- conserve une mémoire persistante anti-doublons dans le dépôt.

## Principes clés

- **100 % GitHub** : GitHub Actions + GitHub Pages + fichiers versionnés dans le dépôt.
- **Pas de serveur** et **pas de base externe**.
- **Mémoire persistante** dans `data/seen_articles.jsonl`.
- **Déduplication stricte** avant publication :
  - URL canonique normalisée,
  - hash stable du contenu,
  - titre normalisé,
  - similarité de titres,
  - suppression des quasi-doublons inter-sources.

## Lancer en local

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
python main.py --log-level INFO
```

## Fichiers importants

- `config/sources.yml` : sources éditoriales et paramètres
- `data/seen_articles.jsonl` : mémoire persistante anti-doublons
- `data/state.json` : dernier état d’exécution
- `site/` : site statique généré
- `.github/workflows/daily_veille.yml` : exécution planifiée + publication Pages

## Ajouter une source

Dans `config/sources.yml` :
- ajouter une entrée `kind: rss` pour un flux RSS/Atom,
- ou une entrée `kind: scrape` avec sélecteurs CSS légers.

## Tests

```bash
pytest -q
```

## Important

Le projet privilégie volontairement une déduplication **plus stricte** que permissive :
si un doute existe entre republier ou bloquer, la logique bloque.
