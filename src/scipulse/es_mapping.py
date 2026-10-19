"""Mappings Elasticsearch (couche Gold).

Choix de mapping :
  - `text` + analyseur `english` (stemming, mots vides) pour la recherche plein texte (titre, resume) ;
  - sous-champ `title.raw` en `keyword` pour trier et agreger sur le titre exact ;
  - `keyword` pour tout ce qui sert a filtrer / agreger (categories, identifiants, domaines) ;
  - `date` pour les horodatages, `integer`/`float` pour les mesures ;
  - 1 shard / 0 replica : cluster mono-noeud (statut `green`).
"""

SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

_TEXT_EN = {"type": "text", "analyzer": "english"}
_TITLE = {**_TEXT_EN, "fields": {"raw": {"type": "keyword", "ignore_above": 512}}}

ARXIV_PAPERS_PROPERTIES = {
    "arxiv_id": {"type": "keyword"},
    "title": _TITLE,
    "abstract": _TEXT_EN,
    "authors": {"type": "keyword"},
    "n_authors": {"type": "integer"},
    "primary_category": {"type": "keyword"},
    "category_group": {"type": "keyword"},
    "categories": {"type": "keyword"},
    "doi": {"type": "keyword"},
    "journal_ref": {"type": "keyword", "ignore_above": 512},
    "n_versions": {"type": "integer"},
    "submitted_at": {"type": "date"},
    "updated_at": {"type": "date"},
    "year": {"type": "integer"},
    "url": {"type": "keyword"},
}

ENRICHED_PROPERTIES = {
    **ARXIV_PAPERS_PROPERTIES,
    "top_terms": {"type": "keyword"},
    "has_citation_data": {"type": "boolean"},
    "openalex_id": {"type": "keyword"},
    "cited_by_count": {"type": "integer"},
    "n_references": {"type": "integer"},
    "cites_recent": {"type": "integer"},
    "citation_velocity": {"type": "float"},
    "topic": {"type": "keyword"},
    "subfield": {"type": "keyword"},
    "field": {"type": "keyword"},
    "domain": {"type": "keyword"},
    "has_hn": {"type": "boolean"},
    "hn_n_posts": {"type": "integer"},
    "hn_score_total": {"type": "integer"},
    "hn_score_max": {"type": "integer"},
    "hn_comments_total": {"type": "integer"},
    "hn_engagement": {"type": "integer"},
    "first_hn_at": {"type": "date"},
    "age_years": {"type": "float"},
    "category_year_count": {"type": "integer"},
    "category_growth_yoy": {"type": "float"},
    "velocity_pct_in_category": {"type": "float"},
    "citation_component": {"type": "float"},
    "hn_component": {"type": "float"},
    "recency_component": {"type": "float"},
    "impact_score": {"type": "float"},
    "impact_rank_in_category": {"type": "integer"},
    "academic_star_no_hn": {"type": "boolean"},
    "viral_low_citation": {"type": "boolean"},
}

PIPELINE_METRICS_PROPERTIES = {
    "@timestamp": {"type": "date"},
    "source": {"type": "keyword"},
    "rows_bronze": {"type": "integer"},
    "rows_silver": {"type": "integer"},
    "rows_rejected": {"type": "integer"},
    "rejected_pct": {"type": "float"},
    "ge_evaluated": {"type": "integer"},
    "ge_passed": {"type": "integer"},
    "ge_failed": {"type": "integer"},
    "ge_error_rate_pct": {"type": "float"},
    "ge_success": {"type": "boolean"},
    "rows_gold": {"type": "integer"},
}


def body(properties: dict) -> dict:
    return {"settings": SETTINGS, "mappings": {"properties": properties}}
