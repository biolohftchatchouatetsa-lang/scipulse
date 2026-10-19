"""Bronze Hacker News.

Deux modes d'ingestion, tous deux écrits dans hn-raw/ :

1. Micro-batch (toutes les 5 min) : API officielle Firebase (top/new/best stories).
   -> hn-raw/dt=YYYY-MM-DD/hour=HH/batch-<timestamp>.jsonl
2. Backfill historique : API de recherche Algolia (HN Search, publique), filtrée sur
   les stories dont l'URL pointe vers arxiv.org. Indispensable : le flux temps réel seul
   contient très peu d'articles ArXiv, la jointure ArXiv x HN serait quasi vide.
   -> hn-raw/backfill/algolia-YYYY-MM.jsonl

Les enregistrements sont conservés bruts ; on ajoute seulement deux champs techniques
(_source, _ingested_at) pour la traçabilité. Les deux formats sont harmonisés en Silver.
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from scipulse.config import BUCKET_HN_RAW, put_jsonl

log = logging.getLogger(__name__)
FIREBASE = "https://hacker-news.firebaseio.com/v0"
ALGOLIA = "https://hn.algolia.com/api/v1/search_by_date"
LISTS = ("topstories", "newstories", "beststories")


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=20), reraise=True)
def _get_json(url: str, **params):
    r = requests.get(url, params=params or None, timeout=20)
    r.raise_for_status()
    return r.json()


def ingest_microbatch() -> dict:
    now = datetime.now(timezone.utc)
    ids: set[int] = set()
    for name in LISTS:
        ids.update(_get_json(f"{FIREBASE}/{name}.json") or [])
    log.info("%d identifiants uniques à récupérer", len(ids))

    def fetch(item_id: int):
        try:
            return _get_json(f"{FIREBASE}/item/{item_id}.json")
        except Exception as exc:  # un item en échec ne doit pas faire tomber le batch
            log.warning("item %s ignoré : %s", item_id, exc)
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        items = [it for it in pool.map(fetch, sorted(ids)) if it]

    stamp = now.isoformat()
    lines = [json.dumps({**it, "_source": "firebase", "_ingested_at": stamp}, ensure_ascii=False) for it in items]
    key = f"dt={now:%Y-%m-%d}/hour={now:%H}/batch-{now:%Y%m%dT%H%M%S}.jsonl"
    put_jsonl(BUCKET_HN_RAW, key, lines)
    return {"items": len(lines), "key": key}


def _month_windows(start_year: int, end: datetime):
    y, m = start_year, 1
    while datetime(y, m, 1, tzinfo=timezone.utc) <= end:
        a = datetime(y, m, 1, tzinfo=timezone.utc)
        b = datetime(y + (m == 12), 1 if m == 12 else m + 1, 1, tzinfo=timezone.utc)
        yield a, b
        y, m = b.year, b.month


def backfill_arxiv_stories(start_year: int = 2015) -> dict:
    """Une requête Algolia par mois (Algolia plafonne à 1000 résultats par requête)."""
    now = datetime.now(timezone.utc)
    stamp = now.isoformat()
    total = 0
    for a, b in _month_windows(start_year, now):
        data = _get_json(
            ALGOLIA,
            query="arxiv.org",
            tags="story",
            restrictSearchableAttributes="url",
            numericFilters=f"created_at_i>={int(a.timestamp())},created_at_i<{int(b.timestamp())}",
            hitsPerPage=1000,
        )
        if data.get("nbHits", 0) > 1000:
            log.warning("%s : %d résultats, plafond Algolia atteint", a.strftime("%Y-%m"), data["nbHits"])
        hits = data.get("hits", [])
        if not hits:
            continue
        lines = [json.dumps({**h, "_source": "algolia", "_ingested_at": stamp}, ensure_ascii=False) for h in hits]
        put_jsonl(BUCKET_HN_RAW, f"backfill/algolia-{a:%Y-%m}.jsonl", lines)
        total += len(lines)
    return {"stories": total}
