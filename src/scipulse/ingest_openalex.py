"""Bronze citations : OpenAlex -> MinIO citations-raw/.

Choix d'OpenAlex (justification detaillee dans le README) : gratuit, sans cle, acces par
lot via filtre DOI, et fournit les references (referenced_works) utiles pour l'extension graphe.

Les articles ArXiv ont un DOI deterministe : 10.48550/arxiv.<id>. On interroge donc
OpenAlex par lots de 50 DOI (filtre `doi:a|b|c`) pour les articles presents en Bronze ArXiv.
Le champ `locations` est conserve : c'est lui qui permet de retrouver l'identifiant ArXiv de
chaque work (le champ `doi` d'OpenAlex est souvent le DOI de la revue).

Robustesse (apres un incident de limitation de debit, HTTP 429) :
  - debit plafonne (OPENALEX_MIN_INTERVAL secondes entre deux requetes, tous threads confondus) ;
  - en cas de 429, attente du delai `Retry-After` annonce par l'API (sinon backoff exponentiel) ;
  - AUCUNE suppression avant la fin : les fichiers deja ecrits sont conserves et la relance
    reprend ou l'ingestion s'est arretee (`{"fresh": true}` dans la conf du DAG pour repartir de zero) ;
  - un lot qui echoue durablement est compte et signale, il ne fait pas tomber toute l'ingestion ;
  - si l'API est durablement indisponible, l'ingestion s'arrete proprement en gardant l'existant.
"""
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

from scipulse.config import BUCKET_ARXIV_RAW, BUCKET_CITATIONS_RAW, delete_prefix, env, iter_jsonl, list_keys, put_jsonl

log = logging.getLogger(__name__)
API = "https://api.openalex.org/works"
BATCH = 50
GROUP = 20  # 20 requetes (~1000 works) par fichier
MAX_ATTEMPTS = 6
SELECT = "id,doi,title,publication_year,publication_date,cited_by_count,counts_by_year,referenced_works,primary_topic,type,locations"

_lock = threading.Lock()
_last_call = [0.0]
_dead = [0]  # lots consecutifs definitivement perdus (disjoncteur)


class OpenAlexUnavailable(RuntimeError):
    """OpenAlex refuse durablement nos requetes : inutile d'insister."""


def arxiv_doi(arxiv_id: str) -> str:
    return f"https://doi.org/10.48550/arxiv.{arxiv_id}".lower()


def latest_arxiv_ids(limit: int) -> list[str]:
    keys = sorted(k for k in list_keys(BUCKET_ARXIV_RAW) if k.endswith(".jsonl"))
    if not keys:
        raise RuntimeError("Bronze ArXiv vide : lancer d'abord le DAG 10_arxiv_bronze")
    latest = keys[-1].split("/")[0]  # dt=YYYY-MM-DD le plus recent
    ids: list[str] = []
    for key in (k for k in keys if k.startswith(latest + "/")):
        for line in iter_jsonl(BUCKET_ARXIV_RAW, key):
            ids.append(json.loads(line)["id"])
            if limit and len(ids) >= limit:
                return ids
    return ids


def _throttle() -> None:
    interval = float(env("OPENALEX_MIN_INTERVAL", "0.25"))
    with _lock:
        wait = _last_call[0] + interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.monotonic()


def fetch_batch(arxiv_ids: list[str]) -> list[dict] | None:
    """Un lot de DOI -> liste de works, ou None si le lot est abandonne."""
    params = {
        "filter": "doi:" + "|".join(arxiv_doi(i) for i in arxiv_ids),
        "per-page": BATCH,
        "select": SELECT,
        "mailto": env("OPENALEX_MAILTO", "anonymous@example.com"),
    }
    if env("OPENALEX_API_KEY", ""):
        params["api_key"] = env("OPENALEX_API_KEY")
    for attempt in range(MAX_ATTEMPTS):
        _throttle()
        try:
            r = requests.get(API, params=params, timeout=60)
        except requests.RequestException as exc:
            log.warning("erreur reseau (%s), tentative %d/%d", exc, attempt + 1, MAX_ATTEMPTS)
            time.sleep(min(5 * 2**attempt, 60))
            continue
        if r.status_code == 429:
            try:
                wait = float(r.headers.get("Retry-After", ""))
            except ValueError:
                wait = min(5 * 2**attempt, 120)
            log.warning("429 Too Many Requests : attente %.0fs (tentative %d/%d)", wait, attempt + 1, MAX_ATTEMPTS)
            time.sleep(min(wait, 300))
            continue
        if r.status_code >= 500:
            time.sleep(min(5 * 2**attempt, 60))
            continue
        r.raise_for_status()
        with _lock:
            _dead[0] = 0
        return r.json().get("results", [])
    with _lock:
        _dead[0] += 1
        if _dead[0] >= 3:
            raise OpenAlexUnavailable("3 lots consecutifs definitivement en echec (limitation de debit ou panne)")
    return None


def ingest(ds: str, fresh: bool = False) -> dict:
    limit = int(env("OPENALEX_MAX_PAPERS", env("ARXIV_SAMPLE_SIZE", "50000")))
    ids = latest_arxiv_ids(limit)
    batches = [ids[i : i + BATCH] for i in range(0, len(ids), BATCH)]
    n_parts = -(-len(batches) // GROUP)
    log.info("%d articles ArXiv -> %d requetes OpenAlex -> %d fichiers", len(ids), len(batches), n_parts)

    partition = f"dt={ds}/"
    if fresh:
        log.info("fresh=true : purge de %s (%d fichiers)", partition, delete_prefix(BUCKET_CITATIONS_RAW, partition))
    existing = set(list_keys(BUCKET_CITATIONS_RAW, partition))
    stamp = datetime.now(timezone.utc).isoformat()
    written = skipped = failed_batches = found = 0
    _dead[0] = 0

    with ThreadPoolExecutor(max_workers=3) as pool:
        for part in range(n_parts):
            key = f"{partition}part-{part:04d}.jsonl"
            if key in existing:
                skipped += 1
                continue
            group = batches[part * GROUP : (part + 1) * GROUP]
            try:
                results = list(pool.map(fetch_batch, group))
                missing = [i for i, r in enumerate(results) if r is None]
                if missing:  # une seconde chance apres une pause, pour les lots abandonnes
                    log.warning("fichier %d : %d/%d lots en echec, nouvelle tentative dans 30s", part, len(missing), len(group))
                    time.sleep(30)
                    for i in missing:
                        results[i] = fetch_batch(group[i])
            except OpenAlexUnavailable as exc:
                raise RuntimeError(
                    f"{exc}. {written} fichiers ecrits, {skipped} deja presents (conserves) : "
                    f"relancer plus tard, l'ingestion reprendra la ou elle s'est arretee."
                ) from exc
            failed = sum(r is None for r in results)
            failed_batches += failed
            works = [w for r in results if r for w in r]
            if works:
                put_jsonl(BUCKET_CITATIONS_RAW, key, (json.dumps({**w, "_source": "openalex", "_ingested_at": stamp}, ensure_ascii=False) for w in works))
                found += len(works)
                written += 1
            log.info("progression : fichier %d/%d (%d works, %d lots en echec)", part + 1, n_parts, len(works), failed)

    # nettoyage final : fichiers orphelins d'une ancienne execution plus longue
    valid = {f"{partition}part-{p:04d}.jsonl" for p in range(n_parts)}
    stale = [k for k in list_keys(BUCKET_CITATIONS_RAW, partition) if k not in valid]
    result = {"queried": len(ids), "files_written": written, "files_already_present": skipped,
              "works_fetched_this_run": found, "failed_batches": failed_batches, "stale_files": len(stale)}
    log.info("OpenAlex termine : %s", result)
    return result