"""Couche Silver : nettoyage et normalisation par source -> Parquet dans MinIO.

Chaque `build_*` est une fonction PURE (liste d'enregistrements Bronze -> DataFrame propre
+ rapport de qualite), donc testable sans MinIO. Les `run_*` font les entrees/sorties.

Regles de nettoyage (justifiees par l'analyse des donnees Bronze) :
  ArXiv    : id normalise (sans version), titre/resume sans retours a la ligne, categories en
             liste + categorie principale, dates RFC-2822 -> timestamp UTC, annee deduite de
             l'identifiant si la date manque, doublons supprimes, lignes sans titre rejetees.
  HN       : deux formats (Firebase / Algolia) harmonises ; id ArXiv extrait de l'URL ; score et
             nb de commentaires manquants -> 0 ; doublons : on garde la lecture la plus recente.
  Citations: id ArXiv retrouve via `locations` (le champ doi d'OpenAlex est le DOI de la revue) ;
             DOI normalise ; works sans id ArXiv rejetes (non jointables) ; doublons -> le + cite.
"""
import io
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urlparse

import pandas as pd

from scipulse.normalize import (
    arxiv_id_from_doi,
    arxiv_id_from_url,
    clean_text,
    normalize_arxiv_id,
    normalize_doi,
    year_from_arxiv_id,
)

log = logging.getLogger(__name__)
ARXIV_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


# --------------------------------------------------------------------------- ArXiv
def build_arxiv(records: Iterable[dict]) -> tuple[pd.DataFrame, dict]:
    stats: Counter = Counter()
    rows = []
    for r in records:
        stats["raw"] += 1
        arxiv_id = normalize_arxiv_id(r.get("id"))
        if not arxiv_id:
            stats["dropped_invalid_id"] += 1
            continue
        title = clean_text(r.get("title"))
        if title is None:
            stats["dropped_missing_title"] += 1
            continue
        cats = (r.get("categories") or "").split()
        if not cats:
            stats["missing_category_set_unknown"] += 1
            cats = ["unknown"]
        versions = r.get("versions") or []
        authors = [
            clean_text(f"{a[1]} {a[0]}")
            for a in (r.get("authors_parsed") or [])
            if isinstance(a, list) and len(a) >= 2 and (a[0] or a[1])
        ]
        rows.append(
            {
                "arxiv_id": arxiv_id,
                "title": title,
                "abstract": clean_text(r.get("abstract")),
                "authors": authors,
                "n_authors": len(authors),
                "primary_category": cats[0],
                "category_group": cats[0].split(".")[0],
                "categories": cats,
                "doi": normalize_doi(r.get("doi")),
                "journal_ref": clean_text(r.get("journal-ref")),
                "n_versions": len(versions),
                "_submitted_raw": versions[0].get("created") if versions else None,
                "_updated_raw": r.get("update_date"),
                "url": f"https://arxiv.org/abs/{arxiv_id}",
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df, {"source": "arxiv", **stats}

    df["submitted_at"] = pd.to_datetime(df.pop("_submitted_raw"), format=ARXIV_DATE_FORMAT, errors="coerce", utc=True)
    df["updated_at"] = pd.to_datetime(df.pop("_updated_raw"), errors="coerce", utc=True)
    stats["submitted_at_missing"] = int(df["submitted_at"].isna().sum())
    df["year"] = df["submitted_at"].dt.year.astype("float64")
    missing_year = df["year"].isna()
    df.loc[missing_year, "year"] = df.loc[missing_year, "arxiv_id"].map(year_from_arxiv_id)
    stats["year_imputed_from_id"] = int(missing_year.sum())
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype("int64")

    before = len(df)
    df = df.sort_values("arxiv_id").drop_duplicates("arxiv_id", keep="last").reset_index(drop=True)
    stats["duplicates_removed"] = before - len(df)
    stats["kept"] = len(df)
    stats["abstract_missing"] = int(df["abstract"].isna().sum())
    return df, {"source": "arxiv", **stats}


# --------------------------------------------------------------------------- Hacker News
def _hn_row(r: dict) -> dict | None:
    if r.get("_source") == "algolia":
        raw_id = r.get("story_id") or r.get("objectID")
        row = dict(author=r.get("author"), title=r.get("title"), url=r.get("url"),
                   score=r.get("points"), n_comments=r.get("num_comments"), ts=r.get("created_at_i"))
    else:  # firebase
        if r.get("type") != "story" or r.get("deleted") or r.get("dead"):
            return None
        raw_id = r.get("id")
        row = dict(author=r.get("by"), title=r.get("title"), url=r.get("url"),
                   score=r.get("score"), n_comments=r.get("descendants"), ts=r.get("time"))
    try:
        row["hn_id"] = int(raw_id)
    except (TypeError, ValueError):
        return None
    row["source"] = r.get("_source", "unknown")
    row["ingested_at"] = r.get("_ingested_at")
    return row


def build_hn(records: Iterable[dict]) -> tuple[pd.DataFrame, dict]:
    stats: Counter = Counter()
    rows = []
    for r in records:
        stats["raw"] += 1
        row = _hn_row(r)
        if row is None:
            stats["dropped_not_a_story_or_no_id"] += 1
            continue
        row["title"] = clean_text(row["title"])
        if row["title"] is None:
            stats["dropped_missing_title"] += 1
            continue
        url = clean_text(row["url"])
        row["url"] = url
        row["domain"] = (urlparse(url).netloc.lower().removeprefix("www.") or None) if url else None
        row["arxiv_id"] = arxiv_id_from_url(url)
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df, {"source": "hn", **stats}

    df["created_at"] = pd.to_datetime(df.pop("ts"), unit="s", utc=True, errors="coerce")
    df = df.dropna(subset=["created_at"]).copy()
    for col in ("score", "n_comments"):
        stats[f"{col}_missing_set_to_0"] = int(df[col].isna().sum())
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).clip(lower=0).astype("int64")
    df["year"] = df["created_at"].dt.year.astype("int64")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce")

    before = len(df)
    df = df.sort_values("ingested_at").drop_duplicates("hn_id", keep="last").sort_values("hn_id").reset_index(drop=True)
    stats["duplicates_removed"] = before - len(df)
    stats["kept"] = len(df)
    stats["with_arxiv_id"] = int(df["arxiv_id"].notna().sum())
    return df, {"source": "hn", **stats}


# --------------------------------------------------------------------------- Citations (OpenAlex)
def _arxiv_id_of_work(r: dict) -> str | None:
    for loc in r.get("locations") or []:
        if not isinstance(loc, dict):
            continue
        for key in ("landing_page_url", "pdf_url", "id"):
            found = arxiv_id_from_url(loc.get(key)) or arxiv_id_from_doi(loc.get(key))
            if found:
                return found
    return arxiv_id_from_doi(r.get("doi"))


def build_citations(records: Iterable[dict]) -> tuple[pd.DataFrame, dict]:
    stats: Counter = Counter()
    rows = []
    for r in records:
        stats["raw"] += 1
        arxiv_id = _arxiv_id_of_work(r)
        if not arxiv_id:
            stats["dropped_no_arxiv_id"] += 1
            continue
        topic = r.get("primary_topic") or {}
        refs = [x.rsplit("/", 1)[-1] for x in (r.get("referenced_works") or []) if isinstance(x, str)]
        rows.append(
            {
                "arxiv_id": arxiv_id,
                "openalex_id": str(r.get("id", "")).rsplit("/", 1)[-1] or None,
                "doi": normalize_doi(r.get("doi")),
                "title": clean_text(r.get("title")),
                "publication_year": r.get("publication_year"),
                "publication_date": r.get("publication_date"),
                "cited_by_count": r.get("cited_by_count"),
                "n_references": len(refs),
                "referenced_works": refs,
                "counts_by_year": [
                    {"year": int(c["year"]), "cited_by_count": int(c["cited_by_count"])}
                    for c in (r.get("counts_by_year") or [])
                    if c.get("year") is not None and c.get("cited_by_count") is not None
                ],
                "topic": topic.get("display_name"),
                "subfield": (topic.get("subfield") or {}).get("display_name"),
                "field": (topic.get("field") or {}).get("display_name"),
                "domain": (topic.get("domain") or {}).get("display_name"),
                "work_type": r.get("type"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df, {"source": "citations", **stats}

    stats["cited_by_count_missing_set_to_0"] = int(df["cited_by_count"].isna().sum())
    df["cited_by_count"] = pd.to_numeric(df["cited_by_count"], errors="coerce").fillna(0).clip(lower=0).astype("int64")
    df["publication_year"] = pd.to_numeric(df["publication_year"], errors="coerce").astype("Int64")
    df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce", utc=True)

    before = len(df)
    df = df.sort_values("cited_by_count").drop_duplicates("arxiv_id", keep="last").sort_values("arxiv_id").reset_index(drop=True)
    stats["duplicates_removed"] = before - len(df)
    stats["kept"] = len(df)
    return df, {"source": "citations", **stats}


# --------------------------------------------------------------------------- Entrees / sorties MinIO
def _latest_partition_records(bucket: str) -> Iterable[dict]:
    from scipulse.config import iter_jsonl, list_keys

    keys = sorted(k for k in list_keys(bucket) if k.endswith(".jsonl") and k.startswith("dt="))
    if not keys:
        raise RuntimeError(f"Bronze vide : {bucket}")
    latest = keys[-1].split("/")[0]
    log.info("%s : partition %s", bucket, latest)
    for key in (k for k in keys if k.startswith(latest + "/")):
        for line in iter_jsonl(bucket, key):
            yield json.loads(line)


def _all_records(bucket: str) -> Iterable[dict]:
    from scipulse.config import iter_jsonl, list_keys

    for key in sorted(k for k in list_keys(bucket) if k.endswith(".jsonl")):
        for line in iter_jsonl(bucket, key):
            yield json.loads(line)


def write_parquet(df: pd.DataFrame, bucket: str) -> str:
    from scipulse.config import delete_prefix, s3_client

    delete_prefix(bucket, "data/")
    buf = io.BytesIO()
    # Spark ne lit pas les timestamps en nanosecondes : on force la microseconde.
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy", coerce_timestamps="us", allow_truncated_timestamps=True)
    s3_client().put_object(Bucket=bucket, Key="data/part-0000.parquet", Body=buf.getvalue())
    return f"s3://{bucket}/data/part-0000.parquet"


def write_report(report: dict, bucket: str) -> None:
    from scipulse.config import s3_client

    now = datetime.now(timezone.utc)
    report = {**report, "run_at": now.isoformat()}
    body = json.dumps(report, ensure_ascii=True, indent=2, default=str).encode("utf-8")
    s3 = s3_client()
    s3.put_object(Bucket=bucket, Key="_reports/latest.json", Body=body)
    s3.put_object(Bucket=bucket, Key=f"_reports/run-{now:%Y%m%dT%H%M%S}.json", Body=body)


def _run(source: str, bronze: str, clean: str, records, builder, expectations) -> dict:
    from scipulse import quality

    df, report = builder(records)
    if df.empty:
        raise RuntimeError(f"Silver {source} : aucune ligne valide")
    report["great_expectations"] = quality.validate(source, df, expectations)
    report["columns"] = {c: str(t) for c, t in df.dtypes.items()}
    report["path"] = write_parquet(df, clean)
    write_report(report, clean)
    log.info("Silver %s : %s", source, {k: v for k, v in report.items() if k not in ("columns", "great_expectations")})
    return {k: v for k, v in report.items() if k != "columns"}


def run_arxiv() -> dict:
    from scipulse import quality
    from scipulse.config import BUCKET_ARXIV_CLEAN, BUCKET_ARXIV_RAW

    return _run("arxiv", BUCKET_ARXIV_RAW, BUCKET_ARXIV_CLEAN, _latest_partition_records(BUCKET_ARXIV_RAW), build_arxiv, quality.arxiv_expectations)


def run_hn() -> dict:
    from scipulse import quality
    from scipulse.config import BUCKET_HN_CLEAN, BUCKET_HN_RAW

    return _run("hn", BUCKET_HN_RAW, BUCKET_HN_CLEAN, _all_records(BUCKET_HN_RAW), build_hn, quality.hn_expectations)


def run_citations() -> dict:
    from scipulse import quality
    from scipulse.config import BUCKET_CITATIONS_CLEAN, BUCKET_CITATIONS_RAW

    return _run("citations", BUCKET_CITATIONS_RAW, BUCKET_CITATIONS_CLEAN, _latest_partition_records(BUCKET_CITATIONS_RAW), build_citations, quality.citations_expectations)
