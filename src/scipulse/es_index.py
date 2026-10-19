"""Indexation Elasticsearch du Module 1 : `arxiv-papers` (ArXiv Silver) et `pipeline-metrics` (monitoring)."""
import io
import json
import logging
import math
from datetime import datetime, timezone

import pandas as pd

from scipulse import es_mapping
from scipulse.config import BUCKET_ARXIV_CLEAN, BUCKET_CITATIONS_CLEAN, BUCKET_HN_CLEAN, env, list_keys, s3_client

log = logging.getLogger(__name__)


def _es():
    from elasticsearch import Elasticsearch

    return Elasticsearch(env("ES_HOST", "http://elasticsearch:9200"), request_timeout=60)


def recreate_index(es, name: str, properties: dict) -> None:
    if es.indices.exists(index=name):
        es.indices.delete(index=name)
    es.indices.create(index=name, **es_mapping.body(properties))


def _clean(value):
    """Valeurs compatibles JSON : NaN/NaT -> None, Timestamp -> ISO 8601."""
    if isinstance(value, (list, tuple)):
        return list(value)
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
        if isinstance(value, list):
            return value
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def index_arxiv_papers() -> dict:
    from elasticsearch import helpers

    raw = s3_client().get_object(Bucket=BUCKET_ARXIV_CLEAN, Key="data/part-0000.parquet")["Body"].read()
    df = pd.read_parquet(io.BytesIO(raw))
    es = _es()
    recreate_index(es, "arxiv-papers", es_mapping.ARXIV_PAPERS_PROPERTIES)
    fields = list(es_mapping.ARXIV_PAPERS_PROPERTIES)
    actions = (
        {"_index": "arxiv-papers", "_id": row["arxiv_id"], "_source": {f: _clean(row.get(f)) for f in fields}}
        for row in df.to_dict("records")
    )
    ok, errors = helpers.bulk(es, actions, chunk_size=2000, raise_on_error=False, stats_only=True)
    es.indices.refresh(index="arxiv-papers")
    count = es.count(index="arxiv-papers")["count"]
    log.info("arxiv-papers : %s indexes, %s erreurs, %s documents", ok, errors, count)
    if errors:
        raise RuntimeError(f"{errors} erreurs d'indexation")
    return {"indexed": ok, "documents": count}


def _read_report(bucket: str, key: str) -> dict | None:
    try:
        return json.loads(s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001 - rapport absent
        return None


def index_pipeline_metrics() -> dict:
    """Un document par (execution, source) pour le dashboard de monitoring."""
    from elasticsearch import helpers

    es = _es()
    recreate_index(es, "pipeline-metrics", es_mapping.PIPELINE_METRICS_PROPERTIES)
    docs = []
    for source, bucket in (("arxiv", BUCKET_ARXIV_CLEAN), ("hn", BUCKET_HN_CLEAN), ("citations", BUCKET_CITATIONS_CLEAN)):
        for key in sorted(k for k in list_keys(bucket, "_reports/run-") if k.endswith(".json")):
            rep = _read_report(bucket, key)
            if not rep:
                continue
            ge = rep.get("great_expectations") or {}
            raw, kept = int(rep.get("raw", 0)), int(rep.get("kept", 0))
            evaluated = int(ge.get("evaluated", 0))
            docs.append(
                {
                    "@timestamp": rep["run_at"],
                    "source": source,
                    "rows_bronze": raw,
                    "rows_silver": kept,
                    "rows_rejected": raw - kept,
                    "rejected_pct": round(100 * (raw - kept) / raw, 2) if raw else 0.0,
                    "ge_evaluated": evaluated,
                    "ge_passed": int(ge.get("passed", 0)),
                    "ge_failed": int(ge.get("failed_count", 0)),
                    "ge_error_rate_pct": round(100 * int(ge.get("failed_count", 0)) / evaluated, 2) if evaluated else 0.0,
                    "ge_success": bool(ge.get("success", False)),
                }
            )
    gold = _read_report(BUCKET_ARXIV_CLEAN, "_reports/gold_run.json")
    if gold:
        docs.append({"@timestamp": gold["run_at"], "source": "gold", "rows_gold": int(gold["rows"])})
    helpers.bulk(es, ({"_index": "pipeline-metrics", "_source": d} for d in docs))
    es.indices.refresh(index="pipeline-metrics")
    log.info("pipeline-metrics : %d documents", len(docs))
    return {"documents": len(docs)}
