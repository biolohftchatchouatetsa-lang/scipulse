"""Validation de la couche Silver avec Great Expectations (0.18) + Data Docs.

Une suite par source :
  ArXiv : 10 expectations | Hacker News : 9 | Citations : 7   (brief : >= 5, >= 5, >= 3)
Les seuils `mostly` tolerent un faible taux d'anomalies residuelles ; une expectation en echec
est consignee dans le rapport de qualite (et dans les Data Docs) sans bloquer le pipeline,
sauf si GE_STRICT=true.

Le projet Great Expectations (suites JSON, resultats, Data Docs) est stocke dans GX_ROOT
(par defaut un volume persistant d'Airflow). Pour l'exporter dans le depot :
    docker compose cp airflow-scheduler:/opt/airflow/logs/great_expectations ./great_expectations_export
"""
import contextlib
import fcntl
import logging
import os

import pandas as pd

from scipulse.normalize import ARXIV_ID_REGEX

log = logging.getLogger(__name__)
CATEGORY_REGEX = r"^[A-Za-z\-]+(\.[A-Za-z\-]+)?$"
DOI_REGEX = r"^10\.\d{4,9}/\S+$"


# ---------------------------------------------------------------- suites d'expectations
def arxiv_expectations(v):
    v.expect_column_values_to_not_be_null("arxiv_id")
    v.expect_column_values_to_be_unique("arxiv_id")
    v.expect_column_values_to_match_regex("arxiv_id", ARXIV_ID_REGEX)
    v.expect_column_values_to_not_be_null("title")
    v.expect_column_values_to_not_be_null("primary_category")
    v.expect_column_values_to_match_regex("primary_category", CATEGORY_REGEX, mostly=0.99)
    v.expect_column_values_to_be_between("year", min_value=1991, max_value=2026)
    v.expect_column_values_to_not_be_null("submitted_at", mostly=0.99)
    v.expect_column_value_lengths_to_be_between("abstract", min_value=20, mostly=0.99)
    v.expect_column_values_to_be_between("n_authors", min_value=1, mostly=0.99)


def hn_expectations(v):
    v.expect_column_values_to_not_be_null("hn_id")
    v.expect_column_values_to_be_unique("hn_id")
    v.expect_column_values_to_not_be_null("title")
    v.expect_column_values_to_be_between("score", min_value=0, max_value=100000)
    v.expect_column_values_to_be_between("n_comments", min_value=0, max_value=100000)
    v.expect_column_values_to_not_be_null("created_at")
    v.expect_column_values_to_be_between("year", min_value=2006, max_value=2026)
    v.expect_column_values_to_match_regex("arxiv_id", ARXIV_ID_REGEX)  # valeurs nulles ignorees
    v.expect_column_values_to_be_in_set("source", ["firebase", "algolia"])


def citations_expectations(v):
    v.expect_column_values_to_not_be_null("arxiv_id")
    v.expect_column_values_to_be_unique("arxiv_id")
    v.expect_column_values_to_not_be_null("openalex_id")
    v.expect_column_values_to_be_between("cited_by_count", min_value=0)
    v.expect_column_values_to_be_between("publication_year", min_value=1900, max_value=2026, mostly=0.99)
    v.expect_column_values_to_match_regex("doi", DOI_REGEX, mostly=0.95)
    v.expect_column_values_to_be_between("n_references", min_value=0)


# ---------------------------------------------------------------- execution
def _context():
    import great_expectations as gx

    root = os.environ.get("GX_ROOT", "/opt/airflow/logs/great_expectations")
    os.makedirs(root, exist_ok=True)
    return gx.get_context(project_root_dir=root)


def _scalar_columns(df: pd.DataFrame) -> pd.DataFrame:
    """GE valide des colonnes scalaires : on exclut les colonnes de type liste."""
    keep = [c for c in df.columns if not df[c].map(lambda x: isinstance(x, (list, dict))).any()]
    return df[keep]


@contextlib.contextmanager
def _gx_lock():
    """Verrou exclusif : le projet GE (fichiers YAML/JSON) ne supporte pas les ecritures concurrentes."""
    root = os.environ.get("GX_ROOT", "/opt/airflow/logs/great_expectations")
    os.makedirs(root, exist_ok=True)
    with open(os.path.join(root, ".gx.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def validate(source: str, df: pd.DataFrame, expectations_fn) -> dict:
    with _gx_lock():
        return _validate_unlocked(source, df, expectations_fn)


def _validate_unlocked(source: str, df: pd.DataFrame, expectations_fn) -> dict:
    ctx = _context()
    suite_name = f"{source}_silver_suite"
    ds = ctx.sources.add_or_update_pandas(name="silver_pandas")
    try:
        asset = ds.get_asset(f"{source}_silver")
    except LookupError:
        asset = ds.add_dataframe_asset(name=f"{source}_silver")
    batch_request = asset.build_batch_request(dataframe=_scalar_columns(df))

    ctx.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = ctx.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    expectations_fn(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = ctx.add_or_update_checkpoint(name=f"{source}_silver_checkpoint", validator=validator)
    result = checkpoint.run()
    ctx.build_data_docs()

    vr = result.list_validation_results()[0]
    failed = [
        {
            "expectation": r.expectation_config.expectation_type,
            "column": r.expectation_config.kwargs.get("column"),
            "unexpected_percent": r.result.get("unexpected_percent"),
            "unexpected_count": r.result.get("unexpected_count"),
        }
        for r in vr.results
        if not r.success
    ]
    stats = vr.statistics
    summary = {
        "suite": suite_name,
        "success": bool(vr.success),
        "evaluated": stats["evaluated_expectations"],
        "passed": stats["successful_expectations"],
        "failed_count": stats["unsuccessful_expectations"],
        "success_percent": round(float(stats["success_percent"]), 2),
        "failed": failed,
    }
    level = logging.INFO if summary["success"] else logging.WARNING
    log.log(level, "Great Expectations %s : %s/%s OK %s", source, summary["passed"], summary["evaluated"], failed or "")
    if os.environ.get("GE_STRICT", "false").lower() == "true" and not summary["success"]:
        raise RuntimeError(f"Validation Great Expectations en echec pour {source}: {failed}")
    return summary
