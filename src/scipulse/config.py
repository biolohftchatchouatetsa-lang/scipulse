"""Configuration et helpers MinIO partagés par toutes les étapes du pipeline."""
import os
from typing import Iterable, Iterator

import boto3
from botocore.config import Config

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")

BUCKET_ARXIV_RAW = "arxiv-raw"
BUCKET_HN_RAW = "hn-raw"
BUCKET_CITATIONS_RAW = "citations-raw"
BUCKET_ARXIV_CLEAN = "arxiv-clean"
BUCKET_HN_CLEAN = "hn-clean"
BUCKET_CITATIONS_CLEAN = "citations-clean"


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_ROOT_USER"),
        aws_secret_access_key=env("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def put_jsonl(bucket: str, key: str, lines: Iterable[str]) -> int:
    """Écrit des lignes JSON (déjà sérialisées) dans MinIO. Retourne le nombre de lignes."""
    lines = list(lines)
    body = ("\n".join(lines) + "\n").encode("utf-8")
    s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    return len(lines)


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    s3 = s3_client()
    keys, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            return keys
        token = resp["NextContinuationToken"]


def delete_prefix(bucket: str, prefix: str) -> int:
    keys = list_keys(bucket, prefix)
    s3 = s3_client()
    for i in range(0, len(keys), 1000):
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
        )
    return len(keys)


def iter_jsonl(bucket: str, key: str) -> Iterator[str]:
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"]
    for raw in body.iter_lines():
        if raw:
            yield raw.decode("utf-8")
