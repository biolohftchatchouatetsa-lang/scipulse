"""Bronze ArXiv : dump Kaggle (JSON Lines) -> MinIO arxiv-raw/, lignes inchangées.

Le dump complet (~2,7 M d'articles, > 4 Go) est trop lourd pour un portable. On en
extrait un échantillon reproductible (reservoir sampling, graine fixe) sur le périmètre
informatique (catégorie principale cs.*), cohérent avec le signal Hacker News.
Les lignes retenues sont écrites TELLES QUELLES : aucune transformation.
"""
import json
import logging
import os
import random

from scipulse.config import BUCKET_ARXIV_RAW, DATA_DIR, delete_prefix, env, put_jsonl

log = logging.getLogger(__name__)
CHUNK_SIZE = 10_000


def dump_path() -> str:
    return os.path.join(DATA_DIR, env("ARXIV_DUMP_FILE", "arxiv-metadata-oai-snapshot.json"))


def check_dump() -> str:
    path = dump_path()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Dump ArXiv introuvable : {path}\n"
            "Téléchargez-le depuis https://www.kaggle.com/datasets/Cornell-University/arxiv "
            "(arxiv-metadata-oai-snapshot.json) et placez-le dans le dossier ./data/"
        )
    log.info("Dump trouvé : %s (%.1f Go)", path, os.path.getsize(path) / 1e9)
    return path


def sample_dump(path: str, sample_size: int, category_prefix: str = "cs.", seed: int = 42):
    """Reservoir sampling (algorithme R) : échantillon uniforme en un seul passage.

    Retourne (lignes_retenues, nb_lignes_éligibles_vues).
    """
    if sample_size <= 0:
        raise ValueError("ARXIV_SAMPLE_SIZE doit être > 0")
    rng = random.Random(seed)
    reservoir: list[str] = []
    seen = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if category_prefix:
                try:
                    categories = json.loads(line).get("categories") or ""
                except json.JSONDecodeError:
                    continue
                if not categories.split(" ")[0].startswith(category_prefix):
                    continue
            seen += 1
            if len(reservoir) < sample_size:
                reservoir.append(line)
            else:
                j = rng.randint(0, seen - 1)
                if j < sample_size:
                    reservoir[j] = line
    return reservoir, seen


def ingest(ds: str) -> dict:
    """Écrit l'échantillon dans arxiv-raw/dt=<ds>/part-XXXX.jsonl (idempotent)."""
    path = check_dump()
    sample_size = int(env("ARXIV_SAMPLE_SIZE", "50000"))
    prefix_cat = os.environ.get("ARXIV_CATEGORY_PREFIX", "cs.")
    lines, seen = sample_dump(path, sample_size, prefix_cat)
    log.info("%d lignes éligibles (catégorie '%s*'), %d retenues", seen, prefix_cat, len(lines))

    partition = f"dt={ds}/"
    removed = delete_prefix(BUCKET_ARXIV_RAW, partition)  # idempotence : on repart propre
    if removed:
        log.info("Partition %s purgée (%d fichiers)", partition, removed)

    written = 0
    for i in range(0, len(lines), CHUNK_SIZE):
        written += put_jsonl(BUCKET_ARXIV_RAW, f"{partition}part-{i // CHUNK_SIZE:04d}.jsonl", lines[i : i + CHUNK_SIZE])
    return {"eligible": seen, "written": written, "partition": partition}
