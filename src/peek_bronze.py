import json
from collections import Counter

from scipulse.config import iter_jsonl, list_keys


def short(v, n=100):
    s = json.dumps(v, ensure_ascii=True)
    return s if len(s) <= n else s[:n] + "...(tronque)"


def peek(bucket):
    keys = sorted(k for k in list_keys(bucket) if k.endswith(".jsonl"))
    print(f"\n=== {bucket} : {len(keys)} fichiers")
    if not keys:
        return
    count = 0
    sources = Counter()
    samples = {}
    for key in keys:
        for line in iter_jsonl(bucket, key):
            rec = json.loads(line)
            count += 1
            src = rec.get("_source", "dump")
            sources[src] += 1
            samples.setdefault(src, rec)
    print(f"lignes : {count} | par source : {dict(sources)}")
    print("fichiers (5 premiers) :", keys[:5])
    for src, rec in samples.items():
        print(f"--- exemple [{src}] ---")
        for k, v in rec.items():
            print(f"  {k}: {short(v)}")


for b in ("arxiv-raw", "hn-raw", "citations-raw"):
    peek(b)
