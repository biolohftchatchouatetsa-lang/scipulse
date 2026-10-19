"""Sonde : une seule requete a OpenAlex, pour voir la reponse exacte (statut, limites, message)."""
import os

import requests

ids = ["2511.04317", "2408.09469"]
mailto = os.environ.get("OPENALEX_MAILTO", "")
params = {
    "filter": "doi:" + "|".join(f"https://doi.org/10.48550/arxiv.{i}" for i in ids),
    "per-page": 2,
    "select": "id,doi",
    "mailto": mailto,
}
if os.environ.get("OPENALEX_API_KEY"):
    params["api_key"] = os.environ["OPENALEX_API_KEY"]
r = requests.get("https://api.openalex.org/works", params=params, timeout=30)
print("statut HTTP :", r.status_code)
print("mailto envoye :", repr(mailto))
for k, v in r.headers.items():
    if k.lower().startswith(("retry-after", "x-ratelimit", "ratelimit", "x-openalex")):
        print(f"{k}: {v}")
print("corps (400 premiers caracteres) :", r.text[:400])