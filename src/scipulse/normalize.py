"""Normalisation des cles de jointure : identifiant ArXiv, DOI, texte.

Ces fonctions sont le point de raccordement entre les trois sources :
    ArXiv.id  <->  HN.url (extraction)  <->  OpenAlex.locations / doi
"""
import re

_ID = r"(?P<id>\d{4}\.\d{4,5}|[a-z\-]+(?:\.[a-z]{2})?/\d{7})"
_URL_RE = re.compile(
    r"(?:arxiv\.org|ar5iv\.org|ar5iv\.labs\.arxiv\.org|alphaxiv\.org)/(?:abs|pdf|html|format)/" + _ID + r"(?:v\d+)?",
    re.IGNORECASE,
)
_DOI_ARXIV_RE = re.compile(r"10\.48550/arxiv\.(?P<id>.+)$", re.IGNORECASE)
_VALID_ID = re.compile(r"^(\d{4}\.\d{4,5}|[a-z\-]+/\d{7})$")
ARXIV_ID_REGEX = r"^(\d{4}\.\d{4,5}|[a-z\-]+/\d{7})$"  # reutilise par Great Expectations


def normalize_arxiv_id(raw) -> str | None:
    """'arXiv:2511.04317v2' -> '2511.04317' ; 'math.AG/0601001' -> 'math/0601001'."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    s = re.sub(r"^arxiv:", "", s)
    s = re.sub(r"v\d+$", "", s)
    s = re.sub(r"^([a-z\-]+)\.[a-z]{2}/", r"\1/", s)
    return s if _VALID_ID.match(s) else None


def arxiv_id_from_url(url) -> str | None:
    """Extrait l'identifiant ArXiv d'une URL (abs, pdf, html, ar5iv...)."""
    if not isinstance(url, str):
        return None
    m = _URL_RE.search(url)
    return normalize_arxiv_id(m.group("id")) if m else None


def arxiv_id_from_doi(doi) -> str | None:
    """'10.48550/arXiv.2511.04317' -> '2511.04317'."""
    if not isinstance(doi, str):
        return None
    m = _DOI_ARXIV_RE.search(doi.strip())
    return normalize_arxiv_id(m.group("id")) if m else None


def normalize_doi(raw) -> str | None:
    """Minuscule, sans prefixe https://doi.org/ (consigne du brief)."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    s = re.sub(r"^(https?://)?(dx\.)?doi\.org/", "", s)
    s = re.sub(r"^doi:\s*", "", s)
    return s or None


def clean_text(s) -> str | None:
    """Espaces/retours a la ligne multiples -> un espace ; vide -> None."""
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def year_from_arxiv_id(arxiv_id: str) -> int | None:
    """L'identifiant encode l'annee : 2511.04317 -> 2025 ; hep-th/9901001 -> 1999."""
    try:
        if "/" in arxiv_id:
            yy = int(arxiv_id.split("/")[1][:2])
            return 1900 + yy if yy >= 91 else 2000 + yy
        return 2000 + int(arxiv_id[:2])
    except (ValueError, IndexError):
        return None
