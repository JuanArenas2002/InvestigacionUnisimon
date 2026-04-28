# ══════════════════════════════════════════════════════════════════
# SCOPUS — Estadísticas de autor por Scopus Author ID
# Listo para Google Colab — copy & paste
# ══════════════════════════════════════════════════════════════════

import os
import requests

# ── CONFIGURACIÓN ─────────────────────────────────────────────────
API_KEY   = os.environ["SCOPUS_API_KEY"]   # export SCOPUS_API_KEY=<tu_key>
AUTHOR_ID = "57193767797"                  # ← cambia este ID
# ──────────────────────────────────────────────────────────────────

BASE_URL = "https://api.elsevier.com/content"
HEADERS  = {"X-ELS-APIKey": API_KEY, "Accept": "application/json"}


# ── 1. MÉTRICAS ────────────────────────────────────────────────────
def get_metrics(author_id):
    r = requests.get(
        f"{BASE_URL}/author/author_id/{author_id}",
        headers=HEADERS,
        params={"view": "METRICS"},
        timeout=15,
    )
    r.raise_for_status()
    entry = r.json().get("author-retrieval-response", [{}])
    entry = entry[0] if isinstance(entry, list) else entry
    core  = entry.get("coredata", {})
    return {
        "h_index":        int(entry.get("h-index", 0)),
        "document_count": int(core.get("document-count", 0)),
        "citation_count": int(core.get("citation-count", 0)),
        "cited_by_count": int(core.get("cited-by-count", 0)),
        "coauthor_count": int(core.get("coauthor-count") or 0),
    }


# ── 2. PERFIL ──────────────────────────────────────────────────────
def get_profile(author_id):
    r = requests.get(
        f"{BASE_URL}/author/author_id/{author_id}",
        headers=HEADERS,
        params={"view": "ENHANCED"},
        timeout=15,
    )
    r.raise_for_status()
    entry   = r.json().get("author-retrieval-response", [{}])
    entry   = entry[0] if isinstance(entry, list) else entry
    profile = entry.get("author-profile", {})
    pref    = profile.get("preferred-name", {})
    name    = f"{pref.get('given-name','')} {pref.get('surname','')}".strip() \
              or pref.get("indexed-name", "")

    inst = ""
    aff_list = (profile.get("affiliation-current") or {}).get("affiliation", [])
    if isinstance(aff_list, dict):
        aff_list = [aff_list]
    if aff_list:
        ip   = aff_list[0].get("ip-doc", {})
        inst = ip.get("afdispname") or ip.get("sort-name", "")

    areas_raw = entry.get("subject-areas", {}).get("subject-area", [])
    if isinstance(areas_raw, dict):
        areas_raw = [areas_raw]
    areas = list(dict.fromkeys(
        a.get("$", "") or a.get("@abbrev", "")
        for a in areas_raw if isinstance(a, dict)
    ))[:8]

    pub_range = profile.get("publication-range", {})

    return {
        "name":      name,
        "inst":      inst,
        "orcid":     profile.get("orcid", "N/A"),
        "areas":     areas,
        "year_from": pub_range.get("@start", "?"),
        "year_to":   pub_range.get("@end",   "?"),
    }


# ── 3. PUBLICACIONES ───────────────────────────────────────────────
def get_publications(author_id, max_results=200):
    pubs, start, count = [], 0, 25
    while len(pubs) < max_results:
        r = requests.get(
            f"{BASE_URL}/search/scopus",
            headers=HEADERS,
            params={
                "query": f"AU-ID({author_id})",
                "start": start,
                "count": count,
                "sort":  "-citedby-count",
                "field": "dc:title,prism:doi,prism:coverDate,"
                         "citedby-count,prism:publicationName,subtypeDescription",
            },
            timeout=20,
        )
        r.raise_for_status()
        results = r.json().get("search-results", {})
        total   = int(results.get("opensearch:totalResults", 0))
        entries = results.get("entry", [])
        if not entries or entries[0].get("error"):
            break
        for e in entries:
            pubs.append({
                "title":    e.get("dc:title", ""),
                "doi":      e.get("prism:doi", ""),
                "year":     (e.get("prism:coverDate", "") or "")[:4],
                "journal":  e.get("prism:publicationName", ""),
                "type":     e.get("subtypeDescription", ""),
                "cited_by": int(e.get("citedby-count", 0)),
            })
        start += count
        if start >= min(total, max_results):
            break
    return pubs


# ══════════════════════════════════════════════════════════════════
# EJECUTAR Y MOSTRAR
# ══════════════════════════════════════════════════════════════════

metrics = get_metrics(AUTHOR_ID)
profile = get_profile(AUTHOR_ID)
pubs    = get_publications(AUTHOR_ID)

cpp     = round(metrics["citation_count"] / metrics["document_count"], 2) \
          if metrics["document_count"] else 0
top_pub = max(pubs, key=lambda p: p["cited_by"]) if pubs else {}

# Distribución por tipo
from collections import Counter
tipos = Counter(p["type"] for p in pubs)

# Distribución por año
from collections import defaultdict
by_year = defaultdict(int)
for p in pubs:
    if p["year"]:
        by_year[p["year"]] += 1

print("=" * 60)
print(f"  AUTOR       : {profile['name']}")
print(f"  INSTITUCIÓN : {profile['inst']}")
print(f"  ORCID       : {profile['orcid']}")
print(f"  ACTIVIDAD   : {profile['year_from']} – {profile['year_to']}")
print(f"  ÁREAS       : {', '.join(profile['areas'][:3])}")
print("=" * 60)
print(f"  Índice H        : {metrics['h_index']}")
print(f"  Publicaciones   : {metrics['document_count']}")
print(f"  Citas totales   : {metrics['citation_count']}")
print(f"  Veces citado    : {metrics['cited_by_count']}")
print(f"  Co-autores      : {metrics['coauthor_count'] or 'N/D'}")
print(f"  CPP             : {cpp}")
print("-" * 60)
print("  TIPOS DE PUBLICACIÓN:")
for tipo, n in tipos.most_common():
    print(f"    {tipo:<30} {n}")
print("-" * 60)
if top_pub:
    print(f"  PUB MÁS CITADA  : {top_pub['cited_by']} citas ({top_pub['year']})")
    print(f"  Título          : {top_pub['title'][:65]}")
    print(f"  DOI             : {top_pub['doi'] or 'N/A'}")
print("=" * 60)

# ── Tabla con pandas ───────────────────────────────────────────────
try:
    import pandas as pd
    df = pd.DataFrame(pubs).sort_values("cited_by", ascending=False)
    df.index = range(1, len(df) + 1)
    print(f"\nTop 20 publicaciones (de {len(df)} totales):\n")
    print(df[["year","cited_by","title","journal"]].head(20).to_string())
except ImportError:
    print("\npandas no disponible — instala con: pip install pandas")


