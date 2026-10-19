# Guide Kibana : dashboards a construire

Kibana : http://localhost:5601 . Les dashboards se construisent dans l'interface (menu **Analytics > Dashboard > Create**, visualisations avec **Lens**). Apres construction, exporter les objets : **Stack Management > Saved Objects > Export** (fichier `.ndjson` a placer dans `screenshots/` avec les captures).

## 0. Data views (Stack Management > Data Views > Create)

| Nom | Index pattern | Champ temps |
|---|---|---|
| Pipeline metrics | `pipeline-metrics` | `@timestamp` |
| ArXiv papers | `arxiv-papers` | `submitted_at` |
| ArXiv enrichi | `arxiv-papers-enriched` | `submitted_at` |

Regler la plage de temps en haut a droite sur **Last 30 years** (les articles vont jusqu'a 1991).

## 1. Dashboard « Monitoring du pipeline » (data view *Pipeline metrics*)

| Visualisation | Type Lens | Configuration |
|---|---|---|
| Volume par couche et par source | Bar vertical | X : `source` ; Y : somme de `rows_bronze` et somme de `rows_silver` (deux series) |
| Taux de rejet | Bar horizontal | X : moyenne de `rejected_pct` par `source` |
| Taux d'erreur Great Expectations | Metric ou Bar | moyenne de `ge_error_rate_pct` par `source` |
| Reussite des suites GE | Donut | `ge_success` (true / false) |
| Expectations reussies / en echec | Bar empile | `ge_passed` et `ge_failed` par `source` |
| Fraicheur des donnees | Table | Par `source` : dernier `@timestamp` (Last value) |
| Volume Gold | Metric | somme de `rows_gold` |

## 2. Dashboard « Exploration ArXiv » (data view *ArXiv papers*)

| Visualisation | Type Lens | Configuration |
|---|---|---|
| Articles par annee | Area / Line | X : `submitted_at` (annuel) ; Y : count |
| Top categories | Bar horizontal | `primary_category` (top 15) ; count |
| Repartition par groupe | Donut | `category_group` |
| Evolution des 5 categories principales | Line, split series | X : `submitted_at` ; split : `primary_category` (top 5) |
| Nombre d'auteurs | Histogram | `n_authors` |
| Recherche plein texte | barre de recherche KQL | ex. `abstract : "diffusion models"` |

## 3. Dashboard « Impact scientifique » (data view *ArXiv enrichi*)

| Visualisation | Type Lens | Configuration |
|---|---|---|
| Distribution du score d'impact | Histogram | `impact_score` |
| Top 20 articles | Table | colonnes `title.raw`, `primary_category`, `cited_by_count`, `hn_engagement`, `impact_score` ; tri decroissant sur `impact_score` |
| Score moyen par categorie | Bar | `primary_category` ; moyenne de `impact_score` |
| Citations vs engagement HN | Scatter (XY) | X : `cited_by_count` ; Y : `hn_engagement` (echelle log) |
| Cas « tres cites, absents de HN » | Table filtree | filtre `academic_star_no_hn : true` |
| Cas « viraux, peu cites » | Table filtree | filtre `viral_low_citation : true` |
| Croissance par categorie | Line | X : `year` ; Y : moyenne de `category_growth_yoy` ; split `primary_category` |
| Termes distinctifs | Tag cloud / Bar | `top_terms` (top 30) |
| Domaines disciplinaires | Treemap | `field` > `subfield` |

## 4. Captures d'ecran a fournir (dossier `screenshots/`)

1. `airflow_dags.png` : liste des DAGs, tous en succes.
2. `airflow_graph_99.png` : vue graphe de `99_pipeline_full`.
3. `minio_buckets.png` : buckets et fichiers Parquet (console http://localhost:9001).
4. `ge_data_docs.png` : page Data Docs de Great Expectations.
5. `spark_ui.png` ou la sortie `.explain()` dans les logs du DAG `31_gold_spark`.
6. `kibana_monitoring.png`, `kibana_exploration.png`, `kibana_impact.png` : les trois dashboards.
