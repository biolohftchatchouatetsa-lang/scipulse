# SciPulse & SciPulse Insights

Pipeline DataOps de veille scientifique : **ArXiv** (articles) + **OpenAlex** (citations) + **Hacker News** (attention de la communaute technique), organise en architecture **Medallion** (Bronze / Silver / Gold), orchestre par **Airflow**, transforme avec **Spark**, valide avec **Great Expectations** et expose dans **Elasticsearch / Kibana**.

## Architecture

```
 Sources                 Bronze (MinIO, JSON Lines)      Silver (MinIO, Parquet)        Gold (Elasticsearch)
 -------                 --------------------------      ----------------------        --------------------
 Dump ArXiv (Kaggle) --> arxiv-raw/dt=<date>/        --> arxiv-clean/data/        --+--> arxiv-papers           (Module 1)
 OpenAlex API        --> citations-raw/dt=<date>/    --> citations-clean/data/    --+
 HN Firebase (5 min) --> hn-raw/dt=.../hour=.../     --> hn-clean/data/           --+--> arxiv-papers-enriched  (Module 2, Spark)
 HN Algolia (histo.) --> hn-raw/backfill/                                               pipeline-metrics        (monitoring)
                                  Airflow orchestre tout   |   Great Expectations valide chaque Silver
```

| Service | Role | URL |
|---|---|---|
| Airflow | orchestration | http://localhost:8080 (admin / admin) |
| MinIO (Silo) | data lake S3 | http://localhost:9001 (minioadmin / minioadmin123) |
| Elasticsearch | serving Gold | http://localhost:9200 |
| Kibana | dashboards | http://localhost:5601 |
| Spark master | UI du mini-cluster (optionnel) | http://localhost:8081 |
| PostgreSQL | metastore Airflow | interne |

## Demarrage

Prerequis : Docker Desktop (8 Go de RAM alloues), le dump ArXiv (`arxiv-metadata-oai-snapshot.json`, archive Kaggle *Cornell-University/arxiv*) place dans `data/`.

```bash
cp .env.example .env            # Windows PowerShell : copy .env.example .env
docker compose up -d --build    # ou : make up
```
Premier demarrage : 10 a 15 minutes (images + build Airflow avec Java, pyspark et les JARs S3/Elasticsearch). Verifier : `docker compose ps` (tous `running`/`healthy`).

> Si une commande `airflow dags trigger` repond `DagNotFound`, lancer d'abord `airflow dags reserialize` (la base d'Airflow n'est pas encore a jour), par ex. : `docker compose exec airflow-scheduler airflow dags reserialize`.

## Executer le pipeline

Tous les DAGs doivent etre **actifs** (interrupteur bleu dans Airflow). Option 1 : un seul DAG chef d'orchestre.
```bash
docker compose exec airflow-scheduler airflow dags trigger 99_pipeline_full
```
Option 2 : DAG par DAG, dans cet ordre.

| Ordre | DAG | Role | Duree indicative |
|---|---|---|---|
| 0 | `00_smoke_test` | MinIO, Elasticsearch, Java/Spark joignables | < 1 min |
| 1 | `10_arxiv_bronze` | echantillon reproductible du dump -> `arxiv-raw` | ~25 min |
| 2 | `12_hn_backfill` | historique HN pointant vers arxiv.org (Algolia) | ~2 min |
| 3 | `13_openalex_bronze` | citations OpenAlex par lots de 50 DOI | 5-15 min |
| (continu) | `11_hn_bronze` | micro-batch HN toutes les 5 minutes (Firebase) | - |
| 4 | `20_silver` | nettoyage + Great Expectations + Parquet | ~3 min |
| 5 | `30_index_arxiv_papers` | Silver ArXiv -> index `arxiv-papers` + `pipeline-metrics` | ~2 min |
| 6 | `31_gold_spark` | job Spark -> `arxiv-papers-enriched` | ~5 min |

## Choix techniques (et justification)

- **Echantillon ArXiv** : le dump complet (2,7 M d'articles, 5,6 Go) est trop lourd pour un portable. *Reservoir sampling* a graine fixe (reproductible) de 50 000 articles de la categorie principale `cs.*`, qui correspond au public de Hacker News. Reglable : `ARXIV_SAMPLE_SIZE`, `ARXIV_CATEGORY_PREFIX`.
- **Backfill Hacker News** : le flux temps reel contient tres peu d'articles ArXiv ; sans l'historique Algolia, la jointure serait quasi vide.
- **OpenAlex** plutot que Semantic Scholar : gratuit, sans cle, filtre par lot de DOI, champ `referenced_works` (graphe de citations). Le DOI ArXiv est deterministe (`10.48550/arxiv.<id>`). *Point d'attention decouvert sur les donnees reelles* : le champ `doi` d'OpenAlex est le DOI de la revue, pas l'identifiant ArXiv ; l'identifiant est donc retrouve via `locations`.
- **Bronze fidele** : enregistrements bruts, seuls `_source` et `_ingested_at` sont ajoutes (tracabilite). Ingestion idempotente (la partition `dt=<date>` est ecrasee).
- **Silver** : une table propre par source, regles justifiees par l'analyse du Bronze (voir ci-dessous et `notebooks/01`). Parquet avec timestamps en microsecondes (Spark 3.5 ne lit pas les nanosecondes).
- **Great Expectations 0.18** : ArXiv 10 expectations, HN 9, citations 7 (minimum du brief : 5, 5, 3). Les anomalies sont consignees sans bloquer le pipeline (`GE_STRICT=true` pour le rendre bloquant). Data Docs : voir *Great Expectations* plus bas.
- **Spark en mode local** : `SPARK_MASTER=local[2]` par defaut (portable). Le mini-cluster `spark-master`/`spark-worker` est demarre et utilisable via `SPARK_MASTER=spark://spark-master:7077` sur une machine plus puissante.
- **Logstash** : cite dans les objectifs du brief mais sans etape correspondante ; l'ingestion passe par Airflow (Python) et l'ecriture Elasticsearch par Spark / l'API bulk. Aucune brique Logstash n'est necessaire.
- **MinIO -> Silo** : les images officielles `minio/minio` ont ete retirees de Docker Hub et de quay.io. Le data lake utilise `pgsty/silo`, un fork communautaire maintenu, compatible (API S3, variables `MINIO_*`, client `mc`).

### Nettoyage Silver (extrait ; detail dans `src/scipulse/silver.py`)

| Source | Probleme observe | Traitement |
|---|---|---|
| ArXiv | retours a la ligne / espaces multiples | normalisation des espaces |
| ArXiv | dates RFC-2822 en chaine | timestamp UTC |
| ArXiv | `categories` en une chaine | liste + `primary_category` + `category_group` |
| ArXiv | date absente | annee deduite de l'identifiant |
| HN | deux schemas (Firebase / Algolia) | schema unique |
| HN | identifiant ArXiv dans l'URL (`/abs/`, `/pdf/..pdf`, `v2`) | extraction par regex -> `arxiv_id` |
| HN | meme story lue plusieurs fois | lecture la plus recente conservee |
| OpenAlex | `doi` = DOI de la revue | `arxiv_id` via `locations` ; DOI normalise (minuscule, sans prefixe) |
| Toutes | doublons, lignes inexploitables | dedoublonnage, rejet compte dans le rapport |

### Gold : Spark (`spark/gold_job.py`)

1. **Jointure** : `arxiv` (table de reference) joint en jointure gauche aux citations et a l'agregat HN par article ; comparaison *sort-merge* vs *broadcast* avec `.explain()` (plans et temps conserves dans le rapport `gold_run.json`).
2. **Window functions** : `lag` (croissance annuelle par categorie), `percent_rank` (percentile de velocite de citation par categorie), `dense_rank` (rang d'impact par categorie).
3. **TF-IDF** (Spark ML : tokenisation, mots vides, `CountVectorizer`, `IDF`) -> 8 termes distinctifs par article (`top_terms`).
4. **Score d'impact** = `100 x (0,50 C + 0,30 H + 0,20 R)` ; `C = log1p(citations)/log1p(p99)`, `H = log1p(score+commentaires HN)/log1p(p99)`, `R = 0,5^(age/3 ans)`. Citations : signal valide par les pairs (poids le plus fort) ; HN : attention precoce ; fraicheur : compense l'absence de citations des articles recents. Echelle log (lois de puissance), plafond au 99e percentile.
5. **Ecriture** via le connecteur `elasticsearch-hadoop` dans un index au **mapping explicite** (`src/scipulse/es_mapping.py`) ; repli automatique sur l'API bulk si le connecteur echoue (le mode utilise est consigne dans le rapport).

## Validation

`notebooks/03_validation.ipynb` : coherence entre couches (comptes, absence de doublons, couverture, nombre de posts HN exact), conformite du score (bornes, recalcul de la formule), **stabilite** sur deux moities disjointes du corpus (test de Kolmogorov-Smirnov, correlation score x citations), verification manuelle d'un echantillon de rattachements HN -> ArXiv, pertinence de la recherche plein texte, procedure d'idempotence.
Tests automatises du code (sans Docker) : normalisation, nettoyage, ecriture Parquet, Great Expectations et job Spark ont ete executes sur des donnees synthetiques reprenant les schemas reels.

## Great Expectations

Le projet GE (suites, resultats, Data Docs) est dans le volume persistant d'Airflow. Pour l'exporter dans le depot :
```bash
docker compose cp airflow-scheduler:/opt/airflow/logs/great_expectations ./great_expectations_export
```
Les Data Docs (a ouvrir dans un navigateur) : `great_expectations_export/gx/uncommitted/data_docs/local_site/index.html`. Les suites d'expectations (JSON, versionnables) : `great_expectations_export/gx/expectations/{arxiv,hn,citations}_silver_suite.json`.

## Livrables et organisation du depot

```
dags/            DAGs Airflow (00 test, 10-13 Bronze, 20 Silver, 30-31 Gold, 99 orchestration)
src/scipulse/    code metier : ingestion, normalisation, Silver, qualite GE, mappings et indexation ES
spark/           job Spark Gold
notebooks/       01 qualite, 02 pipeline Spark, 03 validation
docs/            guide Kibana (dashboards et captures a produire)
screenshots/     captures Airflow, MinIO, GE, Kibana
airflow/         Dockerfile et dependances de l'image Airflow
docker-compose.yml, Makefile, .env.example
```

## Problemes rencontres (et solutions)

| Probleme | Cause | Solution |
|---|---|---|
| `pull access denied for minio/minio` | images officielles retirees | fork `pgsty/silo` |
| Scheduler en boucle de redemarrage, `PermissionError` sur `/opt/airflow/logs` | dossier monte depuis Windows non inscriptible par l'utilisateur Airflow | volume Docker nomme `airflow-logs` + `chown` dans `airflow-init` |
| `DagNotFound` apres declenchement | DAGs lus mais pas encore serialises en base | `airflow dags reserialize` avant `trigger` |
| `doi` OpenAlex ne correspond pas a l'identifiant ArXiv | OpenAlex fusionne preprint et version publiee | extraction via `locations` |
| Timestamps illisibles par Spark | pandas ecrit en nanosecondes | `coerce_timestamps="us"` |

## Limites connues

- Echantillon (50 000 articles `cs.*`) : les resultats ne sont pas representatifs de tout ArXiv.
- Couverture Hacker News faible par construction (peu d'articles ArXiv y sont discutes) ; le score traite l'absence de donnee comme un zero.
- Un article sans donnee OpenAlex a `C = 0` ; le drapeau `has_citation_data` permet de les isoler.
- Les poids du score (0,5 / 0,3 / 0,2) sont un choix argumente, non optimises sur une verite terrain.
