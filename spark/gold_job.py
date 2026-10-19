"""Job Spark Gold : Silver (ArXiv + citations OpenAlex + Hacker News) -> index Elasticsearch `arxiv-papers-enriched`.

Etapes :
  1. Lecture des trois tables Silver (Parquet sur MinIO via s3a).
  2. Agregation HN par article, jointure 3 sources (ArXiv = table de reference, jointures gauches),
     comparaison sort-merge vs broadcast avec `.explain()`.
  3. Window functions : croissance annuelle par categorie (lag), percentile de velocite de
     citation par categorie (percent_rank), rang d'impact par categorie (dense_rank).
  4. TF-IDF sur titre + resume (Spark ML) -> mots-cles distinctifs par article.
  5. Score d'impact composite (voir SCORE ci-dessous) et indicateurs de cas d'usage.
  6. Ecriture dans Elasticsearch via le connecteur elasticsearch-hadoop.

SCORE D'IMPACT (0-100) = 100 * ( 0.50*C + 0.30*H + 0.20*R )
  C = log1p(citations) / log1p(p99 des citations)         -> impact scientifique valide par les pairs
  H = log1p(score+commentaires HN) / log1p(p99 HN)        -> attention de la communaute technique
  R = 0.5 ** (age_en_annees / 3)                          -> fraicheur (demi-vie de 3 ans)
Echelle logarithmique car citations et votes suivent une loi de puissance ; plafonnement au p99
pour qu'un article aberrant n'ecrase pas l'echelle ; R compense le fait qu'un article recent n'a
pas eu le temps d'etre cite.
"""
import json
import logging
import math
import os
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

log = logging.getLogger("gold")
INDEX = "arxiv-papers-enriched"
W_CIT, W_HN, W_REC = 0.50, 0.30, 0.20
HALF_LIFE_YEARS = 3.0


def env(name, default=None):
    return os.environ.get(name, default)


def spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("scipulse-gold")
        .master(env("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", env("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", env("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", env("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    return builder.getOrCreate()


def plan(df) -> str:
    return df._jdf.queryExecution().simpleString()


# ------------------------------------------------------------------ jointures
def hn_aggregate(hn):
    return (
        hn.filter(F.col("arxiv_id").isNotNull())
        .groupBy("arxiv_id")
        .agg(
            F.count("*").alias("hn_n_posts"),
            F.sum("score").alias("hn_score_total"),
            F.max("score").alias("hn_score_max"),
            F.sum("n_comments").alias("hn_comments_total"),
            F.min("created_at").alias("first_hn_at"),
        )
    )


def citations_slim(cit):
    # on retire referenced_works (liste volumineuse inutile ici) avant de joindre
    return cit.select(
        "arxiv_id", "openalex_id", "cited_by_count", "n_references",
        "topic", "subfield", "field", "domain", "counts_by_year",
    )


def compare_join_strategies(spark, arxiv, cit, hn_agg) -> dict:
    """Meme jointure executee en sort-merge puis en broadcast ; on garde les plans et les temps."""
    out = {}
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    for name, threshold, wrap in (
        ("sort_merge", "-1", lambda d: d),
        ("broadcast", "10485760", F.broadcast),
    ):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
        joined = arxiv.join(wrap(cit), "arxiv_id", "left").join(wrap(hn_agg), "arxiv_id", "left")
        t0 = time.time()
        n = joined.count()
        out[name] = {"seconds": round(time.time() - t0, 2), "rows": n, "plan": plan(joined)}
        log.info("Jointure %s : %.2fs, %d lignes\n%s", name, out[name]["seconds"], n, out[name]["plan"])
        print(f"\n===== EXPLAIN ({name}) =====\n{out[name]['plan']}\n")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    return out


# ------------------------------------------------------------------ TF-IDF
def add_top_terms(spark, df, k=8):
    text = df.select("arxiv_id", F.concat_ws(" ", F.col("title"), F.col("abstract")).alias("text"))
    pipe = Pipeline(stages=[
        RegexTokenizer(inputCol="text", outputCol="tokens", pattern="[^a-zA-Z]+", minTokenLength=3, toLowercase=True),
        StopWordsRemover(inputCol="tokens", outputCol="words"),
        CountVectorizer(inputCol="words", outputCol="tf", vocabSize=30000, minDF=5),
        IDF(inputCol="tf", outputCol="tfidf"),
    ])
    model = pipe.fit(text)
    vocab_bc = spark.sparkContext.broadcast(list(model.stages[2].vocabulary))

    def top_terms(v):
        if v is None or len(v.indices) == 0:
            return []
        best = sorted(zip(v.values, v.indices), reverse=True)[:k]
        return [vocab_bc.value[int(i)] for _, i in best]

    udf = F.udf(top_terms, ArrayType(StringType()))
    terms = model.transform(text).select("arxiv_id", udf("tfidf").alias("top_terms"))
    return df.join(terms, "arxiv_id", "left")


# ------------------------------------------------------------------ construction du Gold
def build_gold(spark, arxiv, cit, hn, now=None):
    now = now or datetime.now(timezone.utc)
    cur_year = now.year
    report = {"weights": {"citations": W_CIT, "hacker_news": W_HN, "recency": W_REC}, "half_life_years": HALF_LIFE_YEARS}

    hn_agg = hn_aggregate(hn)
    cit_s = citations_slim(cit)
    report["join_strategies"] = compare_join_strategies(spark, arxiv, cit_s, hn_agg)

    df = arxiv.join(F.broadcast(cit_s), "arxiv_id", "left").join(F.broadcast(hn_agg), "arxiv_id", "left")
    df = df.withColumn("has_citation_data", F.col("openalex_id").isNotNull()).withColumn("has_hn", F.col("hn_n_posts").isNotNull())
    df = df.cache()
    total = df.count()
    report["coverage"] = {
        "arxiv_papers": total,
        "with_citation_data": df.filter("has_citation_data").count(),
        "with_hn": df.filter("has_hn").count(),
        "with_both": df.filter("has_citation_data AND has_hn").count(),
    }
    for c in ("hn_n_posts", "hn_score_total", "hn_score_max", "hn_comments_total", "cited_by_count", "n_references"):
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
    df = df.withColumn("hn_engagement", F.col("hn_score_total") + F.col("hn_comments_total"))

    # age et velocite de citation
    submitted = F.coalesce(F.col("submitted_at"), F.to_timestamp(F.concat(F.col("year").cast("string"), F.lit("-07-01"))))
    df = df.withColumn("age_years", F.greatest(F.lit(0.0), (F.lit(int(now.timestamp())) - submitted.cast("long")) / F.lit(365.25 * 86400)))
    recent = F.aggregate(
        F.filter("counts_by_year", lambda c: c["year"] >= cur_year - 1),
        F.lit(0).cast("long"),
        lambda acc, c: acc + c["cited_by_count"].cast("long"),
    )
    df = df.withColumn("cites_recent", F.coalesce(recent, F.lit(0).cast("long")))
    window_years = F.least(F.lit(2), F.greatest(F.lit(1), F.lit(cur_year) - F.col("year") + 1))
    df = df.withColumn("citation_velocity", (F.col("cites_recent") / window_years).cast("float")).drop("counts_by_year")

    # window functions par categorie
    yearly = arxiv.groupBy("primary_category", "year").agg(F.count("*").alias("category_year_count"))
    w_year = Window.partitionBy("primary_category").orderBy("year")
    yearly = (
        yearly.withColumn("prev", F.lag("category_year_count").over(w_year))
        .withColumn("category_growth_yoy", F.when(F.col("prev") > 0, (F.col("category_year_count") - F.col("prev")) / F.col("prev")))
        .drop("prev")
    )
    df = df.join(yearly, ["primary_category", "year"], "left")
    df = df.withColumn("velocity_pct_in_category", F.percent_rank().over(Window.partitionBy("primary_category").orderBy("citation_velocity")))

    # TF-IDF
    df = add_top_terms(spark, df)

    # score d'impact
    def p(frame, col, q):
        v = frame.approxQuantile(col, [q], 0.0)  # erreur relative nulle = percentile exact
        return v[0] if v else 0.0

    cited = df.filter("has_citation_data")
    hn_only = df.filter("has_hn")
    cit_p99 = max(p(cited, "cited_by_count", 0.99), 1.0)
    hn_p99 = max(p(hn_only, "hn_engagement", 0.99), 1.0) if hn_only.count() else 1.0
    cit_p50, cit_p90 = (p(cited, "cited_by_count", 0.5), p(cited, "cited_by_count", 0.9)) if cited.count() else (0.0, 0.0)
    hn_p75 = p(hn_only, "hn_engagement", 0.75) if hn_only.count() else 0.0
    report["thresholds"] = {"cit_p99": cit_p99, "hn_p99": hn_p99, "cit_p50": cit_p50, "cit_p90": cit_p90, "hn_p75": hn_p75}

    df = (
        df.withColumn("citation_component", F.least(F.lit(1.0), F.log1p("cited_by_count") / F.lit(math.log1p(cit_p99))))
        .withColumn("hn_component", F.least(F.lit(1.0), F.log1p("hn_engagement") / F.lit(math.log1p(hn_p99))))
        .withColumn("recency_component", F.pow(F.lit(0.5), F.col("age_years") / F.lit(HALF_LIFE_YEARS)))
        .withColumn(
            "impact_score",
            F.round(F.lit(100.0) * (W_CIT * F.col("citation_component") + W_HN * F.col("hn_component") + W_REC * F.col("recency_component")), 2),
        )
    )
    df = df.withColumn("impact_rank_in_category", F.dense_rank().over(Window.partitionBy("primary_category").orderBy(F.desc("impact_score"))))
    df = df.withColumn("academic_star_no_hn", F.col("has_citation_data") & (F.col("cited_by_count") >= F.lit(cit_p90)) & (F.col("hn_n_posts") == 0))
    df = df.withColumn("viral_low_citation", F.col("has_hn") & (F.col("hn_engagement") >= F.lit(hn_p75)) & (F.col("cited_by_count") <= F.lit(cit_p50)))
    for c in ("citation_component", "hn_component", "recency_component", "age_years"):
        df = df.withColumn(c, F.round(F.col(c).cast("double"), 4).cast("float"))

    from scipulse.es_mapping import ENRICHED_PROPERTIES

    cols = [c for c in ENRICHED_PROPERTIES if c in df.columns]
    return df.select(*cols), report


# ------------------------------------------------------------------ ecriture
def _doc(row):
    d = {}
    for k, v in row.asDict().items():
        d[k] = v.isoformat() if hasattr(v, "isoformat") else v
    return d


def write_elasticsearch(gold) -> str:
    from elasticsearch import Elasticsearch, helpers

    from scipulse import es_mapping

    es = Elasticsearch(env("ES_HOST", "http://elasticsearch:9200"), request_timeout=120)
    if es.indices.exists(index=INDEX):
        es.indices.delete(index=INDEX)
    es.indices.create(index=INDEX, **es_mapping.body(es_mapping.ENRICHED_PROPERTIES))
    try:
        (
            gold.write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", env("ES_NODES", "elasticsearch"))
            .option("es.port", env("ES_PORT", "9200"))
            .option("es.nodes.wire.only", "true")
            .option("es.mapping.id", "arxiv_id")
            .mode("append")
            .save(INDEX)
        )
        method = "elasticsearch-hadoop"
    except Exception as exc:  # noqa: BLE001
        log.warning("Connecteur elasticsearch-hadoop en echec (%s) : repli sur l'API bulk", str(exc)[:300])
        helpers.bulk(es, ({"_index": INDEX, "_id": r["arxiv_id"], "_source": _doc(r)} for r in gold.toLocalIterator()))
        method = "bulk-fallback"
    es.indices.refresh(index=INDEX)
    log.info("%s : %d documents (%s)", INDEX, es.count(index=INDEX)["count"], method)
    return method


def save_report(report: dict) -> None:
    body = json.dumps(report, indent=2, default=str).encode("utf-8")
    local = env("GOLD_REPORT_PATH")
    if local:
        with open(local, "wb") as f:
            f.write(body)
        return
    from scipulse.config import BUCKET_ARXIV_CLEAN, s3_client

    s3 = s3_client()
    s3.put_object(Bucket=BUCKET_ARXIV_CLEAN, Key="_reports/gold_run.json", Body=body)
    s3.put_object(Bucket=BUCKET_ARXIV_CLEAN, Key=f"_reports/gold-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json", Body=body)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    arxiv = spark.read.parquet(env("SILVER_ARXIV", "s3a://arxiv-clean/data/"))
    hn = spark.read.parquet(env("SILVER_HN", "s3a://hn-clean/data/"))
    cit = spark.read.parquet(env("SILVER_CITATIONS", "s3a://citations-clean/data/"))
    gold, report = build_gold(spark, arxiv, cit, hn)
    gold = gold.cache()
    report["rows"] = gold.count()
    report["run_at"] = datetime.now(timezone.utc).isoformat()
    if env("GOLD_SKIP_ES") != "1":
        report["es_write_method"] = write_elasticsearch(gold)
    save_report(report)
    log.info("Gold termine : %s", {k: v for k, v in report.items() if k != "join_strategies"})
    spark.stop()


if __name__ == "__main__":
    main()
