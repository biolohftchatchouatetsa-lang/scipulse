#!/usr/bin/env bash
# Vérifie que chaque service de la stack répond.
check() {
  local name="$1" url="$2"
  if curl -fsS -o /dev/null --max-time 5 "$url"; then
    echo "  [OK]   $name"
  else
    echo "  [FAIL] $name ($url)"
    FAIL=1
  fi
}
FAIL=0
echo "Santé de la stack SciPulse :"
check "Elasticsearch" "http://localhost:9200/_cluster/health"
check "Kibana"        "http://localhost:5601/api/status"
check "MinIO"         "http://localhost:9000/minio/health/live"
check "Airflow"       "http://localhost:8080/health"
check "Spark master"  "http://localhost:8081"
exit $FAIL
