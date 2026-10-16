.PHONY: up down restart ps logs health reset env

env:
	@test -f .env || (cp .env.example .env && echo ".env créé depuis .env.example")

# Démarre toute la stack de zéro (build inclus)
up: env
	docker compose up -d --build
	@echo ""
	@echo "Stack en cours de démarrage (1 à 3 min la première fois)."
	@echo "  Airflow       http://localhost:8080  (admin / admin)"
	@echo "  MinIO         http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  Kibana        http://localhost:5601"
	@echo "  Elasticsearch http://localhost:9200"
	@echo "  Spark UI      http://localhost:8081"
	@echo "Vérifier l'état : make health"

down:
	docker compose down

restart: down up

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# Test de fumée : chaque service répond-il ?
health:
	@bash scripts/healthcheck.sh

# Repart de zéro : supprime conteneurs ET volumes (données perdues)
reset:
	docker compose down -v
