DB_IMAGE := timescale/timescaledb:2.21.1-pg16
DB_CONTAINER_NAME := deeplook-db
DB_NAME := deeplook
USER_NAME := postgres
PASSWORD := postgres
MIGRATION_DIR := crates/schema/migrations
DATABASE_URL := postgres://$(USER_NAME):$(PASSWORD)@localhost/$(DB_NAME)
REDIS_IMAGE := redis:8.0.3-alpine
REDIS_CONTAINER_NAME := deeplook-redis


postgres:
	docker run --name $(DB_CONTAINER_NAME) -p 5432:5432 -e POSTGRES_USER=$(USER_NAME) -e POSTGRES_PASSWORD=$(PASSWORD) -d $(DB_IMAGE) 

redis:
	docker run --name $(REDIS_CONTAINER_NAME) -p 6379:6379 -d $(REDIS_IMAGE) redis-server --notify-keyspace-events 'K$$'

createdb:
	docker exec -it $(DB_CONTAINER_NAME) createdb --user=$(USER_NAME) --owner=$(USER_NAME) $(DB_NAME)

dropdb:
	docker exec -it $(DB_CONTAINER_NAME) su - $(USER_NAME) -c "dropdb $(DB_NAME)"

migrateup:
	cd $(MIGRATION_DIR) && diesel migration run

migratedown:
	cd $(MIGRATION_DIR) && diesel migration revert

indexer:
	source .env && cargo run -p deeplook-indexer -- --first-checkpoint=150000000 --skip-watermark

api:
	source .env && cargo run -p deeplook-server -- --metrics-address=0.0.0.0:9185

orderbook:
	source .env && cargo run -p deeplook-orderbook

.PHONY: postgres redis createdb dropdb migrateup migratedown indexer api orderbook
