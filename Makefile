IMAGE := postgres:17
CONTAINER_NAME := deeplook-db
REDIS_IMAGE := redis:8.0.3-alpine
REDIS_CONTAINER_NAME := deeplook-redis
DB_NAME := deeplook_v2
USER_NAME := postgres
PASSWORD := postgres
MIGRATION_DIR := crates/schema/migrations
DATABASE_URL := postgres://$(USER_NAME):$(PASSWORD)@localhost/$(DB_NAME)


postgres:
	# after starting fresh fresh db container, install timescaledb: https://docs.tigerdata.com/self-hosted/latest/install/installation-linux/#install-and-configure-timescaledb-on-postgresql
	docker run --name $(CONTAINER_NAME) -p 5432:5432 -e POSTGRES_USER=$(USER_NAME) -e POSTGRES_PASSWORD=$(PASSWORD) -d $(IMAGE) 

redis:
	docker run --name $(REDIS_CONTAINER_NAME) -p 6379:6379 -d $(REDIS_IMAGE) 

createdb:
	docker exec -it $(CONTAINER_NAME) createdb --user=$(USER_NAME) --owner=$(USER_NAME) $(DB_NAME)

dropdb:
	docker exec -it $(CONTAINER_NAME) su - $(USER_NAME) -c "dropdb $(DB_NAME)"

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
