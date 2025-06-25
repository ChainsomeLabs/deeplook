IMAGE := postgres:17
CONTAINER_NAME := deeplook-db
DB_NAME := deeplook
USER_NAME := postgres
PASSWORD := postgres
MIGRATION_DIR := crates/schema/migrations
DATABASE_URL := postgres://$(USER_NAME):$(PASSWORD)@localhost/$(DB_NAME)


postgres:
	# after starting fresh fresh db container, install timescaledb: https://docs.tigerdata.com/self-hosted/latest/install/installation-linux/#install-and-configure-timescaledb-on-postgresql
	docker run --name $(CONTAINER_NAME) -p 5432:5432 -e POSTGRES_USER=$(USER_NAME) -e POSTGRES_PASSWORD=$(PASSWORD) -d $(IMAGE) 

createdb:
	docker exec -it $(CONTAINER_NAME) createdb --user=$(USER_NAME) --owner=$(USER_NAME) $(DB_NAME)

dropdb:
	docker exec -it $(CONTAINER_NAME) su - $(USER_NAME) -c "dropdb $(DB_NAME)"

migrateup:
	cd $(MIGRATION_DIR) && diesel migration run

migratedown:
	cd $(MIGRATION_DIR) && diesel migration revert

indexer:
	source .env && cargo run -p deepbook-indexer -- --first-checkpoint=150000000 --skip-watermark

api:
	source .env && cargo run -p deepbook-server -- --metrics-address=0.0.0.0:9185

.PHONY: postgres createdb dropdb migrateup migratedown
