.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build Kafka2Hbase
	./gradlew build

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: up
up: ## Bring up Kafka2Hbase in Docker with supporting services
	docker-compose up --build -d

.PHONY: restart
restart: ## Restart Kafka2Hbase and all supporting services
	docker-compose restart

.PHONY: down
down: ## Bring down the Kafka2Hbase Docker container and support services
	docker-compose down

.PHONY: destroy
destroy: down ## Bring down the Kafka2Hbase Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

.PHONY: integration
integration: up ## Run the integration tests in a Docker container
	docker-compose run --rm integration-test ./gradlew --rerun-tasks integration

.PHONY: hbase-shell
hbase-shell: ## Open an Hbase shell onto the running Hbase container
	docker-compose run --rm hbase shell

