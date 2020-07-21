S3_READY_REGEX=^Ready\.$

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

.PHONY: build
build: ## Build Kafka2Hbase
	./gradlew :unit build -x test

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: services
services: ## Bring up Kafka2Hbase in Docker with supporting services
	docker-compose up -d zookeeper kafka hbase aws-s3
	@{ \
		while ! docker logs aws-s3 2> /dev/null | grep -q $(S3_READY_REGEX); do \
        	echo Waiting for s3.; \
            sleep 2; \
        done; \
	}
	docker-compose up s3-provision
	docker-compose up -d kafka2s3

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
integration: ## Run the integration tests in a Docker container
	docker-compose run --rm integration-test ./gradlew --rerun-tasks integration

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all: down destroy build-base build dist up test integration

.PHONY: hbase-shell
hbase-shell: ## Open an Hbase shell onto the running Hbase container
	docker-compose run --rm hbase shell

.PHONY: test
test: ## Run the unit tests
	./gradlew --rerun-tasks unit

.PHONY: build-base
build-base: ## build the base images which certain images extend.
	@{ \
		pushd docker; \
		docker build --tag dwp-java:latest --file .java/Dockerfile . ; \
		docker build --tag dwp-python-preinstall:latest --file ./python/Dockerfile . ; \
		popd; \
    }
