SHELL=bash
S3_READY_REGEX=^Ready\.$
RDBMS_READY_REGEX='mysqld: ready for connections'
aws_dev_account=NOT_SET
temp_image_name=NOT_SET
aws_default_region=NOT_SET

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

local-build: ## Build Kafka2Hbase with gradle
	gradle :unit build -x test

local-dist: ## Assemble distribution files in build/dist with gradle
	gradle assembleDist

local-test: ## Run the unit tests with gradle
	gradle --rerun-tasks unit

local-all: local-build local-test local-dist ## Build and test with gradle

rdbms: ## Bring up and provision mysql
	docker-compose -f docker-compose.yaml up -d metadatastore
	@{ \
		while ! docker logs metadatastore 2>&1 | grep "^Version"; do \
			echo Waiting for metadatastore.; \
			sleep 2; \
		done; \
		sleep 5; \
	}
	docker exec -i metadatastore mysql --host=127.0.0.1 --user=root --password=password metadatastore  < ./docker/metadatastore/create_table.sql
	docker exec -i metadatastore mysql --host=127.0.0.1 --user=root --password=password metadatastore  < ./docker/metadatastore/grant_user.sql

services: rdbms ## Bring up supporting services in docker
	docker-compose -f docker-compose.yaml up --build -d zookeeper kafka hbase aws-s3
	@{ \
		while ! docker logs aws-s3 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for s3.; \
			sleep 2; \
		done; \
	}
	docker-compose up --build s3-provision
	docker-compose up --build -d kafka2s3


mysql_root: ## Get a client session on the metadatastore database.
	docker exec -it metadatastore mysql --host=127.0.0.1 --user=root --password=password metadatastore

mysql_k2hbwriter: ## Get a client session on the metadatastore database.
	docker exec -it metadatastore mysql --host=127.0.0.1 --user=k2hbwriter --password=password metadatastore

up: services ## Bring up Kafka2Hbase in Docker with supporting services
	docker-compose -f docker-compose.yaml up --build -d kafka2hbase kafka2hbaseequality

restart: ## Restart Kafka2Hbase and all supporting services
	docker-compose restart

down: ## Bring down the Kafka2Hbase Docker container and support services
	docker-compose down

destroy: down ## Bring down the Kafka2Hbase Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

integration-test: ## Run the integration tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-test ;\
		docker rm integration-test ;\
		set -e ;\
	}
	docker-compose -f docker-compose.yaml run --name integration-test integration-test gradle --no-daemon --rerun-tasks integration-test -x test -x integration-load-test -x integration-test-equality

integration-test-equality: ## Run the integration tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-test ;\
		docker rm integration-test ;\
		set -e ;\
	}
	docker-compose -f docker-compose.yaml run --name integration-test integration-test gradle --no-daemon --rerun-tasks integration-test-equality -x test -x integration-load-test -x integration-test

integration-load-test: ## Run the integration load tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-load-test ;\
		docker rm integration-load-test ;\
		set -e ;\
	}
	docker-compose -f docker-compose.yaml run --name integration-load-test integration-test gradle --no-daemon --rerun-tasks integration-load-test -x test -x integration-test

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all: down destroy build up integration-test integration-test-equality

hbase-shell: ## Open an Hbase shell onto the running Hbase container
	docker-compose run --rm hbase shell

build: build-base ## build main images
	docker-compose build

build-base: ## build the base images which certain images extend.
	@{ \
		pushd docker; \
		docker build --tag dwp-java:latest --file .java/Dockerfile . ; \
		docker build --tag dwp-python-preinstall:latest --file ./python/Dockerfile . ; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-kotlin-slim-gradle-k2hb:latest --file ./gradle/Dockerfile . ; \
		rm -rf settings.gradle.kts gradle.properties ; \
		popd; \
	}

push-local-to-ecr: #Push a temp version of k2hb to AWS DEV ECR
	@{ \
		export AWS_DEV_ACCOUNT=$(aws_dev_account); \
		export TEMP_IMAGE_NAME=$(temp_image_name); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		aws ecr get-login-password --region ${AWS_DEFAULT_REGION} --profile dataworks-development | docker login --username AWS --password-stdin ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com; \
		docker tag kafka2hbase ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
		docker push ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
	}
