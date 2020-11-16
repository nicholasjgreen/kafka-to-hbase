SHELL=bash
S3_READY_REGEX=^Ready\.$
RDBMS_READY_REGEX='mysqld: ready for connections'
aws_dev_account=NOT_SET
temp_image_name=NOT_SET
aws_default_region=NOT_SET
tutorial_topic=my-topic4
tutorial_partitions=2

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

hbase-up: ## Bring up and provision zookeeper and hbase
	docker-compose -f docker-compose.yaml up -d zookeeper hbase
	@{ \
		echo Waiting for hbase.; \
		while ! docker logs hbase 2>&1 | grep "Master has completed initialization" ; do \
			sleep 2; \
			echo Waiting for hbase.; \
		done; \
		sleep 5; \
		echo ...hbase ready.; \
	}

rdbms: ## Bring up and provision mysql
	docker-compose -f docker-compose.yaml up -d metadatastore
	@{ \
		while ! docker logs metadatastore 2>&1 | grep "^Version" | grep 3306; do \
			echo Waiting for metadatastore.; \
			sleep 2; \
		done; \
		sleep 5; \
	}
	docker exec -i metadatastore mysql --host=127.0.0.1 --user=root --password=password metadatastore  < ./docker/metadatastore/create_table.sql
	docker exec -i metadatastore mysql --host=127.0.0.1 --user=root --password=password metadatastore  < ./docker/metadatastore/grant_user.sql

services: hbase-up rdbms ## Bring up supporting services in docker
	docker-compose -f docker-compose.yaml up --build -d kafka aws-s3
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

integration-test-ucfs-and-equality: ## Run the integration tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-test ;\
		docker rm integration-test ;\
		set -e ;\
	}
	docker-compose -f docker-compose.yaml build integration-test
	docker-compose -f docker-compose.yaml run --name integration-test integration-test gradle --no-daemon --rerun-tasks integration-test integration-test-equality -x test -x integration-load-test

integration-tests:
	docker-compose -f docker-compose.yaml run --name integration-test integration-test gradle --no-daemon --rerun-tasks integration-test
	docker-compose -f docker-compose.yaml run --name integration-test-equality integration-test gradle --no-daemon --rerun-tasks integration-test-equality
	docker-compose -f docker-compose.yaml run --name integration-load-test integration-test gradle --no-daemon --rerun-tasks integration-load-test

integration-load-test: ## Run the integration load tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-load-test ;\
		docker rm integration-load-test ;\
		set -e ;\
	}
	docker-compose -f docker-compose.yaml build integration-test
	docker-compose -f docker-compose.yaml run --name integration-load-test integration-test gradle --no-daemon --rerun-tasks integration-load-test -x test -x integration-test -x integration-test-equality

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all: down destroy build up integration-tests

hbase-shell: ## Open an hbase shell in the running hbase container
	docker exec -it hbase hbase shell

kafka-shell: ## Open an shell in the running kafka broker container in root
	docker exec -it kafka sh

kafka-shell-bin: ## Open an shell in the running kafka broker container in /opt/kafka/bin
	docker exec -w "/opt/kafka/bin" -it kafka sh -c 'ls'

build: build-base ## build main images
	docker-compose build

build-base: ## Build the base images which certain images extend.
	@{ \
		pushd docker; \
		docker build --tag dwp-java:latest --file .java/Dockerfile . ; \
		docker build --tag dwp-python-preinstall:latest --file ./python/Dockerfile . ; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-kotlin-slim-gradle-k2hb:latest --file ./gradle/Dockerfile . ; \
		rm -rf settings.gradle.kts gradle.properties ; \
		popd; \
	}

push-local-to-ecr: ## Push a temp version of k2hb to AWS DEV ECR
	@{ \
		export AWS_DEV_ACCOUNT=$(aws_dev_account); \
		export TEMP_IMAGE_NAME=$(temp_image_name); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		aws ecr get-login-password --region ${AWS_DEFAULT_REGION} --profile dataworks-development | docker login --username AWS --password-stdin ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com; \
		docker tag kafka2hbase ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
		docker push ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
	}

kafka-command: ## Run an arbitrary command in the kafka server
	docker exec -w "/opt/kafka/bin" -it kafka sh -c '$(command)'

tutorial-list-all: ## List topics in the kafka server
	make kafka-command command="./kafka-topics.sh --zookeeper zookeeper:2181 --list"

tutorial-list-topic: ## List only tutorial_topic in the kafka server
	make kafka-command command="./kafka-topics.sh --zookeeper zookeeper:2181 --list | fgrep $(tutorial_topic)"

tutorial-create-topic: ## Create tutorial_topic in the kafka server
	make kafka-command command="./kafka-topics.sh --if-not-exists --create --topic $(tutorial_topic) --zookeeper zookeeper:2181 --replication-factor 1 --partitions $(tutorial_partition)"

tutorial-describe-topic: ## Describe tutorial_topic in the kafka server
	make kafka-command command="./kafka-topics.sh --describe --topic $(tutorial_topic) --zookeeper zookeeper:2181"

tutorial-publish-simple: ## Publish to tutorial_topic in the kafka server with just a value, and null key. Starts a shell where we can type commands.
	make kafka-command command="./kafka-console-producer.sh --broker-list localhost:9092 --topic $(tutorial_topic)"

tutorial-publish-with-key: ## Publish to tutorial_topic in the kafka server with a key:value. Starts a shell where we can type commands.
	make kafka-command command="./kafka-console-producer.sh --broker-list localhost:9092 --topic $(tutorial_topic) --property parse.key=true --property key.separator=:"

tutorial-subscribe-by-group: ## Subscribe to tutorial_topic:all in the kafka server. Starts a shell where we can observe.
	make kafka-command command="./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(tutorial_topic) --group my-consumer-group --from-beginning --property print.key=true --property print.value=true"

tutorial-subscribe-by-partition: ## Subscribe to tutorial_topic:tutorial_partition in the kafka server. Starts a shell where we can observe.
	make kafka-command command="./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(tutorial_topic) --from-beginning --partition $(tutorial_partition) --property print.key=true --property print.value=true"
