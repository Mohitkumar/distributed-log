
GO_CMD := go
BIN := bin
DOCKER_COMPOSE := docker-compose
COMPOSE_FILE := infra/docker-compose.yml

build: build-mlog build-server build-producer build-consumer build-topic

build-mlog:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/mlog .

build-server:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/server ./cmd/server

build-producer:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/producer ./cmd/producer

build-consumer:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/consumer ./cmd/consumer

build-topic:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/topic ./cmd/topic

run:
	$(GO_CMD) run .

# 3-node Docker cluster
cluster-up:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --build

cluster-down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down

cluster-restart:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --build
cluster-logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

# Create a topic from host (cluster in Docker: node1 RPC exposed on 9094; bridge forwards localhost:9094 -> container)
create-topic:
	@test -n "$(topic)" || (echo "usage: make create-topic topic=NAME [replicas=N] [addrs=ADDRS]"; exit 1)
	$(GO_CMD) run ./cmd/topic create --addrs $(or $(addrs),127.0.0.1:9094) --topic "$(topic)" --replicas $(or $(replicas),1)

# Delete a topic
delete-topic:
	@test -n "$(topic)" || (echo "usage: make delete-topic topic=NAME [addrs=ADDRS]"; exit 1)
	$(GO_CMD) run ./cmd/topic delete --addrs $(or $(addrs),127.0.0.1:9094) --topic "$(topic)"

# List topics with leader and replica info
list-topics:
	$(GO_CMD) run ./cmd/topic list --addrs $(or $(addrs),127.0.0.1:9094)

# Producer connect: produce messages from stdin to topic (interactive)
producer-connect:
	@test -n "$(topic)" || (echo "usage: make producer-connect topic=NAME [addrs=ADDRS]"; exit 1)
	$(GO_CMD) run ./cmd/producer connect --addrs $(or $(addrs),127.0.0.1:9094) --topic "$(topic)"

# Consumer connect: consume messages from topic (streaming to stdout)
consumer-connect:
	@test -n "$(topic)" || (echo "usage: make consumer-connect topic=NAME [addrs=ADDRS] [id=ID]"; exit 1)
	$(GO_CMD) run ./cmd/consumer connect --addrs $(or $(addrs),127.0.0.1:9092) --topic "$(topic)" --id $(or $(id),default)

# 3-node local cluster (scripts; run from repo root)
local-cluster-start:
	./scripts/start-local-cluster.sh

local-cluster-stop:
	./scripts/stop-local-cluster.sh

test:
	$(GO_CMD) test -v ./...