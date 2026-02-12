
GO_CMD := go
BIN := bin
DOCKER_COMPOSE := docker-compose
COMPOSE_FILE := infra/docker-compose.yml

build: build-mlog build-server build-producer build-consumer

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
	@test -n "$(topic)" || (echo "usage: make create-topic topic=NAME [replicas=N]"; exit 1)
	$(GO_CMD) run ./cmd/producer create-topic --addr 127.0.0.1:9094 --topic "$(topic)" --replicas $(or $(replicas),1)

# 3-node local cluster (scripts; run from repo root)
local-cluster-start:
	./scripts/start-local-cluster.sh

local-cluster-stop:
	./scripts/stop-local-cluster.sh

test:
	$(GO_CMD) test -v ./...