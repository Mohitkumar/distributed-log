
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

cluster-logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

# 3-node local cluster (scripts; run from repo root)
local-cluster-start:
	./scripts/start-local-cluster.sh

local-cluster-stop:
	./scripts/stop-local-cluster.sh

test:
	$(GO_CMD) test -v ./...