# Builds
GENERATOR_SRC := cmd/generator/main.go
PROCESSOR_SRC := cmd/processor/main.go
GATEWAY_SRC := cmd/gateway/main.go

.PHONY: up down restart logs gen proc deps clean

# --- Infrastructure ---

# Start Kafka, Zookeeper, and Redis in the background
up:
	docker-compose up -d

# Stop all containers
down:
	docker-compose down

# Restart infrastructure (useful if Kafka gets stuck)
restart: down up

# View logs for the infrastructure (Kafka/Redis)
logs:
	docker-compose logs -f kafka1 kafka2 kafka3 redis

# --- Application ---

# Run the Generator (Producer)
gen:
	go run $(GENERATOR_SRC)

# Run the Processor (Consumer)
proc:
	go run $(PROCESSOR_SRC)

# Run the Gateway (WS Server)
gtw:
	go run $(GATEWAY_SRC)

# Tidy up Go modules
deps:
	go mod tidy
