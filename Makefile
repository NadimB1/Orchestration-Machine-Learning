# Define the default goal
.DEFAULT_GOAL := start

# Docker compose file
DOCKER_COMPOSE_FILE := docker-compose.yaml

build_test_api:
	@echo "Building test_api..."
	@(cd api && docker build -t test_api .)

build_streamlit_app:
	@echo "Building streamlit_app..."
	@(cd streamlit_dashboard && docker build -t streamlit_app .)

build: build_test_api build_streamlit_app

start: clean build
	@echo "Starting services with Docker Compose..."
	@docker compose -f $(DOCKER_COMPOSE_FILE) up -d --build

stop:
	@echo "Stopping services..."
	@docker compose -f $(DOCKER_COMPOSE_FILE) down

logs:
	@docker compose -f $(DOCKER_COMPOSE_FILE) logs -f

.PHONY: clean build_test_api build_streamlit_app build start stop logs
