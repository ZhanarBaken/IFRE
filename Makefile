.PHONY: help dev up build down logs shell up-prod env

DC = docker compose -f local.yml
DC_PROD = docker compose -f production.yml

help:
	@echo "Targets:"
	@echo "  dev    - run local dev stack (reload, no rebuild)"
	@echo "  up     - run docker compose (build)"
	@echo "  build  - build docker image"
	@echo "  down   - stop containers"
	@echo "  logs   - follow logs (dev)"
	@echo "  shell  - open shell in dev container"

# Local dev (bind mount + reload)
dev:
	$(DC) up

# Standard docker compose (build image)
up:
	$(DC) up

build:
	$(DC) build

down:
	$(DC) down

logs:
	$(DC) logs -f

shell:
	$(DC) exec ifre-api sh

up-prod:
	$(DC_PROD) up -d --build

env:
	mkdir -p .envs/.local
	@if [ ! -f .envs/.local/.env ]; then \
		cp .env.example .envs/.local/.env; \
		echo "Created .envs/.local/.env from .env.example"; \
	else \
		echo ".envs/.local/.env already exists"; \
	fi
