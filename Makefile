SHELL := /bin/bash
ENV_FILE ?= .env

ifneq ($(wildcard $(ENV_FILE)),)
include $(ENV_FILE)
export $(shell sed -n 's/=.*//p' $(ENV_FILE))
endif

PUBLIC_URL_FILE ?= .local/public-url
API_HOST ?= 127.0.0.1
API_PORT ?= 8000
MCP_HOST ?= 127.0.0.1
MCP_PORT ?= 8001
IMAGE_RAG_HOST ?= 127.0.0.1
IMAGE_RAG_PORT ?= 8002
DEBUG_DASHBOARD_HOST ?= 127.0.0.1
DEBUG_DASHBOARD_PORT ?= 8899
STARTUP_TIMEOUT_S ?= 600
IMAGE_RAG_DEVICE ?= auto
IMAGE_RAG_MODEL ?= auto
IMAGE_RAG_SYNC_ON_START ?= true

.PHONY: help tunnel stack stack-local connector-url dashboard

help:
	@echo "Available targets:"
	@echo "  Uses $(ENV_FILE) automatically when present."
	@echo
	@echo "  make tunnel"
	@echo "    Open a Cloudflare tunnel for the MCP port and save the public URL to $(PUBLIC_URL_FILE)."
	@echo
	@echo "  make stack"
	@echo "    Start api, image-rag, and mcp using the saved tunnel URL if present."
	@echo "    Falls back to http://$(MCP_HOST):$(MCP_PORT) when no tunnel URL file exists."
	@echo
	@echo "  make stack-local"
	@echo "    Start api, image-rag, and mcp with a local-only MCP base URL."
	@echo
	@echo "  make connector-url"
	@echo "    Print the ChatGPT connector URL from $(PUBLIC_URL_FILE)."
	@echo
	@echo "  make dashboard"
	@echo "    Run the local pipeline debugger UI at http://$(DEBUG_DASHBOARD_HOST):$(DEBUG_DASHBOARD_PORT)/debug"
	@echo
	@echo "Useful overrides:"
	@echo "  make stack ENV_FILE=.env.local"
	@echo "  make tunnel MCP_PORT=8011"
	@echo "  make stack IMAGE_RAG_DEVICE=auto STARTUP_TIMEOUT_S=1200"
	@echo "  make stack PUBLIC_BASE_URL=https://example.trycloudflare.com"
	@echo "  make dashboard DEBUG_DASHBOARD_PORT=8900"

tunnel:
	@./scripts/open_cloudflare_tunnel.sh "$(MCP_PORT)" "$(PUBLIC_URL_FILE)"

stack:
	@target_url="$${PUBLIC_BASE_URL:-}"; \
	if [[ -z "$${target_url}" ]]; then \
	  if [[ -f "$(PUBLIC_URL_FILE)" ]]; then \
	    target_url="$(PUBLIC_URL_FILE)"; \
	  else \
	    target_url="http://$(MCP_HOST):$(MCP_PORT)"; \
	  fi; \
	fi; \
	API_HOST="$(API_HOST)" \
	API_PORT="$(API_PORT)" \
	MCP_HOST="$(MCP_HOST)" \
	MCP_PORT="$(MCP_PORT)" \
	IMAGE_RAG_HOST="$(IMAGE_RAG_HOST)" \
	IMAGE_RAG_PORT="$(IMAGE_RAG_PORT)" \
	STARTUP_TIMEOUT_S="$(STARTUP_TIMEOUT_S)" \
	IMAGE_RAG_DEVICE="$(IMAGE_RAG_DEVICE)" \
	IMAGE_RAG_MODEL="$(IMAGE_RAG_MODEL)" \
	IMAGE_RAG_SYNC_ON_START="$(IMAGE_RAG_SYNC_ON_START)" \
	./scripts/start_local_stack.sh "$${target_url}"

stack-local:
	@API_HOST="$(API_HOST)" \
	API_PORT="$(API_PORT)" \
	MCP_HOST="$(MCP_HOST)" \
	MCP_PORT="$(MCP_PORT)" \
	IMAGE_RAG_HOST="$(IMAGE_RAG_HOST)" \
	IMAGE_RAG_PORT="$(IMAGE_RAG_PORT)" \
	STARTUP_TIMEOUT_S="$(STARTUP_TIMEOUT_S)" \
	IMAGE_RAG_DEVICE="$(IMAGE_RAG_DEVICE)" \
	IMAGE_RAG_MODEL="$(IMAGE_RAG_MODEL)" \
	IMAGE_RAG_SYNC_ON_START="$(IMAGE_RAG_SYNC_ON_START)" \
	./scripts/start_local_stack.sh "http://$(MCP_HOST):$(MCP_PORT)"

connector-url:
	@if [[ -f "$(PUBLIC_URL_FILE)" ]]; then \
	  public_url="$$(tr -d '\r' < "$(PUBLIC_URL_FILE)" | head -n 1)"; \
	  echo "$${public_url}/mcp"; \
	else \
	  echo "No saved tunnel URL found at $(PUBLIC_URL_FILE)." >&2; \
	  exit 1; \
	fi

dashboard:
	@echo "Pipeline debugger → http://$(DEBUG_DASHBOARD_HOST):$(DEBUG_DASHBOARD_PORT)/debug"
	@IMAGE_RAG_BASE_URL="$${IMAGE_RAG_BASE_URL:-http://$(IMAGE_RAG_HOST):$(IMAGE_RAG_PORT)}" \
	uv run uvicorn benchmarks.debug_server:app --host "$(DEBUG_DASHBOARD_HOST)" --port "$(DEBUG_DASHBOARD_PORT)"
