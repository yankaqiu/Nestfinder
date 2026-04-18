#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/.local/logs"
mkdir -p "${LOG_DIR}"

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
MCP_HOST="${MCP_HOST:-127.0.0.1}"
MCP_PORT="${MCP_PORT:-8001}"
IMAGE_RAG_HOST="${IMAGE_RAG_HOST:-127.0.0.1}"
IMAGE_RAG_PORT="${IMAGE_RAG_PORT:-8002}"
STARTUP_TIMEOUT_S="${STARTUP_TIMEOUT_S:-600}"

DEFAULT_PUBLIC_BASE_URL="http://${MCP_HOST}:${MCP_PORT}"
PUBLIC_BASE_URL_INPUT="${1:-${DEFAULT_PUBLIC_BASE_URL}}"

resolve_public_base_url() {
  local input="$1"
  if [[ -f "${input}" ]]; then
    tr -d '\r' < "${input}" | head -n 1
    return
  fi
  printf '%s\n' "${input}"
}

PUBLIC_BASE_URL="$(resolve_public_base_url "${PUBLIC_BASE_URL_INPUT}")"

if [[ "${PUBLIC_BASE_URL}" != http://* && "${PUBLIC_BASE_URL}" != https://* ]]; then
  echo "Expected a public base URL or a file containing one, got: ${PUBLIC_BASE_URL_INPUT}" >&2
  exit 1
fi

LISTINGS_RAW_DATA_DIR="${LISTINGS_RAW_DATA_DIR:-${ROOT_DIR}/raw_data}"
LISTINGS_DB_PATH="${LISTINGS_DB_PATH:-${ROOT_DIR}/data/listings.db}"
IMAGE_RAG_DB_URI="${IMAGE_RAG_DB_URI:-${ROOT_DIR}/data/image-rag/milvus.db}"
IMAGE_RAG_STATE_DB="${IMAGE_RAG_STATE_DB:-${ROOT_DIR}/data/image-rag/state.db}"
IMAGE_RAG_DEVICE="${IMAGE_RAG_DEVICE:-auto}"
IMAGE_RAG_MODEL="${IMAGE_RAG_MODEL:-auto}"
IMAGE_RAG_SYNC_ON_START="${IMAGE_RAG_SYNC_ON_START:-true}"

export LISTINGS_RAW_DATA_DIR
export LISTINGS_DB_PATH

mkdir -p "$(dirname "${LISTINGS_DB_PATH}")"
mkdir -p "$(dirname "${IMAGE_RAG_DB_URI}")"
mkdir -p "$(dirname "${IMAGE_RAG_STATE_DB}")"

WIDGET_MANIFEST="${ROOT_DIR}/apps_sdk/web/dist/.vite/manifest.json"

ensure_widget_build() {
  if [[ -f "${WIDGET_MANIFEST}" ]]; then
    return
  fi

  echo "Widget build missing. Building apps_sdk/web..."
  (
    cd "${ROOT_DIR}/apps_sdk/web"
    if [[ -f "${HOME}/.nvm/nvm.sh" ]]; then
      # shellcheck disable=SC1090
      source "${HOME}/.nvm/nvm.sh"
      nvm use 22 >/dev/null
    fi
    npm install
    npm run build
  )
}

wait_for_http() {
  local url="$1"
  local label="$2"
  for _ in $(seq 1 "${STARTUP_TIMEOUT_S}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "Timed out waiting for ${label} at ${url} after ${STARTUP_TIMEOUT_S}s" >&2
  exit 1
}

PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
  wait >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

start_service() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  echo "Starting ${name}..."
  (
    cd "${ROOT_DIR}"
    "$@"
  ) >"${log_file}" 2>&1 &
  local pid=$!
  PIDS+=("${pid}")
  echo "  pid=${pid} log=${log_file}"
}

ensure_widget_build

start_service \
  api \
  env \
    IMAGE_RAG_BASE_URL="http://${IMAGE_RAG_HOST}:${IMAGE_RAG_PORT}" \
    uv run uvicorn app.main:app --host "${API_HOST}" --port "${API_PORT}"

wait_for_http "http://${API_HOST}:${API_PORT}/health" "api"

start_service \
  image-rag \
  env \
    LISTINGS_RAW_DATA_DIR="${LISTINGS_RAW_DATA_DIR}" \
    LISTINGS_DB_PATH="${LISTINGS_DB_PATH}" \
    IMAGE_RAG_DB_URI="${IMAGE_RAG_DB_URI}" \
    IMAGE_RAG_STATE_DB="${IMAGE_RAG_STATE_DB}" \
    IMAGE_RAG_DEVICE="${IMAGE_RAG_DEVICE}" \
    IMAGE_RAG_MODEL="${IMAGE_RAG_MODEL}" \
    IMAGE_RAG_SYNC_ON_START="${IMAGE_RAG_SYNC_ON_START}" \
    uv run uvicorn services.image_rag.main:app --host "${IMAGE_RAG_HOST}" --port "${IMAGE_RAG_PORT}"

wait_for_http "http://${IMAGE_RAG_HOST}:${IMAGE_RAG_PORT}/health" "image-rag"

start_service \
  mcp \
  env \
    APPS_SDK_LISTINGS_API_BASE_URL="http://${API_HOST}:${API_PORT}" \
    APPS_SDK_PUBLIC_BASE_URL="${PUBLIC_BASE_URL}" \
    APPS_SDK_PORT="${MCP_PORT}" \
    uv run uvicorn apps_sdk.server.main:app --host "${MCP_HOST}" --port "${MCP_PORT}"

echo
echo "Local stack is up."
echo "  API:        http://${API_HOST}:${API_PORT}"
echo "  Image RAG:  http://${IMAGE_RAG_HOST}:${IMAGE_RAG_PORT}"
echo "  MCP:        http://${MCP_HOST}:${MCP_PORT}/mcp"
echo "  Public URL: ${PUBLIC_BASE_URL}"
echo
echo "ChatGPT connector URL:"
echo "  ${PUBLIC_BASE_URL}/mcp"
echo
echo "Logs:"
echo "  ${LOG_DIR}/api.log"
echo "  ${LOG_DIR}/image-rag.log"
echo "  ${LOG_DIR}/mcp.log"
echo
echo "Press Ctrl+C to stop all three services."

wait
