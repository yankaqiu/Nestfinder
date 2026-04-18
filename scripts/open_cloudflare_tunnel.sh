#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/.local/logs"
mkdir -p "${LOG_DIR}"

PORT="${1:-8001}"
OUTPUT_FILE="${2:-}"
LOG_FILE="${LOG_DIR}/cloudflared-${PORT}.log"

rm -f "${LOG_FILE}"

cleanup() {
  if [[ -n "${TAIL_PID:-}" ]] && kill -0 "${TAIL_PID}" >/dev/null 2>&1; then
    kill "${TAIL_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
    kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

echo "Opening Cloudflare tunnel to http://127.0.0.1:${PORT} ..."
if command -v cloudflared >/dev/null 2>&1; then
  cloudflared tunnel --url "http://127.0.0.1:${PORT}" >"${LOG_FILE}" 2>&1 &
else
  bash -lc "printf 'y\n' | npx cloudflared tunnel --url 'http://127.0.0.1:${PORT}'" >"${LOG_FILE}" 2>&1 &
fi
TUNNEL_PID=$!

PUBLIC_URL=""
for _ in $(seq 1 60); do
  if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
    echo "cloudflared exited before producing a public URL." >&2
    cat "${LOG_FILE}" >&2 || true
    exit 1
  fi

  PUBLIC_URL="$(grep -oE 'https://[-a-z0-9]+\.trycloudflare\.com' "${LOG_FILE}" | head -n 1 || true)"
  if [[ -n "${PUBLIC_URL}" ]]; then
    break
  fi
  sleep 1
done

if [[ -z "${PUBLIC_URL}" ]]; then
  echo "Timed out waiting for Cloudflare to return a public URL." >&2
  cat "${LOG_FILE}" >&2 || true
  exit 1
fi

echo
echo "Public base URL:"
echo "  ${PUBLIC_URL}"
echo
echo "ChatGPT connector URL:"
echo "  ${PUBLIC_URL}/mcp"

if [[ -n "${OUTPUT_FILE}" ]]; then
  mkdir -p "$(dirname "${OUTPUT_FILE}")"
  printf '%s\n' "${PUBLIC_URL}" > "${OUTPUT_FILE}"
  echo
  echo "Wrote URL to:"
  echo "  ${OUTPUT_FILE}"
fi

echo
echo "Tunnel log:"
echo "  ${LOG_FILE}"
echo
echo "Keep this script running while you use the connector."
echo "Press Ctrl+C to stop the tunnel."
echo

tail -n +1 -f "${LOG_FILE}" &
TAIL_PID=$!
wait "${TUNNEL_PID}"
