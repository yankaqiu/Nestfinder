# ChatGPT Connector Setup

This is the short setup flow we used to connect this repo's MCP app to ChatGPT.

## What runs where

- FastAPI listings backend: `http://localhost:8000`
- MCP bridge for ChatGPT: `http://localhost:8001/mcp`
- Public connector URL: `https://YOUR-PUBLIC-URL/mcp`

ChatGPT talks to the MCP bridge on `8001`, and the MCP bridge calls the listings API on `8000`.

## 1. Build the widget

Use a recent Node version. We hit issues with Node `v14`, so use Node `22`.

```bash
cd apps_sdk/web
source ~/.nvm/nvm.sh
nvm use 22
npm install
npm run build
```

If the build fails with a permissions error on `dist/`, remove the generated folder and rebuild:

```bash
rm -rf dist
npm run build
```

## 2. Start the listings backend

In one terminal:

```bash
uv run uvicorn app.main:app --reload --port 8000
```

Quick health check:

```bash
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{"status":"ok"}
```

## 3. Get a public HTTPS URL for the MCP server

In another terminal, open a tunnel to port `8001`:

```bash
npx cloudflared tunnel --url http://localhost:8001
```

Cloudflare will print a public URL like:

```text
https://random-name.trycloudflare.com
```

Keep this terminal running.

## 4. Start the MCP bridge

In a third terminal:

```bash
export APPS_SDK_LISTINGS_API_BASE_URL=http://localhost:8000
export APPS_SDK_PUBLIC_BASE_URL=https://YOUR-PUBLIC-URL
uv run uvicorn apps_sdk.server.main:app --reload --port 8001
```

Replace `https://YOUR-PUBLIC-URL` with the Cloudflare URL from step 3.

The ChatGPT connector URL will be:

```text
https://YOUR-PUBLIC-URL/mcp
```

## 5. Add the connector in ChatGPT

In ChatGPT:

1. Open `Settings`
2. Go to `Apps & Connectors`
3. Open `Advanced settings`
4. Enable `Developer mode`
5. Go to `Settings -> Connectors`
6. Click `Create`
7. Fill in:
   - Name: `Nestfinder`
   - Description: `Search Swiss real-estate listings and show ranked results on a map`
   - Connector URL: `https://YOUR-PUBLIC-URL/mcp`
8. Click `Create`

If the connection works, ChatGPT should show the `search_listings` tool.

## 6. Use it in chat

1. Start a new chat
2. Click the `+` next to the composer
3. Click `More`
4. Select your connector
5. Prompt it, for example:

```text
Find a bright 3-room apartment in Zurich under 2800 CHF with balcony
```

## Common issues we hit

### Widget build fails with permission denied

Cause: `apps_sdk/web/dist` was owned by `root`.

Fix:

```bash
cd apps_sdk/web
rm -rf dist
source ~/.nvm/nvm.sh
nvm use 22
npm run build
```

### Widget build fails with `Unexpected token '??='`

Cause: old Node version.

Fix:

```bash
source ~/.nvm/nvm.sh
nvm use 22
```

### ChatGPT says the dedicated search failed to connect

Cause in our case: the backend on `8000` was not running, so the MCP bridge had nothing to call.

Check all three are running:

- backend on `8000`
- MCP bridge on `8001`
- Cloudflare tunnel

## Quick verification

Backend:

```bash
curl http://127.0.0.1:8000/health
```

Protocol smoke test:

```bash
uv run python scripts/mcp_smoke.py --url https://YOUR-PUBLIC-URL/mcp
```

## Notes

- The tunnel URL only works while `cloudflared` is running.
- If you change `APPS_SDK_PUBLIC_BASE_URL`, restart the MCP server.
- This setup only exposes the current harness behavior. Search quality will still depend on implementing real hard-filter extraction and ranking.
