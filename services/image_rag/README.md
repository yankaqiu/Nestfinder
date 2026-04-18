# Image RAG Service

This service provides text-to-image retrieval over listing images.

## What it does

- stores image embeddings in Milvus Lite
- persists both vector state and sync state on disk
- backfills the corpus on startup in the background
- lazily indexes missing candidate listings during search
- searches only inside the candidate `listing_ids` supplied by the caller

## API

- `GET /health`
- `GET /status`
- `POST /search`
- `POST /admin/sync`

Search request:

```json
{
  "query_text": "bright apartment with a modern kitchen",
  "listing_ids": ["123", "456"],
  "top_k": 20
}
```

## Recommended local run mode on Apple Silicon

Run the image service natively so PyTorch can use MPS:

```bash
export IMAGE_RAG_DEVICE=auto
export IMAGE_RAG_MODEL=auto
export IMAGE_RAG_DB_URI=./data/image-rag/milvus.db
export IMAGE_RAG_STATE_DB=./data/image-rag/state.db
uv run uvicorn services.image_rag.main:app --reload --port 8002
```

At startup the service prints:

- selected device
- whether CUDA is available
- whether MPS is available
- the selected model

## Docker Compose

Compose includes an `image-rag` service on port `8002`.

On macOS this runs in CPU mode by default:

```bash
docker compose up --build image-rag
```

## Main env vars

- `IMAGE_RAG_DEVICE=auto|cpu|mps|cuda`
- `IMAGE_RAG_MODEL=auto|<hf model name>`
- `IMAGE_RAG_DB_URI`
- `IMAGE_RAG_STATE_DB`
- `IMAGE_RAG_SYNC_ON_START=true|false`
- `IMAGE_RAG_IMAGE_BATCH_SIZE`
- `IMAGE_RAG_QUERY_BATCH_SIZE`
- `IMAGE_RAG_CANDIDATE_CHUNK_SIZE`
