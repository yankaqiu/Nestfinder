FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY app ./app
COPY apps_sdk ./apps_sdk
COPY services ./services
RUN pip install --no-cache-dir uv && uv pip install --system .

COPY raw_data ./raw_data
COPY README.md challenge.md ./

ENV LISTINGS_RAW_DATA_DIR=/app/raw_data
ENV LISTINGS_DB_PATH=/data/listings.db

EXPOSE 8000
EXPOSE 8002

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
