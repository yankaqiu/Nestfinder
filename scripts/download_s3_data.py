#!/usr/bin/env python3
"""Download listing data and images from S3 using boto3.

Usage (with the helper venv):
    .venv-dl/bin/python scripts/download_s3_data.py list
    .venv-dl/bin/python scripts/download_s3_data.py raw-data
    .venv-dl/bin/python scripts/download_s3_data.py images
    .venv-dl/bin/python scripts/download_s3_data.py images-only robinreal sred structured
    .venv-dl/bin/python scripts/download_s3_data.py all
"""
from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-central-2")
S3_BUCKET = os.getenv("LISTINGS_S3_BUCKET", "crawl-data-951752554117-eu-central-2-an")
S3_PREFIX = os.getenv("LISTINGS_S3_PREFIX", "prod")

MAX_WORKERS = 16


def get_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def list_objects(client, prefix: str, delimiter: str = ""):
    """Paginate through all objects under a prefix."""
    paginator = client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
    if delimiter:
        kwargs["Delimiter"] = delimiter
    for page in paginator.paginate(**kwargs):
        yield from page.get("Contents", [])
        if delimiter:
            yield from [
                {"Key": cp["Prefix"], "Size": 0, "is_prefix": True}
                for cp in page.get("CommonPrefixes", [])
            ]


def download_file(client, key: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return f"  skip (exists): {dest}"
    client.download_file(S3_BUCKET, key, str(dest))
    return f"  downloaded: {dest}"


def download_parallel(keys_and_dests: list[tuple[str, Path]]):
    """Download files in parallel with a thread pool."""
    client = get_client()
    total = len(keys_and_dests)
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(download_file, client, key, dest): (key, dest)
            for key, dest in keys_and_dests
        }
        for future in as_completed(futures):
            done += 1
            msg = future.result()
            if done % 50 == 0 or done == total:
                print(f"  [{done}/{total}] {msg}")


# ── Commands ──────────────────────────────────────────────────────────


def cmd_list():
    """Show top-level prefixes/objects in the bucket under S3_PREFIX."""
    client = get_client()
    prefix = f"{S3_PREFIX}/"
    print(f"Listing s3://{S3_BUCKET}/{prefix}\n")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            print(f"  DIR  {cp['Prefix']}")
        for obj in page.get("Contents", []):
            size_mb = obj["Size"] / (1024 * 1024)
            print(f"  {size_mb:8.1f} MB  {obj['Key']}")


def cmd_raw_data():
    """Download raw_data.zip or all non-image files."""
    client = get_client()
    zip_key = f"{S3_PREFIX}/raw_data.zip"
    zip_dest = PROJECT_ROOT / "raw_data.zip"
    raw_dir = PROJECT_ROOT / "raw_data"

    # Try downloading raw_data.zip first
    try:
        client.head_object(Bucket=S3_BUCKET, Key=zip_key)
        print(f"Found {zip_key}, downloading...")
        client.download_file(S3_BUCKET, zip_key, str(zip_dest))
        print(f"Extracting to {raw_dir}/ ...")
        raw_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_dest) as zf:
            zf.extractall(raw_dir)
        print("Done. raw_data/ is ready.")
        return
    except client.exceptions.ClientError:
        pass

    # Fallback: download everything except images
    print("raw_data.zip not found. Syncing non-image files...")
    raw_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{S3_PREFIX}/"
    pairs = []
    for obj in list_objects(client, prefix):
        key = obj["Key"]
        if "/images/" in key or key.endswith("/"):
            continue
        rel = key[len(prefix):]
        pairs.append((key, raw_dir / rel))

    print(f"Found {len(pairs)} non-image files to download.")
    download_parallel(pairs)
    print("Done.")


def cmd_images(sources: list[str] | None = None):
    """Download images. If sources is None, download all."""
    client = get_client()
    dest_root = PROJECT_ROOT / "downloads" / "prod"

    if sources:
        prefixes = [f"{S3_PREFIX}/{s}/images/" for s in sources]
        print(f"Downloading images for: {', '.join(sources)}")
    else:
        prefixes = [f"{S3_PREFIX}/"]
        print("Downloading ALL images...")

    pairs = []
    for pfx in prefixes:
        for obj in list_objects(client, pfx):
            key = obj["Key"]
            if sources is None and "/images/" not in key:
                continue
            if key.endswith("/"):
                continue
            rel = key[len(f"{S3_PREFIX}/"):]
            pairs.append((key, dest_root / rel))

    print(f"Found {len(pairs)} image files to download.")
    if not pairs:
        print("Nothing to download.")
        return
    download_parallel(pairs)
    print(f"Done. Images saved under {dest_root}/")


def cmd_all():
    cmd_raw_data()
    print()
    cmd_images()


USAGE = """\
Usage: uv run python scripts/download_s3_data.py [COMMAND]

Commands:
  list          List top-level prefixes in the bucket
  raw-data      Download raw_data.zip (or non-image files) into ./raw_data/
  images        Download ALL images into ./downloads/prod/
  images-only   Download images for specific sources (robinreal, sred, structured)
  all           Download everything (raw data + all images)

Examples:
  uv run python scripts/download_s3_data.py list
  uv run python scripts/download_s3_data.py raw-data
  uv run python scripts/download_s3_data.py images
  uv run python scripts/download_s3_data.py images-only robinreal sred
  uv run python scripts/download_s3_data.py all
"""

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(USAGE)
        sys.exit(0)

    command = args[0]
    if command == "list":
        cmd_list()
    elif command == "raw-data":
        cmd_raw_data()
    elif command == "images":
        cmd_images()
    elif command == "images-only":
        if len(args) < 2:
            print("ERROR: specify at least one source (robinreal, sred, structured)")
            sys.exit(1)
        cmd_images(args[1:])
    elif command == "all":
        cmd_all()
    else:
        print(USAGE)
