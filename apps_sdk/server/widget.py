from __future__ import annotations

import json
import os
from pathlib import Path


WIDGET_TEMPLATE_URI = "ui://widget/listings-map-list.html"
WIDGET_TITLE = "Listings Map And Ranked List"
WIDGET_MIME_TYPE = "text/html;profile=mcp-app"


def get_widget_dist_dir() -> Path:
    configured = os.getenv("APPS_SDK_WIDGET_DIST_DIR")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[1] / "web" / "dist"


def get_public_base_url() -> str:
    return os.getenv("APPS_SDK_PUBLIC_BASE_URL", "http://localhost:8001").rstrip("/")


def _load_manifest(dist_dir: Path) -> dict[str, object]:
    manifest_path = dist_dir / ".vite" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Widget manifest not found at {manifest_path}. Run `npm run build` in apps_sdk/web first."
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def resolve_widget_asset_fallback(*, dist_dir: Path, request_path: str) -> Path | None:
    normalized = request_path.lstrip("/")
    if not normalized.startswith("assets/index-"):
        return None

    manifest = _load_manifest(dist_dir)
    main_entry = manifest.get("src/main.tsx") or manifest.get("index.html")
    if not isinstance(main_entry, dict):
        return None

    if normalized.endswith(".js"):
        script_path = main_entry.get("file")
        if isinstance(script_path, str):
            candidate = dist_dir / script_path
            if candidate.exists():
                return candidate

    if normalized.endswith(".css"):
        css_paths = main_entry.get("css", [])
        if isinstance(css_paths, list) and css_paths:
            first_css = css_paths[0]
            if isinstance(first_css, str):
                candidate = dist_dir / first_css
                if candidate.exists():
                    return candidate

    return None


def load_widget_html(*, dist_dir: Path, public_base_url: str) -> str:
    manifest = _load_manifest(dist_dir)
    main_entry = manifest.get("src/main.tsx") or manifest.get("index.html")
    if not isinstance(main_entry, dict):
        raise KeyError("Expected Vite manifest entry for src/main.tsx or index.html.")

    script_path = main_entry["file"]
    css_paths = main_entry.get("css", [])

    css_links = "\n".join(
        f'<link rel="stylesheet" href="{public_base_url}/widget-assets/{path}">'
        for path in css_paths
    )

    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{WIDGET_TITLE}</title>
    {css_links}
  </head>
  <body>
    <div id="root"></div>
    <script>window.__NESTFINDER_API_BASE__ = "{public_base_url}";</script>
    <script type="module" src="{public_base_url}/widget-assets/{script_path}"></script>
  </body>
</html>
"""
