from __future__ import annotations

import os
from typing import Any

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.requests import Request as StarletteRequest
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.staticfiles import StaticFiles

from apps_sdk.server.client import get_listings_api_client
from apps_sdk.server.widget import (
    WIDGET_MIME_TYPE,
    WIDGET_TEMPLATE_URI,
    get_public_base_url,
    get_widget_dist_dir,
    load_widget_html,
    resolve_widget_asset_fallback,
)
from app.preferences import (
    VALID_ACTIONS,
    build_user_profile,
    get_events,
    record_event,
)

SEARCH_TOOL_NAME = "search_listings"
RECORD_PREF_TOOL_NAME = "record_preference"
GET_PREFS_TOOL_NAME = "get_user_preferences"
GET_PROFILE_TOOL_NAME = "get_user_profile"
MAP_RESOURCE_ORIGINS = [
    "https://a.basemaps.cartocdn.com",
    "https://b.basemaps.cartocdn.com",
    "https://c.basemaps.cartocdn.com",
    "https://d.basemaps.cartocdn.com",
    "https://assets.comparis.ch",
    "https://assets-comparis.b-cdn.net",
]


class SearchListingsInput(BaseModel):
    query: str = Field(..., description="Natural-language property search query.")
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    session_id: str | None = Field(default=None, description="Opaque session identifier for personalization.")

    model_config = ConfigDict(extra="forbid")


class RecordPreferenceInput(BaseModel):
    listing_id: str = Field(..., description="ID of the listing the user interacted with.")
    action: str = Field(
        ...,
        description=f"User action type. One of: {', '.join(sorted(VALID_ACTIONS))}.",
    )
    query: str | None = Field(default=None, description="The query that surfaced this listing.")
    session_id: str | None = Field(default=None, description="Opaque session identifier.")

    model_config = ConfigDict(extra="forbid")


class GetUserPreferencesInput(BaseModel):
    session_id: str | None = Field(default=None, description="Filter events by session.")
    listing_id: str | None = Field(default=None, description="Filter events by listing ID.")
    action: str | None = Field(default=None, description="Filter by action type.")
    limit: int = Field(default=25, ge=1, le=200)

    model_config = ConfigDict(extra="forbid")


class GetUserProfileInput(BaseModel):
    session_id: str | None = Field(default=None, description="Session ID to build the profile for.")

    model_config = ConfigDict(extra="forbid")


class PublicWidgetStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope: dict[str, Any]) -> Response:
        response = await super().get_response(path, scope)
        if response.status_code == 404:
            fallback_path = resolve_widget_asset_fallback(
                dist_dir=get_widget_dist_dir(),
                request_path=path,
            )
            if fallback_path is not None:
                response = FileResponse(fallback_path)
        if response.status_code < 400:
            response.headers.setdefault("Access-Control-Allow-Origin", "*")
            response.headers.setdefault("Access-Control-Allow-Methods", "GET, OPTIONS")
            response.headers.setdefault("Access-Control-Allow-Headers", "*")
            response.headers.setdefault("Cross-Origin-Resource-Policy", "cross-origin")
        return response


def _split_env_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _transport_security_settings() -> TransportSecuritySettings:
    allowed_hosts = _split_env_list(os.getenv("MCP_ALLOWED_HOSTS"))
    allowed_origins = _split_env_list(os.getenv("MCP_ALLOWED_ORIGINS"))
    if not allowed_hosts and not allowed_origins:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )


def build_tool_descriptor() -> types.Tool:
    return types.Tool(
        name=SEARCH_TOOL_NAME,
        title="Search listings",
        description="Search Swiss real-estate listings from the harness and render a ranked list with map pins.",
        inputSchema=SearchListingsInput.model_json_schema(),
        annotations=types.ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            openWorldHint=False,
        ),
        _meta=build_tool_meta(),
    )


def build_record_pref_descriptor() -> types.Tool:
    return types.Tool(
        name=RECORD_PREF_TOOL_NAME,
        title="Record user preference",
        description="Log a user interaction with a listing such as click, favorite, or dismiss.",
        inputSchema=RecordPreferenceInput.model_json_schema(),
        annotations=types.ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            openWorldHint=False,
        ),
    )


def build_get_prefs_descriptor() -> types.Tool:
    return types.Tool(
        name=GET_PREFS_TOOL_NAME,
        title="Get user preferences",
        description="Retrieve logged user listing interactions for personalization.",
        inputSchema=GetUserPreferencesInput.model_json_schema(),
        annotations=types.ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            openWorldHint=False,
        ),
    )


def build_get_profile_descriptor() -> types.Tool:
    return types.Tool(
        name=GET_PROFILE_TOOL_NAME,
        title="Get user profile",
        description="Build a simple preference profile from a user's click and favorite history.",
        inputSchema=GetUserProfileInput.model_json_schema(),
        annotations=types.ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            openWorldHint=False,
        ),
    )


def build_search_tool_result(
    *,
    query: str,
    payload: dict[str, Any],
) -> types.CallToolResult:
    count = len(payload.get("listings", []))
    summary = f"Showing {count} listing{'s' if count != 1 else ''} for “{query}”."
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=summary)],
        structuredContent=payload,
        _meta=build_tool_result_meta(),
    )


def build_tool_meta() -> dict[str, Any]:
    return {
        "securitySchemes": [{"type": "noauth"}],
        "ui": {
            "resourceUri": WIDGET_TEMPLATE_URI,
            "visibility": ["model", "app"],
        },
        "openai/outputTemplate": WIDGET_TEMPLATE_URI,
        "openai/toolInvocation/invoking": "Searching listings…",
        "openai/toolInvocation/invoked": "Listings ready",
        "openai/widgetAccessible": True,
    }


def build_tool_result_meta() -> dict[str, Any]:
    return {"openai/outputTemplate": WIDGET_TEMPLATE_URI}


def build_resource_contents_meta(*, public_base_url: str | None = None) -> dict[str, Any]:
    base_url = public_base_url or get_public_base_url()
    return {
        "ui": {
            "prefersBorder": False,
            "csp": {
                "connectDomains": [base_url, *MAP_RESOURCE_ORIGINS],
                "resourceDomains": [base_url, *MAP_RESOURCE_ORIGINS],
            },
        },
        "openai/widgetAccessible": True,
    }


mcp = FastMCP(
    name="datathon2026-listings-app",
    stateless_http=True,
    transport_security=_transport_security_settings(),
)


@mcp._mcp_server.list_tools()
async def _list_tools() -> list[types.Tool]:
    return [
        build_tool_descriptor(),
        build_record_pref_descriptor(),
        build_get_prefs_descriptor(),
        build_get_profile_descriptor(),
    ]


@mcp._mcp_server.list_resources()
async def _list_resources() -> list[types.Resource]:
    return [
        types.Resource(
            name="Listings map and ranked list",
            title="Listings map and ranked list",
            uri=WIDGET_TEMPLATE_URI,
            description="Combined ranked list and map widget for listing search results.",
            mimeType=WIDGET_MIME_TYPE,
            _meta=build_resource_contents_meta(),
        )
    ]


async def _handle_read_resource(req: types.ReadResourceRequest) -> types.ServerResult:
    if str(req.params.uri) != WIDGET_TEMPLATE_URI:
        raise ValueError(f"Unknown resource: {req.params.uri}")

    html = load_widget_html(
        dist_dir=get_widget_dist_dir(),
        public_base_url=get_public_base_url(),
    )
    return types.ServerResult(
        types.ReadResourceResult(
            contents=[
                types.TextResourceContents(
                    uri=WIDGET_TEMPLATE_URI,
                    mimeType=WIDGET_MIME_TYPE,
                    text=html,
                    _meta=build_resource_contents_meta(),
                )
            ]
        )
    )


async def _handle_call_tool(req: types.CallToolRequest) -> types.ServerResult:
    name = req.params.name
    args = req.params.arguments or {}

    if name == SEARCH_TOOL_NAME:
        try:
            search_input = SearchListingsInput.model_validate(args)
        except ValidationError as exc:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Invalid input: {exc.errors()}")],
                    isError=True,
                )
            )

        response_payload = await get_listings_api_client().search_listings(
            query=search_input.query,
            limit=search_input.limit,
            offset=search_input.offset,
            session_id=search_input.session_id,
        )
        return types.ServerResult(
            build_search_tool_result(query=search_input.query, payload=response_payload)
        )

    if name == RECORD_PREF_TOOL_NAME:
        try:
            pref_input = RecordPreferenceInput.model_validate(args)
        except ValidationError as exc:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Invalid input: {exc.errors()}")],
                    isError=True,
                )
            )
        try:
            event_id = record_event(
                listing_id=pref_input.listing_id,
                action=pref_input.action,
                query=pref_input.query,
                session_id=pref_input.session_id,
            )
        except ValueError as exc:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text=str(exc))],
                    isError=True,
                )
            )
        return types.ServerResult(
            types.CallToolResult(
                content=[
                    types.TextContent(
                        type="text",
                        text=f"Recorded '{pref_input.action}' for listing {pref_input.listing_id} (event #{event_id}).",
                    )
                ],
                structuredContent={
                    "event_id": event_id,
                    "listing_id": pref_input.listing_id,
                    "action": pref_input.action,
                },
            )
        )

    if name == GET_PREFS_TOOL_NAME:
        try:
            prefs_input = GetUserPreferencesInput.model_validate(args)
        except ValidationError as exc:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Invalid input: {exc.errors()}")],
                    isError=True,
                )
            )
        events = get_events(
            session_id=prefs_input.session_id,
            listing_id=prefs_input.listing_id,
            action=prefs_input.action,
            limit=prefs_input.limit,
        )
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Found {len(events)} event(s).")],
                structuredContent={"events": events, "count": len(events)},
            )
        )

    if name == GET_PROFILE_TOOL_NAME:
        try:
            profile_input = GetUserProfileInput.model_validate(args)
        except ValidationError as exc:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Invalid input: {exc.errors()}")],
                    isError=True,
                )
            )
        profile = build_user_profile(session_id=profile_input.session_id)
        lines = ["**User preference profile**\n"]
        if profile["preferred_cities"]:
            lines.append(f"- Preferred cities: {', '.join(profile['preferred_cities'])}")
        if profile["preferred_features"]:
            lines.append(f"- Preferred features: {', '.join(profile['preferred_features'])}")
        if profile["price_range"]:
            price_range = profile["price_range"]
            lines.append(f"- Price range: CHF {price_range['min']}–{price_range['max']}/mo")
        if profile["recent_searches"]:
            lines.append(f"- Recent searches: {'; '.join(profile['recent_searches'][:5])}")
        if profile["favorite_listing_ids"]:
            lines.append(f"- Favorited listings: {', '.join(profile['favorite_listing_ids'])}")
        if not any(
            [
                profile["preferred_cities"],
                profile["preferred_features"],
                profile["favorite_listing_ids"],
            ]
        ):
            lines.append("No interaction history yet.")
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text="\n".join(lines))],
                structuredContent=profile,
            )
        )

    else:
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Unknown tool: {name}")],
                isError=True,
            )
        )


mcp._mcp_server.request_handlers[types.ReadResourceRequest] = _handle_read_resource
mcp._mcp_server.request_handlers[types.CallToolRequest] = _handle_call_tool

app = mcp.streamable_http_app()


async def _preferences_http(request: StarletteRequest) -> JSONResponse:
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    if request.method == "OPTIONS":
        return JSONResponse({}, headers=cors_headers)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400, headers=cors_headers)

    listing_id = str(body.get("listing_id") or "").strip()
    action = str(body.get("action") or "click").strip()
    query = body.get("query")
    session_id = body.get("session_id")
    if not listing_id:
        return JSONResponse({"error": "listing_id required"}, status_code=400, headers=cors_headers)
    try:
        event_id = record_event(
            listing_id=listing_id,
            action=action,
            query=query,
            session_id=session_id,
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400, headers=cors_headers)

    return JSONResponse({"event_id": event_id, "ok": True}, headers=cors_headers)


app.add_route("/preferences", _preferences_http, methods=["POST", "OPTIONS"])
_widget_dist_dir = get_widget_dist_dir()
_widget_dist_dir.mkdir(parents=True, exist_ok=True)
app.mount(
    "/widget-assets",
    PublicWidgetStaticFiles(directory=str(_widget_dist_dir)),
    name="widget-assets",
)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("APPS_SDK_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
