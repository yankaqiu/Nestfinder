"""
sbb_agent.py — SBB AI Agent powered by Claude
==============================================
Claude interprets free-form queries, decides which transport.opendata.ch
endpoints to call (and with what parameters), executes them, and returns
a natural-language answer.

No hardcoded intent matching — Claude figures it all out.

Requirements
------------
    pip install anthropic

Environment
-----------
    export ANTHROPIC_API_KEY=sk-ant-...

Usage
-----
    # As a library
    from sbb_agent import SBBAgent
    agent = SBBAgent()
    print(agent.query(lat=47.3769, lng=8.5417,
                      question="What's the next train from the closest station to Zürich HB?"))

    # As a CLI
    python sbb_agent.py --lat 47.3769 --lng 8.5417 \
        --question "Distance to Bern and travel time?"
"""

import json
import math
import os
import urllib.parse
import urllib.request
import argparse
from typing import Any

import anthropic

# ---------------------------------------------------------------------------
# Transport API base
# ---------------------------------------------------------------------------

TRANSPORT_BASE = "https://transport.opendata.ch/v1"



def _http_get(path: str, params: dict) -> dict:
    qs = urllib.parse.urlencode(
        {k: v for k, v in params.items() if v is not None}, doseq=True
    )
    url = f"{TRANSPORT_BASE}/{path}?{qs}"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "sbb-agent/2.0"},
    )
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.loads(resp.read().decode())


# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------

def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Tool implementations — these are what Claude calls
# ---------------------------------------------------------------------------

def tool_find_locations(
    query: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    location_type: str = "all",
) -> dict:
    """
    Search for stations, POIs, or addresses.
    Use (lat, lng) to find locations near a coordinate, or `query` for name search.
    location_type: 'all' | 'station' | 'poi' | 'address'
    """
    params: dict[str, Any] = {"type": location_type}
    if query:
        params["query"] = query
    if lat is not None:
        params["x"] = lat
    if lng is not None:
        params["y"] = lng

    data = _http_get("locations", params)
    stations = data.get("stations", [])

    # Enrich with haversine distance if we have a reference point
    if lat is not None and lng is not None:
        for s in stations:
            coord = s.get("coordinate") or {}
            slat, slng = coord.get("x"), coord.get("y")
            if slat and slng:
                s["distance_m"] = round(_haversine(lat, lng, float(slat), float(slng)))
        stations.sort(key=lambda s: s.get("distance_m", 999_999))

    return {"stations": stations[:10]}  # cap for token efficiency


def tool_get_connections(
    from_location: str,
    to_location: str,
    date: str | None = None,
    time: str | None = None,
    is_arrival_time: bool = False,
    transportations: list[str] | None = None,
    limit: int = 3,
) -> dict:
    """
    Get connections between two locations (names or station IDs).
    transportations: list of 'train','tram','bus','ship','cableway'
    """
    params: dict[str, Any] = {
        "from": from_location,
        "to": to_location,
        "limit": min(limit, 6),
    }
    if date:
        params["date"] = date
    if time:
        params["time"] = time
    if is_arrival_time:
        params["isArrivalTime"] = 1
    if transportations:
        params["transportations[]"] = transportations

    data = _http_get("connections", params)

    # Slim down for token efficiency
    slim = []
    for c in data.get("connections", []):
        slim.append(
            {
                "from_station": (c.get("from") or {}).get("station", {}).get("name"),
                "to_station": (c.get("to") or {}).get("station", {}).get("name"),
                "departure": (c.get("from") or {}).get("departure"),
                "arrival": (c.get("to") or {}).get("arrival"),
                "duration": c.get("duration"),
                "transfers": c.get("transfers"),
                "products": c.get("products"),
                "sections_count": len(c.get("sections") or []),
            }
        )
    return {"connections": slim}


def tool_get_stationboard(
    station: str | None = None,
    station_id: str | None = None,
    limit: int = 8,
    transportations: list[str] | None = None,
    datetime_str: str | None = None,
    board_type: str = "departure",
) -> dict:
    """
    Get live departure or arrival board for a station.
    board_type: 'departure' | 'arrival'
    """
    params: dict[str, Any] = {
        "limit": min(limit, 20),
        "type": board_type,
    }
    if station_id:
        params["id"] = station_id
    elif station:
        params["station"] = station
    else:
        return {"error": "Provide station name or station_id"}

    if transportations:
        params["transportations[]"] = transportations
    if datetime_str:
        params["datetime"] = datetime_str

    data = _http_get("stationboard", params)

    slim = []
    for entry in data.get("stationboard", []):
        stop = entry.get("stop") or {}
        slim.append(
            {
                "line": f"{entry.get('category','')}{entry.get('number','')}",
                "operator": entry.get("operator"),
                "destination": entry.get("to"),
                "departure": stop.get("departure"),
                "platform": stop.get("platform"),
                "delay": (stop.get("prognosis") or {}).get("departure"),
            }
        )
    return {
        "station": (data.get("station") or {}).get("name"),
        "board": slim,
    }


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "find_locations",
        "description": (
            "Search for Swiss public transport stations, stops, POIs, or addresses. "
            "Either provide a text `query` (name search) or `lat`+`lng` (nearby search). "
            "Returns up to 10 locations with coordinates and distance from the origin point."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Name to search for, e.g. 'Zürich HB'"},
                "lat": {"type": "number", "description": "Latitude for nearby search"},
                "lng": {"type": "number", "description": "Longitude for nearby search"},
                "location_type": {
                    "type": "string",
                    "enum": ["all", "station", "poi", "address"],
                    "description": "Filter by location type (default: all)",
                },
            },
        },
    },
    {
        "name": "get_connections",
        "description": (
            "Get the next public transport connections between two locations. "
            "Accepts station names or IDs as from/to. "
            "Optionally filter by transport type and specify date/time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "from_location": {"type": "string", "description": "Departure station name or ID"},
                "to_location": {"type": "string", "description": "Arrival station name or ID"},
                "date": {"type": "string", "description": "Date YYYY-MM-DD (optional)"},
                "time": {"type": "string", "description": "Time HH:MM (optional)"},
                "is_arrival_time": {"type": "boolean", "description": "Treat time as arrival time"},
                "transportations": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["train", "tram", "bus", "ship", "cableway"]},
                    "description": "Filter by transport modes",
                },
                "limit": {"type": "integer", "description": "Number of connections (1-6, default 3)"},
            },
            "required": ["from_location", "to_location"],
        },
    },
    {
        "name": "get_stationboard",
        "description": (
            "Get the live departure or arrival board for a specific station. "
            "Provide either the station name or its numeric ID."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "station": {"type": "string", "description": "Station name, e.g. 'Winterthur'"},
                "station_id": {"type": "string", "description": "Station ID, e.g. '8503400'"},
                "limit": {"type": "integer", "description": "Number of entries (default 8)"},
                "transportations": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["train", "tram", "bus", "ship", "cableway"]},
                },
                "datetime_str": {
                    "type": "string",
                    "description": "Date+time filter 'YYYY-MM-DD HH:MM'",
                },
                "board_type": {
                    "type": "string",
                    "enum": ["departure", "arrival"],
                    "description": "departure (default) or arrival board",
                },
            },
        },
    },
]

TOOL_FN_MAP = {
    "find_locations": tool_find_locations,
    "get_connections": tool_get_connections,
    "get_stationboard": tool_get_stationboard,
}

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert Swiss public transport assistant with access to the
live transport.opendata.ch API (which covers all SBB trains, trams, buses, boats, etc.).

You have three tools:
1. find_locations  — find stations/stops near a coordinate OR by name
2. get_connections — get connections between two places with travel time & transfers
3. get_stationboard — get live departures/arrivals at a station

Guidelines:
- The user always provides their position as (lat, lng). Use this as context.
- Chain tool calls as needed. For example, to answer "distance to Zürich HB":
    1. find_locations(lat=..., lng=..., location_type='station') → get nearest station
    2. find_locations(query='Zürich HB', location_type='station') → get Zürich HB coords
    3. get_connections(from=nearest_station, to='Zürich HB') → travel time
- Always report BOTH straight-line distance (from distance_m field) AND travel time.
- For "closest train station" queries, prefer stations with IDs starting with 85
  (those are mainline rail stations in the Swiss UIC numbering).
- Be concise but complete. Format numbers clearly (e.g. "1.2 km", "14 min").
- If a tool returns an error or empty result, explain it gracefully.
- Today's date/time context: use it if the user asks about "next" departures without specifying time."""


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class SBBAgent:
    def __init__(self, api_key: str | None = None, model: str = "claude-sonnet-4-5"):
        self.client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
        self.model = model

    def query(self, lat: float, lng: float, question: str, verbose: bool = False) -> str:
        """
        Run the agent for a given location and question.
        Returns the final answer as a string.
        """
        user_message = (
            f"My current position: latitude={lat}, longitude={lng}\n\n"
            f"Question: {question}"
        )
        messages = [{"role": "user", "content": user_message}]

        if verbose:
            print(f"\n[Agent] Question: {question}")
            print(f"[Agent] Position: lat={lat}, lng={lng}")

        # Agentic loop
        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            if verbose:
                print(f"[Agent] Stop reason: {response.stop_reason}")

            # Collect text and tool_use blocks
            tool_uses = []
            text_blocks = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_uses.append(block)
                elif block.type == "text":
                    text_blocks.append(block.text)

            # If Claude is done, return the final text
            if response.stop_reason == "end_turn" or not tool_uses:
                return "\n".join(text_blocks).strip()

            # Execute all tool calls
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tu in tool_uses:
                fn = TOOL_FN_MAP.get(tu.name)
                if fn is None:
                    result = {"error": f"Unknown tool: {tu.name}"}
                else:
                    try:
                        # Inject user's lat/lng into find_locations if not provided
                        args = dict(tu.input)
                        if tu.name == "find_locations":
                            if "lat" not in args and "query" not in args:
                                args["lat"] = lat
                                args["lng"] = lng
                        result = fn(**args)
                        if verbose:
                            print(f"[Tool] {tu.name}({json.dumps(args, ensure_ascii=False)[:120]})")
                            print(f"       → {json.dumps(result, ensure_ascii=False)[:200]}")
                    except Exception as e:
                        result = {"error": str(e)}

                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )

            messages.append({"role": "user", "content": tool_results})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SBB AI Agent")
    parser.add_argument("--lat", type=float, default=47.3769, help="Latitude")
    parser.add_argument("--lng", type=float, default=8.5417, help="Longitude")
    parser.add_argument("--question", type=str, default="What is the distance to Zürich HB?")
    parser.add_argument("--verbose", action="store_true", help="Show tool calls")
    args = parser.parse_args()

    agent = SBBAgent(api_key="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")
    answer = agent.query(lat=args.lat, lng=args.lng, question=args.question, verbose=args.verbose)
    print("\n" + "=" * 60)
    print("ANSWER:")
    print("=" * 60)
    print(answer)