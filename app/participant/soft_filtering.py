from __future__ import annotations

import re
from typing import Any

_JUNK_RE = re.compile(
    r"\bparkplatz\b|\bparking\s+spot\b|\beinstellplatz\b|\btiefgarage\b"
    r"|\bhobbyraum\b|\babstellraum\b|\blager\b|\bstorage\b"
    r"|\bplace\s+de\s+parc\b|\bbox\b|\bgarage\s+box\b",
    re.I,
)

_RESIDENTIAL_SIGNALS = re.compile(
    r"\bwohnung\b|\bapartment\b|\bzimmer\b|\bstudio\b|\bloft\b|\bmaisonette\b"
    r"|\bappartement\b|\bpièces?\b|\blocali\b|\bhaus\b|\bhouse\b|\bvilla\b"
    r"|\breihenhaus\b|\battika\b|\bdachwohnung\b|\bmansarde\b",
    re.I,
)


def _is_junk(candidate: dict[str, Any]) -> bool:
    title = (candidate.get("title") or "").lower()
    if _JUNK_RE.search(title) and not _RESIDENTIAL_SIGNALS.search(title):
        return True
    price = candidate.get("price")
    if price is not None and price < 100:
        return True
    return False


def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    filtered = [c for c in candidates if not _is_junk(c)]
    return filtered if filtered else candidates
