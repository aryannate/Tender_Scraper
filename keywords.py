"""
Keyword filtering for IT / software / AI tenders.

Why a strict matcher is needed
------------------------------
GeM's server-side `fullText` search is a TOKEN-OR match, not a phrase match:
searching "machine learning" returns 167k bids (matching "machine" OR
"learning" — e.g. grinding wheels), and "face recognition" returns paint and
brooms. So we can only use server search to *shrink* the candidate set; the real
filtering must happen here, with word-boundary / phrase-aware regexes.

Each keyword becomes a regex that:
  * treats spaces and hyphens as interchangeable separators
    ("AI-based" matches "ai based" and "ai-based"),
  * allows an optional trailing plural "s" ("chatbot" matches "chatbots"),
  * is anchored on non-alphanumeric boundaries so short tokens don't over-match
    ("app" matches "web app" but NOT "apparel"; "vision" does NOT match
    "supervision").
"""

from __future__ import annotations

import re

# The user's target keyword list (deduplicated, whitespace-trimmed).
KEYWORDS: list[str] = [
    "AI",
    "Artificial Intelligence",
    "GPU",
    "GPU Server",
    "AI Server",
    "HPC",
    "High Performance Computing",
    "Supercomputer",
    "Compute Cluster",
    "H100",
    "H200",
    "B200",
    "B300",
    "GB200",
    "GB300",
    "DGX",
    "HGX",
    "NVL72",
    "RTX PRO",
    "NVIDIA",
    "CUDA",
    "InfiniBand",
    "Spectrum-X",
    "NVLink",
    "NVSwitch",
    "400GbE",
    "800GbE",
    "Parallel Storage",
    "Parallel File System",
    "Lustre",
    "BeeGFS",
    "GPFS",
    "Slurm",
    "Kubernetes",
    "AI Workstation",
    "Workstation",
    "Server",
    "Storage",
    "HCI",
    "VDI",
    "Private Cloud",
    "Data Center",
    "Data Centre",
]


# Empanelment / expression-of-interest / rate-contract detection. Used to
# recognize that a notice IS an empanelment; whether it's KEPT is decided by
# EMPANELMENT_KEYWORDS below (only IT-domain empanelments are wanted).
# Shared here so pipeline + dashboard agree.
EMPANELMENT_RE = re.compile(
    r"empanel|enlist|panel of|rate contract|expression of interest|\bEOI\b|"
    r"pre[- ]?qualif|\bRFE\b|selection of (?:consultant|agenc|consortium|vendor|partner)",
    re.IGNORECASE,
)


def is_empanelment(*texts: str) -> bool:
    return bool(EMPANELMENT_RE.search(" ".join(t for t in texts if t)))


# Domain keywords an empanelment must ALSO match to be kept (user-specified).
# An empanelment notice about e.g. drug supply or freight forwarders is dropped;
# one matching the same IT/AI keyword list as regular tenders is kept — the
# empanelments tab uses the identical KEYWORDS list, not a separate one.
EMPANELMENT_KEYWORDS: list[str] = KEYWORDS


def _compile(keyword: str) -> re.Pattern:
    parts = re.split(r"[\s\-]+", keyword.strip())
    body = r"[\s\-]+".join(re.escape(p) for p in parts if p)
    # non-alphanumeric boundaries + optional trailing plural
    pattern = r"(?<![A-Za-z0-9])" + body + r"s?(?![A-Za-z0-9])"
    return re.compile(pattern, re.IGNORECASE)


class KeywordMatcher:
    def __init__(self, keywords: list[str] | None = None):
        self.keywords = keywords if keywords is not None else KEYWORDS
        self._patterns = [(kw, _compile(kw)) for kw in self.keywords]

    def match(self, *texts: str) -> list[str]:
        """Return the list of keywords found across the given text fields."""
        haystack = " ".join(t for t in texts if t)
        if not haystack:
            return []
        hits = [kw for kw, rx in self._patterns if rx.search(haystack)]
        return hits

    def matches(self, *texts: str) -> bool:
        return bool(self.match(*texts))


def load_keywords(path: str | None) -> list[str]:
    """Load one keyword per line from a file, or fall back to the built-in list."""
    if not path:
        return KEYWORDS
    with open(path, encoding="utf-8-sig") as fh:
        kws = []
        seen = set()
        for line in fh:
            keyword = line.strip()
            if keyword and not keyword.startswith("#") and keyword.casefold() not in seen:
                kws.append(keyword)
                seen.add(keyword.casefold())
    if not kws:
        raise ValueError(f"Keyword file contains no keywords: {path}")
    return kws
