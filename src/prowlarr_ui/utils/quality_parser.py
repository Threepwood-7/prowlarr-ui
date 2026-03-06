"""Parse quality tags from release titles"""

import re

# Ordered by priority (first match wins for each category)
# Word boundaries (\b) prevent false matches inside words
# (e.g. "CAM" in "Camilla", "TS" in "Monsters", "DV" in "Adventure")
_RESOLUTION = [
    (re.compile(r"\b2160p\b|\b4K\b|\bUHD\b", re.IGNORECASE), "2160p"),
    (re.compile(r"\b1080p\b", re.IGNORECASE), "1080p"),
    (re.compile(r"\b720p\b", re.IGNORECASE), "720p"),
    (re.compile(r"\b480p\b|\bSD\b", re.IGNORECASE), "480p"),
]

_SOURCE = [
    (re.compile(r"\bREMUX\b", re.IGNORECASE), "REMUX"),
    (re.compile(r"\bBlu-?Ray\b|\bBDRip\b|\bBRRip\b", re.IGNORECASE), "BluRay"),
    (re.compile(r"\bWEB-?DL\b", re.IGNORECASE), "WEB-DL"),
    (re.compile(r"\bWEBRip\b|\bWEB\b", re.IGNORECASE), "WEBRip"),
    (re.compile(r"\bHDRip\b", re.IGNORECASE), "HDRip"),
    (re.compile(r"\bHDTV\b", re.IGNORECASE), "HDTV"),
    (re.compile(r"\bDVDRip\b|\bDVD\b", re.IGNORECASE), "DVD"),
    (re.compile(r"\bCAM\b|\bTS\b|\bHDTS\b|\bTELESYNC\b", re.IGNORECASE), "CAM"),
]

_CODEC = [
    (re.compile(r"\bx265\b|\bH\.?265\b|\bHEVC\b", re.IGNORECASE), "HEVC"),
    (re.compile(r"\bx264\b|\bH\.?264\b|\bAVC\b", re.IGNORECASE), "x264"),
    (re.compile(r"\bAV1\b", re.IGNORECASE), "AV1"),
    (re.compile(r"\bXviD\b|\bDivX\b", re.IGNORECASE), "XviD"),
]

_HDR = [
    (re.compile(r"\bDovi\b|\bDoVi\b|\bDolby[\.\s]?Vision\b", re.IGNORECASE), "DV"),
    (re.compile(r"\bDV\b"), "DV"),  # Case-sensitive: only uppercase "DV"
    (re.compile(r"\bHDR10\+", re.IGNORECASE), "HDR10+"),
    (re.compile(r"\bHDR10\b", re.IGNORECASE), "HDR10"),
    (re.compile(r"\bHDR\b", re.IGNORECASE), "HDR"),
]


def parse_quality(title: str) -> str:
    """Extract quality tags from a release title, return compact string."""
    parts = []
    for patterns in (_RESOLUTION, _SOURCE, _CODEC, _HDR):
        for regex, label in patterns:
            if regex.search(title):
                parts.append(label)
                break
    return " ".join(parts)
