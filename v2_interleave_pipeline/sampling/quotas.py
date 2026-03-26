from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


def parse_category_ratio_json(s: str) -> Dict[str, float]:
    """
    Accept JSON string or path to json file.
    Example:
      {"0205": 0.4, "0201": 0.6}
    """
    import json
    import os

    if os.path.exists(s):
        with open(s, "r", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        obj = json.loads(s)
    if not isinstance(obj, dict):
        raise ValueError("category_ratio_json must be a dict")
    out: Dict[str, float] = {}
    for k, v in obj.items():
        if not isinstance(k, str):
            k = str(k)
        if not isinstance(v, (int, float)):
            continue
        out[k] = float(v)
    if not out:
        raise ValueError("category_ratio_json empty/invalid")
    return out


def normalize_ratios(ratios: Dict[str, float]) -> Dict[str, float]:
    total = sum(v for v in ratios.values() if v > 0)
    if total <= 0:
        raise ValueError("ratios total must be > 0")
    return {k: (v / total) for k, v in ratios.items() if v > 0}


def compute_quotas(ratios: Dict[str, float], sample_total: int) -> Dict[str, int]:
    """
    Convert ratios to integer quotas that sum to <= sample_total.
    Remaining rounding slack is handled by largest remainder.
    """
    if sample_total <= 0:
        raise ValueError("sample_total must be > 0")
    ratios = normalize_ratios(ratios)
    raw = {k: ratios[k] * sample_total for k in ratios}
    base = {k: int(v) for k, v in raw.items()}
    used = sum(base.values())
    remain = sample_total - used
    if remain > 0:
        # largest remainder wins
        remainders = sorted(((raw[k] - base[k], k) for k in raw), reverse=True)
        for i in range(min(remain, len(remainders))):
            _, k = remainders[i]
            base[k] += 1
    return {k: v for k, v in base.items() if v > 0}


def reservoir_sample_unique_md5(
    *,
    md5_stream: Iterable[Tuple[str, str]],
    # md5_stream: iterable of (category_code, pdf_md5) pairs that already passed gates
    quotas: Dict[str, int],
    seed: int,
) -> Dict[str, List[str]]:
    """
    Reservoir sampling (approx) per category.
    - Ensures uniqueness within each worker reservoir for a given category.
    - Caller should oversample at worker-level if doing multi-process merge.
    """
    rng = random.Random(seed)

    k_by_cat = quotas
    reservoir: Dict[str, List[str]] = {cat: [] for cat in k_by_cat}
    seen_in_cat: Dict[str, Set[str]] = {cat: set() for cat in k_by_cat}
    seen_cnt: Dict[str, int] = {cat: 0 for cat in k_by_cat}

    for cat, md5 in md5_stream:
        if cat not in k_by_cat:
            continue
        if md5 in seen_in_cat[cat]:
            continue
        seen_in_cat[cat].add(md5)
        seen_cnt[cat] += 1
        k = k_by_cat[cat]
        if k <= 0:
            continue
        r = reservoir[cat]
        if len(r) < k:
            r.append(md5)
        else:
            # standard reservoir: replace with prob k/seen_cnt
            j = rng.randrange(0, seen_cnt[cat])
            if j < k:
                r[j] = md5
    return reservoir

