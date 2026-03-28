from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple


def _merge_int_counters(dst: Dict[str, int], src: Dict[str, Any]) -> None:
    for k, v in src.items():
        try:
            dst[k] = dst.get(k, 0) + int(v)
        except (TypeError, ValueError):
            continue


def _minmax_merge(
    cur_min: Optional[int],
    cur_max: Optional[int],
    mn: Any,
    mx: Any,
) -> Tuple[Optional[int], Optional[int]]:
    if mn is None and mx is None:
        return cur_min, cur_max
    try:
        imn = int(mn) if mn is not None else None
        imx = int(mx) if mx is not None else None
    except (TypeError, ValueError):
        return cur_min, cur_max
    if imn is not None:
        cur_min = imn if cur_min is None else min(cur_min, imn)
    if imx is not None:
        cur_max = imx if cur_max is None else max(cur_max, imx)
    return cur_min, cur_max


def records_seen_from_saved(obj: Dict[str, Any]) -> int:
    """
    Row count for one subset (all jsonl lines scanned in analyze).
    Supports current and older reservoir_stats.json shapes.
    """
    for key in ("records_seen", "total_seen"):
        v = obj.get(key)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    o = obj.get("overall_stats") or {}
    v = o.get("records_seen")
    if v is not None:
        try:
            return int(v)
        except (TypeError, ValueError):
            pass
    return 0


def extract_full_stats_from_saved(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Support new `full_stats` block and legacy top-level keys."""
    if "full_stats" in obj and isinstance(obj["full_stats"], dict):
        return obj["full_stats"]
    tt = obj.get("token_total") or {}
    ci = obj.get("clean_image_num") or obj.get("image_num") or {}
    if not isinstance(tt, dict):
        tt = {}
    if not isinstance(ci, dict):
        ci = {}
    return {
        "layout_decisions_count": dict(obj.get("layout_decisions_count") or {}),
        "nsfw_decisions_count": dict(obj.get("nsfw_decisions_count") or {}),
        "language_count": dict(obj.get("language_count") or {}),
        "category_code_count": dict(obj.get("category_code_count") or {}),
        "category_name_count": dict(obj.get("category_name_count") or {}),
        "token_total": dict(tt),
        "clean_image_num": dict(ci),
    }


def extract_extra_stats(obj: Dict[str, Any]) -> Dict[str, Any]:
    o = obj.get("overall_stats") or {}
    ex = o.get("extra_stats") or obj.get("extra_stats") or {}
    return dict(ex) if isinstance(ex, dict) else {}


def merge_extra_stats(blocks: List[Dict[str, Any]], total_records: int) -> Dict[str, Any]:
    """Merge `img_nonzero_and_lang_zh_en_and_layout_parse` style blocks by summing counts."""
    key = "img_nonzero_and_lang_zh_en_and_layout_parse"
    total_count = 0
    for ex in blocks:
        sub = ex.get(key) if isinstance(ex, dict) else None
        if isinstance(sub, dict) and "count" in sub:
            try:
                total_count += int(sub["count"])
            except (TypeError, ValueError):
                pass
    denom = max(0, int(total_records))
    return {
        key: {
            "count": total_count,
            "ratio": (total_count / denom) if denom else 0.0,
            "denominator": denom,
        }
    }


def build_combined_full_stats(per_dataset_full: List[Dict[str, Any]]) -> Dict[str, Any]:
    layout: Dict[str, int] = {}
    nsfw: Dict[str, int] = {}
    lang: Dict[str, int] = {}
    cat_code: Dict[str, int] = {}
    cat_name: Dict[str, int] = {}
    tmin: Optional[int] = None
    tmax: Optional[int] = None
    imin: Optional[int] = None
    imax: Optional[int] = None

    for fs in per_dataset_full:
        _merge_int_counters(layout, fs.get("layout_decisions_count") or {})
        _merge_int_counters(nsfw, fs.get("nsfw_decisions_count") or {})
        _merge_int_counters(lang, fs.get("language_count") or {})
        _merge_int_counters(cat_code, fs.get("category_code_count") or {})
        _merge_int_counters(cat_name, fs.get("category_name_count") or {})
        tt = fs.get("token_total") or {}
        tmin, tmax = _minmax_merge(tmin, tmax, tt.get("min"), tt.get("max"))
        ci = fs.get("clean_image_num") or {}
        imin, imax = _minmax_merge(imin, imax, ci.get("min"), ci.get("max"))

    return {
        "layout_decisions_count": layout,
        "nsfw_decisions_count": nsfw,
        "language_count": lang,
        "category_code_count": cat_code,
        "category_name_count": cat_name,
        "token_total": {"min": tmin, "max": tmax},
        "clean_image_num": {"min": imin, "max": imax},
    }


def aggregate_stats_from_out_root(
    out_root: str,
    datasets_list_path: Optional[str] = None,
) -> Tuple[List[str], Dict[str, Any], Dict[str, Any]]:
    """
    Load per-dataset `analysis/reservoir_stats.json` under out_root.
    Returns: (ordered_names, datasets_map, combined_block)
    """
    names: List[str] = []
    if datasets_list_path and os.path.isfile(datasets_list_path):
        with open(datasets_list_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().replace("\r", "")
                if not s or s.startswith("#"):
                    continue
                names.append(s)
    else:
        if not os.path.isdir(out_root):
            raise FileNotFoundError(f"out_root not found: {out_root}")
        for name in sorted(os.listdir(out_root)):
            p = os.path.join(out_root, name, "analysis", "reservoir_stats.json")
            if os.path.isfile(p):
                names.append(name)

    datasets_map: Dict[str, Any] = {}
    per_full: List[Dict[str, Any]] = []
    extra_blocks: List[Dict[str, Any]] = []
    total_records = 0

    for name in names:
        path = os.path.join(out_root, name, "analysis", "reservoir_stats.json")
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        datasets_map[name] = data
        per_full.append(extract_full_stats_from_saved(data))
        extra_blocks.append(extract_extra_stats(data))
        total_records += records_seen_from_saved(data)

    combined_full = build_combined_full_stats(per_full)
    combined = {
        "records_seen": total_records,
        "full_stats": combined_full,
        "extra_stats": merge_extra_stats(extra_blocks, total_records),
        "description": "Pool-level: sum over subsets; each dimension count is sum of per-subset full_stats (all jsonl lines per subset).",
    }

    return names, datasets_map, combined


def write_pooled_stats_json(
    out_root: str,
    output_json: str,
    datasets_list_path: Optional[str] = None,
) -> Tuple[str, int]:
    names, datasets_map, combined = aggregate_stats_from_out_root(out_root, datasets_list_path)
    ds_keys = [n for n in names if n in datasets_map]
    payload = {
        "out_root": os.path.abspath(out_root),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_names": ds_keys,
        "subset_count": len(ds_keys),
        "datasets": datasets_map,
        "combined": combined,
    }
    out_abs = os.path.abspath(output_json)
    parent = os.path.dirname(out_abs)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return output_json, len(datasets_map)
