from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Set, Tuple, Union


def _get(d: Dict[str, Any], path: Sequence[str], default: Any = None) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def get_image_num(record: Dict[str, Any]) -> Optional[int]:
    """
    Prefer `meta_info.statistics_info.clean_image_num`.
    Fallback to counting `images` interleave slots that are not null.
    """
    val = _get(record, ["meta_info", "statistics_info", "clean_image_num"], default=None)
    if isinstance(val, int):
        return val
    images = record.get("images")
    if isinstance(images, list):
        cnt = 0
        for x in images:
            if x is not None:
                cnt += 1
        return cnt
    return None


def get_total_token(record: Dict[str, Any]) -> Optional[int]:
    val = _get(record, ["meta_info", "statistics_info", "clean_total_token"], default=None)
    return val if isinstance(val, int) else None


def get_layout_decision_score(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    decision = _get(record, ["meta_info", "origin_pdf_layout_score", "decision"], default=None)
    score = _get(record, ["meta_info", "origin_pdf_layout_score", "score"], default=None)
    if isinstance(score, (int, float)):
        return decision, float(score)
    return decision, None


def get_nsfw_decision_score(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    decision = _get(record, ["meta_info", "origin_pdf_nsfw_score", "decision"], default=None)
    score = _get(record, ["meta_info", "origin_pdf_nsfw_score", "score"], default=None)
    if isinstance(score, (int, float)):
        return decision, float(score)
    return decision, None


def get_language(record: Dict[str, Any]) -> Optional[str]:
    lang = _get(record, ["meta_info", "language_fasttext", "language"], default=None)
    return lang if isinstance(lang, str) else None


def get_main_category(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Compatible with:
    - dict schema:
        "category_cls_v1.3": { "category_name": "...", "category_code": "...", "score": 0.37 }
    - list schema:
        "category_cls_v1.3": [ { "category_name": "...", "category_code": "...", "score": 0.63 }, ... ]

    Returns: (category_code, category_name, score) for the best item (max score) when list.
    """
    obj = _get(record, ["meta_info", "category_cls_v1.3"], default=None)

    def parse_one(d: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        code = d.get("category_code")
        name = d.get("category_name")
        score = d.get("score")
        code_s = code if isinstance(code, str) else None
        name_s = name if isinstance(name, str) else None
        score_f = float(score) if isinstance(score, (int, float)) else None
        return code_s, name_s, score_f

    if isinstance(obj, dict):
        return parse_one(obj)

    if isinstance(obj, list):
        best: Tuple[Optional[str], Optional[str], Optional[float]] = (None, None, None)
        best_score = float("-inf")
        for it in obj:
            if not isinstance(it, dict):
                continue
            code, name, score = parse_one(it)
            s = score if score is not None else float("-inf")
            if s > best_score:
                best_score = s
                best = (code, name, score)
        return best

    return (None, None, None)


def get_main_category_code(record: Dict[str, Any]) -> Optional[str]:
    code, _, _ = get_main_category(record)
    return code


@dataclass(frozen=True)
class GatesConfig:
    # images gating
    min_image_num: int = 1

    # token length gating
    token_total_min: int = 0
    token_total_max: int = 10**18

    # layout gating
    layout_decision_ok: Optional[Set[str]] = None  # if None -> accept any except DROP
    layout_score_min: float = float("-inf")

    # nsfw gating
    nsfw_decision_ok: Optional[Set[str]] = None  # if None -> accept any except DROP
    nsfw_score_max: float = float("inf")

    # language gating
    languages_ok: Optional[Set[str]] = None

    # chapter filtering (stage selection only; true_chunk uses separate logic)
    chapter_level: Optional[int] = None


def passes_gates(record: Dict[str, Any], cfg: GatesConfig) -> bool:
    # images
    img_num = get_image_num(record)
    if img_num is None:
        return False
    if img_num < cfg.min_image_num:
        return False

    # token length
    tt = get_total_token(record)
    if tt is None:
        return False
    if tt < cfg.token_total_min or tt > cfg.token_total_max:
        return False

    # layout
    layout_decision, layout_score = get_layout_decision_score(record)
    if layout_decision is None or layout_score is None:
        return False
    if cfg.layout_decision_ok is None:
        if layout_decision == "DROP":
            return False
    else:
        if layout_decision not in cfg.layout_decision_ok:
            return False
    if layout_score < cfg.layout_score_min:
        return False

    # nsfw
    nsfw_decision, nsfw_score = get_nsfw_decision_score(record)
    if nsfw_decision is None or nsfw_score is None:
        return False
    if cfg.nsfw_decision_ok is None:
        if nsfw_decision == "DROP":
            return False
    else:
        if nsfw_decision not in cfg.nsfw_decision_ok:
            return False
    if nsfw_score > cfg.nsfw_score_max:
        return False

    # language
    if cfg.languages_ok is not None:
        lang = get_language(record)
        if lang is None or lang not in cfg.languages_ok:
            return False

    # chapter_level filtering (use existing chapter_info presence)
    if cfg.chapter_level is not None:
        ch = record.get("meta_info", {}).get("chapter_info")
        if not isinstance(ch, dict):
            return False
        level_set = {v for v in ch.values() if isinstance(v, int)}
        if cfg.chapter_level not in level_set:
            return False

    return True

