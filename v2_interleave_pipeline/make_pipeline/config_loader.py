from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from v2_interleave_pipeline.filters.gates import GatesConfig
from v2_interleave_pipeline.chapter_process.slice_panguml_item import ChapterSliceParams


@dataclass
class MakeDataConfig:
    """顶层配置：run_mode + 门控 + 章节切分。"""

    run_mode: str  # "make_data"
    gates: GatesConfig
    chapter_split_enabled: bool
    chapter_read_mode: str
    chapter_slice: ChapterSliceParams
    # 切分未产出任何行时：是否回写整行
    emit_original_when_split_empty: bool
    # 同一数据集内相同 pdf_md5 只保留首次出现的行（章节切分多行同 md5 也只留第一行）
    dedupe_pdf_md5: bool = True


def _to_set(x: Any) -> Optional[Set[str]]:
    if x is None:
        return None
    if isinstance(x, list):
        return set(str(s) for s in x)
    if isinstance(x, str) and x.strip() == "":
        return None
    raise ValueError(f"expected list or null for set field, got {type(x)}")


def gates_from_dict(d: Dict[str, Any]) -> GatesConfig:
    ls = d.get("layout_score_min")
    ns = d.get("nsfw_score_max")
    return GatesConfig(
        min_image_num=int(d.get("min_image_num", 1)),
        max_image_num=int(d.get("max_image_num", 10**18)),
        token_total_min=int(d.get("token_total_min", 0)),
        token_total_max=int(d.get("token_total_max", 10**18)),
        layout_decision_ok=_to_set(d.get("layout_decision_ok")),
        layout_score_min=float("-inf") if ls is None else float(ls),
        nsfw_decision_ok=_to_set(d.get("nsfw_decision_ok")),
        nsfw_score_max=float("inf") if ns is None else float(ns),
        languages_ok=_to_set(d.get("languages_ok")),
        chapter_level=int(d["chapter_level"]) if d.get("chapter_level") is not None else None,
    )


def load_make_config(path: str) -> MakeDataConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("config root must be a JSON object")

    run_mode = str(raw.get("run_mode", "make_data"))
    gates = gates_from_dict(raw.get("gates") or {})
    ch = raw.get("chapter_split") or {}
    chapter_split_enabled = bool(ch.get("enabled", False))
    chapter_read_mode = str(ch.get("read_mode", "read_panguml"))
    chapter_slice = ChapterSliceParams(
        min_page_num=int(ch.get("min_page_num", 15)),
        max_page_num=int(ch.get("max_page_num", 100)),
        min_imgs_count=int(ch.get("min_imgs_count", 1)),
        min_texts_len=int(ch.get("min_texts_len", 100)),
        verbose=bool(ch.get("verbose", False)),
    )
    emit_original = bool(raw.get("emit_original_when_split_empty", True))
    dedupe_pdf_md5 = bool(raw.get("dedupe_pdf_md5", True))

    return MakeDataConfig(
        run_mode=run_mode,
        gates=gates,
        chapter_split_enabled=chapter_split_enabled,
        chapter_read_mode=chapter_read_mode,
        chapter_slice=chapter_slice,
        emit_original_when_split_empty=emit_original,
        dedupe_pdf_md5=dedupe_pdf_md5,
    )


def load_make_config_optional(path: Optional[str]) -> MakeDataConfig:
    if path and os.path.isfile(path):
        return load_make_config(path)
    return MakeDataConfig(
        run_mode="make_data",
        gates=GatesConfig(),
        chapter_split_enabled=False,
        chapter_read_mode="read_panguml",
        chapter_slice=ChapterSliceParams(),
        emit_original_when_split_empty=True,
        dedupe_pdf_md5=True,
    )
