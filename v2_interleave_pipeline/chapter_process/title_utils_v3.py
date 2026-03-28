#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 mineru middle.json 的 pdf_info 中按文档顺序抽取 title 块，生成与 chapter_info 对齐的 tid -> TitleInfo。
tid 为从 0 递增的标题序号（与行内 meta chapter_info 的 key 一致）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from v2_interleave_pipeline.chapter_process.mineru_code.enum_class import BlockType
from v2_interleave_pipeline.chapter_process.mineru_code.vlm_middle_json_mkcontent import merge_para_with_text


@dataclass
class TitleInfo:
    page_idx: int
    block_listidx: int
    text: str


def print_filter_res(
    filter_bundle: Tuple[List[Any], List[Any], List[Any], List[Any], List[Any]],
    title_info: Dict[int, TitleInfo],
) -> str:
    """调试用：打印「模型分级未覆盖的 title tid」等摘要。"""
    missing_tids, *_rest = filter_bundle
    keys = set(title_info.keys())
    return (
        f"[print_filter_res] title_blocks={len(keys)} "
        f"chapter_info_missing_tids={len(missing_tids)} sample={missing_tids[:15]}"
    )


class TitleUtils:
    """从原始 middle.json 的 pdf_info_list 提取标题索引，供 chapter_info / 切分区间使用。"""

    def __init__(self, pdf_info_list: List[Dict[str, Any]], lang: str = "zh") -> None:
        self.pdf_info_list = pdf_info_list
        self.lang = lang
        self.title_info_dict: Dict[int, TitleInfo] = {}

    def extract_title_info(self) -> None:
        self.title_info_dict.clear()
        tid = 0
        pages = sorted(self.pdf_info_list, key=lambda x: x.get("page_idx", 0))
        for page in pages:
            pidx = int(page.get("page_idx", 0))
            for bidx, block in enumerate(page.get("para_blocks", [])):
                if block.get("type") != BlockType.TITLE:
                    continue
                text = merge_para_with_text(block, formula_enable=True, img_buket_path="images").strip()
                self.title_info_dict[tid] = TitleInfo(page_idx=pidx, block_listidx=bidx, text=text)
                tid += 1
