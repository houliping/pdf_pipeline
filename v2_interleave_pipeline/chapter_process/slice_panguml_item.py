#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""单条 pangu 记录 → 章节切块 → 多条 json 行（供数据制作流水线调用）。"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    import moxing as mox  # type: ignore
except ImportError:
    mox = None  # type: ignore


def _open_text_read(path: str):
    if mox is not None:
        return mox.file.File(path, "r", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


from v2_interleave_pipeline.chapter_process.title_utils_v3 import TitleUtils, print_filter_res
from v2_interleave_pipeline.chapter_process.mineru_code.enum_class import BlockType
from v2_interleave_pipeline.chapter_process.utils.mineru_utils import (
    get_content_by_page_idx_and_block_listidx,
    get_images_by_middle_json,
    merge_block_pairs,
    remake_page_info_list,
)
from v2_interleave_pipeline.chapter_process.utils.convert_panguml import content2panguml

from v2_interleave_pipeline.chapter_process.slice_by_chapter import (
    get_title_idxs_by_level,
    get_title_idxs_by_level_and_bounds,
    get_middle_contents_info,
    remake_images_key,
    split_cblocks_by_level_and_bounds,
)


@dataclass
class ChapterSliceParams:
    min_page_num: int = 15
    max_page_num: int = 100
    min_imgs_count: int = 1
    min_texts_len: int = 100
    verbose: bool = False


def panguml_record_to_item(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """单行 pool jsonl → slice 所需 item；缺字段返回 None。"""
    try:
        meta = record.get("meta_info") or {}
        addr = meta.get("address") or {}
        parse_p = addr.get("s3_parse_address")
        clean_p = addr.get("s3_clean_address")
        if not parse_p or not clean_p:
            return None
        raw_ch = meta.get("chapter_info") or {}
        title_levels: Dict[int, int] = {}
        for k, v in raw_ch.items():
            try:
                title_levels[int(k)] = int(v)
            except (TypeError, ValueError):
                continue
        return {
            "middle_json_path": parse_p,
            "title_levels": title_levels,
            "cleaned_middle_json_path": clean_p,
            "id": record.get("id", ""),
            "texts": record["texts"],
            "images": record["images"],
            "img_cnt": record["img_cnt"],
            "text_cnt": record["text_cnt"],
            "source_record": record,
        }
    except Exception:
        return None


def _vlog(params: ChapterSliceParams, msg: str) -> None:
    if params.verbose:
        print(msg)


def slice_panguml_item_to_rows(
    item: Dict[str, Any],
    read_mode: str,
    params: ChapterSliceParams,
) -> List[Dict[str, Any]]:
    """
    对单条 item（read_jsonl_from_panguml_jsonl 结构）做章节切分。
    跳过/失败返回 []。
    """
    min_page_num = params.min_page_num
    max_page_num = params.max_page_num
    min_imgs_count = params.min_imgs_count
    min_texts_len = params.min_texts_len

    input_level = 1
    middle_json_path = item["middle_json_path"]
    cleaned_middle_json_path = item["cleaned_middle_json_path"]
    all_title_levels = item["title_levels"]
    row_id = item["id"]

    if not all_title_levels:
        _vlog(params, "【无标题分级】无法按章节切分")
        return []

    md_dir = os.path.dirname(cleaned_middle_json_path)
    pdf_name = os.path.basename(md_dir)

    try:
        with _open_text_read(middle_json_path) as middle_fo, _open_text_read(cleaned_middle_json_path) as cleaned_middle_fo:
            middle_json = json.load(middle_fo)
            cleaned_middle_json = json.load(cleaned_middle_fo)
    except Exception as e:
        _vlog(params, f"读取 middle 失败: {e}")
        return []

    pdf_info_list = middle_json["pdf_info"]
    cleaned_pdf_info_list = cleaned_middle_json["pdf_info"]
    page_count = len(cleaned_pdf_info_list)
    first_bound = (0, -1)
    last_bound = (page_count, 0)

    _vlog(params, f"{md_dir} 共{page_count}页")

    if page_count <= min_page_num:
        _vlog(params, "【短PDF】不按章节切分")
        return []

    title_util = TitleUtils(pdf_info_list, lang="zh")
    title_util.extract_title_info()
    title_info = title_util.title_info_dict

    _vlog(
        params,
        print_filter_res(
            (list(set(title_info.keys()) - set(all_title_levels.keys())), [], [], [], []),
            title_info,
        ),
    )

    real_pdf_info_list = remake_page_info_list(cleaned_pdf_info_list, title_info, title_levels=all_title_levels)

    classed_image = get_images_by_middle_json(pdf_info_list)
    if classed_image[BlockType.IMAGE]:
        enable_formula = True
        enable_table = True
    elif classed_image[BlockType.TABLE]:
        enable_formula = True
        enable_table = False
    elif classed_image[BlockType.INTERLINE_EQUATION]:
        enable_formula = False
        enable_table = False
    else:
        enable_formula = True
        enable_table = True

    main_title_idxs = get_title_idxs_by_level(input_level, all_title_levels)
    while not main_title_idxs:
        _vlog(params, f"第{input_level}级标题为空，下移一级")
        input_level += 1
        main_title_idxs = get_title_idxs_by_level(input_level, all_title_levels)

    main_title_idxs.sort(reverse=False)

    init_cblocks = split_cblocks_by_level_and_bounds(
        "ROOT", input_level, (first_bound, last_bound), title_info, all_title_levels, page_count
    )

    slice_cblocks: List[Any] = []
    cblocks = init_cblocks[:]
    while cblocks:
        cblock = cblocks.pop(0)
        left_bound, right_bound, parent_tid, cblocks_level = cblock
        if cblocks_level == 4:
            slice_cblocks.append(cblock)
            continue
        if right_bound[0] - left_bound[0] > max_page_num:
            current_titles_in_block = get_title_idxs_by_level_and_bounds(
                cblocks_level, (left_bound, right_bound), title_info, all_title_levels, page_count
            )
            sub_parent_tid = "_".join(map(str, current_titles_in_block))
            new_cblocks = split_cblocks_by_level_and_bounds(
                sub_parent_tid, cblocks_level + 1, (left_bound, right_bound), title_info, all_title_levels, page_count
            )
            cblocks[:0] = new_cblocks
            continue
        slice_cblocks.append(cblock)

    final_cblocks: List[Any] = []
    i = 0
    temp_list = slice_cblocks[:]

    while i < len(temp_list):
        curr_start, curr_end, p_tid, c_lvl = temp_list[i]
        current_cblock = (curr_start, curr_end, p_tid, c_lvl)

        mid_contents, mid_blocks = get_content_by_page_idx_and_block_listidx(
            real_pdf_info_list,
            curr_start[0],
            curr_end[0],
            curr_start[1],
            curr_end[1],
            True,
            formula_enable=enable_formula,
            table_enable=enable_table,
        )
        m_imgs, _, m_text_len = get_middle_contents_info(mid_contents, mid_blocks)

        if m_imgs >= min_imgs_count and m_text_len >= min_texts_len:
            final_cblocks.append(temp_list[i])
            i += 1
            continue

        merged = False
        if i + 1 < len(temp_list):
            n_start, n_end, n_p_tid, n_c_lvl = temp_list[i + 1]
            if n_p_tid == p_tid and n_c_lvl == c_lvl:
                if (n_end[0] - curr_start[0] + 1) <= max_page_num:
                    temp_list[i] = (curr_start, n_end, p_tid, c_lvl)
                    temp_list.pop(i + 1)
                    merged = True
                    continue

        if not merged and len(final_cblocks) > 0:
            prev_start, prev_end, p_p_tid, p_c_lvl = final_cblocks[-1]
            if p_p_tid == p_tid and p_c_lvl == c_lvl:
                if (curr_end[0] - prev_start[0] + 1) <= max_page_num:
                    final_cblocks[-1] = (prev_start, curr_end, p_p_tid, p_c_lvl)
                    merged = True
                    i += 1
                    continue

        if not merged:
            final_cblocks.append(temp_list[i])
            i += 1

    final_items: List[Dict[str, Any]] = []
    for idx, final_item in enumerate(final_cblocks):
        f_start, f_end, f_p_tid, f_lvl = final_item
        mid_contents, _ = get_content_by_page_idx_and_block_listidx(
            real_pdf_info_list,
            f_start[0],
            f_end[0],
            f_start[1],
            f_end[1],
            False,
            formula_enable=enable_formula,
            table_enable=enable_table,
        )
        slice_item = content2panguml(mid_contents, "", "", False)
        slice_item = remake_images_key(slice_item, pdf_name)
        if slice_item is None:
            continue
        slice_item["page_location"] = (f_start, f_end)
        slice_item["id"] = f"{row_id}_chapter_{idx}"
        if read_mode == "read_panguml" and item.get("source_record"):
            merged_row = deepcopy(item["source_record"])
            merged_row.update(slice_item)
            slice_item = merged_row
        final_items.append(slice_item)

    if params.verbose and read_mode == "read_panguml" and "images" in item and final_items:
        new_images, new_texts = [], []
        new_img_cnt, new_text_cnt = 0, 0
        new_page_locations = []
        for fi in final_items:
            new_images.extend(fi["images"])
            new_texts.extend(fi["texts"])
            new_img_cnt += fi["img_cnt"]
            new_text_cnt += fi["text_cnt"]
            new_page_locations.append(fi["page_location"])
        if new_images == item["images"]:
            print("图片验证 成功！")
        if new_texts == item["texts"]:
            print("文字验证 成功！")
        if new_img_cnt == item["img_cnt"]:
            print("图片数量验证 成功！")
        if new_text_cnt == item["text_cnt"]:
            print("文字数量验证 成功！")
        if "page_location" in item and merge_block_pairs(new_page_locations) == item["page_location"]:
            print("位置验证 成功！")

    return final_items
