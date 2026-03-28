#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
File_Name: mineru_utils.py
Function: 二次开发mineru的工具
Author: d30073006
Created_Time: 2026/2/27
"""
from typing import List, Dict, Any, Tuple, Optional, Set
from copy import deepcopy

from v2_interleave_pipeline.chapter_process.mineru_code.enum_class import BlockType, ContentType, MakeMode
from v2_interleave_pipeline.chapter_process.mineru_code.vlm_middle_json_mkcontent import (
    mk_blocks_to_markdown,
    merge_para_with_text,
)

# ----------------------------
# 最常公用的
# ----------------------------
def remake_page_info_list(pdf_info_list, title_info, title_levels=None, fixed_level=None):
    """
    重新赋值page_info_list，有两种方式：
    1. title_levels：已分好具体级别
    2. fixed_level：固定级别
    """
    if bool(title_levels) == bool(fixed_level is not None):
        raise ValueError("参数冲突：必须且只能提供 'title_levels' 或 'fixed_level' 其中之一。")

    new_page_info_list = deepcopy(pdf_info_list)
    for tid, current_info in title_info.items():
        page_idx = current_info.page_idx
        block_listidx = current_info.block_listidx

        target_block = new_page_info_list[page_idx]['para_blocks'][block_listidx]

        # 修改
        if target_block.get("type") == "title":
            if title_levels:
                # 方式 1：从映射字典中取值
                if tid in title_levels:
                    target_block["level"] = title_levels[tid]
                else:
                    target_block["level"] = 0  # 不在模型分级的结果里就默认为0
                    # print(f"警告：索引 {tid} 不在 title_levels 字典中")
            else:
                # 方式 2：使用固定级别
                target_block["level"] = fixed_level

    return new_page_info_list


def get_content_by_page_idx_and_block_listidx(
        pdf_info_list: List[Dict[str, Any]],
        start_page_idx: int,
        end_page_idx: int,
        start_block_listidx: int,
        end_block_listidx: int,
        is_return_blocks: bool = False,
        formula_enable=True,
        table_enable=True
) -> Tuple[List[str], List[Any]]:
    """
    依据页码和 block 列表索引，获取中间内容（开区间：不包含 start_block 和 end_block）。

    Args:
        pdf_info_list: PDF 信息大列表
        start_page_idx: 起始页页码
        end_page_idx: 结束页页码
        start_block_listidx: 起始 block 在该页 list 中的索引
        end_block_listidx: 结束 block 在该页 list 中的索引
        is_return_blocks: 是否返回原始 block 对象列表

    Returns:
        (output_markdown_lines, filter_blocks)
    """

    # 1. 跨页拦截：如果开始页在结束页之后，直接返回空
    if start_page_idx > end_page_idx:
        return [], []

    # 2. 同页拦截：检查是否存在中间内容
    # 如果在同一页，且 start >= end - 1，说明中间没有元素
    if start_page_idx == end_page_idx and start_block_listidx >= (end_block_listidx - 1):
        return [], []

    # 按页码排序，确保处理顺序正确
    sorted_pages = sorted(pdf_info_list, key=lambda x: x.get('page_idx', -1))

    output_markdown_lines = []
    filter_blocks = []

    for page_info in sorted_pages:
        page_num = page_info.get('page_idx', -1)

        # 范围外页面跳过
        if not (start_page_idx <= page_num <= end_page_idx):
            continue

        paras_of_layout = page_info.get('para_blocks', [])
        if not paras_of_layout:
            continue

        filtered = []

        # --- 核心切片逻辑 ---
        if page_num == start_page_idx and page_num == end_page_idx:
            # 情况 A: 同页提取
            # 逻辑：(start, end) -> 切片 [start+1 : end]
            filtered = paras_of_layout[start_block_listidx + 1: end_block_listidx]

        elif page_num == start_page_idx:
            # 情况 B: 起始页
            # 逻辑：(start, 末尾] -> 切片 [start+1 : ]
            filtered = paras_of_layout[start_block_listidx + 1:]

        elif page_num == end_page_idx:
            # 情况 C: 结束页
            # 逻辑：[开头, end) -> 切片 [ : end]
            filtered = paras_of_layout[: end_block_listidx]

        else:
            # 情况 D: 中间完整页
            filtered = paras_of_layout

        if not filtered:
            continue

        # 收集原始 block (如果需要)
        if is_return_blocks:
            filter_blocks.extend(filtered)

        # 转换为 Markdown
        markdown_lines = mk_blocks_to_markdown(
            filtered,
            make_mode=MakeMode.MM_MD,
            formula_enable=formula_enable,
            table_enable=table_enable,
            img_buket_path='images'
        )
        output_markdown_lines.extend(markdown_lines)

    return output_markdown_lines, filter_blocks


def get_images_by_middle_json(pdf_info_list, block_types=[BlockType.IMAGE, BlockType.TABLE, BlockType.INTERLINE_EQUATION]):
    """
    :param is_all:
    :return:
    """
    # 初始化分类字典
    image_class = {
        BlockType.IMAGE: [],
        BlockType.TABLE: [],
        BlockType.INTERLINE_EQUATION: []
    }
    for pdf_info in pdf_info_list:
        for para_block in pdf_info.get("para_blocks", []):
            p_type = para_block.get("type")

            # 如果该块类型不在我们关心的范围内，跳过
            if p_type not in block_types:
                continue

            # 遍历块内部的所有子块（blocks）或行（lines）
            # 1. 处理 IMAGE 和 TABLE（它们通常有子 blocks 结构）
            if p_type in [BlockType.IMAGE, BlockType.TABLE]:
                for sub_block in para_block.get("blocks", []):
                    # 检查所有包含内容的行
                    for line in sub_block.get("lines", []):
                        for span in line.get("spans", []):
                            img_path = span.get("image_path")
                            if img_path:
                                image_class[p_type].append(img_path)

            # 2. 处理 INTERLINE_EQUATION（有时直接在 para_block 级别包含 lines）
            elif p_type == BlockType.INTERLINE_EQUATION:
                for line in para_block.get("lines", []):
                    for span in line.get("spans", []):
                        img_path = span.get("image_path")
                        if img_path:
                            image_class[p_type].append(img_path)
    return image_class

# ----------------------------
# 清洗时专用
# ----------------------------
def get_content_by_para_block(para_block, formula_enable=True, table_enable=True):
    """
    获取单个para_block的内容

    :param para_block:
    :return:
    """
    res = mk_blocks_to_markdown(
        [para_block],
        make_mode=MakeMode.MM_MD,
        formula_enable=formula_enable,
        table_enable=table_enable,
        img_buket_path='images'
    )
    if res:
        return res[0].strip()
    else:
        return ''


def aggregate_to_open_optimized(identified_pos_set, pdf_info_list):
    """
    聚合N个block为M个连续block_pairs

    :param identified_pos_set:
    :param pdf_info_list:
    :return:
    """
    if not identified_pos_set:
        return []

    # 1. 排序：利用 Python 元组原生的高效比较
    sorted_pos = sorted(list(identified_pos_set))

    # 2. 预加载页面长度，避免在循环中重复 call len()
    page_lens = [len(page["para_blocks"]) for page in pdf_info_list]

    merged = []
    if not sorted_pos: return []

    s_node = sorted_pos[0]
    p_node = sorted_pos[0]

    for c_node in sorted_pos[1:]:
        # 连续性判定：同页差1，或者跨页接力
        if (c_node[0] == p_node[0] and c_node[1] == p_node[1] + 1) or \
                (c_node[0] == p_node[0] + 1 and c_node[1] == 0 and p_node[1] == page_lens[p_node[0]] - 1):
            p_node = c_node
        else:
            merged.append(yield_open_pair(s_node, p_node, page_lens))
            s_node = c_node
            p_node = c_node

    merged.append(yield_open_pair(s_node, p_node, page_lens))
    return merged


def yield_open_pair(start_node, end_node, page_lens):
    """
    辅助函数：专门处理开边界转换逻辑
    """
    sp, sb = start_node
    ep, eb = end_node

    # 左开：永远向前推一个
    open_start = (sp, sb - 1)

    # 右开：只有在该页最后一个时才跨页进位
    if eb >= page_lens[ep] - 1:
        open_end = (ep + 1, 0)
    else:
        open_end = (ep, eb + 1)

    return open_start, open_end


def merge_block_pairs(block_pairs):
    """
    对 (start_pos, end_pos) 形式的区间进行物理排序并合并重叠部分。
    start_pos/end_pos 格式为 (page_idx, block_idx)
    """
    if not block_pairs:
        return []

    # 1. 必须先排序！
    # 排序规则：按 start_pos 的 page_idx 升序，再按 block_idx 升序
    sorted_pairs = sorted(list(block_pairs), key=lambda x: (x[0][0], x[0][1]))

    # 2. 合并重叠
    merged = []
    if sorted_pairs:
        merged.append(sorted_pairs[0])

        for i in range(1, len(sorted_pairs)):
            current_start, current_end = sorted_pairs[i]
            prev_start, prev_end = merged[-1]

            # 判定：当前区间的起点 (current_start) 是否在上一区间的终点 (prev_end) 之前或重合
            # 元组比较 (p1, b1) <= (p2, b2) 在 Python 中天然支持物理顺序比较
            if current_start < prev_end:
                # 存在重叠，更新上一个区间的终点为两者中的较大值
                # 使用 max 确保即便一个区间完全包裹另一个区间也能正确处理
                merged[-1] = (prev_start, max(prev_end, current_end))
            else:
                # 物理上不连续，作为独立区间加入
                merged.append((current_start, current_end))

    return merged