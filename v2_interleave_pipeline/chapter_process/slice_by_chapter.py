#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
File_Name: slice_by_chapter.py
Function: 小规模按章节切分，输入为单个jsonl，输出也为单个jsonl
Author: d30073006
Created_Time: 2026/3/27
"""
import os
import json
import ast
from copy import deepcopy
from typing import Any, Dict, Optional
from collections import defaultdict
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import tarfile
import shutil
from PIL import Image

try:
    import moxing as mox  # type: ignore
except ImportError:  # 本地开发无 moxing 时仍可 import 本模块
    mox = None  # type: ignore


def _open_text_read(path: str):
    """与训练环境 mox.file.File 对齐；本地无 moxing 时用 open。"""
    if mox is not None:
        return mox.file.File(path, "r", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


from v2_interleave_pipeline.chapter_process.title_utils_v3 import TitleUtils, print_filter_res
from v2_interleave_pipeline.chapter_process.mineru_code.enum_class import BlockType, ContentType
from v2_interleave_pipeline.chapter_process.utils.mineru_utils import (
    get_content_by_page_idx_and_block_listidx,
    get_images_by_middle_json,
    merge_block_pairs,
    remake_page_info_list,
)
from v2_interleave_pipeline.chapter_process.utils.convert_panguml import content2panguml

"""
1. 识别封面和参考文献和其他无用内容
2. 获取第N级标题
3. 限定阈值：小于阈值扩展，大于阈值放过（模型会自动截断，但是要保证图文成对）

"""

# ----------------------------
# 1. 读取工具
# ----------------------------
def read_jsonl_from_model_jsonl(jsonl_path):
    """从模型分级输出的JSONL中提取标题分级字典"""
    jsonl_data = []
    error_data = []  # 模型失败存储

    with _open_text_read(jsonl_path) as f:
        for md_idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            raw_level_str = ""
            try:
                title_item = json.loads(line)
                md_dir = title_item['pdf_name']
                pdf_name = os.path.basename(md_dir)
                middle_json_path = os.path.join(md_dir, f'{pdf_name}_middle.json')

                QA_data = title_item.get('data', [])

                # 查找 assistant 角色
                for turn in QA_data:
                    if turn.get('role') == "assistant":
                        contents = turn.get('content', [])
                        if len(contents) != 1:
                            print(f"第 {md_idx} 行： 长度不等于1")
                            continue

                        raw_level_str = contents[0].get('text', {}).get('string', "").strip()

                        if not raw_level_str:
                            print(f"第 {md_idx} 行：Assistant 内容为空")
                            continue

                        # --- 核心转换逻辑 ---
                        try:
                            # 1. 尝试直接 json 解析
                            title_levels = json.loads(raw_level_str)
                        except json.JSONDecodeError:
                            # 2. 如果失败（通常是因为 key 没带引号），使用 ast 解析
                            # 替换换行符并处理成 Python 字面量
                            title_levels = ast.literal_eval(raw_level_str.replace('\n', ''))

                        # 二次加工title_level，使其键为整数
                        new_title_levels = dict()
                        for key, value in title_levels.items():
                            if int(value) < 1 or int(value) > 4:  # TODO 过滤
                                continue

                            new_title_levels[int(key)] = int(value)

                        # print(f"成功获取第 {md_idx} 行字典: {new_title_levels}")
                        # print(f"类型: {type(new_title_levels)}")
                        jsonl_data.append(
                            {"middle_json_path": middle_json_path,
                             "title_levels": new_title_levels,
                             'cleaned_middle_json_path': middle_json_path,
                             'id': f"伪造_{md_idx}"})
            except Exception as e:
                # print(f"处理第 {md_idx} 行时发生错误: {e}, {raw_level_str}")
                print(f"发生异常 原因：{e}")
                error_data.append({"idx": md_idx, "error_reason": str(e)})

    return jsonl_data, error_data


def read_jsonl_from_panguml_jsonl(jsonl_path):
    """从归档的panguml格式JSONL中提取标题分级字典"""
    jsonl_data = []
    error_data = []
    with _open_text_read(jsonl_path) as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                meta = item.get("meta_info", {})
                addr = meta.get("address", {})
                raw_ch = meta.get("chapter_info") or {}
                # JSON 里 key 常为字符串，与 title_info 的 int tid 对齐
                title_levels: Dict[int, int] = {}
                for k, v in raw_ch.items():
                    try:
                        title_levels[int(k)] = int(v)
                    except (TypeError, ValueError):
                        continue
                # 新增：构建图片路径 -> 图片完整信息的映射，用于后续回填 width/height 等
                images_map: Dict[str, Dict[str, Any]] = {}
                for img in item.get("images") or []:
                    if isinstance(img, dict):
                        rel_path = img.get("relative_img_path")
                        if isinstance(rel_path, str) and rel_path:
                            images_map[rel_path] = img
                jsonl_data.append(
                    {
                        "middle_json_path": addr["s3_parse_address"],
                        "title_levels": title_levels,
                        "cleaned_middle_json_path": addr["s3_clean_address"],
                        "id": item["id"],
                        "texts": item["texts"],
                        "images": item["images"],
                        "img_cnt": item["img_cnt"],
                        "text_cnt": item["text_cnt"],
                        "images_map": images_map,
                        "page_location": item.get("page_location"),
                        "source_record": item,
                    }
                )
            except Exception as e:
                # print(f"发生异常 原因：{e}")
                error_data.append({"idx": idx, "error_reason": str(e)})

    return jsonl_data, error_data

# ----------------------------
# 2. mineru二次开发工具
# ----------------------------

def get_title_idxs_by_level(input_level, title_levels):
    """
    根据 level 获取标题索引列表
    :param input_level: 整数类型，目标层级
    :param title_levels: 字典类型 {index: level}
    :return: 包含所有匹配索引的列表
    """
    # 遍历字典，如果值等于 input_level，则收集其键
    return [idx for idx, lvl in title_levels.items() if lvl == input_level]


def get_title_idxs_by_level_and_bounds(level, bounds, title_info, all_title_levels, page_count):
    """
    根据层级和位置范围来获取目标标题
    """
    left_bound, right_bound = bounds

    # 特殊指定范围【用不到】
    if left_bound is None:
        left_bound = (0, -1)
    if right_bound is None:
        right_bound = (page_count, 0)

    target_tids = []

    for tid, l in all_title_levels.items():
        # 层级是否匹配
        if l != level:
            continue

        info = title_info[tid]
        current_pos = (info.page_idx, info.block_listidx)

        # 新增：开区间匹配，避免边界标题重复归属
        if left_bound < current_pos < right_bound:
            target_tids.append(tid)

    return sorted(target_tids)


def split_cblocks_by_level_and_bounds(parent_tid, level, bounds, title_info, all_title_levels, page_count):
    """
    基于level且位于bounds内，划分N子块。前提这个bounds不能跨两个level-1级的内容！
    """
    left_bound, right_bound = bounds
    levels = get_title_idxs_by_level_and_bounds(level, bounds, title_info, all_title_levels, page_count)
    if not levels:
        return [(left_bound, right_bound, parent_tid, level)]   # 此间没有新的标题等级，不可再划分

    split_cblocks = []

    i = 0
    first_title_info = title_info[levels[0]]
    if (first_title_info.page_idx, first_title_info.block_listidx) != (0, 0):
        i = -1

    while i < len(levels):
        # 首尾处理
        if i == -1:  # 第一快
            c_p = left_bound[0]
            c_b = left_bound[1]
        else:
            tid = levels[i]
            c_p = title_info[tid].page_idx
            c_b = title_info[tid].block_listidx - 1  # 首包含

        n_idx = i + 1
        if n_idx == len(levels):  # 末尾
            n_p = right_bound[0]
            n_b = right_bound[1]
        else:
            n_tid = levels[n_idx]
            n_p = title_info[n_tid].page_idx
            n_b = title_info[n_tid].block_listidx

        split_cblocks.append(((c_p, c_b), (n_p, n_b), parent_tid, level))
        i += 1

    return split_cblocks


def get_middle_contents_info(contents, blocks):
    """
    获取内容的信息：图片数量和文字长度
    """
    # 正则表达是匹配图片
    img_pattern = re.compile(r"!\[\]\((images/[a-f0-9]{64}\.jpg)\)")

    real_img_count = 0
    text_len = 0
    for content in contents:
        matches = img_pattern.findall(content)
        real_img_count += len(matches)

        # 计算文本长度
        pure_text = img_pattern.sub('', content)

        lines = pure_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            text_len += len(line)

    # 验证2： 实际图片数量和使用图片数量是否一致
    target_img_count = 0
    for para_block in blocks:
        if para_block['type'] == BlockType.IMAGE:
            for block in para_block['blocks']:
                if block['type'] == BlockType.IMAGE_BODY:
                    for line in block['lines']:
                        for span in line['spans']:
                            if span['type'] == ContentType.IMAGE:
                                if span.get('image_path', ''):
                                    target_img_count += 1

    if target_img_count != real_img_count:
        print(
            f"【警告】：文本中的实际图片数量({real_img_count}) != middle.json的图片数量({target_img_count})。原因：可能存在空表格内容自动转为图片。")

    return real_img_count, target_img_count, text_len


def remake_images_key(panguml_json, pdf_name):
    """
    把images的开头转为pdfname
    """
    images = panguml_json["images"]
    for image in images:
        if image is None:
            continue

        false_path = image["relative_img_path"]
        image["relative_img_path"] = f"{pdf_name}/{os.path.basename(false_path)}"

    return panguml_json


def reflect_image_w_h(panguml_json, images_map: Dict[str, Dict[str, Any]]):
    """
    新增：用源行 images 的完整字段回填切块后图片（如 width/height）。
    """
    images = panguml_json.get("images", [])
    for i, image_item in enumerate(images):
        if image_item is None:
            continue
        rel_path = image_item.get("relative_img_path")
        if isinstance(rel_path, str) and rel_path in images_map:
            images[i] = images_map[rel_path]
    return panguml_json
# ----------------------------
# 3. 主函数
# ----------------------------

def process_main(
    read_mode,
    jsonl_path,
    local_dir,
    min_page_num: int = 15,
    max_page_num: int = 100,
    min_imgs_count: int = 1,
    min_texts_len: int = 100,
    max_level: int = 4,
    write_tar: bool = False,
):
    """主入口函数（参数化版本）"""
    # ============== 步骤一：获取处理的middle_json和分级结果对 ==============
    if read_mode == "read_model":
        # 方法一：读取模型输出JSONL，并构造分级结果
        jsonl_data, error_data = read_jsonl_from_model_jsonl(jsonl_path)
    elif read_mode == "read_panguml":
        # 方法二：读取panguml格式jsonl
        jsonl_data, error_data = read_jsonl_from_panguml_jsonl(jsonl_path)
    else:
        raise ValueError(f"未知的读取模式: {read_mode}，请检查配置。")

    count_valid_pair = len(jsonl_data)
    count_fail_pair = len(error_data)

    print("-" * 30)
    print(f"解析 JSONL 完成！")
    print(f"✅ 成功数量: {count_valid_pair}")
    print(f"❌ 失败数量: {count_fail_pair}")

    # 如果有失败，打印前 3 条错误原因供排查
    if count_fail_pair > 0:
        print("错误样例 (前3条):")
        for err in error_data[:3]:
            print(f"  - 行号 {err['idx']}: {err['error_reason']}")
    print("-" * 30)

    # 容错检查
    if count_valid_pair == 0:
        print("⚠️ 警告：没有解析到任何有效数据，请检查输入文件路径或格式！")
        return

    # ============== 步骤二：输入切分基准（第几级标题 & 文本最大长度）和设置路径 ==============
    """
    1. 更换Markdown层级
    2. 标题相邻，取最前面的标题
    3. 图片数量应该遵循“原生图没有取表格图，表格图没有取公式图？”
    4. 拆分合并逻辑
    拆分：页数超过100页。再取下一个标题；保证图片和文字数量（有效图片1个，文字3个，一条数据len大于100）
    合并：简单取下一个同级标题

    一条数据的“文字+图片”:大部分在8k，可接受1M
    """
    # 以上阈值由函数参数控制

    local_download_dir = os.path.join(local_dir, "download_dir")
    local_output_dir = os.path.join(local_dir, "output_dir")
    os.makedirs(local_download_dir, exist_ok=True)
    os.makedirs(local_output_dir, exist_ok=True)

    jsonl_name = os.path.basename(jsonl_path)
    jsonl_idx = os.path.splitext(jsonl_name)[0]
    local_jsonl_path = os.path.join(local_output_dir, jsonl_name)

    if write_tar:
        tar_name = jsonl_name.replace(".jsonl", ".tar")
        local_tar_path = os.path.join(local_output_dir, tar_name)

    # ============== 步骤三：遍历每个有效标题的item ==============
    for item in jsonl_data:
        input_level = 1     # 初设等级为1

        # 读取middle_json内容和标题分级信息
        middle_json_path = item['middle_json_path']
        cleaned_middle_json_path = item['cleaned_middle_json_path']
        all_title_levels = item['title_levels']
        id = item['id']

        print(id, cleaned_middle_json_path, middle_json_path, all_title_levels)

        # 过滤1：没有标题分级结果
        if not all_title_levels:
            print(f"【无标题分级】无法按章节切分！")
            continue

        md_dir = os.path.dirname(cleaned_middle_json_path)
        pdf_name = os.path.basename(md_dir)

        with _open_text_read(middle_json_path) as middle_fo, _open_text_read(cleaned_middle_json_path) as cleaned_middle_fo:
            middle_json = json.load(middle_fo)  # 目标2
            cleaned_middle_json = json.load(cleaned_middle_fo)

        pdf_info_list = middle_json['pdf_info']
        cleaned_pdf_info_list = cleaned_middle_json['pdf_info']
        page_count = len(cleaned_pdf_info_list)
        first_bound = (0, -1)
        last_bound = (page_count, 0)

        print(f"{'==' * 50}\n{md_dir}\n{'==' * 50}")
        print(f"共{page_count}页")

        # 过滤2：剔除短PDF
        if page_count <= min_page_num:
            print(f"【短PDF】不按章节切分！")
            continue

        # 提取原始的标题，以防模型漏输出
        title_util = TitleUtils(pdf_info_list, lang='zh')      # 不需要传入真正的语种，因为这里只是简单的提取标题的操作
        title_util.extract_title_info()
        title_info = title_util.title_info_dict

        print(print_filter_res((list(set(title_info.keys()) - set(all_title_levels.keys())), [], [], [], []), title_info))

        #  重新赋值标题等级
        real_pdf_info_list = remake_page_info_list(cleaned_pdf_info_list, title_info, title_levels=all_title_levels)

        # 获取该本的图片情况。决定是否需要表格图或者公式图
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

        # 确定初始切分级别和标题id，预防第1级没标题
        main_title_idxs = get_title_idxs_by_level(input_level, all_title_levels)  # 切分的基准
        while input_level <= max_level and not main_title_idxs:
            print(f"【重大警告】 取第{input_level}级标题为空，则下移一级")
            input_level += 1
            main_title_idxs = get_title_idxs_by_level(input_level, all_title_levels)  # 切分的基准
        if not main_title_idxs:
            print(f"【无可用标题】1~{max_level}级均为空，跳过: {id}")
            continue

        main_title_idxs.sort(reverse=False)  # 排序

        # 打印：遍历主要级别【可删】
        print(f"章节切分依据：第{input_level}级标题")
        for tid in main_title_idxs:
            info = title_info[tid]
            print(tid, info.text, info.page_idx, info.block_listidx)
        print("\n")

        # 分块：按初始标题等级
        init_cblocks = split_cblocks_by_level_and_bounds(
            "ROOT", input_level, (first_bound, last_bound), title_info, all_title_levels, page_count
        )
        print(f"按照第{input_level} 划分的结果是：{init_cblocks}\n")

        # 递归切分
        slice_cblocks = []
        cblocks = init_cblocks[:]
        while cblocks:
            cblock = cblocks.pop(0)

            # 1. 达到预设的最大深度（如4级），不再向下切，直接保留
            left_bound, right_bound, parent_tid, cblocks_level = cblock
            if cblocks_level == max_level:
                slice_cblocks.append(cblock)
                continue

            # 2. 检查是否触发“切分”条件
            if right_bound[0] - left_bound[0] > max_page_num:
                current_titles_in_block = get_title_idxs_by_level_and_bounds(
                    cblocks_level, (left_bound, right_bound), title_info, all_title_levels, page_count
                )
                sub_parent_tid = "_".join(map(str, current_titles_in_block))
                # print(sub_parent_tid)
                new_cblocks = split_cblocks_by_level_and_bounds(
                    sub_parent_tid, cblocks_level + 1, (left_bound, right_bound), title_info, all_title_levels,
                    page_count
                )
                cblocks[:0] = new_cblocks
                continue

            slice_cblocks.append(cblock)

        print(f"切分步骤后cblocks：")
        for i, cblock in enumerate(slice_cblocks):
            print(f"块{i}: {cblock}")

        # 保留与合并
        final_cblocks = []
        i = 0
        temp_list = slice_cblocks[:]

        while i < len(temp_list):
            curr_start, curr_end, p_tid, c_lvl = temp_list[i]
            current_cblock = (curr_start, curr_end, p_tid, c_lvl)

            # 1. 计算当前块质量
            mid_contents, mid_blocks = get_content_by_page_idx_and_block_listidx(
                real_pdf_info_list, curr_start[0], curr_end[0], curr_start[1], curr_end[1],
                True, formula_enable=enable_formula, table_enable=enable_table
            )
            m_imgs, _, m_text_len = get_middle_contents_info(mid_contents, mid_blocks)

            # 判定：是否达标
            if m_imgs >= min_imgs_count and m_text_len >= min_texts_len:
                final_cblocks.append(temp_list[i])
                print(f">>> [保留] {current_cblock}，当前图片: {m_imgs}；字数: {m_text_len} ")
                i += 1
                continue

            merged = False

            # A. 优先向右扩张（链式反应）
            if i + 1 < len(temp_list):
                n_start, n_end, n_p_tid, n_c_lvl = temp_list[i + 1]

                # 逻辑边界检查
                if n_p_tid == p_tid and n_c_lvl == c_lvl:
                    # 物理边界检查：合并后的页数跨度
                    if (n_end[0] - curr_start[0] + 1) <= max_page_num:
                        # 合并：修改当前块终点，移除右邻居
                        target_neighbor = temp_list[i + 1]
                        temp_list[i] = (curr_start, n_end, p_tid, c_lvl)
                        temp_list.pop(i + 1)
                        merged = True
                        print(f"【右合并】{current_cblock} 吞并了邻居 {target_neighbor}")
                        continue  # 立即重新开始 while 头部循环

            # B. 向左归并（妥协方案）
            if not merged and len(final_cblocks) > 0:
                prev_start, prev_end, p_p_tid, p_c_lvl = final_cblocks[-1]

                if p_p_tid == p_tid and p_c_lvl == c_lvl:
                    if (curr_end[0] - prev_start[0] + 1) <= max_page_num:
                        # 归并入已确定的 final 列表最后一个元素
                        final_cblocks[-1] = (prev_start, curr_end, p_p_tid, p_c_lvl)
                        merged = True
                        i += 1
                        print(f"<<< [左归并] {current_cblock} 动力不足，并入左侧老大哥")
                        continue

            # C. 孤立保留（既吃不到右边，也够不着左边）
            if not merged:
                print(f"!!! [强制保留] 碎片块无法再合并: {m_text_len}字")
                final_cblocks.append(temp_list[i])
                i += 1

        # 打印结果【可删除】
        print("\n" + "=" * 50)
        print(f"最终合并完成，共生成 {len(final_cblocks)} 个块：")

        for idx, final_item in enumerate(final_cblocks):
            f_start, f_end, f_p_tid, f_lvl = final_item

            # 提取实际内容
            mid_contents, _ = get_content_by_page_idx_and_block_listidx(
                real_pdf_info_list, f_start[0], f_end[0], f_start[1], f_end[1],
                False, formula_enable=enable_formula, table_enable=enable_table
            )

            print(f"--- [最终块 {idx}] Parent: {f_p_tid} 层级: {f_lvl} ---")
            print(f"范围: {f_start} -> {f_end}")
            print(f"内容摘要: {mid_contents}")  # 打印前100字预览
            print("-" * 30)

        # 转为jsonl
        final_items = []
        for idx, final_item in enumerate(final_cblocks):
            f_start, f_end, f_p_tid, f_lvl = final_item
            mid_contents, _ = get_content_by_page_idx_and_block_listidx(
                real_pdf_info_list, f_start[0], f_end[0], f_start[1], f_end[1],
                False, formula_enable=enable_formula, table_enable=enable_table
            )
            slice_item = content2panguml(mid_contents, "", "", False)
            if slice_item is not None:
                slice_item = remake_images_key(slice_item, pdf_name)
                if item.get("images_map"):
                    slice_item = reflect_image_w_h(slice_item, item["images_map"])
                # 更新字段
                slice_item["page_location"] = (f_start, f_end)
                slice_item["id"] = f"{id}_chapter_{idx}"    # chapter实则为第几段
                # 在输入 pangu 行基础上合并：切块字段覆盖，其余继承原行
                if read_mode == "read_panguml" and item.get("source_record"):
                    merged = deepcopy(item["source_record"])
                    merged.update(slice_item)
                    slice_item = merged
                final_items.append(slice_item)

            else:
                print(f"异常切块，分割后为空{f_start} -> {f_end}")

        print(final_items)

        # 验证3: 拼接切块是否覆盖原行（仅 read_panguml 且字段齐全时）
        new_images: list[Any] = []
        new_texts: list[Any] = []
        new_img_cnt, new_text_cnt = 0, 0
        new_page_locations = []

        for final_item in final_items:
            new_images.extend(final_item["images"])
            # content2panguml 使用 key `texts`
            new_texts.extend(final_item["texts"])
            new_img_cnt += final_item["img_cnt"]
            new_text_cnt += final_item["text_cnt"]
            new_page_locations.append(final_item["page_location"])

        if read_mode == "read_panguml" and "images" in item:
            if new_images == item["images"]:
                print(f"图片验证 成功！")
            else:
                print(f"图片验证 失败！")
            if new_texts == item["texts"]:
                print(f"文字验证 成功！")
            else:
                print(f"文字验证 失败！")
            if new_img_cnt == item["img_cnt"]:
                print(f"图片数量验证 成功！")
            else:
                print(f"图片数量验证 失败！")
            if new_text_cnt == item["text_cnt"]:
                print(f"文字数量验证 成功！")
            else:
                print(f"文字数量验证 失败！")
            if "page_location" in item and merge_block_pairs(new_page_locations) == item["page_location"]:
                print(f"位置验证 成功！")
            elif "page_location" in item:
                print(f"位置验证 失败！")


if __name__ == "__main__":
    # 读取模型输出的jsonl | 读归档池子的jsonl。目的均为获取`middle_json的路径`和`模型输出的分级结果`
    read_mode = "read_model" # read_model | read_panguml
    jsonl_path = "0708_mid-process_result.jsonl"
    local_dir = "/home/ma-user/work/pdf_parser_postprocess/cache"
    process_main(read_mode, jsonl_path, local_dir)