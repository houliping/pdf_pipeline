#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
File_Name: convert_panguml.py
Function: main process entry
Author: d30073006
Created_Time: 2026/3/9
"""

import re
import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image
import tarfile
import contextlib

try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

from v2_interleave_pipeline.chapter_process.utils.mineru_utils import (
    get_content_by_page_idx_and_block_listidx,
)
from v2_interleave_pipeline.chapter_process.mineru_code.enum_class import BlockType


def get_image_size(image_path):
    """
    尝试获取图片尺寸，优先用 imagesize，然后回退到 PIL。
    成功返回 (w, h)；失败返回 (-1, -1)，并将 image_path 记录到 error_log。

    error_log 应包含三个 list：
        - 'not_exists'
        - 'imagesize_fail'
        - 'pil_fail'
    """
    # 情况一：路径不存在
    if not os.path.exists(image_path):
        return -1, -1

    # 情况三：Pillow 回退也失败
    try:
        with Image.open(image_path) as img:
            w, h = img.size

        return w, h

    except Exception as e:
        return -1, -1


def content2panguml(contents, md_dir, local_md_root, save_wh=False):
    # img_pattern = re.compile(r"!\[\]\((images/[a-f0-9]{64}\.jpg)\)")
    # table_pattern = re.compile(r"<table></table>")
    combined_pattern = re.compile(r"(!\[\]\(images/[a-f0-9]{64}\.jpg\)|<table>.*?</table>)", re.DOTALL)

    text_list = []
    img_list = []
    text_cnt, img_cnt = 0, 0
    for content in contents:
        content = content.strip()
        if not content:
            continue

        sub_parts = combined_pattern.split(content)
        for part in sub_parts:
            part = part.strip()
            if not part:
                continue

            if re.fullmatch(r'[#$\s]*', part):  # 删除的遗留符号
                continue

            if part.startswith("![](") and part.endswith(".jpg)"):
                path_only = part[4:-1]
                # 前面已经图片完整校验
                if save_wh:
                    w, h = get_image_size(os.path.join(local_md_root, path_only))
                    with open("img_info.txt", "a", encoding="utf-8") as fo:
                        fo.write(f"{os.path.basename(md_dir)}/{os.path.basename(path_only)}-->{(w, h)}\n")

                img_list.append({"relative_img_path": f"{path_only}"})
                text_list.append(None)
                img_cnt += 1
            else:
                text_list.append(part)
                img_list.append(None)
                text_cnt += 1

    if not text_list and not img_list:
        return None
    return {"texts": text_list, "images": img_list, "text_cnt": text_cnt, "img_cnt": img_cnt}


def contents2jsons(pdf_info_list, final_cblocks, md_dir, local_md_root, s3_paths=None, extra_info=None,
                   classed_image=None, save_wh=False):
    error_info = []
    jsons = []

    # 这里判断是否用原图，还是表格，还是公式
    if classed_image[BlockType.IMAGE]:
        enable_formula = True
        enable_table = True
    elif classed_image[BlockType.TABLE]:
        enable_formula = True
        enable_table = False
        logger.info(f"{md_dir} {local_md_root} 没有原生图，采用表格图")
    elif classed_image[BlockType.INTERLINE_EQUATION]:
        enable_formula = False
        enable_table = False
        logger.info(f"{md_dir} {local_md_root} 没有原生图和表格图，采用公式图")
    else:
        enable_formula = True
        enable_table = True
        logger.info(f"{md_dir} {local_md_root} 没有原生图、表格图和公式图，仅保留文字")

    for item in final_cblocks:
        start, end = item

        # 提取实际内容
        mid_contents, _ = get_content_by_page_idx_and_block_listidx(
            pdf_info_list, start[0], end[0], start[1], end[1], False, formula_enable=enable_formula,
            table_enable=enable_table
        )

        panguml_data = content2panguml(mid_contents, md_dir, local_md_root, save_wh=save_wh)

        if panguml_data is None:
            error_info.append(f"图片找不到/无有效内容_{start}_{end}-->{md_dir}/{os.path.basename(md_dir)}_middle.json")
        else:
            # 加入其他信息
            panguml_data["md_dir"] = md_dir
            panguml_data["img_dir"] = local_md_root
            panguml_data["page_location"] = (start, end)
            if extra_info:
                panguml_data.update(extra_info)  # 添加unique_id

            if s3_paths:
                s3_pdf_dir, s3_md_dir, s3_clean_dir = s3_paths
                rel_path = md_dir.replace(s3_md_dir, "")
                pdf_name = os.path.basename(rel_path)

                middle_path = f"{md_dir}/{pdf_name}_middle.json"
                clean_path = f"{s3_clean_dir}{rel_path}/{pdf_name}_cleaned_middle.json"
                if pdf_name.lower() == ".pdf":  # 特殊case。说明原始就没有名字
                    pdf_path = f"{s3_pdf_dir}{rel_path}"
                    logger.warning(f"遇到特殊案例：{md_dir}-->{pdf_path}、{middle_path}、{clean_path}")
                else:
                    pdf_path = f"{s3_pdf_dir}{rel_path}.pdf"

                panguml_data["meta_info"] = {"address": {
                    "s3_origin_address": pdf_path,
                    "s3_parse_address": middle_path,
                    "s3_clean_address": clean_path}
                }

            jsons.append(panguml_data)

    return jsons, error_info


def process_item(item, tar_file, existing_files, lock, write_images=True, mode='item'):
    try:
        image_root = item.get("img_dir")
        pdf_name = os.path.basename(item['md_dir'])
        current_images = item.get("images", [])  # 原始 item 里的图

        # 1. 内部辅助函数：处理单张图片的写入逻辑
        def safe_add_to_tar(abs_path, arc_name):
            if not write_images or tar_file is None:
                return True
            try:
                with lock:
                    if arc_name not in existing_files:
                        tar_file.add(abs_path, arcname=arc_name)
                        existing_files.add(arc_name)
                return True
            except Exception as e:
                logger.error(f'Add error {abs_path}: {e}')
                return False

        # 2. 处理 item 显式要求的图片（更新路径 + 物理写入）
        updated_images = []
        processed_rel_paths = set()  # 记录已处理的相对路径，防止补全时重复

        for img_obj in current_images:
            if img_obj is None:
                updated_images.append(None)
                continue

            rel_path = img_obj["relative_img_path"]
            abs_path = os.path.join(image_root, rel_path)

            # 物理检查
            if write_images and not os.path.exists(abs_path):
                return None  # 如果 item 明确要求的图都不存在，该条数据可能损坏

            arc_name = f"{pdf_name}/{os.path.basename(rel_path)}"
            if not safe_add_to_tar(abs_path, arc_name):
                return None

            updated_images.append({"relative_img_path": arc_name})
            processed_rel_paths.add(rel_path)

        # 3. 补全需求：如果是 follow_all 模式，把文件夹下没提到的图也塞进 Tar
        if mode == 'all':
            img_dir_path = os.path.join(image_root, "images")
            if os.path.exists(img_dir_path):
                for f_name in os.listdir(img_dir_path):
                    f_rel_path = f"images/{f_name}"
                    if f_rel_path in processed_rel_paths:
                        continue

                    f_abs_path = os.path.join(img_dir_path, f_name)
                    f_arc_name = f"{pdf_name}/{f_name}"

                    # 写入 Tar 但不需要更新 item["images"] (因为它们在正文中没出现)
                    if not safe_add_to_tar(f_abs_path, f_arc_name):
                        continue

        item["images"] = updated_images
        return item

    except Exception as e:
        logger.error(f"Process error: {e}")
        return None


def write_jsonl_tar(data, tar_path, jsonl_path, batch_size=200, thread_num=1, write_images=True, save_mode="item"):
    """将数据写入 .tar 和 .jsonl 文件"""
    write_records = []
    if not data:
        return 0, write_records  # 如果缓冲区为空，则直接返回

    try:
        # 确定写入模式
        if write_images:
            write_tar_mode = "a" if os.path.exists(tar_path) and tarfile.is_tarfile(tar_path) else "w"
            tar_cm = tarfile.open(tar_path, mode=write_tar_mode)
        else:
            tar_cm = contextlib.nullcontext()

        # 打开 .tar 文件进行增量写入
        with tar_cm as tar_file, open(jsonl_path, "a", encoding="utf-8") as jsonl_fo:

            # 获取现有的文件名集合（仅在追加模式下获取）
            existing_files = set()
            if write_images and tar_file:
                existing_files = set(tar_file.getnames()) if write_tar_mode == "a" else set()

            # 定义线程锁
            lock = threading.Lock()

            # 使用多线程并行处理
            error_times = 0
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                for i in range(0, len(data), batch_size):
                    batch_data = data[i:i + batch_size]
                    valid_items = []  # 缓存有效的 JSONL 条目

                    future_to_md = {
                        executor.submit(process_item, item, tar_file, existing_files, lock, write_images,
                                        save_mode): item.get("md_dir", "")
                        for item in batch_data
                    }
                    # 使用 as_completed 处理已完成的任务
                    for future in as_completed(future_to_md):
                        md_dir = future_to_md[future]
                        middle_path = f"{md_dir}/{os.path.basename(md_dir)}_middle.json"
                        try:
                            # 设置单条数据处理的最长等待时间
                            result = future.result(timeout=3600)
                            if result is not None:
                                valid_items.append(result)
                                write_status = "写入成功"
                            else:
                                error_times += 1
                                write_status = "写入失败"
                        except TimeoutError as e:
                            write_status = "写入超时"
                            logger.error(f"{write_status} middle_json={middle_path}, 原因：{e}")
                            error_times += 1

                        except Exception as e:
                            write_status = "写入异常"
                            logger.error(f"{write_status} middle_json={middle_path}, 原因：{e}")
                            error_times += 1

                        finally:
                            write_records.append(f"{write_status}-->{middle_path}")

                    # 每个 batch 写入一次，保证数据及时落盘
                    if valid_items:
                        jsonl_buffer = [json.dumps(item, ensure_ascii=False) for item in valid_items]
                        jsonl_fo.write("\n".join(jsonl_buffer) + "\n")
                        jsonl_fo.flush()  # 强制刷入磁盘

            return error_times, write_records

    except Exception as e:
        print(f"Error in write_jsonl_tar: {e}")
        # 发生异常时，认为所有数据项都处理失败
        bug_records = []
        for item in data:
            md_dir = item["md_dir"]
            middle_path = f"{md_dir}/{os.path.basename(md_dir)}_middle.json"
            bug_records.append(f"写入整个程序异常-->{middle_path}")
        return len(data), bug_records


if __name__ == '__main__':
    pass