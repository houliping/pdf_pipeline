from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Set

from v2_interleave_pipeline.filters.gates import passes_gates
from v2_interleave_pipeline.io.jsonl_stream import list_jsonl_shards, write_jsonl
from v2_interleave_pipeline.make_pipeline.config_loader import MakeDataConfig
from v2_interleave_pipeline.chapter_process.slice_panguml_item import (
    panguml_record_to_item,
    slice_panguml_item_to_rows,
)


def _maybe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _iter_jsonl_loose(path: str):
    """跳过坏行，不中断整 shard。"""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _out_shard_path(
    out_root: str,
    output_version: str,
    dataset_name: str,
    jsonl_dir_name: str,
    shard_name: str,
) -> str:
    return os.path.join(out_root, output_version, dataset_name, jsonl_dir_name, shard_name)


def run_make_data_for_dataset(
    *,
    pool_root: str,
    dataset_name: str,
    jsonl_dir_name: str,
    jsonl_glob: str,
    out_root: str,
    output_version: str,
    cfg: MakeDataConfig,
    seen_md5: Set[str],
    report_every: int = 5000,
) -> Dict[str, Any]:
    """
    处理单个数据集。

    去重在全池维度：seen_md5 由 run_make_data_pool 传入并跨 dataset 共享。
    同一 pdf_md5 全池只处理一次（第一条满足门控的行进入处理流程）。
    章节切分后，每条输出行的 pdf_md5 改为 {原md5}_chapter_{idx}，全部保留。
    """
    jsonl_dir = os.path.join(pool_root, dataset_name, jsonl_dir_name)
    if not os.path.isdir(jsonl_dir):
        raise FileNotFoundError(f"jsonl dir not found: {jsonl_dir}")

    shards = list_jsonl_shards(jsonl_dir, pattern=jsonl_glob)
    stats: Dict[str, Any] = {
        "dataset_name": dataset_name,
        "lines_in": 0,
        "lines_pass_gate": 0,
        "lines_out": 0,
        "skipped_gate": 0,
        "dedupe_skipped": 0,
        "split_batches": 0,
        "errors": 0,
    }

    t0 = time.time()
    for shard in shards:
        out_path = _out_shard_path(out_root, output_version, dataset_name, jsonl_dir_name, shard.shard_name)
        _maybe_mkdir(os.path.dirname(out_path))
        out_rows: List[Dict[str, Any]] = []

        for rec in _iter_jsonl_loose(shard.path):
            stats["lines_in"] += 1

            if not passes_gates(
                rec,
                cfg.gates,
                allow_empty_chapter_info=cfg.chapter_split_enabled,
            ):
                stats["skipped_gate"] += 1
                continue
            stats["lines_pass_gate"] += 1

            # --- 全池 pdf_md5 去重（在输入侧，按原始 pdf 粒度） ---
            if cfg.dedupe_pdf_md5:
                orig_md5 = rec.get("pdf_md5")
                if isinstance(orig_md5, str) and orig_md5:
                    if orig_md5 in seen_md5:
                        stats["dedupe_skipped"] += 1
                        continue
                    seen_md5.add(orig_md5)
                else:
                    orig_md5 = None
            else:
                orig_md5 = rec.get("pdf_md5") if isinstance(rec.get("pdf_md5"), str) else None

            # --- 章节切分 ---
            if cfg.chapter_split_enabled and cfg.chapter_read_mode == "read_panguml":
                item = panguml_record_to_item(rec)
                if item is not None:
                    # If chapter_info doesn't contain the configured `chapter_level`,
                    # we still want to keep the row and let the splitter try `chapter_level + 1`.
                    # If `chapter_level + 1` doesn't exist either, splitter returns [] and
                    # `emit_original_when_split_empty` will handle it.
                    base_level = cfg.gates.chapter_level
                    if base_level is not None:
                        ch = rec.get("meta_info", {}).get("chapter_info")
                        if not isinstance(ch, dict):
                            ch = rec.get("chapter_info")
                        if isinstance(ch, dict) and len(ch) > 0:
                            level_set: Set[int] = set()
                            for v in ch.values():
                                try:
                                    level_set.add(int(v))
                                except (TypeError, ValueError):
                                    pass

                            if base_level in level_set:
                                item["input_level_start"] = base_level
                                item["input_level_max"] = None
                            else:
                                item["input_level_start"] = base_level + 1
                                item["input_level_max"] = base_level + 1

                    try:
                        rows = slice_panguml_item_to_rows(
                            item,
                            "read_panguml",
                            cfg.chapter_slice,
                        )
                    except Exception:
                        stats["errors"] += 1
                        rows = []
                    if rows:
                        stats["split_batches"] += 1
                        for chunk_idx, chunk_row in enumerate(rows):
                            if orig_md5:
                                chunk_row["pdf_md5"] = f"{orig_md5}_chapter_{chunk_idx}"
                            out_rows.append(chunk_row)
                            stats["lines_out"] += 1
                        continue
                    if cfg.emit_original_when_split_empty:
                        out_rows.append(rec)
                        stats["lines_out"] += 1
                    continue

            # 不切分：直接输出
            out_rows.append(rec)
            stats["lines_out"] += 1

            if stats["lines_in"] % report_every == 0:
                elapsed = time.time() - t0
                print(
                    f"[make-data] {dataset_name} {shard.shard_name} "
                    f"lines_in={stats['lines_in']} out={stats['lines_out']} "
                    f"dedupe_skip={stats['dedupe_skipped']} "
                    f"elapsed={elapsed:.1f}s",
                    flush=True,
                )

        if out_rows:
            write_jsonl(out_path, out_rows)

    stats["seconds"] = round(time.time() - t0, 3)
    return stats


def list_datasets(pool_root: str) -> List[str]:
    if not os.path.isdir(pool_root):
        raise FileNotFoundError(pool_root)
    return sorted(
        d
        for d in os.listdir(pool_root)
        if os.path.isdir(os.path.join(pool_root, d)) and not d.startswith(".")
    )


def run_make_data_pool(
    *,
    pool_root: str,
    dataset_name: Optional[str],
    jsonl_dir_name: str,
    jsonl_glob: str,
    out_root: str,
    output_version: str,
    cfg: MakeDataConfig,
) -> List[Dict[str, Any]]:
    _maybe_mkdir(os.path.join(out_root, output_version))
    names = [dataset_name] if dataset_name else list_datasets(pool_root)

    seen_md5: Set[str] = set()
    results = []
    for ds in names:
        jd = os.path.join(pool_root, ds, jsonl_dir_name)
        if not os.path.isdir(jd):
            print(f"[make-data] skip missing jsonl dir: {jd}")
            continue
        print(f"[make-data] === dataset={ds} ===", flush=True)
        st = run_make_data_for_dataset(
            pool_root=pool_root,
            dataset_name=ds,
            jsonl_dir_name=jsonl_dir_name,
            jsonl_glob=jsonl_glob,
            out_root=out_root,
            output_version=output_version,
            cfg=cfg,
            seen_md5=seen_md5,
        )
        results.append(st)

    pool_summary = {
        "total_unique_pdf_md5": len(seen_md5),
        "total_lines_out": sum(r["lines_out"] for r in results),
        "total_dedupe_skipped": sum(r["dedupe_skipped"] for r in results),
    }
    summary_path = os.path.join(out_root, output_version, "make_data_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({"pool_summary": pool_summary, "datasets": results}, f, ensure_ascii=False, indent=2)
    print(
        f"[make-data] done unique_pdf_md5={pool_summary['total_unique_pdf_md5']} "
        f"lines_out={pool_summary['total_lines_out']} "
        f"dedupe_skipped={pool_summary['total_dedupe_skipped']} "
        f"summary -> {summary_path}",
        flush=True,
    )
    return results
