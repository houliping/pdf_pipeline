from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from v2_interleave_pipeline.filters.gates import (
    GatesConfig,
    get_image_num,
    get_language,
    get_main_category,
    get_main_category_code,
    get_total_token,
    passes_gates,
)
from v2_interleave_pipeline.io.jsonl_stream import JsonlShard, iter_jsonl, list_jsonl_shards, write_jsonl
from v2_interleave_pipeline.sampling.quotas import compute_quotas, normalize_ratios, parse_category_ratio_json, reservoir_sample_unique_md5


def _maybe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def load_category_code_name_map(txt_path: str) -> Dict[str, str]:
    """
    Parse `category_code.txt` to map category_code -> category_name (Chinese).

    The file is not strictly structured; we take the first 2 whitespace-separated tokens:
      <code> <category_name> ...
    """
    import re

    if not os.path.exists(txt_path):
        return {}
    mapping: Dict[str, str] = {}
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip().replace("\r", "")
            if not s:
                continue
            parts = s.split()
            if len(parts) < 2:
                continue
            code = parts[0]
            if not re.match(r"^\d{2,4}$", code):
                continue
            name_cn = parts[1]
            mapping[code] = name_cn
    return mapping


def _load_selected_manifest(selected_manifest: str) -> Dict[str, Any]:
    with open(selected_manifest, "r", encoding="utf-8") as f:
        return json.load(f)


def _log(msg: str) -> None:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def _iter_passed_records_for_file(
    shard_path: str,
    gates_cfg: GatesConfig,
) -> Iterable[Tuple[str, str]]:
    for rec in iter_jsonl(shard_path):
        # gating
        from v2_interleave_pipeline.filters.gates import passes_gates

        if not passes_gates(rec, gates_cfg):
            continue
        pdf_md5 = rec.get("pdf_md5")
        if not isinstance(pdf_md5, str) or not pdf_md5:
            continue
        cat = get_main_category_code(rec)
        if not cat:
            continue
        yield (cat, pdf_md5)


def _worker_select_one_shard(
    shard: JsonlShard,
    gates_cfg: GatesConfig,
    quotas: Dict[str, int],
    reservoir_k_per_cat: Dict[str, int],
    seed: int,
) -> Dict[str, List[str]]:
    # For this worker, we only need (approx) k samples per category from this shard.
    # We'll merge/trim at master.
    md5_stream = _iter_passed_records_for_file(shard.path, gates_cfg)
    return reservoir_sample_unique_md5(
        md5_stream=md5_stream,
        quotas=reservoir_k_per_cat,
        seed=seed ^ (shard.shard_idx if shard.shard_idx >= 0 else hash(shard.shard_name)),
    )


def dedupe_output_jsonl_dir(input_dir: str, output_dir: str) -> None:
    """
    Deduplicate output lines by `pdf_md5`.
    Reads all `data_*.jsonl` from input_dir and writes deduped to output_dir
    with the same file names.
    """
    _maybe_mkdir(output_dir)
    shards = list_jsonl_shards(input_dir)

    seen: Set[str] = set()
    # Prepare output writers per shard to preserve filenames.
    # Since we want determinism, we keep file order and line order.
    out_rows_by_shard: Dict[str, List[Dict[str, Any]]] = {s.shard_name: [] for s in shards}

    for s in shards:
        for rec in iter_jsonl(s.path):
            pdf_md5 = rec.get("pdf_md5")
            if not isinstance(pdf_md5, str) or not pdf_md5:
                continue
            if pdf_md5 in seen:
                continue
            seen.add(pdf_md5)
            out_rows_by_shard[s.shard_name].append(rec)

    for s in shards:
        out_path = os.path.join(output_dir, s.shard_name)
        write_jsonl(out_path, out_rows_by_shard[s.shard_name])


def run_analyze(args: argparse.Namespace) -> None:
    t0 = time.time()
    # Reservoir sample lines for viz
    shards = list_jsonl_shards(args.jsonl_dir)
    _log(f"[analyze] shards={len(shards)} jsonl_dir={args.jsonl_dir}")
    reservoir_size = int(args.viz_reservoir_size)
    rng = random.Random(args.seed)
    report_every_records = max(1, int(args.report_every_records))
    report_every_seconds = max(1, int(args.report_every_seconds))

    reservoir: List[Dict[str, Any]] = []
    total_seen = 0
    extra_match_count = 0  # img_num>0 AND language in zh/en AND layout_decision==PARSE
    per_file_stats: Dict[str, Dict[str, Any]] = {}
    last_report_records = 0
    last_report_time = time.time()
    started = last_report_time

    for shard in shards:
        shard_seen = 0
        shard_extra_match_count = 0
        _log(f"[analyze] scanning shard={shard.shard_name}")
        for rec in iter_jsonl(shard.path):
            total_seen += 1
            shard_seen += 1
            img_num_now = get_image_num(rec)
            lang_now = get_language(rec)
            lang_norm = lang_now.lower() if isinstance(lang_now, str) else None
            layout_now = rec.get("meta_info", {}).get("origin_pdf_layout_score", {}).get("decision")
            layout_norm = layout_now.upper() if isinstance(layout_now, str) else None
            is_zh_en = lang_norm in {"zh", "zh-cn", "zh_cn", "en", "english", "chinese"}
            if isinstance(img_num_now, int) and img_num_now > 0 and is_zh_en and layout_norm == "PARSE":
                extra_match_count += 1
                shard_extra_match_count += 1

            if len(reservoir) < reservoir_size:
                reservoir.append(rec)
            else:
                j = rng.randrange(0, total_seen)
                if j < reservoir_size:
                    reservoir[j] = rec

            now = time.time()
            if (total_seen - last_report_records >= report_every_records) or (now - last_report_time >= report_every_seconds):
                elapsed = max(1e-6, now - started)
                speed = total_seen / elapsed
                _log(
                    f"[analyze] progress records={total_seen} reservoir={len(reservoir)} "
                    f"speed={speed:.1f} rec/s elapsed={elapsed:.1f}s"
                )
                last_report_records = total_seen
                last_report_time = now

            if args.max_records is not None and total_seen >= args.max_records:
                break
        per_file_stats[shard.shard_name] = {
            "records_seen": shard_seen,
            "extra_stats": {
                "img_nonzero_and_lang_zh_en_and_layout_parse": {
                    "count": shard_extra_match_count,
                    "ratio": (shard_extra_match_count / shard_seen) if shard_seen > 0 else 0.0,
                    "denominator": shard_seen,
                }
            },
        }
        if args.max_records is not None and total_seen >= args.max_records:
            break

    _log(
        f"[analyze] scan finished records={total_seen} reservoir={len(reservoir)} "
        f"elapsed={time.time()-started:.1f}s"
    )

    # Collect features
    t_collect = time.time()
    token_vals: List[int] = []
    image_vals: List[int] = []
    layout_decisions: List[str] = []
    nsfw_decisions: List[str] = []
    languages: List[str] = []
    cat_codes: List[str] = []
    cat_names: List[str] = []

    cat_map = load_category_code_name_map(args.category_code_txt) if args.category_code_txt else {}

    for rec in reservoir:
        tt = get_total_token(rec)
        if isinstance(tt, int):
            token_vals.append(tt)
        # image num from stats if present (avoid extra scans)
        img_num = rec.get("meta_info", {}).get("statistics_info", {}).get("clean_image_num")
        if isinstance(img_num, int):
            image_vals.append(img_num)
        ld = rec.get("meta_info", {}).get("origin_pdf_layout_score", {}).get("decision")
        if isinstance(ld, str):
            layout_decisions.append(ld)
        nd = rec.get("meta_info", {}).get("origin_pdf_nsfw_score", {}).get("decision")
        if isinstance(nd, str):
            nsfw_decisions.append(nd)
        lang = get_language(rec)
        if isinstance(lang, str):
            languages.append(lang)
        cat_code, cat_name, _cat_score = get_main_category(rec)
        if cat_code:
            cat_codes.append(cat_code)
        if cat_name:
            cat_names.append(cat_name)

    overall_stats = {
        "records_seen": total_seen,
        "extra_stats": {
            "img_nonzero_and_lang_zh_en_and_layout_parse": {
                "count": extra_match_count,
                "ratio": (extra_match_count / total_seen) if total_seen > 0 else 0.0,
                "denominator": total_seen,
            }
        },
    }

    stats = {
        "reservoir_size": len(reservoir),
        "total_seen": total_seen,
        "token_total": {"min": min(token_vals) if token_vals else None, "max": max(token_vals) if token_vals else None},
        "image_num": {"min": min(image_vals) if image_vals else None, "max": max(image_vals) if image_vals else None},
        "layout_decisions_count": {},
        "nsfw_decisions_count": {},
        "language_count": {},
        "category_code_count": {},
        "category_name_count": {},
        "overall_stats": overall_stats,
        "per_file_stats": per_file_stats,
        # Backward compatibility
        "extra_stats": overall_stats["extra_stats"],
    }
    from collections import Counter

    stats["layout_decisions_count"] = Counter(layout_decisions)
    stats["nsfw_decisions_count"] = Counter(nsfw_decisions)
    stats["language_count"] = Counter(languages)
    stats["category_code_count"] = Counter(cat_codes)
    stats["category_name_count"] = Counter(cat_names)

    out_analysis = os.path.join(args.out_dir, "analysis")
    _maybe_mkdir(out_analysis)
    with open(os.path.join(out_analysis, "reservoir_stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    _log(f"[analyze] wrote reservoir_stats.json in {time.time() - t_collect:.1f}s")

    # Plot
    if args.no_plot:
        _log("[analyze] --no_plot enabled, skip plotting and finish.")
        return

    t_plot = time.time()
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        print("matplotlib not available; only reservoir_stats.json written.")
        return

    def save_hist(vals: List[int], path: str, title: str) -> None:
        if not vals:
            return
        plt.figure(figsize=(10, 5))
        plt.hist(vals, bins=60)
        plt.title(title)
        plt.tight_layout()
        plt.savefig(path)
        plt.close()

    def save_bar(counter: Dict[str, int], path: str, title: str, top_k: int = 30) -> None:
        if not counter:
            return
        items = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:top_k]
        labels = [k for k, _ in items]
        counts = [v for _, v in items]
        plt.figure(figsize=(12, 5))
        plt.bar(labels, counts)
        plt.xticks(rotation=90, fontsize=8)
        plt.title(title)
        plt.tight_layout()
        plt.savefig(path)
        plt.close()

    save_hist(token_vals, os.path.join(out_analysis, "hist_clean_total_token.png"), "clean_total_token (reservoir)")
    save_hist(image_vals, os.path.join(out_analysis, "hist_clean_image_num.png"), "clean_image_num (reservoir)")
    save_bar(stats["language_count"], os.path.join(out_analysis, "bar_language.png"), "language_fasttext.language (top)")
    # Visualize category name if present; fallback to code->txt mapping; otherwise show code.
    if stats["category_name_count"]:
        save_bar(
            stats["category_name_count"],
            os.path.join(out_analysis, "bar_main_category_name.png"),
            "category_name (top)",
        )
    elif cat_map:
        top_cats = sorted(stats["category_code_count"].items(), key=lambda x: x[1], reverse=True)[:30]
        label_map = {code: f"{cat_map.get(code, code)}({code})" for code, _ in top_cats}
        labels = [label_map[code] for code, _ in top_cats]
        counts = [cnt for _, cnt in top_cats]
        try:
            import matplotlib.pyplot as plt  # type: ignore

            plt.figure(figsize=(14, 5))
            plt.bar(labels, counts)
            plt.xticks(rotation=90, fontsize=8)
            plt.title("category (top by reservoir)")
            plt.tight_layout()
            plt.savefig(os.path.join(out_analysis, "bar_main_category_cn.png"))
            plt.close()
        except Exception:
            save_bar(
                stats["category_code_count"],
                os.path.join(out_analysis, "bar_main_category_code.png"),
                "category_code (top)",
            )
    else:
        save_bar(
            stats["category_code_count"],
            os.path.join(out_analysis, "bar_main_category_code.png"),
            "category_code (top)",
        )
    save_bar(stats["layout_decisions_count"], os.path.join(out_analysis, "bar_layout_decision.png"), "layout decision")
    save_bar(stats["nsfw_decisions_count"], os.path.join(out_analysis, "bar_nsfw_decision.png"), "nsfw decision")
    _log(f"[analyze] plot stage done in {time.time() - t_plot:.1f}s")

    # Simple threshold sweep for token_total_min
    if args.sweep_token_total_min_list:
        t_sweep = time.time()
        token_mins = [int(x) for x in args.sweep_token_total_min_list.split(",") if x.strip()]
        if token_mins:
            sweep_rows = []
            for tmin in token_mins:
                _log(f"[analyze] sweep token_total_min={tmin}")
                sweep_cfg = GatesConfig(
                    min_image_num=int(args.base_min_image_num),
                    token_total_min=int(tmin),
                    token_total_max=int(args.base_token_total_max),
                    layout_decision_ok=set(args.base_layout_decision_ok.split(",")) if args.base_layout_decision_ok else None,
                    layout_score_min=float(args.base_layout_score_min),
                    nsfw_decision_ok=set(args.base_nsfw_decision_ok.split(",")) if args.base_nsfw_decision_ok else None,
                    nsfw_score_max=float(args.base_nsfw_score_max),
                    languages_ok=set(args.base_languages_ok.split(",")) if args.base_languages_ok else None,
                    chapter_level=int(args.chapter_level) if args.chapter_level is not None else None,
                )
                pass_cnt = 0
                for rec in reservoir:
                    if passes_gates(rec, sweep_cfg):
                        pass_cnt += 1
                sweep_rows.append({"token_total_min": tmin, "pass_cnt": pass_cnt, "reservoir_size": len(reservoir)})
                _log(
                    f"[analyze] sweep result token_total_min={tmin} pass_cnt={pass_cnt}/{len(reservoir)}"
                )

            with open(os.path.join(out_analysis, "sweep_token_total_min.json"), "w", encoding="utf-8") as f:
                json.dump(sweep_rows, f, ensure_ascii=False, indent=2)

            try:
                import matplotlib.pyplot as plt  # type: ignore

                xs = [r["token_total_min"] for r in sweep_rows]
                ys = [r["pass_cnt"] for r in sweep_rows]
                plt.figure(figsize=(8, 5))
                plt.plot(xs, ys, marker="o")
                plt.title("Pass count vs token_total_min (reservoir)")
                plt.xlabel("token_total_min")
                plt.ylabel("pass_cnt")
                plt.grid(True)
                plt.tight_layout()
                plt.savefig(os.path.join(out_analysis, "sweep_token_total_min.png"))
                plt.close()
            except Exception:
                pass
        _log(f"[analyze] sweep stage done in {time.time() - t_sweep:.1f}s")

    _log(f"[analyze] all done in {time.time() - t0:.1f}s -> {out_analysis}")


def run_select(args: argparse.Namespace) -> None:
    shards = list_jsonl_shards(args.jsonl_dir)
    quotas = compute_quotas(parse_category_ratio_json(args.category_ratio_json), args.sample_total)

    gates_cfg = GatesConfig(
        min_image_num=int(args.min_image_num),
        token_total_min=int(args.token_total_min),
        token_total_max=int(args.token_total_max),
        layout_decision_ok=set(args.layout_decision_ok.split(",")) if args.layout_decision_ok else None,
        layout_score_min=float(args.layout_score_min),
        nsfw_decision_ok=set(args.nsfw_decision_ok.split(",")) if args.nsfw_decision_ok else None,
        nsfw_score_max=float(args.nsfw_score_max),
        languages_ok=set(args.languages_ok.split(",")) if args.languages_ok else None,
        chapter_level=int(args.chapter_level) if args.chapter_level is not None else None,
    )

    num_workers = max(1, int(args.num_workers))
    oversample_factor = float(args.reservoir_oversample_factor)
    seed = int(args.seed)

    # Per-worker reservoir size (approx)
    reservoir_k_per_cat: Dict[str, int] = {}
    for cat, q in quotas.items():
        k = int(q * oversample_factor / max(1, num_workers))
        if q > 0 and k <= 0:
            k = 1
        reservoir_k_per_cat[cat] = k

    all_candidates_by_cat: Dict[str, Set[str]] = {cat: set() for cat in quotas}
    # Scan shards in parallel by file (no shared state)
    with cf.ProcessPoolExecutor(max_workers=num_workers) as ex:
        futures = []
        for shard in shards:
            futures.append(
                ex.submit(
                    _worker_select_one_shard,
                    shard,
                    gates_cfg,
                    quotas,
                    reservoir_k_per_cat,
                    seed,
                )
            )
        for fut in cf.as_completed(futures):
            part = fut.result()
            for cat, md5_list in part.items():
                if cat not in all_candidates_by_cat:
                    all_candidates_by_cat[cat] = set()
                all_candidates_by_cat[cat].update(md5_list)

    # Final trim by quotas
    rng = random.Random(seed)
    selected_by_cat: Dict[str, List[str]] = {}
    selected_all: List[str] = []
    for cat, q in quotas.items():
        cand = list(all_candidates_by_cat.get(cat, set()))
        rng.shuffle(cand)
        take = min(q, len(cand))
        selected = cand[:take]
        selected_by_cat[cat] = selected
        selected_all.extend(selected)

    manifest = {
        "dataset_name": args.dataset_name,
        "stage": "select",
        "quotas": quotas,
        "selected_by_category": {k: len(v) for k, v in selected_by_cat.items()},
        "selected_pdf_md5": selected_all,
        "seed": seed,
        "gates": asdict(gates_cfg),
    }

    out_path = os.path.join(args.output_dir, "selected_manifest.json")
    _maybe_mkdir(args.output_dir)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[select] selected_pdf_md5={len(selected_all)} -> {out_path}")


def run_emit(args: argparse.Namespace) -> None:
    manifest = _load_selected_manifest(args.selected_manifest)
    selected_set: Set[str] = set(manifest.get("selected_pdf_md5", []))
    if not selected_set:
        raise RuntimeError("selected_manifest has empty selected_pdf_md5")

    shards = list_jsonl_shards(args.jsonl_dir)
    raw_out_dir = os.path.join(args.output_dir, "jsonl_res_sample_raw")
    _maybe_mkdir(raw_out_dir)

    def emit_one_shard(shard: JsonlShard) -> None:
        out_path = os.path.join(raw_out_dir, shard.shard_name)
        rows: List[Dict[str, Any]] = []
        for rec in iter_jsonl(shard.path):
            pdf_md5 = rec.get("pdf_md5")
            if isinstance(pdf_md5, str) and pdf_md5 in selected_set:
                rows.append(rec)
        write_jsonl(out_path, rows)

    num_workers = max(1, int(args.num_workers))
    with cf.ProcessPoolExecutor(max_workers=num_workers) as ex:
        list(ex.map(emit_one_shard, shards))

    # Dedupe after parallel emit (cheap: output size ~sample_total)
    final_out_dir = os.path.join(args.output_dir, "jsonl_res_sample")
    dedupe_output_jsonl_dir(raw_out_dir, final_out_dir)
    print(f"[emit] wrote deduped output -> {final_out_dir}")


def run_chunk(args: argparse.Namespace) -> None:
    from v2_interleave_pipeline.chunking.chapter_splitter import ChunkConfig, split_one_record_by_chapter
    from v2_interleave_pipeline.chunking.minerU_middle_adapter import MinerU2MiddleAdapter

    in_dir = os.path.join(args.input_dir, "jsonl_res_sample")
    if not os.path.isdir(in_dir):
        raise FileNotFoundError(f"input_dir/jsonl_res_sample not found: {in_dir}")

    shards = list_jsonl_shards(in_dir)
    out_dir = os.path.join(args.output_dir, "chunked_jsonl_res")
    _maybe_mkdir(out_dir)

    cfg = ChunkConfig(
        chapter_level=int(args.chapter_level),
        adapter=MinerU2MiddleAdapter(),
        image_token_guess=int(args.image_token_guess),
        drop_empty=bool(args.drop_empty),
    )

    def chunk_one_shard(shard: JsonlShard) -> None:
        out_path = os.path.join(out_dir, shard.shard_name)
        out_rows: List[Dict[str, Any]] = []
        for rec in iter_jsonl(shard.path):
            try:
                chunks = split_one_record_by_chapter(rec, cfg=cfg, debug=bool(args.debug))
            except Exception:
                if args.debug:
                    raise
                continue
            out_rows.extend(chunks)
        write_jsonl(out_path, out_rows)

    num_workers = max(1, int(args.num_workers))
    with cf.ProcessPoolExecutor(max_workers=num_workers) as ex:
        list(ex.map(chunk_one_shard, shards))
    print(f"[chunk] done -> {out_dir}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    # analyze
    pa = sub.add_parser("analyze", help="reservoir stats + visualization")
    pa.add_argument("--root", required=False, default=".")
    pa.add_argument("--dataset_name", required=True)
    pa.add_argument("--jsonl_dir_name", default="jsonl_res")
    pa.add_argument("--out_dir", required=True)
    pa.add_argument("--viz_reservoir_size", type=int, default=200000)
    pa.add_argument("--max_records", type=int, default=None)
    pa.add_argument("--seed", type=int, default=1337)
    pa.add_argument("--num_workers", type=int, default=1)  # reserved
    pa.add_argument("--report_every_records", type=int, default=20000)
    pa.add_argument("--report_every_seconds", type=int, default=10)
    pa.add_argument("--no_plot", action="store_true")
    pa.add_argument(
        "--category_code_txt",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "category_code.txt"),
        help="category_code.txt for mapping code->Chinese name",
    )

    pa.add_argument("--chapter_level", type=int, default=None)
    pa.add_argument("--base_min_image_num", type=int, default=1)
    pa.add_argument("--base_token_total_max", type=int, default=10**18)
    pa.add_argument("--base_layout_decision_ok", type=str, default="")
    pa.add_argument("--base_layout_score_min", type=float, default=float("-inf"))
    pa.add_argument("--base_nsfw_decision_ok", type=str, default="")
    pa.add_argument("--base_nsfw_score_max", type=float, default=float("inf"))
    pa.add_argument("--base_languages_ok", type=str, default="")
    pa.add_argument(
        "--sweep_token_total_min_list",
        type=str,
        default="",
        help="comma-separated ints, e.g. 0,200,500,1000. Empty disables sweep.",
    )

    pa.set_defaults(func=lambda args: run_analyze(args))

    # select
    ps = sub.add_parser("select", help="select pdf_md5 with category quotas (two-phase)")
    ps.add_argument("--root", required=False, default=".")
    ps.add_argument("--dataset_name", required=True)
    ps.add_argument("--jsonl_dir_name", default="jsonl_res")
    ps.add_argument("--output_dir", required=True)

    ps.add_argument("--sample_total", type=int, required=True)
    ps.add_argument("--category_ratio_json", required=True, help='json string or path')

    # gates
    ps.add_argument("--min_image_num", type=int, default=1)
    ps.add_argument("--token_total_min", type=int, default=0)
    ps.add_argument("--token_total_max", type=int, default=10**18)

    ps.add_argument("--layout_decision_ok", type=str, default="", help="comma-separated, empty means only reject DROP")
    ps.add_argument("--layout_score_min", type=float, default=float("-inf"))
    ps.add_argument("--nsfw_decision_ok", type=str, default="", help="comma-separated, empty means only reject DROP")
    ps.add_argument("--nsfw_score_max", type=float, default=float("inf"))

    ps.add_argument("--languages_ok", type=str, default="", help="comma-separated, empty disables")
    ps.add_argument("--chapter_level", type=int, default=None)

    ps.add_argument("--num_workers", type=int, default=4)
    ps.add_argument("--reservoir_oversample_factor", type=float, default=1.25)
    ps.add_argument("--seed", type=int, default=1337)
    ps.set_defaults(func=lambda args: run_select(args))

    # emit
    pe = sub.add_parser("emit", help="write selected jsonl lines into new dir")
    pe.add_argument("--root", required=False, default=".")
    pe.add_argument("--dataset_name", required=True)
    pe.add_argument("--jsonl_dir_name", default="jsonl_res")
    pe.add_argument("--selected_manifest", required=True)
    pe.add_argument("--output_dir", required=True)
    pe.add_argument("--num_workers", type=int, default=4)
    pe.set_defaults(func=lambda args: run_emit(args))

    # chunk
    pc = sub.add_parser("chunk", help="true_chunk: split selected records by chapter_level")
    pc.add_argument("--root", required=False, default=".")
    pc.add_argument("--dataset_name", required=True)
    pc.add_argument("--input_dir", required=True, help="the output_dir of emit/select (must contain jsonl_res_sample)")
    pc.add_argument("--output_dir", required=True, help="final output root")
    pc.add_argument("--chapter_level", type=int, required=True)
    pc.add_argument("--image_token_guess", type=int, default=0)
    pc.add_argument("--drop_empty", type=int, default=1)
    pc.add_argument("--num_workers", type=int, default=4)
    pc.add_argument("--debug", action="store_true")
    pc.set_defaults(func=lambda args: run_chunk(args))

    return p


def main() -> None:
    p = build_parser()
    args = p.parse_args()

    # Normalize paths
    dataset_root = os.path.join(args.root, args.dataset_name)
    args.jsonl_dir = os.path.join(dataset_root, args.jsonl_dir_name)

    args.func(args)


if __name__ == "__main__":
    main()

