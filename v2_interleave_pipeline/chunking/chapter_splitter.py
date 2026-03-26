from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .chunk_slice import recompute_statistics_for_chunk, slice_interleave
from .minerU_middle_adapter import MinerU2MiddleAdapter
from .middle_adapter import MiddleAdapter


def _get(d: Dict[str, Any], path: Sequence[str], default: Any = None) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def load_middle_json(parse_address: str) -> Dict[str, Any]:
    """
    parse_address is expected to point to:
    - a directory containing `middle.json`, OR
    - the `middle.json` file itself.
    Supports local paths and optional `s3://`/`obs://` via moxing.
    """
    if not isinstance(parse_address, str) or not parse_address:
        raise ValueError("s3_parse_address is empty")

    # Local filesystem path
    if parse_address.startswith("/") or parse_address.startswith("./"):
        if parse_address.endswith("middle.json"):
            p = parse_address
        else:
            p = os.path.join(parse_address, "middle.json")
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    # Remote path via moxing (optional)
    if parse_address.startswith("s3://") or parse_address.startswith("obs://"):
        try:
            import moxing as mox  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                f"moxing not available for remote parse_address: {parse_address}"
            ) from e

        if parse_address.endswith("middle.json"):
            p = parse_address
        else:
            p = parse_address.rstrip("/") + "/middle.json"
        raw = mox.file.read(p)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    raise ValueError(f"Unsupported parse_address scheme: {parse_address}")


def split_id(new_id_prefix: str, chunk_idx: int) -> str:
    return f"{new_id_prefix}#{chunk_idx}"


@dataclass(frozen=True)
class ChunkConfig:
    chapter_level: int
    adapter: Optional[MiddleAdapter] = None

    # token statistics update (cheap, not exact)
    image_token_guess: int = 0

    # allow empty chunks; usually we should drop empties
    drop_empty: bool = True


def split_one_record_by_chapter(
    record: Dict[str, Any],
    *,
    cfg: ChunkConfig,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    texts = record.get("texts")
    images = record.get("images")
    if not isinstance(texts, list) or not isinstance(images, list) or len(texts) != len(images):
        raise ValueError("record texts/images missing or misaligned")
    interleave_len = len(texts)

    chapter_info = _get(record, ["meta_info", "chapter_info"], default={})
    if not isinstance(chapter_info, dict):
        return []

    filtered_title_idxs: List[int] = []
    for k, v in chapter_info.items():
        try:
            ki = int(k)
        except Exception:
            continue
        if v == cfg.chapter_level:
            filtered_title_idxs.append(ki)
    if not filtered_title_idxs:
        return []

    parse_address = _get(record, ["meta_info", "address", "s3_parse_address"], default=None)
    if not isinstance(parse_address, str) or not parse_address:
        raise ValueError("missing meta_info.address.s3_parse_address")

    middle_obj = load_middle_json(parse_address)
    adapter = cfg.adapter or MinerU2MiddleAdapter()
    title_idx_per_slot = adapter.build_title_idx_per_slot(
        middle_obj=middle_obj, interleave_len=interleave_len
    )
    if title_idx_per_slot is None:
        if debug:
            # Minimal debug payload for caller to print.
            middle_keys = list(middle_obj.keys())
            raise RuntimeError(f"Failed to map title_idx per slot; middle keys={middle_keys[:50]}")
        raise RuntimeError("Failed to map title_idx per slot from middle.json")

    # Build first occurrence position for each title idx
    first_pos: Dict[int, int] = {}
    for idx, t in enumerate(title_idx_per_slot):
        if isinstance(t, int) and t in filtered_title_idxs and t not in first_pos:
            first_pos[t] = idx

    # Some titles may not map to any slot; skip them.
    ordered_titles = sorted(
        (t for t in filtered_title_idxs if t in first_pos),
        key=lambda t: first_pos[t],
    )
    if not ordered_titles:
        return []

    chunks: List[Dict[str, Any]] = []
    orig_id = record.get("id") if isinstance(record.get("id"), str) else None
    if not orig_id:
        orig_id = record.get("pdf_md5") if isinstance(record.get("pdf_md5"), str) else "unknown"

    for chunk_idx, title_idx in enumerate(ordered_titles):
        start = first_pos[title_idx]
        if chunk_idx + 1 < len(ordered_titles):
            end = first_pos[ordered_titles[chunk_idx + 1]]
        else:
            end = interleave_len
        if end <= start:
            continue

        texts_slice, images_slice = slice_interleave(record, start, end)

        if cfg.drop_empty:
            has_text = any(isinstance(t, str) and t for t in texts_slice)
            has_img = any(x is not None for x in images_slice)
            if not has_text and not has_img:
                continue

        new_record = dict(record)  # shallow copy is enough for id/meta updates
        new_record["texts"] = texts_slice
        new_record["images"] = images_slice

        new_record["orig_id"] = record.get("id", "")
        new_record["chunk_idx"] = chunk_idx
        new_record["chunk_chapter_level"] = cfg.chapter_level
        new_record["chunk_title_idx"] = title_idx

        new_record["id"] = split_id(orig_id, chunk_idx)

        # Update cheap statistics
        stats_updates = recompute_statistics_for_chunk(
            record,
            texts_slice,
            images_slice,
            image_token_guess=cfg.image_token_guess,
        )
        meta_info = new_record.get("meta_info")
        if isinstance(meta_info, dict):
            stats = meta_info.get("statistics_info")
            if not isinstance(stats, dict):
                stats = {}
                meta_info["statistics_info"] = stats
            for k, v in stats_updates.items():
                stats[k] = v
        chunks.append(new_record)

    return chunks

