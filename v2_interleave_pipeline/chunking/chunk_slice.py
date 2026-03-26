from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple


def slice_interleave(record: Dict[str, Any], start: int, end: int) -> Tuple[List[Any], List[Any]]:
    """
    Slice interleaved arrays `texts` and `images` by slot index [start, end).
    Must keep them aligned by index.
    """
    texts = record.get("texts")
    images = record.get("images")
    if not isinstance(texts, list) or not isinstance(images, list):
        raise ValueError("record missing texts/images lists")
    if len(texts) != len(images):
        raise ValueError(f"texts/images length mismatch: {len(texts)} vs {len(images)}")
    n = len(texts)
    start = max(0, min(start, n))
    end = max(0, min(end, n))
    if end < start:
        end = start
    return texts[start:end], images[start:end]


def estimate_tokens_simple(text: str) -> int:
    # Rough estimator: whitespace split. Replace later with a real tokenizer if needed.
    # Keep ASCII-only operations.
    if not text:
        return 0
    return len(text.split())


def recompute_statistics_for_chunk(
    original_record: Dict[str, Any],
    texts_slice: List[Any],
    images_slice: List[Any],
    *,
    image_token_guess: int = 0,
) -> Dict[str, Any]:
    """
    Recompute only fields we can do cheaply:
    - clean_image_num
    - clean_text_token (estimated from texts)
    - clean_total_token (estimated)
    """
    image_num = 0
    for x in images_slice:
        if x is not None:
            image_num += 1

    text_tokens = 0
    for t in texts_slice:
        if isinstance(t, str) and t:
            text_tokens += estimate_tokens_simple(t)

    total_tokens = text_tokens + image_num * int(image_token_guess)
    stats = original_record.get("meta_info", {}).get("statistics_info", {})
    if not isinstance(stats, dict):
        stats = {}
    # Return a partial stats dict; caller merges into meta_info.statistics_info
    return {
        "clean_image_num": int(image_num),
        "clean_text_token": int(text_tokens),
        "clean_total_token": int(total_tokens),
    }

