from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional


try:
    import orjson  # type: ignore

    def _loads(s: str) -> Any:
        return orjson.loads(s)

    def _dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except Exception:  # pragma: no cover
    def _loads(s: str) -> Any:
        return json.loads(s)

    def _dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False)


@dataclass(frozen=True)
class JsonlShard:
    path: str
    shard_name: str  # e.g. data_000123.jsonl
    shard_idx: int  # parsed from file name if possible, else -1


def list_jsonl_shards(local_dir: str, pattern: str = "data_*.jsonl") -> List[JsonlShard]:
    """
    Only supports local filesystem listing.
    If you need s3/obs listing, you can extend this via moxing.
    """
    if not os.path.isdir(local_dir):
        raise FileNotFoundError(f"jsonl dir not found: {local_dir}")

    paths = sorted(glob.glob(os.path.join(local_dir, pattern)))
    shards: List[JsonlShard] = []
    for p in paths:
        base = os.path.basename(p)
        shard_idx = -1
        # expected: data_000123.jsonl
        parts = base.split("_")
        if len(parts) >= 2 and parts[0] == "data":
            idx_part = parts[1].split(".")[0]
            if idx_part.isdigit():
                shard_idx = int(idx_part)
        shards.append(JsonlShard(path=p, shard_name=base, shard_idx=shard_idx))
    return shards


def iter_jsonl(path: str, *, encoding: str = "utf-8") -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding=encoding) as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = _loads(s)
            except Exception as e:
                # Keep this function simple: caller decides how to count/skip.
                raise RuntimeError(f"JSON decode failed: {path}:{line_no}: {e}") from e
            if not isinstance(obj, dict):
                continue
            yield obj


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(_dumps(row))
            f.write("\n")

