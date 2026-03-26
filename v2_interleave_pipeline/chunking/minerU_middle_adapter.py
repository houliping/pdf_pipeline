from __future__ import annotations

import itertools
from typing import Any, Dict, List, Optional, Sequence

from .middle_adapter import MiddleAdapter


class MinerU2MiddleAdapter(MiddleAdapter):
    """
    Heuristic adapter for MinerU2.6 style `middle.json`.

    Since we don't hardcode schema names, this adapter tries multiple strategies:
    - find a list of "blocks/paragraphs/items" where each element has a title_idx-like field
      and (preferably) the list length matches `interleave_len`
    - or build mapping from slot_idx/block_idx fields if available

    When it can't map, it returns None and caller should emit debug keys.
    """

    # common key candidates
    _title_idx_keys = [
        "title_idx",
        "titleIndex",
        "title_id",
        "titleId",
        "heading_idx",
        "headingIndex",
        "heading_id",
        "headingId",
        "section_idx",
        "sectionIndex",
        "section_id",
        "sectionId",
        "chapter_idx",
        "chapterIndex",
        "chapter_id",
        "chapterId",
    ]
    _slot_idx_keys = [
        "slot_idx",
        "slotIndex",
        "interleave_idx",
        "interleaveIndex",
        "block_idx",
        "blockIndex",
        "content_idx",
        "contentIndex",
        "idx",
        "index",
        "position",
        "pos",
    ]

    def build_title_idx_per_slot(
        self, *, middle_obj: Dict[str, Any], interleave_len: int
    ) -> Optional[List[Optional[int]]]:
        title_idx_per_slot = self._try_from_block_list(
            middle_obj=middle_obj, interleave_len=interleave_len
        )
        return title_idx_per_slot

    def _try_from_block_list(
        self, *, middle_obj: Dict[str, Any], interleave_len: int
    ) -> Optional[List[Optional[int]]]:
        # Candidate keys likely to contain block-like lists
        candidate_list_keys = [
            "blocks",
            "block_list",
            "blockList",
            "paragraphs",
            "paragraph_list",
            "items",
            "item_list",
            "content_blocks",
            "contentBlocks",
            "text_blocks",
            "textBlocks",
            "middle_blocks",
            "middleBlocks",
            "elements",
            "element_list",
        ]

        # Strategy 1: direct list length matches interleave_len
        for key in candidate_list_keys:
            lst = middle_obj.get(key)
            if isinstance(lst, list) and len(lst) == interleave_len and lst:
                mapping = self._extract_title_idx_from_list_elems(lst, interleave_len=interleave_len)
                if mapping is not None:
                    return mapping

        # Strategy 2: any list value with dict elements; length close to interleave_len
        for k, v in middle_obj.items():
            if not isinstance(v, list):
                continue
            if not v:
                continue
            if not isinstance(v[0], dict):
                continue
            if len(v) != interleave_len:
                # still allow mapping if it carries explicit slot/block indices
                pass
            mapping = self._extract_title_idx_from_list_elems(
                v, interleave_len=interleave_len, allow_slot_idx=True
            )
            if mapping is not None:
                return mapping

        return None

    def _extract_title_idx_from_list_elems(
        self, elems: List[Any], *, interleave_len: int, allow_slot_idx: bool = False
    ) -> Optional[List[Optional[int]]]:
        # Build mapping array
        title_idx_per_slot: List[Optional[int]] = [None] * interleave_len

        # Heuristic: determine title idx key and (optional) slot idx key per element
        title_key_found = False
        assigned = 0

        for i, el in enumerate(elems):
            if not isinstance(el, dict):
                continue

            t_idx = None
            for tk in self._title_idx_keys:
                if tk in el:
                    cand = el.get(tk)
                    if isinstance(cand, int):
                        t_idx = cand
                        title_key_found = True
                        break
            if t_idx is None:
                continue

            if not allow_slot_idx:
                # direct mapping assumes elems index == slot index
                if 0 <= i < interleave_len:
                    title_idx_per_slot[i] = t_idx
                    assigned += 1
            else:
                # try to map by slot/block index field
                slot_idx = None
                for sk in self._slot_idx_keys:
                    if sk in el:
                        cand = el.get(sk)
                        if isinstance(cand, int) and 0 <= cand < interleave_len:
                            slot_idx = cand
                            break
                if slot_idx is None:
                    # fallback: only if list length matches
                    if len(elems) == interleave_len:
                        slot_idx = i
                if slot_idx is not None and 0 <= slot_idx < interleave_len:
                    title_idx_per_slot[slot_idx] = t_idx
                    assigned += 1

        if not title_key_found:
            return None
        # Require enough assignments to be confident
        if assigned < max(3, interleave_len // 20):
            return None
        return title_idx_per_slot

