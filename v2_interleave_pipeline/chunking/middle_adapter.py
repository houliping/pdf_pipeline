from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence


class MiddleAdapter(ABC):
    @abstractmethod
    def build_title_idx_per_slot(
        self, *, middle_obj: Dict[str, Any], interleave_len: int
    ) -> Optional[List[Optional[int]]]:
        """
        Return a list length == interleave_len where each slot maps to a title_idx in middle.json.

        If mapping fails, return None (caller should emit debug information).
        """
        raise NotImplementedError

