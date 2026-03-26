"""
v2_interleave_pipeline

Lightweight, stage-based pipeline for `V2_InterleaveDataPool`:
- analyze/viz: fast statistics + visualization on a reservoir subset
- select/emit: threshold filter + pdf_md5 dedupe + category quota sampling
- chunk: chapter-level splitting (texts/images interleave slicing) driven by `middle.json`
"""

__all__ = ["__version__"]

__version__ = "0.1.0"

