#!/usr/bin/env bash
set -euo pipefail

# Batch-run analyze for every top-level dataset directory under ROOT.
# - Parallel across datasets (safe speedup)
# - Each dataset writes to its own OUT/<dataset> directory
# - Skips datasets that already have analysis/reservoir_stats.json (unless --no-skip)
# - Optional retry for transient failures
#
# Usage examples:
#   bash scripts/run_all_analyze.sh --root /path/V2_InterleaveDataPool --out /cache/v2_analyze_full --parallel 3
#   bash scripts/run_all_analyze.sh --root ... --out ... --parallel 4 --viz_reservoir_size 200000 --retry 1

ROOT=""
OUT=""
PARALLEL=3
VIZ_RESERVOIR_SIZE=200000
REPORT_EVERY_RECORDS=20000
REPORT_EVERY_SECONDS=10
SWEEP_TOKEN_TOTAL_MIN_LIST=""   # e.g. "0,200,500,1000,2000"
RETRY=0
SKIP_EXISTING=1

usage() {
  cat <<'EOF'
run_all_analyze.sh

Required:
  --root PATH         Local V2_InterleaveDataPool root dir (contains dataset folders)
  --out PATH          Output root dir; each dataset writes to OUT/<dataset>/

Optional:
  --parallel N                 Parallel datasets (default: 3)
  --viz_reservoir_size N       Reservoir size (default: 200000)
  --report_every_records N     Progress log every N records (default: 20000)
  --report_every_seconds N     Progress log every N seconds (default: 10)
  --sweep_token_total_min_list CSV   Enable sweep in analyze (default: disabled)
  --retry N                    Retry each failed dataset N times (default: 0)
  --no-skip                    Do not skip datasets with existing stats

Example:
  bash scripts/run_all_analyze.sh \
    --root /home/ma-user/work/bucket-8107/hlp/V2_InterleaveDataPool \
    --out /cache/v2_analyze_full \
    --parallel 3
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --parallel) PARALLEL="${2:-}"; shift 2 ;;
    --viz_reservoir_size) VIZ_RESERVOIR_SIZE="${2:-}"; shift 2 ;;
    --report_every_records) REPORT_EVERY_RECORDS="${2:-}"; shift 2 ;;
    --report_every_seconds) REPORT_EVERY_SECONDS="${2:-}"; shift 2 ;;
    --sweep_token_total_min_list) SWEEP_TOKEN_TOTAL_MIN_LIST="${2:-}"; shift 2 ;;
    --retry) RETRY="${2:-}"; shift 2 ;;
    --no-skip) SKIP_EXISTING=0; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$ROOT" || -z "$OUT" ]]; then
  echo "ERROR: --root and --out are required" >&2
  usage
  exit 2
fi

if [[ ! -d "$ROOT" ]]; then
  echo "ERROR: root not found: $ROOT" >&2
  exit 2
fi

mkdir -p "$OUT"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

run_one() {
  local dataset_dir="$1"
  local ds
  ds="$(basename "$dataset_dir")"
  local out_dir="$OUT/$ds"
  local stats_json="$out_dir/analysis/reservoir_stats.json"
  local attempts=$((RETRY + 1))

  if [[ "$SKIP_EXISTING" -eq 1 && -f "$stats_json" ]]; then
    echo "skip $ds (exists: $stats_json)"
    return 0
  fi

  mkdir -p "$out_dir"
  for ((i=1; i<=attempts; i++)); do
    echo "==== [$(date '+%F %T')] START $ds (attempt $i/$attempts) ===="

    # Ensure cwd is repo root so imports work.
    (
      cd "$REPO_ROOT"
      PYTHONUNBUFFERED=1 python scripts/v2_interleave_cli.py analyze \
        --dataset_name "$ds" \
        --root "$ROOT" \
        --jsonl_dir_name jsonl_res \
        --out_dir "$out_dir" \
        --viz_reservoir_size "$VIZ_RESERVOIR_SIZE" \
        --report_every_records "$REPORT_EVERY_RECORDS" \
        --report_every_seconds "$REPORT_EVERY_SECONDS" \
        ${SWEEP_TOKEN_TOTAL_MIN_LIST:+--sweep_token_total_min_list "$SWEEP_TOKEN_TOTAL_MIN_LIST"}
    )

    if [[ -f "$stats_json" ]]; then
      echo "==== [$(date '+%F %T')] DONE  $ds ===="
      return 0
    fi
    echo "WARN: $ds finished but stats_json missing: $stats_json" >&2
    sleep 2
  done

  echo "==== [$(date '+%F %T')] FAIL  $ds ====" >&2
  return 1
}

export ROOT OUT PARALLEL VIZ_RESERVOIR_SIZE REPORT_EVERY_RECORDS REPORT_EVERY_SECONDS SWEEP_TOKEN_TOTAL_MIN_LIST RETRY SKIP_EXISTING
export -f run_one

echo "[run_all_analyze] root=$ROOT out=$OUT parallel=$PARALLEL reservoir=$VIZ_RESERVOIR_SIZE retry=$RETRY skip_existing=$SKIP_EXISTING"

FAILED_LIST="$OUT/_failed_datasets.txt"
OK_LIST="$OUT/_ok_datasets.txt"
STARTED_LIST="$OUT/_started_datasets.txt"
: > "$FAILED_LIST"
: > "$OK_LIST"
: > "$STARTED_LIST"

# Enumerate top-level dataset dirs and run in parallel.
find "$ROOT" -mindepth 1 -maxdepth 1 -type d -print0 | \
  xargs -0 -n 1 -P "$PARALLEL" -I {} bash -lc '
    ds="$(basename "{}")"
    echo "$ds" >> "'"$STARTED_LIST"'"
    if run_one "{}"; then
      echo "$ds" >> "'"$OK_LIST"'"
    else
      echo "$ds" >> "'"$FAILED_LIST"'"
    fi
  '

echo "[run_all_analyze] done."
echo "[run_all_analyze] ok_list=$OK_LIST"
echo "[run_all_analyze] failed_list=$FAILED_LIST"

