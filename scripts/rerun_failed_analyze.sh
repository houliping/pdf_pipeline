#!/usr/bin/env bash
set -euo pipefail

# Re-run analyze only for dataset names listed in _failed_datasets.txt (or custom file).
# One dataset name per line (same format as run_all_analyze.sh writes).
#
# Usage:
#   bash scripts/rerun_failed_analyze.sh \
#     --root /home/ma-user/work/bucket-8107/hlp/V2_InterleaveDataPool \
#     --out /cache/v2_analyze_full
#
# Optional:
#   --failed-file PATH   Default: OUT/_failed_datasets.txt
#   (same knobs as run_all_analyze.sh for viz/report/sweep)

ROOT=""
OUT=""
FAILED_FILE=""
VIZ_RESERVOIR_SIZE=200000
REPORT_EVERY_RECORDS=20000
REPORT_EVERY_SECONDS=10
SWEEP_TOKEN_TOTAL_MIN_LIST=""

usage() {
  cat <<'EOF'
rerun_failed_analyze.sh

Required:
  --root PATH   V2_InterleaveDataPool root (contains dataset dirs)
  --out PATH    Same --out you used for run_all_analyze.sh

Optional:
  --failed-file PATH   List of dataset basenames (default: OUT/_failed_datasets.txt)
  --viz_reservoir_size N
  --report_every_records N
  --report_every_seconds N
  --sweep_token_total_min_list CSV

Tip: if retry still fails after a partial/corrupt run:
  rm -rf OUT/<dataset>/analysis

Still-failed names are appended to OUT/_failed_datasets_retry.txt
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --failed-file) FAILED_FILE="${2:-}"; shift 2 ;;
    --viz_reservoir_size) VIZ_RESERVOIR_SIZE="${2:-}"; shift 2 ;;
    --report_every_records) REPORT_EVERY_RECORDS="${2:-}"; shift 2 ;;
    --report_every_seconds) REPORT_EVERY_SECONDS="${2:-}"; shift 2 ;;
    --sweep_token_total_min_list) SWEEP_TOKEN_TOTAL_MIN_LIST="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$ROOT" || -z "$OUT" ]]; then
  echo "ERROR: --root and --out are required" >&2
  usage
  exit 2
fi

[[ -z "$FAILED_FILE" ]] && FAILED_FILE="$OUT/_failed_datasets.txt"

if [[ ! -f "$FAILED_FILE" ]]; then
  echo "ERROR: failed list not found: $FAILED_FILE" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RETRY_FAIL="$OUT/_failed_datasets_retry.txt"
: > "$RETRY_FAIL"

mapfile -t FAILED_DS < <(grep -v '^[[:space:]]*#' "$FAILED_FILE" | sed '/^[[:space:]]*$/d' || true)
if [[ ${#FAILED_DS[@]} -eq 0 ]]; then
  echo "No dataset names in $FAILED_FILE"
  exit 0
fi

echo "[rerun_failed] failed_file=$FAILED_FILE count=${#FAILED_DS[@]}"

for ds in "${FAILED_DS[@]}"; do
  ds="${ds//$'\r'/}"
  [[ -n "$ds" ]] || continue
  out_dir="$OUT/$ds"
  stats_json="$out_dir/analysis/reservoir_stats.json"
  mkdir -p "$out_dir"
  echo "==== [$(date '+%F %T')] RETRY $ds ===="
  set +e
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
  rc=$?
  set -e
  if [[ -f "$stats_json" && $rc -eq 0 ]]; then
    echo "==== [$(date '+%F %T')] OK $ds ===="
  else
    echo "==== [$(date '+%F %T')] STILL FAIL $ds (exit=$rc) ====" >&2
    echo "$ds" >> "$RETRY_FAIL"
  fi
done

echo "[rerun_failed] done. still_failed -> $RETRY_FAIL"
if [[ -s "$RETRY_FAIL" ]]; then
  cat "$RETRY_FAIL"
  exit 1
fi
exit 0
