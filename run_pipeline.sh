#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════════
#  run_pipeline.sh — Execute the full CLIF mobilization analysis pipeline
#
#  Steps:
#    1. Python  01_cohort_identification.py   (cohort + STROBE)
#    2. Python  02_mobilization_analysis.py   (criteria, tables, sensitivity)
#    3. R       03_combined_analysis.R               (CIF, Fine-Gray, forest plots)
#    4. R       04_sensitivity_forest_plots.R           (sensitivity forest plots)
#
#  Usage:  bash run_pipeline.sh
# ════════════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── colours ──────────────────────────────────────────────────────────────────
GREEN="\033[32m"; RED="\033[31m"; CYAN="\033[36m"; YELLOW="\033[33m"
BOLD="\033[1m"; RESET="\033[0m"

# ── paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="${PROJECT_ROOT}/code/logs"
LOG_FILE="${LOG_DIR}/pipeline_${TIMESTAMP}.log"
mkdir -p "$LOG_DIR"

# ── logging ──────────────────────────────────────────────────────────────────
log() { echo -e "$1" | tee -a "$LOG_FILE"; }

log "${CYAN}${BOLD}CLIF Mobilization Pipeline${RESET}"
log "Started: $(date)"
log "Log: ${LOG_FILE}"
log ""

# ── environment (uv) ─────────────────────────────────────────────────────────
if ! command -v uv >/dev/null 2>&1; then
  log "${RED}uv not found. Install it: https://docs.astral.sh/uv/getting-started/installation/${RESET}"
  exit 1
fi

log "Syncing dependencies with uv..."
uv sync --project "${PROJECT_ROOT}" 2>&1 | tee -a "$LOG_FILE"
log "${GREEN}Environment ready${RESET}"
log ""

# ── helpers ──────────────────────────────────────────────────────────────────
export PYTHONUNBUFFERED=1
export MPLBACKEND=Agg
export PYTHONPATH="${PROJECT_ROOT}/code:${PYTHONPATH:-}"

FAILED_STEPS=()
STEP=0
TOTAL=4  # 2 Python + 2 R

run_step() {
  local name="$1"; shift
  STEP=$((STEP + 1))
  log "${CYAN}${BOLD}[${STEP}/${TOTAL}] ${name}${RESET}"
  local start_s=$SECONDS
  local step_log="${LOG_DIR}/${name// /_}_${TIMESTAMP}.log"

  if "$@" 2>&1 | tee "$step_log" | tee -a "$LOG_FILE"; then
    local elapsed=$(( SECONDS - start_s ))
    log "${GREEN}  Done in ${elapsed}s${RESET}"
  else
    local elapsed=$(( SECONDS - start_s ))
    log "${RED}  FAILED after ${elapsed}s${RESET}"
    FAILED_STEPS+=("$name")
  fi
  log ""
}

# ── pipeline (cwd = code/ so relative paths work) ───────────────────────────
cd "${PROJECT_ROOT}/code"

# Python steps
run_step "01 Cohort Identification"       uv run --project "${PROJECT_ROOT}" python 01_cohort_identification.py
run_step "02 Mobilization Analysis"       uv run --project "${PROJECT_ROOT}" python 02_mobilization_analysis.py

# R steps
if command -v Rscript >/dev/null 2>&1; then
  run_step "03 Combined R Analysis"       Rscript --vanilla 03_combined_analysis.R
  run_step "04 Sensitivity Forest Plots"  Rscript --vanilla 04_sensitivity_forest_plots.R
else
  log "${YELLOW}Rscript not found — skipping R analysis.${RESET}"
  log "${YELLOW}Run manually: cd code && Rscript 03_combined_analysis.R && Rscript 04_sensitivity_forest_plots.R${RESET}"
  FAILED_STEPS+=("03 Combined R Analysis (Rscript not found)")
  FAILED_STEPS+=("04 Sensitivity Forest Plots (Rscript not found)")
fi

# ── summary ──────────────────────────────────────────────────────────────────
log "${CYAN}${BOLD}════════════════════════════════════════${RESET}"
log "${CYAN}${BOLD}  PIPELINE SUMMARY${RESET}"
log "${CYAN}${BOLD}════════════════════════════════════════${RESET}"

if [ ${#FAILED_STEPS[@]} -eq 0 ]; then
  log "${GREEN}${BOLD}All steps completed successfully.${RESET}"
else
  log "${RED}${BOLD}Failed steps:${RESET}"
  for s in "${FAILED_STEPS[@]}"; do
    log "${RED}  - ${s}${RESET}"
  done

  # Check if any R steps failed
  R_FAILED=0
  for s in "${FAILED_STEPS[@]}"; do
    case "$s" in *R*|*Forest*) R_FAILED=1 ;; esac
  done
  if [ $R_FAILED -eq 1 ]; then
    log ""
    log "${YELLOW}${BOLD}── R Troubleshooting ──${RESET}"
    log "${YELLOW}If you see 'renv/activate.R' errors, your .Rprofile is loading renv which this project does not use.${RESET}"
    log "${YELLOW}The --vanilla flag should bypass this, but if issues persist:${RESET}"
    log "${YELLOW}  1. Temporarily rename ~/.Rprofile (or the project-level .Rprofile)${RESET}"
    log "${YELLOW}  2. Or run the R scripts manually from an R console:${RESET}"
    log ""
    log "${CYAN}     setwd(\"${PROJECT_ROOT}/code\")${RESET}"
    log "${CYAN}     install.packages(c(\"arrow\", \"cmprsk\", \"data.table\", \"dplyr\", \"ggplot2\",${RESET}"
    log "${CYAN}                        \"tidyverse\", \"writexl\", \"jsonlite\", \"patchwork\", \"tidyr\"))${RESET}"
    log "${CYAN}     source(\"03_combined_analysis.R\")${RESET}"
    log "${CYAN}     source(\"04_sensitivity_forest_plots.R\")${RESET}"
    log ""
  fi
fi

log ""
log "Output files in ${PROJECT_ROOT}/output/final/:"
if [ -d "${PROJECT_ROOT}/output/final" ]; then
  # List generated files with sizes
  find "${PROJECT_ROOT}/output/final" -type f -newer "${LOG_FILE}" -exec ls -lh {} \; 2>/dev/null | \
    awk '{printf "  %-8s %s\n", $5, $NF}' | tee -a "$LOG_FILE" || true
  # If nothing newer, just list everything
  FILE_COUNT=$(find "${PROJECT_ROOT}/output/final" -type f | wc -l | tr -d ' ')
  log "  Total files: ${FILE_COUNT}"
else
  log "  (directory not yet created)"
fi

log ""
log "Full log: ${LOG_FILE}"
log "Finished: $(date)"
