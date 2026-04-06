@echo off
REM ════════════════════════════════════════════════════════════════════════════════
REM  run_pipeline.bat — Execute the full CLIF mobilization analysis pipeline
REM
REM  Steps:
REM    1. Python  01_cohort_identification.py   (cohort + STROBE)
REM    2. Python  02_mobilization_analysis.py   (criteria, tables, sensitivity)
REM    3. R       03_combined_analysis.R               (CIF, Fine-Gray, forest plots)
REM    4. R       04_sensitivity_forest_plots.R           (sensitivity forest plots)
REM
REM  Usage:  run_pipeline.bat [--chunked]
REM
REM  Options:
REM    --chunked   Run 01_cohort_identification.py one year at a time (2018-2024)
REM                to reduce peak memory usage.
REM ════════════════════════════════════════════════════════════════════════════════
setlocal enabledelayedexpansion

REM ── paths ────────────────────────────────────────────────────────────────────
set "PROJECT_ROOT=%~dp0"
REM Remove trailing backslash
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

for /f "tokens=*" %%a in ('powershell -noprofile -command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TIMESTAMP=%%a"

set "LOG_DIR=%PROJECT_ROOT%\code\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\pipeline_%TIMESTAMP%.log"

REM ── logging ──────────────────────────────────────────────────────────────────
call :log "CLIF Mobilization Pipeline"
call :log "Started: %DATE% %TIME%"
call :log "Log: %LOG_FILE%"
call :log ""

REM ── environment (uv) ─────────────────────────────────────────────────────────
where uv >nul 2>nul
if errorlevel 1 (
    call :log "ERROR: uv not found. Install it: https://docs.astral.sh/uv/getting-started/installation/"
    exit /b 1
)

call :log "Syncing dependencies with uv..."
uv sync --project "%PROJECT_ROOT%" >> "%LOG_FILE%" 2>&1
call :log "Environment ready"
call :log ""

REM ── helpers ──────────────────────────────────────────────────────────────────
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "MPLBACKEND=Agg"
set "PYTHONPATH=%PROJECT_ROOT%\code;%PYTHONPATH%"

set FAILED_COUNT=0
set STEP=0

REM ── parse --chunked flag ────────────────────────────────────────────────────
set "CHUNKED=0"
for %%a in (%*) do if "%%a"=="--chunked" set "CHUNKED=1"

if "!CHUNKED!"=="1" (
    set TOTAL=11
    call :log "Running in chunked mode (one year at a time)"
) else (
    set TOTAL=4
)

REM ── pipeline ─────────────────────────────────────────────────────────────────
cd /d "%PROJECT_ROOT%\code"

REM Step 1: Cohort Identification
if "!CHUNKED!"=="1" (
    for %%Y in (2018 2019 2020 2021 2022 2023 2024) do (
        set "COHORT_YEAR=%%Y"
        call :run_step "01 Cohort (%%Y)" uv run --project "%PROJECT_ROOT%" python 01_cohort_identification.py
    )
    set "COHORT_YEAR="
    call :run_step "01b Aggregate Yearly" uv run --project "%PROJECT_ROOT%" python 01b_aggregate_yearly.py
) else (
    call :run_step "01 Cohort Identification" uv run --project "%PROJECT_ROOT%" python 01_cohort_identification.py
)

REM Step 2: Mobilization Analysis
call :run_step "02 Mobilization Analysis" uv run --project "%PROJECT_ROOT%" python 02_mobilization_analysis.py

REM Step 3-4: R scripts
where Rscript >nul 2>nul
if errorlevel 1 (
    call :log "WARNING: Rscript not found — skipping R analysis."
    call :log "Run manually: cd code && Rscript 03_combined_analysis.R && Rscript 04_sensitivity_forest_plots.R"
    set /a FAILED_COUNT+=2
) else (
    call :run_step "03 Combined R Analysis" Rscript --vanilla 03_combined_analysis.R
    call :run_step "04 Sensitivity Forest Plots" Rscript --vanilla 04_sensitivity_forest_plots.R
)

REM ── summary ──────────────────────────────────────────────────────────────────
call :log "════════════════════════════════════════"
call :log "  PIPELINE SUMMARY"
call :log "════════════════════════════════════════"

if %FAILED_COUNT% equ 0 (
    call :log "All steps completed successfully."
) else (
    call :log "%FAILED_COUNT% step(s) failed. Check log for details."
    call :log ""
    call :log "── R Troubleshooting ──"
    call :log "If you see 'renv/activate.R' errors, your .Rprofile is loading renv which this project does not use."
    call :log "The --vanilla flag should bypass this, but if issues persist:"
    call :log "  1. Temporarily rename your .Rprofile"
    call :log "  2. Or run the R scripts manually from an R console:"
    call :log ""
    call :log "     setwd('%PROJECT_ROOT%\code')"
    call :log "     install.packages(c('arrow', 'cmprsk', 'data.table', 'dplyr', 'ggplot2',"
    call :log "                        'tidyverse', 'writexl', 'jsonlite', 'patchwork', 'tidyr'))"
    call :log "     source('03_combined_analysis.R')"
    call :log "     source('04_sensitivity_forest_plots.R')"
    call :log ""
)

call :log ""
call :log "Output files in %PROJECT_ROOT%\output\final\"
call :log "Full log: %LOG_FILE%"
call :log "Finished: %DATE% %TIME%"

endlocal
exit /b 0

REM ── subroutines ──────────────────────────────────────────────────────────────

:run_step
set /a STEP+=1
set "STEP_NAME=%~1"
shift
call :log "[%STEP%/%TOTAL%] %STEP_NAME%"
set "STEP_LOG=%LOG_DIR%\%STEP_NAME: =_%_%TIMESTAMP%.log"
%1 %2 %3 %4 %5 %6 %7 %8 %9 > "%STEP_LOG%" 2>&1
if errorlevel 1 (
    call :log "  FAILED"
    set /a FAILED_COUNT+=1
) else (
    call :log "  Done"
)
call :log ""
exit /b

:log
echo %~1
echo %~1 >> "%LOG_FILE%"
exit /b
