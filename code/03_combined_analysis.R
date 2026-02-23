#!/usr/bin/env Rscript
# ════════════════════════════════════════════════════════════════════════════════
#  Combined Competing-Risk & Stacked Sensitivity Analysis
#
#  Part A  – Main CIF + Fine-Gray analysis (full / 72h / weekday-only)
#  Part B  – Stacked sensitivity analysis (1h anyday → 1h weekday → 4h weekday)
#            Uses parquets from Python Phase 5 (sensitivity_stacked cell)
#            Two cohorts: original (IMV ≥4h) and IMV ≥24h subcohort
# ════════════════════════════════════════════════════════════════════════════════

## 1 ── packages ---------------------------------------------------------------
pkgs <- c("arrow", "cmprsk", "data.table", "dplyr", "jsonlite", "ggplot2",
          "tidyverse", "writexl")
need <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(need))
  install.packages(need, repos = "https://cloud.r-project.org")
invisible(lapply(pkgs, library, character.only = TRUE))

## 2 ── config -----------------------------------------------------------------
# Determine project root
script_dir <- getwd()
if (basename(script_dir) == "code") {
  project_root <- dirname(script_dir)
} else if (dir.exists(file.path(script_dir, "code"))) {
  project_root <- script_dir
} else {
  project_root <- dirname(script_dir)
}

config_path <- file.path(project_root, "config", "config.json")
if (file.exists(config_path)) {
  config <- fromJSON(config_path)
  site_name <- config$site_name
} else {
  site_name <- "SITE"
  warning("config.json not found, using default site name")
}
cat(sprintf("Project root: %s\nSite: %s\n\n", project_root, site_name))

## 3 ── helpers ----------------------------------------------------------------
.pq_cache <- new.env(parent = emptyenv())
read_pq <- function(path) {
  key <- normalizePath(path, mustWork = FALSE)
  if (!exists(key, envir = .pq_cache))
    assign(key, arrow::open_dataset(path) |> collect(), envir = .pq_cache)
  get(key, envir = .pq_cache)
}

cif_df <- function(time, status, max_h = Inf, cause = 1, alpha = 0.05) {
  time   <- pmin(time, max_h)
  status <- ifelse(time >= max_h & status == cause, 0, status)
  fit <- cuminc(ftime = time, fstatus = status, cencode = 0)
  key <- sprintf("%d %d", cause, cause)
  ci  <- fit[[key]]
  out <- data.frame(time = ci$time, est = ci$est)
  if (!is.null(ci$lower)) {
    out$lower <- ci$lower; out$upper <- ci$upper
  } else if (!is.null(ci$var)) {
    z <- qnorm(1 - alpha / 2)
    out$lower <- pmax(0, ci$est - z * sqrt(ci$var))
    out$upper <- pmin(1, ci$est + z * sqrt(ci$var))
  } else {
    out$lower <- out$upper <- NA
  }
  attr(out, "cuminc") <- fit
  out
}

median_time <- function(df) {
  x <- which(df$est >= .5)
  if (length(x)) df$time[x[1]] else Inf
}

analyse_one <- function(name, path, out_csv, max_h = Inf) {
  dat <- read_pq(path)[, c("t_event", "outcome")]
  setnames(dat, c("t_event", "outcome"), c("time", "status"))
  cif <- cif_df(dat$time, dat$status, max_h = max_h)
  fwrite(cif, out_csv)
  list(name = name, cif = cif, median = median_time(cif),
       fit = attr(cif, "cuminc"))
}

## 4 ── paths ------------------------------------------------------------------
int_dir <- file.path(project_root, "output", "intermediate")
out_dir <- file.path(project_root, "output", "final")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(out_dir, "graphs"), showWarnings = FALSE)

# Primary parquets (from Python Cell 14)
paths <- list(
  Patel  = file.path(int_dir, "competing_risk_patel_final.parquet"),
  TEAM   = file.path(int_dir, "competing_risk_team_final.parquet"),
  Yellow = file.path(int_dir, "competing_risk_yellow_final.parquet"),
  Green  = file.path(int_dir, "competing_risk_green_no_red_final.parquet")
)

paths_weekday <- list(
  Patel  = file.path(int_dir, "competing_risk_patel_final_weekday.parquet"),
  TEAM   = file.path(int_dir, "competing_risk_team_final_weekday.parquet"),
  Yellow = file.path(int_dir, "competing_risk_yellow_final_weekday.parquet"),
  Green  = file.path(int_dir, "competing_risk_green_no_red_final_weekday.parquet")
)


# ══════════════════════════════════════════════════════════════════════════════
# PART A – Main Competing-Risk Analysis
# ══════════════════════════════════════════════════════════════════════════════

cat("\n", paste(rep("=", 70), collapse = ""), "\n")
cat("PART A: MAIN COMPETING-RISK ANALYSIS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

## 5 ── CIF full follow-up -----------------------------------------------------
cat("── CIF (full follow-up)\n")
res_full <- lapply(names(paths), \(nm)
  analyse_one(nm, paths[[nm]],
              file.path(out_dir, sprintf("%s_cif.csv", tolower(nm)))))

cols <- c(Patel = "maroon", TEAM = "blue",
          Yellow = "darkgoldenrod1", Green = "darkgreen")

png(file.path(out_dir, "graphs", "cif_overlay.png"),
    width = 1800, height = 1200, res = 200)
plot(0, 0, type = "n",
     xlim = c(0, max(sapply(res_full, \(x) max(x$cif$time)))),
     ylim = c(0, 1), xlab = "Time (hours)", ylab = "CIF",
     main = "All follow-up")
for (r in res_full)
  lines(r$cif$time, r$cif$est, col = cols[r$name], lwd = 2)
legend("bottomright", legend = names(cols), col = cols, lwd = 2, lty = 1)
dev.off()

fwrite(data.frame(Criterion = sapply(res_full, `[[`, "name"),
                  Median_h  = sapply(res_full, `[[`, "median")),
       file.path(out_dir, "median_times.csv"))

## 6 ── CIF truncated at 72 h -------------------------------------------------
cat("── CIF (first 72 h)\n")
res_72 <- lapply(names(paths), \(nm)
  analyse_one(nm, paths[[nm]],
              file.path(out_dir, sprintf("cif_72hrs_%s.csv", tolower(nm))),
              max_h = 72))

png(file.path(out_dir, "graphs", "cif_overlay_72hrs.png"),
    width = 1800, height = 1200, res = 200)
plot(0, 0, type = "n", xlim = c(0, 72), ylim = c(0, 1),
     xlab = "Time (hours)", ylab = "CIF", main = "First 72 h")
for (r in res_72)
  lines(r$cif$time, r$cif$est, col = cols[r$name], lwd = 2)
legend("bottomright", legend = names(cols), col = cols, lwd = 2, lty = 1)
dev.off()

fwrite(data.frame(Criterion = sapply(res_72, `[[`, "name"),
                  Median_h  = sapply(res_72, `[[`, "median")),
       file.path(out_dir, "median_times_72hrs.csv"))

## 7 ── CIF weekday-only (first 72 h) -----------------------------------------
cat("── CIF Weekday-only (first 72 h)\n")
res_weekday_72 <- lapply(names(paths_weekday), \(nm)
  analyse_one(nm, paths_weekday[[nm]],
              file.path(out_dir, sprintf("cif_72hrs_weekday_%s.csv", tolower(nm))),
              max_h = 72))

png(file.path(out_dir, "graphs", "cif_overlay_72hrs_weekday.png"),
    width = 1800, height = 1200, res = 200)
plot(0, 0, type = "n", xlim = c(0, 72), ylim = c(0, 1),
     xlab = "Time (hours)", ylab = "CIF",
     main = "First 72 h (Weekdays Only)")
for (r in res_weekday_72)
  lines(r$cif$time, r$cif$est, col = cols[r$name], lwd = 2)
legend("bottomright", legend = names(cols), col = cols, lwd = 2, lty = 1)
dev.off()

fwrite(data.frame(Criterion = sapply(res_weekday_72, `[[`, "name"),
                  Median_h_weekday = sapply(res_weekday_72, `[[`, "median")),
       file.path(out_dir, "median_times_72hrs_weekday.csv"))

## Weekday vs All-day comparison -----------------------------------------------
cat("── Comparison: All-day vs Weekday median times\n")
comparison_72h <- data.frame(
  Criterion      = sapply(res_72, `[[`, "name"),
  Median_AllDays = sapply(res_72, `[[`, "median"),
  Median_Weekdays = sapply(res_weekday_72, `[[`, "median")
)
comparison_72h$Difference_Hours <- comparison_72h$Median_Weekdays -
  comparison_72h$Median_AllDays
comparison_72h$Percent_Change   <- (comparison_72h$Difference_Hours /
  comparison_72h$Median_AllDays) * 100
fwrite(comparison_72h, file.path(out_dir, "weekday_sensitivity_comparison_72hrs.csv"))

png(file.path(out_dir, "graphs", "cif_comparison_allday_vs_weekday_72hrs.png"),
    width = 2400, height = 1200, res = 200)
par(mfrow = c(1, 2))
plot(0, 0, type = "n", xlim = c(0, 72), ylim = c(0, 1),
     xlab = "Time (hours)", ylab = "CIF", main = "All Days (8am-5pm)")
for (r in res_72)
  lines(r$cif$time, r$cif$est, col = cols[r$name], lwd = 2)
legend("bottomright", legend = names(cols), col = cols, lwd = 2, lty = 1)
plot(0, 0, type = "n", xlim = c(0, 72), ylim = c(0, 1),
     xlab = "Time (hours)", ylab = "CIF", main = "Weekdays Only (8am-5pm)")
for (r in res_weekday_72)
  lines(r$cif$time, r$cif$est, col = cols[r$name], lwd = 2)
legend("bottomright", legend = names(cols), col = cols, lwd = 2, lty = 1)
par(mfrow = c(1, 1))
dev.off()

cat("\n=== WEEKDAY SENSITIVITY ANALYSIS RESULTS (72h) ===\n")
print(comparison_72h)

## 8 ── Fine-Gray sub-hazard models --------------------------------------------
bind_for_fg <- \(path, grp)
  as.data.table(read_pq(path))[, .(encounter_block, t_event, outcome, group = grp)]

make_fg <- function(path_list, max_h = Inf, suffix = "") {
  cr <- rbindlist(list(
    bind_for_fg(path_list$Patel,  "Patel"),
    bind_for_fg(path_list$TEAM,   "TEAM"),
    bind_for_fg(path_list$Yellow, "Yellow"),
    bind_for_fg(path_list$Green,  "Green")
  ))
  cr[, `:=`(
    t_event = pmin(t_event, max_h),
    outcome = ifelse(t_event >= max_h & outcome == 1, 0L, outcome)
  )]
  cr[, group := factor(group, levels = c("Patel", "TEAM", "Yellow", "Green"))]

  events_by_group <- with(cr, table(group, outcome))
  fwrite(as.data.frame(events_by_group),
         file.path(out_dir, sprintf("events_by_group%s.csv", suffix)))

  fg <- crr(ftime = cr$t_event, fstatus = cr$outcome,
            cov1 = model.matrix(~ group, cr)[, -1])
  capture.output(summary(fg),
                 file = file.path(out_dir, sprintf("fg_model_summary%s.txt", suffix)))

  fg_export <- list(
    coef = setNames(unname(fg$coef), colnames(fg$coef)),
    var = fg$var, n = fg$n,
    events = as.numeric(table(cr$outcome))
  )
  write_json(fg_export,
             path = file.path(out_dir, sprintf("subhazard_summary%s.json", suffix)),
             digits = 8, auto_unbox = TRUE)
  invisible(NULL)
}

cat("── Fine-Gray (full follow-up)\n")
make_fg(paths, suffix = "")
cat("── Fine-Gray (first 72 h)\n")
make_fg(paths, max_h = 72, suffix = "_72hrs")
cat("── Fine-Gray Weekday (first 72 h)\n")
make_fg(paths_weekday, max_h = 72, suffix = "_weekday_72hrs")

cat("\nPart A complete.\n")


# ══════════════════════════════════════════════════════════════════════════════
# PART B – Stacked Sensitivity Analysis
# Uses parquets from Python Phase 5: {site}_{criteria}_{type}_competing_risk.parquet
# ══════════════════════════════════════════════════════════════════════════════

cat("\n", paste(rep("=", 70), collapse = ""), "\n")
cat("PART B: STACKED SENSITIVITY ANALYSIS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Criteria mapping (Python name → display name)
criteria_map <- list(
  Chicago = "Chicago Criteria",
  TEAM    = "TEAM Criteria",
  Yellow  = "Consensus (Yellow)",
  Green   = "Consensus (Green)"
)

# Sensitivity types to analyze
sensitivity_types <- list(
  original = c("original_1h_anyday", "original_1h_weekday",
               "original_4h_anyday", "original_4h_weekday"),
  imv24h   = c("imv24h_1h_anyday", "imv24h_1h_weekday",
               "imv24h_4h_anyday", "imv24h_4h_weekday")
)

# Display labels for sensitivity types
type_labels <- c(
  original_1h_anyday  = "1h Any Day (Primary)",
  original_1h_weekday = "1h Weekday Only",
  original_4h_anyday  = "4h Continuous Any Day",
  original_4h_weekday = "4h Continuous Weekday",
  imv24h_1h_anyday    = "IMV>=24h: 1h Any Day",
  imv24h_1h_weekday   = "IMV>=24h: 1h Weekday",
  imv24h_4h_anyday    = "IMV>=24h: 4h Any Day",
  imv24h_4h_weekday   = "IMV>=24h: 4h Weekday"
)

# ── Load all sensitivity parquets ──
cat("Loading sensitivity parquets...\n")
all_sens <- list()
data_summary <- data.frame()

for (crit in names(criteria_map)) {
  for (cohort in names(sensitivity_types)) {
    for (stype in sensitivity_types[[cohort]]) {
      fp <- file.path(int_dir, sprintf("%s_%s_%s_competing_risk.parquet",
                                       site_name, crit, stype))
      if (!file.exists(fp)) {
        cat(sprintf("  SKIP: %s/%s (file not found)\n", crit, stype))
        next
      }
      df <- read_pq(fp)
      df$criteria      <- crit
      df$criteria_name <- criteria_map[[crit]]
      df$cohort        <- cohort
      df$sensitivity   <- stype
      df$label         <- type_labels[[stype]]

      key <- paste(crit, stype, sep = "_")
      all_sens[[key]] <- df

      data_summary <- rbind(data_summary, data.frame(
        Criteria     = criteria_map[[crit]],
        Cohort       = cohort,
        Sensitivity  = stype,
        Label        = type_labels[[stype]],
        N            = nrow(df),
        N_Eligible   = sum(df$outcome == 1),
        Pct_Eligible = round(mean(df$outcome == 1) * 100, 1),
        stringsAsFactors = FALSE
      ))
      cat(sprintf("  %s / %s: %d patients, %.1f%% eligible\n",
                  crit, stype, nrow(df), mean(df$outcome == 1) * 100))
    }
  }
}

cat(sprintf("\nLoaded %d sensitivity datasets\n", length(all_sens)))

if (length(all_sens) == 0) {
  cat("No sensitivity parquets found. Skipping Part B.\n")
} else {

  # ── CIF for each sensitivity definition ──
  cat("\nCalculating CIF for each sensitivity definition...\n")
  all_cif <- list()

  for (key in names(all_sens)) {
    df <- all_sens[[key]]
    tryCatch({
      time   <- df$t_event
      status <- df$outcome
      valid  <- !is.na(time) & !is.na(status) & time > 0
      time <- pmin(time[valid], 72)
      status_v <- status[valid]
      status_v <- ifelse(time >= 72 & status_v == 1, 0, status_v)

      fit <- cuminc(ftime = time, fstatus = status_v, cencode = 0)
      if ("1 1" %in% names(fit)) {
        ce <- fit[["1 1"]]
        cdf <- data.frame(
          time      = ce$time,
          cif       = ce$est,
          se        = sqrt(ce$var),
          criteria  = unique(df$criteria),
          criteria_name = unique(df$criteria_name),
          cohort    = unique(df$cohort),
          sensitivity = unique(df$sensitivity),
          label     = unique(df$label)
        )
        cdf$ci_lower <- pmax(0, cdf$cif - 1.96 * cdf$se)
        cdf$ci_upper <- pmin(1, cdf$cif + 1.96 * cdf$se)
        all_cif[[key]] <- cdf
      }
    }, error = function(e)
      cat(sprintf("  CIF error for %s: %s\n", key, e$message)))
  }

  cif_combined <- bind_rows(all_cif)
  cat(sprintf("CIF computed for %d definitions\n", length(all_cif)))

  # Save per-definition CIF CSVs for federation
  for (key in names(all_cif)) {
    fwrite(all_cif[[key]],
           file.path(out_dir, sprintf("cif_sensitivity_%s_%s.csv", site_name, key)))
  }
  cat(sprintf("  Saved %d per-definition CIF CSVs\n", length(all_cif)))

  # ── Median times ──
  cat("\nComputing median times...\n")
  median_results <- data.frame()
  for (key in names(all_sens)) {
    df <- all_sens[[key]]
    elig <- df[df$outcome == 1, ]
    # Bootstrap SE for median (1000 reps) — needed for federated meta-analysis
    se_med <- NA
    if (nrow(elig) >= 10) {
      set.seed(42)
      boot_medians <- replicate(1000, median(sample(elig$time_eligibility, replace = TRUE), na.rm = TRUE))
      se_med <- round(sd(boot_medians), 2)
    }
    # SE for proportion (Wald)
    se_pct <- if (nrow(df) > 0) {
      p <- nrow(elig) / nrow(df)
      round(sqrt(p * (1 - p) / nrow(df)) * 100, 2)
    } else NA

    median_results <- rbind(median_results, data.frame(
      Site         = site_name,
      Criteria     = unique(df$criteria_name),
      Cohort       = unique(df$cohort),
      Sensitivity  = unique(df$sensitivity),
      Label        = unique(df$label),
      N_Total      = nrow(df),
      N_Eligible   = nrow(elig),
      Pct_Eligible = round(nrow(elig) / nrow(df) * 100, 1),
      SE_Pct       = se_pct,
      Median_Hours = if (nrow(elig) > 0) median(elig$time_eligibility, na.rm = TRUE) else NA,
      SE_Median    = se_med,
      Q1_Hours     = if (nrow(elig) > 0) quantile(elig$time_eligibility, 0.25, na.rm = TRUE) else NA,
      Q3_Hours     = if (nrow(elig) > 0) quantile(elig$time_eligibility, 0.75, na.rm = TRUE) else NA,
      stringsAsFactors = FALSE
    ))
  }
  median_results <- median_results %>% arrange(Criteria, Cohort, Sensitivity)

  cat("\nMedian Times Summary:\n")
  print(median_results %>% select(Criteria, Cohort, Label, Pct_Eligible, Median_Hours, Q1_Hours, Q3_Hours))

  # ── Visualizations (generated BEFORE SHR to ensure plots exist even if crr() is slow) ──
  cat("\nGenerating sensitivity visualizations...\n")

  stacking_colors <- c(
    "1h Any Day (Primary)"      = "#2E7D32",
    "1h Weekday Only"           = "#1976D2",
    "4h Continuous Any Day"     = "#FF9800",
    "4h Continuous Weekday"     = "#C62828",
    "IMV>=24h: 1h Any Day"     = "#2E7D32",
    "IMV>=24h: 1h Weekday"     = "#1976D2",
    "IMV>=24h: 4h Any Day"     = "#FF9800",
    "IMV>=24h: 4h Weekday"     = "#C62828"
  )

  # --- CIF facet plot: Original cohort stacking ---
  cif_original <- cif_combined %>% filter(cohort == "original", time <= 72)
  if (nrow(cif_original) > 0) {
    p1 <- ggplot(cif_original, aes(x = time, y = cif * 100, color = label)) +
      geom_line(linewidth = 1.2) +
      geom_ribbon(aes(ymin = ci_lower * 100, ymax = ci_upper * 100,
                      fill = label), alpha = 0.15, linetype = 0) +
      facet_wrap(~ criteria_name, nrow = 2, ncol = 2) +
      scale_color_manual(values = stacking_colors, name = "Definition") +
      scale_fill_manual(values = stacking_colors, name = "Definition") +
      scale_x_continuous(breaks = seq(0, 72, 12)) +
      scale_y_continuous(breaks = seq(0, 100, 20), limits = c(0, 100)) +
      labs(title = "Stacked Sensitivity: CIF of Mobilization Eligibility",
           subtitle = sprintf("Original Cohort (IMV >= 4h) | Site: %s", site_name),
           x = "Hours from Ventilation Start", y = "Cumulative Incidence (%)") +
      theme_minimal(base_size = 12) +
      theme(legend.position = "bottom",
            strip.text = element_text(size = 11, face = "bold"),
            strip.background = element_rect(fill = "gray95", color = NA),
            panel.border = element_rect(color = "gray80", fill = NA)) +
      geom_vline(xintercept = c(24, 48), linetype = "dotted", alpha = 0.3)

    ggsave(file.path(out_dir, "graphs", "stacked_cif_original.pdf"),
           p1, width = 14, height = 10)
    ggsave(file.path(out_dir, "graphs", "stacked_cif_original.png"),
           p1, width = 14, height = 10, dpi = 300)
    cat("  Created: stacked_cif_original.pdf/png\n")
  }

  # --- CIF facet plot: IMV ≥24h cohort ---
  cif_imv24h <- cif_combined %>% filter(cohort == "imv24h", time <= 72)
  if (nrow(cif_imv24h) > 0) {
    p2 <- ggplot(cif_imv24h, aes(x = time, y = cif * 100, color = label)) +
      geom_line(linewidth = 1.2) +
      geom_ribbon(aes(ymin = ci_lower * 100, ymax = ci_upper * 100,
                      fill = label), alpha = 0.15, linetype = 0) +
      facet_wrap(~ criteria_name, nrow = 2, ncol = 2) +
      scale_color_manual(values = stacking_colors, name = "Definition") +
      scale_fill_manual(values = stacking_colors, name = "Definition") +
      scale_x_continuous(breaks = seq(0, 72, 12)) +
      scale_y_continuous(breaks = seq(0, 100, 20), limits = c(0, 100)) +
      labs(title = "Stacked Sensitivity: CIF of Mobilization Eligibility",
           subtitle = sprintf("IMV >= 24h Subcohort | Site: %s", site_name),
           x = "Hours from Ventilation Start", y = "Cumulative Incidence (%)") +
      theme_minimal(base_size = 12) +
      theme(legend.position = "bottom",
            strip.text = element_text(size = 11, face = "bold"),
            strip.background = element_rect(fill = "gray95", color = NA),
            panel.border = element_rect(color = "gray80", fill = NA)) +
      geom_vline(xintercept = c(24, 48), linetype = "dotted", alpha = 0.3)

    ggsave(file.path(out_dir, "graphs", "stacked_cif_imv24h.pdf"),
           p2, width = 14, height = 10)
    ggsave(file.path(out_dir, "graphs", "stacked_cif_imv24h.png"),
           p2, width = 14, height = 10, dpi = 300)
    cat("  Created: stacked_cif_imv24h.pdf/png\n")
  }

  # --- Eligibility percentage bars ---
  if (nrow(median_results) > 0) {
    elig_bars <- median_results %>%
      filter(Cohort == "original") %>%
      mutate(Label = factor(Label, levels = c(
        "1h Any Day (Primary)", "1h Weekday Only",
        "4h Continuous Any Day", "4h Continuous Weekday"
      )))

    p4 <- ggplot(elig_bars, aes(x = Criteria, y = Pct_Eligible, fill = Label)) +
      geom_bar(stat = "identity", position = "dodge", width = 0.8,
               color = "black", linewidth = 0.3) +
      geom_text(aes(label = sprintf("%.1f%%", Pct_Eligible)),
                position = position_dodge(width = 0.8),
                vjust = -0.5, size = 3) +
      geom_hline(yintercept = 80, linetype = "dashed", color = "darkgreen", linewidth = 0.8) +
      scale_y_continuous(limits = c(0, 105), breaks = seq(0, 100, 20)) +
      scale_fill_manual(values = stacking_colors) +
      labs(title = "Eligibility Rates Across Stacked Definitions (Original Cohort)",
           subtitle = sprintf("Site: %s | Dashed line = 80%% threshold", site_name),
           x = "Mobilization Criteria", y = "Percentage Eligible (%)", fill = "Definition") +
      theme_minimal(base_size = 12) +
      theme(legend.position = "bottom", panel.grid.major.x = element_blank())

    ggsave(file.path(out_dir, "graphs", "stacked_eligibility_bars.pdf"),
           p4, width = 14, height = 8)
    ggsave(file.path(out_dir, "graphs", "stacked_eligibility_bars.png"),
           p4, width = 14, height = 8, dpi = 300)
    cat("  Created: stacked_eligibility_bars.pdf/png\n")
  }

  # ── Save non-SHR results early ──
  cat("\nSaving sensitivity results (non-SHR)...\n")

  write.csv(median_results,
            file.path(out_dir, "stacked_sensitivity_medians.csv"),
            row.names = FALSE)
  cat("  Saved: stacked_sensitivity_medians.csv\n")

  write.csv(data_summary,
            file.path(out_dir, "stacked_sensitivity_data_summary.csv"),
            row.names = FALSE)
  cat("  Saved: stacked_sensitivity_data_summary.csv\n")

  # ── SHR (Fine-Gray) — stacked definitions vs primary (slow, runs last) ──
  cat("\nCalculating subdistribution hazard ratios (24 comparisons, may take a few minutes)...\n")
  flush.console()
  shr_results <- list()
  shr_count <- 0
  shr_total <- sum(sapply(names(criteria_map), function(crit) {
    sum(sapply(names(sensitivity_types), function(cohort_name) {
      stypes <- sensitivity_types[[cohort_name]]
      ref_key <- paste(crit, stypes[1], sep = "_")
      if (!(ref_key %in% names(all_sens))) return(0)
      test_keys <- paste(crit, stypes[-1], sep = "_")
      sum(test_keys %in% names(all_sens))
    }))
  }))

  for (crit in names(criteria_map)) {
    for (cohort_name in names(sensitivity_types)) {
      stypes <- sensitivity_types[[cohort_name]]
      # Reference = first type (1h_anyday)
      ref_key <- paste(crit, stypes[1], sep = "_")
      if (!(ref_key %in% names(all_sens))) next

      test_keys <- paste(crit, stypes[-1], sep = "_")
      test_keys <- test_keys[test_keys %in% names(all_sens)]
      if (length(test_keys) == 0) next

      ref_df <- all_sens[[ref_key]]
      ref_df$def_group <- "reference"

      for (tk in test_keys) {
        shr_count <- shr_count + 1
        cat(sprintf("  [%d/%d] %s / %s / %s ... ", shr_count, shr_total,
                    criteria_map[[crit]], cohort_name, tk))
        flush.console()

        test_df <- all_sens[[tk]]
        test_df$def_group <- "test"
        combined <- bind_rows(ref_df[, c("encounter_block", "t_event", "outcome", "def_group")],
                              test_df[, c("encounter_block", "t_event", "outcome", "def_group")])
        combined$is_test <- as.integer(combined$def_group == "test")

        valid <- !is.na(combined$t_event) & !is.na(combined$outcome) & combined$t_event > 0
        if (sum(valid) < 30) { cat("SKIP (n<30)\n"); next }

        tryCatch({
          tv <- combined$t_event[valid]
          sv <- combined$outcome[valid]
          cov <- matrix(combined$is_test[valid], ncol = 1)

          fg <- crr(tv, sv, cov1 = cov, failcode = 1, cencode = 0)
          se <- sqrt(diag(fg$var))
          fg_key <- paste(crit, cohort_name, unique(test_df$sensitivity), sep = "_")
          shr_results[[fg_key]] <- data.frame(
            Criteria    = criteria_map[[crit]],
            Cohort      = cohort_name,
            Comparison  = unique(test_df$sensitivity),
            Label       = unique(test_df$label),
            Reference   = unique(ref_df$sensitivity),
            SHR         = exp(fg$coef[1]),
            CI_Lower    = exp(fg$coef[1] - 1.96 * se[1]),
            CI_Upper    = exp(fg$coef[1] + 1.96 * se[1]),
            P_Value     = 2 * (1 - pnorm(abs(fg$coef[1] / se[1]))),
            stringsAsFactors = FALSE
          )
          cat(sprintf("SHR=%.3f\n", exp(fg$coef[1])))
          # JSON export for federation (variance-covariance for meta-analysis)
          fg_export <- list(
            coef   = fg$coef[1],
            var    = fg$var[1, 1],
            se     = se[1],
            shr    = exp(fg$coef[1]),
            n      = fg$n,
            events = as.numeric(table(combined$outcome[valid]))
          )
          write_json(fg_export,
                     path = file.path(out_dir, sprintf("shr_sensitivity_%s.json", fg_key)),
                     digits = 8, auto_unbox = TRUE)
        }, error = function(e) {
          cat(sprintf("ERROR: %s\n", e$message))
        })
        flush.console()
      }
    }
  }

  shr_combined <- bind_rows(shr_results)
  if (nrow(shr_combined) > 0) {
    cat("\nSubdistribution Hazard Ratios (vs 1h Any Day):\n")
    print(shr_combined %>% select(Criteria, Cohort, Label, SHR, CI_Lower, CI_Upper, P_Value))
  }

  # --- SHR Forest plot (requires shr_combined) ---
  if (nrow(shr_combined) > 0) {
    forest_data <- shr_combined %>%
      mutate(
        plot_label = paste(Criteria, "-", Label),
        estimate_label = sprintf("%.2f (%.2f-%.2f)", SHR, CI_Lower, CI_Upper)
      )

    p3 <- ggplot(forest_data, aes(y = reorder(plot_label, -SHR), x = SHR)) +
      geom_vline(xintercept = 1, linetype = "dashed", color = "gray50") +
      geom_errorbarh(aes(xmin = CI_Lower, xmax = CI_Upper),
                     height = 0.25, linewidth = 1) +
      geom_point(aes(color = Cohort), size = 4) +
      geom_text(aes(label = estimate_label),
                x = max(forest_data$CI_Upper) + 0.05, hjust = 0, size = 3) +
      scale_color_manual(values = c(original = "#1976D2", imv24h = "#C62828")) +
      labs(title = "Subdistribution Hazard Ratios for Eligibility",
           subtitle = "Reference: 1h Any Day (within each cohort)",
           x = "SHR (95% CI)", y = "") +
      theme_minimal(base_size = 12) +
      theme(panel.grid.major.y = element_blank())

    ggsave(file.path(out_dir, "graphs", "stacked_shr_forest.pdf"),
           p3, width = 14, height = max(6, nrow(forest_data) * 0.6))
    ggsave(file.path(out_dir, "graphs", "stacked_shr_forest.png"),
           p3, width = 14, height = max(6, nrow(forest_data) * 0.6), dpi = 300)
    cat("  Created: stacked_shr_forest.pdf/png\n")
  }

  # ── Save SHR-dependent results ──
  cat("\nSaving SHR results...\n")

  if (nrow(shr_combined) > 0) {
    write.csv(shr_combined,
              file.path(out_dir, "stacked_sensitivity_shr.csv"),
              row.names = FALSE)
    cat("  Saved: stacked_sensitivity_shr.csv\n")
  }

  # Excel workbook with all sheets
  excel_list <- list(
    "Data Summary"        = data_summary,
    "Median Times"        = median_results,
    "SHR Results"         = shr_combined,
    "CIF Timepoints"      = if (nrow(cif_combined) > 0) {
      cif_combined %>%
        filter(time <= 72.5) %>%
        group_by(criteria_name, cohort, label) %>%
        summarise(
          CIF_12h = max(cif[time <= 12.5] * 100, na.rm = TRUE),
          SE_12h  = if (any(time <= 12.5)) tail(se[time <= 12.5], 1) * 100 else NA,
          CIF_24h = max(cif[time <= 24.5] * 100, na.rm = TRUE),
          SE_24h  = if (any(time <= 24.5)) tail(se[time <= 24.5], 1) * 100 else NA,
          CIF_48h = max(cif[time <= 48.5] * 100, na.rm = TRUE),
          SE_48h  = if (any(time <= 48.5)) tail(se[time <= 48.5], 1) * 100 else NA,
          CIF_72h = max(cif[time <= 72.5] * 100, na.rm = TRUE),
          SE_72h  = if (any(time <= 72.5)) tail(se[time <= 72.5], 1) * 100 else NA,
          .groups = "drop"
        )
    } else data.frame()
  )
  write_xlsx(excel_list,
             file.path(out_dir, "stacked_sensitivity_results.xlsx"))
  cat("  Saved: stacked_sensitivity_results.xlsx\n")

  # Federation bundle JSON — all site-level summaries in one file
  federation <- list(
    site           = site_name,
    medians        = median_results,
    shr            = if (nrow(shr_combined) > 0) shr_combined else data.frame(),
    cif_timepoints = if (!is.null(excel_list[["CIF Timepoints"]]) && nrow(excel_list[["CIF Timepoints"]]) > 0)
                       excel_list[["CIF Timepoints"]] else data.frame()
  )
  write_json(federation,
             path = file.path(out_dir, sprintf("federation_sensitivity_%s.json", site_name)),
             digits = 8, auto_unbox = FALSE)
  cat(sprintf("  Saved: federation_sensitivity_%s.json\n", site_name))

  # ── Final summary ──
  cat("\n", paste(rep("=", 70), collapse = ""), "\n")
  cat("STACKED SENSITIVITY SUMMARY\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  cat("A1: 1h Any Day, Business Hours (Primary)\n")
  cat("A2: 1h Weekday Only, Business Hours\n")
  cat("A3: 4h Continuous, Any Day, Business Hours\n")
  cat("A4: 4h Continuous, Weekday, Business Hours\n\n")

  time_summ <- median_results %>%
    group_by(Cohort, Label) %>%
    summarise(Mean_Pct = mean(Pct_Eligible, na.rm = TRUE),
              Mean_Median = mean(Median_Hours, na.rm = TRUE),
              .groups = "drop")
  cat("Mean eligibility and median time by definition:\n")
  print(time_summ)

  if (nrow(shr_combined) > 0) {
    shr_summ <- shr_combined %>%
      group_by(Cohort, Label) %>%
      summarise(Mean_SHR = mean(SHR), Range = sprintf("%.2f-%.2f", min(SHR), max(SHR)),
                .groups = "drop")
    cat("\nSHR summary by definition:\n")
    print(shr_summ)
  }
}

cat(sprintf("\nAll done at %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
cat("Outputs in", normalizePath(out_dir), "\n")
