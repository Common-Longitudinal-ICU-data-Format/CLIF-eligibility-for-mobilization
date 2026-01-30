# 06_comprehensive_sensitivity_analysis.R
# Comprehensive Sensitivity Analysis: Three-Way Comparison
# Definition 1: Original (any single hour eligibility) - REFERENCE
# Definition 2: IMV ≥24h + 4 consecutive hours POST-24h
# Definition 3: Definition 2 + weekend exclusion

# Load required libraries
library(tidyverse)
library(survival)
library(cmprsk)
library(ggplot2)
library(gridExtra)
library(arrow)
library(writexl)
library(forestplot)
library(survminer)

# Set working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Print start time
cat("Comprehensive Sensitivity Analysis started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# ============================================================================
# 1. LOAD ALL DATASETS FOR THREE-WAY COMPARISON
# ============================================================================

cat(paste(rep("=", 80), collapse=""), "\n")
cat("LOADING DATASETS FOR THREE-WAY COMPARISON\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

# Define criteria names
criteria_names <- list(
  patel = "Patel Criteria",
  team = "TEAM Criteria",
  all_green = "Consensus Green",
  any_yellow_or_green_no_red = "Consensus Yellow"
)

# Load all datasets
all_data <- list()
data_summary <- data.frame()

# Definition 1: Original analysis (any eligibility)
cat("Loading Definition 1 (Original - Reference):\n")
for (criteria in names(criteria_names)) {
  # Map file names (original uses different naming)
  file_criteria <- ifelse(criteria == "all_green", "green",
                          ifelse(criteria == "any_yellow_or_green_no_red", "yellow", criteria))

  file_path <- sprintf("../output/intermediate/competing_risk_%s_final.parquet", file_criteria)

  if (file.exists(file_path)) {
    df <- read_parquet(file_path)

    # Filter to 24h cohort for fair comparison
    # First get 24h patients from consecutive analysis
    consec_file <- sprintf("../output/intermediate/competing_risk_%s_consecutive.parquet", criteria)
    if (file.exists(consec_file)) {
      df_24h_cohort <- read_parquet(consec_file)
      patients_24h <- unique(df_24h_cohort$encounter_block)
      df <- df[df$encounter_block %in% patients_24h, ]
    }

    df$definition <- "Original"
    df$definition_num <- 1
    df$criteria_name <- criteria_names[[criteria]]
    df$criteria <- criteria

    key <- paste("def1", criteria, sep="_")
    all_data[[key]] <- df

    data_summary <- rbind(data_summary, data.frame(
      Definition = "Original",
      Criteria = criteria_names[[criteria]],
      N = nrow(df),
      N_Eligible = sum(df$outcome == 1),
      Pct_Eligible = mean(df$outcome == 1) * 100
    ))

    cat(sprintf("  %s: %d patients, %.1f%% eligible\n",
                criteria_names[[criteria]], nrow(df), mean(df$outcome == 1) * 100))
  }
}

# Definition 2: Post-24h + 4 consecutive hours
cat("\nLoading Definition 2 (Post-24h + Consecutive):\n")
for (criteria in names(criteria_names)) {
  file_path <- sprintf("../output/intermediate/competing_risk_%s_consecutive.parquet", criteria)

  if (file.exists(file_path)) {
    df <- read_parquet(file_path)

    # For true post-24h analysis, adjust eligibility times
    # Add 24 hours since we're looking POST-24h
    df$time_eligibility_post24 <- ifelse(!is.na(df$time_eligibility) & df$time_eligibility >= 24,
                                          df$time_eligibility, NA)

    df$definition <- "Post24h_Consecutive"
    df$definition_num <- 2
    df$criteria_name <- criteria_names[[criteria]]
    df$criteria <- criteria

    key <- paste("def2", criteria, sep="_")
    all_data[[key]] <- df

    data_summary <- rbind(data_summary, data.frame(
      Definition = "Post24h_Consecutive",
      Criteria = criteria_names[[criteria]],
      N = nrow(df),
      N_Eligible = sum(!is.na(df$time_eligibility_post24)),
      Pct_Eligible = mean(!is.na(df$time_eligibility_post24)) * 100
    ))

    cat(sprintf("  %s: %d patients, %.1f%% eligible post-24h\n",
                criteria_names[[criteria]], nrow(df), mean(!is.na(df$time_eligibility_post24)) * 100))
  }
}

# Definition 3: Post-24h + consecutive + weekend exclusion
cat("\nLoading Definition 3 (Post-24h + Consecutive + Weekend Exclusion):\n")
for (criteria in names(criteria_names)) {
  file_path <- sprintf("../output/intermediate/competing_risk_%s_weekday_consecutive.parquet", criteria)

  if (file.exists(file_path)) {
    df <- read_parquet(file_path)

    # For true post-24h analysis
    df$time_eligibility_post24 <- ifelse(!is.na(df$time_eligibility) & df$time_eligibility >= 24,
                                          df$time_eligibility, NA)

    df$definition <- "Post24h_Consecutive_Weekend"
    df$definition_num <- 3
    df$criteria_name <- criteria_names[[criteria]]
    df$criteria <- criteria

    key <- paste("def3", criteria, sep="_")
    all_data[[key]] <- df

    data_summary <- rbind(data_summary, data.frame(
      Definition = "Post24h_Consecutive_Weekend",
      Criteria = criteria_names[[criteria]],
      N = nrow(df),
      N_Eligible = sum(!is.na(df$time_eligibility_post24)),
      Pct_Eligible = mean(!is.na(df$time_eligibility_post24)) * 100
    ))

    cat(sprintf("  %s: %d patients, %.1f%% eligible post-24h (weekday only)\n",
                criteria_names[[criteria]], nrow(df), mean(!is.na(df$time_eligibility_post24)) * 100))
  }
}

cat("\nData loading complete. Total datasets loaded:", length(all_data), "\n")

# ============================================================================
# 2. CALCULATE CUMULATIVE INCIDENCE FUNCTIONS
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("CALCULATING CUMULATIVE INCIDENCE FUNCTIONS\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

# Function to calculate CIF for each definition
calculate_cif_three_way <- function(data_list, criteria_name) {
  cif_results <- list()

  for (def_num in 1:3) {
    def_key <- paste0("def", def_num, "_", criteria_name)

    if (def_key %in% names(data_list)) {
      df <- data_list[[def_key]]

      # Use appropriate time variable
      if (def_num == 1) {
        # Original: use standard time
        time_var <- df$t_event
      } else {
        # Post-24h definitions: adjust for post-24h eligibility
        time_var <- df$t_event
      }

      status_var <- df$outcome

      # Remove NA and invalid values
      valid_idx <- !is.na(time_var) & !is.na(status_var) & time_var > 0
      time_var <- time_var[valid_idx]
      status_var <- status_var[valid_idx]

      if (length(time_var) > 0) {
        # Calculate CIF
        cif_result <- cuminc(time_var, status_var, cencode = 0)

        # Extract CIF for eligibility (outcome = 1)
        if ("1 1" %in% names(cif_result)) {
          cif_eligible <- cif_result$`1 1`

          cif_df <- data.frame(
            time = cif_eligible$time,
            cif = cif_eligible$est,
            se = sqrt(cif_eligible$var),
            definition = unique(df$definition),
            definition_num = def_num,
            criteria = criteria_name,
            criteria_name = unique(df$criteria_name)
          )

          # Calculate 95% CI
          cif_df$ci_lower <- pmax(0, cif_df$cif - 1.96 * cif_df$se)
          cif_df$ci_upper <- pmin(1, cif_df$cif + 1.96 * cif_df$se)

          cif_results[[def_key]] <- cif_df
        }
      }
    }
  }

  return(bind_rows(cif_results))
}

# Calculate CIF for all criteria
all_cif_data <- list()

for (criteria in names(criteria_names)) {
  cat(sprintf("Calculating CIF for %s...\n", criteria_names[[criteria]]))
  cif_data <- calculate_cif_three_way(all_data, criteria)
  all_cif_data[[criteria]] <- cif_data
}

# Combine all CIF data
cif_combined <- bind_rows(all_cif_data)

# ============================================================================
# 3. CALCULATE SUBDISTRIBUTION HAZARD RATIOS
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("CALCULATING SUBDISTRIBUTION HAZARD RATIOS\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

# Function to calculate SHR using Fine-Gray model
calculate_shr <- function(data_list, criteria_name) {
  # Combine all definitions for this criteria
  combined_data <- bind_rows(data_list)

  # Prepare for Fine-Gray model
  combined_data$definition_factor <- factor(combined_data$definition,
                                           levels = c("Original",
                                                     "Post24h_Consecutive",
                                                     "Post24h_Consecutive_Weekend"))

  # Create dummy variables (Original as reference)
  combined_data$def2 <- ifelse(combined_data$definition == "Post24h_Consecutive", 1, 0)
  combined_data$def3 <- ifelse(combined_data$definition == "Post24h_Consecutive_Weekend", 1, 0)

  # Prepare time and status
  time_var <- combined_data$t_event
  status_var <- combined_data$outcome

  # Remove NA values
  valid_idx <- !is.na(time_var) & !is.na(status_var) & time_var > 0

  if (sum(valid_idx) < 10) {
    return(NULL)
  }

  time_var <- time_var[valid_idx]
  status_var <- status_var[valid_idx]
  def2 <- combined_data$def2[valid_idx]
  def3 <- combined_data$def3[valid_idx]

  # Create failure status for Fine-Gray
  fstatus <- ifelse(status_var == 1, 1, ifelse(status_var > 1, 2, 0))

  # Fit Fine-Gray model with covariates
  tryCatch({
    fg_model <- crr(time_var, fstatus,
                    cov1 = cbind(def2, def3),
                    failcode = 1,
                    cencode = 0)

    # Extract SHRs and CIs
    coef_def2 <- fg_model$coef[1]
    coef_def3 <- fg_model$coef[2]

    se_def2 <- sqrt(fg_model$var[1, 1])
    se_def3 <- sqrt(fg_model$var[2, 2])

    shr_def2 <- exp(coef_def2)
    shr_def3 <- exp(coef_def3)

    ci_lower_def2 <- exp(coef_def2 - 1.96 * se_def2)
    ci_upper_def2 <- exp(coef_def2 + 1.96 * se_def2)

    ci_lower_def3 <- exp(coef_def3 - 1.96 * se_def3)
    ci_upper_def3 <- exp(coef_def3 + 1.96 * se_def3)

    p_value_def2 <- 2 * (1 - pnorm(abs(coef_def2 / se_def2)))
    p_value_def3 <- 2 * (1 - pnorm(abs(coef_def3 / se_def3)))

    return(data.frame(
      Criteria = criteria_names[[criteria_name]],
      Definition = c("Post24h_Consecutive", "Post24h_Consecutive_Weekend"),
      SHR = c(shr_def2, shr_def3),
      CI_Lower = c(ci_lower_def2, ci_lower_def3),
      CI_Upper = c(ci_upper_def2, ci_upper_def3),
      P_Value = c(p_value_def2, p_value_def3)
    ))

  }, error = function(e) {
    cat(sprintf("  Error calculating SHR for %s: %s\n", criteria_name, e$message))
    return(NULL)
  })
}

# Calculate SHRs for all criteria
shr_results <- list()

for (criteria in names(criteria_names)) {
  cat(sprintf("Calculating SHR for %s...\n", criteria_names[[criteria]]))

  # Get data for all three definitions
  data_for_criteria <- list()
  for (def_num in 1:3) {
    key <- paste0("def", def_num, "_", criteria)
    if (key %in% names(all_data)) {
      data_for_criteria[[key]] <- all_data[[key]]
    }
  }

  if (length(data_for_criteria) == 3) {
    shr_result <- calculate_shr(data_for_criteria, criteria)
    if (!is.null(shr_result)) {
      shr_results[[criteria]] <- shr_result
    }
  }
}

# Combine SHR results
shr_combined <- bind_rows(shr_results)

# Add reference group
reference_rows <- data.frame(
  Criteria = unique(shr_combined$Criteria),
  Definition = "Original (Reference)",
  SHR = 1.00,
  CI_Lower = 1.00,
  CI_Upper = 1.00,
  P_Value = NA
)

shr_final <- bind_rows(reference_rows, shr_combined) %>%
  arrange(Criteria, Definition)

cat("\nSubdistribution Hazard Ratios (Reference: Original Definition):\n")
print(shr_final)

# ============================================================================
# 4. CALCULATE MEDIAN TIMES AND KEY STATISTICS
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("CALCULATING MEDIAN TIMES AND KEY STATISTICS\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

median_results <- data.frame()

for (key in names(all_data)) {
  df <- all_data[[key]]

  # Get eligible patients only
  if (grepl("def1", key)) {
    eligible_df <- df[df$outcome == 1, ]
    time_col <- "time_eligibility"
  } else {
    # For post-24h definitions
    eligible_df <- df[!is.na(df$time_eligibility_post24), ]
    time_col <- "time_eligibility_post24"
  }

  if (nrow(eligible_df) > 0 && time_col %in% names(eligible_df)) {
    times <- eligible_df[[time_col]]
    times <- times[!is.na(times)]

    if (length(times) > 0) {
      median_time <- median(times)
      q1_time <- quantile(times, 0.25)
      q3_time <- quantile(times, 0.75)

      # Calculate cumulative incidence at key timepoints
      ci_24h <- mean(times <= 24) * 100
      ci_48h <- mean(times <= 48) * 100
      ci_72h <- mean(times <= 72) * 100

      result_row <- data.frame(
        Definition = unique(df$definition),
        Criteria = unique(df$criteria_name),
        N_Total = nrow(df),
        N_Eligible = length(times),
        Pct_Eligible = (length(times) / nrow(df)) * 100,
        Median_Hours = median_time,
        Q1_Hours = q1_time,
        Q3_Hours = q3_time,
        CI_24h = ci_24h,
        CI_48h = ci_48h,
        CI_72h = ci_72h
      )

      median_results <- rbind(median_results, result_row)
    }
  }
}

# Sort results
median_results <- median_results %>%
  arrange(Criteria, Definition)

cat("\nMedian Time to Eligibility by Definition:\n")
print(median_results %>% select(Definition, Criteria, Median_Hours, Q1_Hours, Q3_Hours))

# ============================================================================
# 5. CREATE VISUALIZATIONS
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("CREATING VISUALIZATIONS\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

# Color scheme for definitions
def_colors <- c(
  "Original" = "#2E7D32",
  "Post24h_Consecutive" = "#1976D2",
  "Post24h_Consecutive_Weekend" = "#C62828"
)

# 1. Three-way CIF comparison plot
if (nrow(cif_combined) > 0) {
  pdf("../output/final/three_way_cif_comparison.pdf", width = 14, height = 10)

  # Filter to 72 hours for clarity
  cif_72h <- cif_combined %>% filter(time <= 72)

  p1 <- ggplot(cif_72h, aes(x = time, y = cif * 100, color = definition)) +
    geom_line(size = 1.2) +
    geom_ribbon(aes(ymin = ci_lower * 100, ymax = ci_upper * 100, fill = definition),
                alpha = 0.2, linetype = 0) +
    facet_wrap(~ criteria_name, nrow = 2, ncol = 2) +
    scale_color_manual(values = def_colors, name = "Definition") +
    scale_fill_manual(values = def_colors, name = "Definition") +
    scale_x_continuous(breaks = seq(0, 72, 12), limits = c(0, 72)) +
    scale_y_continuous(breaks = seq(0, 100, 20)) +
    labs(
      title = "Three-Way Sensitivity Analysis: Cumulative Incidence of Mobilization Eligibility",
      subtitle = "Comparing Original vs Post-24h Consecutive vs Post-24h Consecutive with Weekend Exclusion",
      x = "Hours from Ventilation Start",
      y = "Cumulative Incidence (%)"
    ) +
    theme_minimal() +
    theme(
      legend.position = "bottom",
      strip.text = element_text(size = 11, face = "bold"),
      strip.background = element_rect(fill = "gray95", color = NA),
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11),
      panel.grid.minor = element_blank(),
      panel.border = element_rect(color = "gray80", fill = NA)
    ) +
    geom_vline(xintercept = c(24, 48, 72), linetype = "dotted", alpha = 0.3)

  print(p1)
  dev.off()

  cat("Created: three_way_cif_comparison.pdf\n")
}

# 2. Forest plot for SHRs
if (nrow(shr_final) > 0) {
  pdf("../output/final/shr_forest_plot.pdf", width = 10, height = 8)

  # Prepare data for forest plot
  forest_data <- shr_final %>%
    filter(Definition != "Original (Reference)") %>%
    mutate(
      label = paste(Criteria, "-", Definition),
      estimate_label = sprintf("%.2f (%.2f-%.2f)", SHR, CI_Lower, CI_Upper),
      p_label = ifelse(is.na(P_Value), "",
                       ifelse(P_Value < 0.001, "<0.001", sprintf("%.3f", P_Value)))
    )

  p2 <- ggplot(forest_data, aes(y = reorder(label, -SHR), x = SHR)) +
    geom_point(size = 3) +
    geom_errorbarh(aes(xmin = CI_Lower, xmax = CI_Upper), height = 0.2) +
    geom_vline(xintercept = 1, linetype = "dashed", color = "gray50") +
    scale_x_continuous(limits = c(0.5, 1.5), breaks = seq(0.5, 1.5, 0.25)) +
    labs(
      title = "Subdistribution Hazard Ratios for Mobilization Eligibility",
      subtitle = "Reference: Original Definition (Any Single Hour of Eligibility)",
      x = "Subdistribution Hazard Ratio (95% CI)",
      y = ""
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11),
      panel.grid.major.y = element_blank()
    )

  # Add text annotations
  p2 <- p2 +
    geom_text(aes(x = 1.45, label = estimate_label), hjust = 1, size = 3) +
    geom_text(aes(x = 0.55, label = p_label), hjust = 0, size = 3)

  print(p2)
  dev.off()

  cat("Created: shr_forest_plot.pdf\n")
}

# 3. Median time comparison plot
if (nrow(median_results) > 0) {
  pdf("../output/final/median_time_comparison.pdf", width = 12, height = 8)

  p3 <- ggplot(median_results, aes(x = Definition, y = Median_Hours, fill = Definition)) +
    geom_bar(stat = "identity", position = "dodge") +
    geom_errorbar(aes(ymin = Q1_Hours, ymax = Q3_Hours), width = 0.2, position = position_dodge(0.9)) +
    facet_wrap(~ Criteria, scales = "free_x") +
    scale_fill_manual(values = def_colors) +
    labs(
      title = "Median Time to Eligibility by Definition",
      subtitle = "Error bars show IQR (25th-75th percentile)",
      x = "",
      y = "Median Time to Eligibility (hours)"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "none",
      strip.text = element_text(size = 11, face = "bold"),
      strip.background = element_rect(fill = "gray95", color = NA)
    )

  print(p3)
  dev.off()

  cat("Created: median_time_comparison.pdf\n")
}

# ============================================================================
# 6. SAVE RESULTS TO EXCEL
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("SAVING RESULTS TO EXCEL\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

# Prepare all results for Excel export
excel_list <- list(
  "Summary Statistics" = median_results,
  "SHR Results" = shr_final,
  "Data Summary" = data_summary
)

# Add CIF at key timepoints
if (nrow(cif_combined) > 0) {
  cif_timepoints <- cif_combined %>%
    filter(time %in% c(24, 48, 72) | (time >= 23.5 & time <= 24.5) |
           (time >= 47.5 & time <= 48.5) | (time >= 71.5 & time <= 72.5)) %>%
    group_by(criteria_name, definition) %>%
    summarise(
      CI_24h = max(cif[time <= 24.5] * 100, na.rm = TRUE),
      CI_48h = max(cif[time <= 48.5] * 100, na.rm = TRUE),
      CI_72h = max(cif[time <= 72.5] * 100, na.rm = TRUE),
      .groups = 'drop'
    )

  excel_list[["CIF Timepoints"]] <- cif_timepoints
}

# Write to Excel
write_xlsx(excel_list, "../output/final/comprehensive_sensitivity_results.xlsx")
cat("Saved: comprehensive_sensitivity_results.xlsx\n")

# Also save individual CSV files
write.csv(median_results, "../output/final/median_times_three_definitions.csv", row.names = FALSE)
write.csv(shr_final, "../output/final/shr_results_three_definitions.csv", row.names = FALSE)
cat("Saved: Individual CSV files\n")

# ============================================================================
# 7. FINAL SUMMARY REPORT
# ============================================================================

cat("\n", paste(rep("=", 80), collapse=""), "\n")
cat("COMPREHENSIVE SENSITIVITY ANALYSIS SUMMARY\n")
cat(paste(rep("=", 80), collapse=""), "\n\n")

cat("Three-Way Comparison Complete:\n")
cat("------------------------------\n")
cat("Definition 1 (Reference): Original - Any single hour of eligibility\n")
cat("Definition 2: Post-24h assessment + 4 consecutive hours required\n")
cat("Definition 3: Definition 2 + Weekend exclusion\n\n")

cat("Key Findings:\n")
cat("-------------\n")

# Report median SHRs
if (nrow(shr_final) > 0) {
  def2_shr <- shr_final %>%
    filter(Definition == "Post24h_Consecutive") %>%
    summarise(mean_shr = mean(SHR, na.rm = TRUE))

  def3_shr <- shr_final %>%
    filter(Definition == "Post24h_Consecutive_Weekend") %>%
    summarise(mean_shr = mean(SHR, na.rm = TRUE))

  cat(sprintf("1. Average SHR for Post-24h Consecutive: %.2f\n", def2_shr$mean_shr))
  cat(sprintf("2. Average SHR for Post-24h + Weekend: %.2f\n", def3_shr$mean_shr))
}

# Report median time delays
if (nrow(median_results) > 0) {
  original_median <- median_results %>%
    filter(Definition == "Original") %>%
    summarise(mean_median = mean(Median_Hours, na.rm = TRUE))

  post24_median <- median_results %>%
    filter(Definition == "Post24h_Consecutive") %>%
    summarise(mean_median = mean(Median_Hours, na.rm = TRUE))

  cat(sprintf("3. Average delay from consecutive requirement: %.1f hours\n",
              post24_median$mean_median - original_median$mean_median))
}

cat("\nAll results saved to ../output/final/\n")
cat(sprintf("\nAnalysis completed at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))