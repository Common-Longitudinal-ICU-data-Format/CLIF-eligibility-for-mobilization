#!/usr/bin/env Rscript
# ──────────────────────────────────────────────────────────────────────────────
#  Sensitivity Analysis — Improved Forest Plots
#  Reads: stacked_sensitivity_shr.csv, stacked_sensitivity_medians.csv
#  Produces: 3 publication-ready figures
# ──────────────────────────────────────────────────────────────────────────────

library(ggplot2)
library(dplyr)
library(tidyr)
library(patchwork)

# ── Paths ──
# Detect project root: look for config/ directory walking up from getwd()
find_project_root <- function() {
  d <- getwd()
  for (i in 1:5) {
    if (dir.exists(file.path(d, "config"))) return(d)
    d <- dirname(d)
  }
  stop("Could not find project root (directory containing config/)")
}
base_dir <- find_project_root()
out_dir  <- file.path(base_dir, "output", "final")
fig_dir  <- file.path(out_dir, "graphs")

shr_df    <- read.csv(file.path(out_dir, "stacked_sensitivity_shr.csv"),
                       stringsAsFactors = FALSE)
medians   <- read.csv(file.path(out_dir, "stacked_sensitivity_medians.csv"),
                       stringsAsFactors = FALSE)

# ── Criteria ordering (most lenient → most restrictive) ──
crit_order  <- c("Chicago Criteria", "Consensus (Yellow)",
                  "TEAM Criteria", "Consensus (Green)")
crit_colors <- c("Chicago Criteria"   = "#B03A2E",
                  "Consensus (Yellow)" = "#D4AC0D",
                  "TEAM Criteria"      = "#2471A3",
                  "Consensus (Green)"  = "#1E8449")

# ── Structural restriction ordering (least → most restrictive) ──
restriction_order <- c("1h Any Day (Primary)", "1h Weekday Only",
                        "4h Continuous Any Day", "4h Continuous Weekday")
restriction_order_imv <- c("IMV>=24h: 1h Any Day", "IMV>=24h: 1h Weekday",
                            "IMV>=24h: 4h Weekday")

# ── Clean up ──
shr_df <- shr_df %>%
  mutate(
    Criteria  = factor(Criteria, levels = crit_order),
    SHR_label = sprintf("%.2f (%.2f-%.2f)", SHR, CI_Lower, CI_Upper)
  )

medians <- medians %>%
  mutate(Criteria = factor(Criteria, levels = crit_order))


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 1: SHR Forest Plot — Faceted by Criteria
#   Each panel = one criteria set. Rows = structural restrictions.
#   This makes the within-criteria comparison clear.
# ══════════════════════════════════════════════════════════════════════════════

# Assign clean restriction labels — order matters: match specific before general
shr_df <- shr_df %>%
  mutate(Restriction = case_when(
    grepl("1h Weekday", Label)                                   ~ "1h Weekday",
    grepl("4h Continuous Any Day|4h Any Day", Label)             ~ "4h Any Day",
    grepl("4h Continuous Weekday|4h Weekday", Label)             ~ "4h Weekday",
    TRUE                                                         ~ Label
  ))

restriction_clean <- c("1h Weekday", "4h Any Day", "4h Weekday")

p1_orig <- shr_df %>%
  filter(Cohort == "original") %>%
  mutate(Restriction = factor(Restriction, levels = rev(restriction_clean))) %>%
  ggplot(aes(x = SHR, y = Restriction, color = Criteria)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "gray60") +
  geom_pointrange(aes(xmin = CI_Lower, xmax = CI_Upper), size = 0.6) +
  geom_text(aes(label = SHR_label), hjust = -0.15, size = 2.8, show.legend = FALSE) +
  facet_wrap(~Criteria, ncol = 2, scales = "free_x") +
  scale_color_manual(values = crit_colors, guide = "none") +
  scale_x_continuous(limits = c(0.3, 1.15), breaks = seq(0.3, 1.0, 0.1)) +
  labs(
    title    = "Subdistribution Hazard Ratios by Eligibility Criteria",
    subtitle = "Reference: 1h Any Day within each criteria | Original cohort (IMV ≥4h)",
    x = "SHR (95% CI)",
    y = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    strip.text       = element_text(face = "bold", size = 11),
    panel.grid.minor = element_blank(),
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(color = "gray40", size = 10),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(file.path(fig_dir, "shr_forest_faceted_original.png"),
       p1_orig, width = 11, height = 5, dpi = 300)
cat("Saved: shr_forest_faceted_original.png\n")


# Same for IMV ≥24h
p1_imv <- shr_df %>%
  filter(Cohort == "imv24h") %>%
  mutate(Restriction = factor(Restriction, levels = rev(restriction_clean))) %>%
  ggplot(aes(x = SHR, y = Restriction, color = Criteria)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "gray60") +
  geom_pointrange(aes(xmin = CI_Lower, xmax = CI_Upper), size = 0.6) +
  geom_text(aes(label = SHR_label), hjust = -0.15, size = 2.8, show.legend = FALSE) +
  facet_wrap(~Criteria, ncol = 2, scales = "free_x") +
  scale_color_manual(values = crit_colors, guide = "none") +
  scale_x_continuous(limits = c(0.3, 1.15), breaks = seq(0.3, 1.0, 0.1)) +
  labs(
    title    = "Subdistribution Hazard Ratios by Eligibility Criteria",
    subtitle = "Reference: 1h Any Day within each criteria | IMV ≥24h subcohort",
    x = "SHR (95% CI)",
    y = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    strip.text       = element_text(face = "bold", size = 11),
    panel.grid.minor = element_blank(),
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(color = "gray40", size = 10),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(file.path(fig_dir, "shr_forest_faceted_imv24h.png"),
       p1_imv, width = 11, height = 4.5, dpi = 300)
cat("Saved: shr_forest_faceted_imv24h.png\n")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 2: Eligibility Cascade — Stacked Bar / Dot Plot
#   Shows ABSOLUTE eligibility % for each criteria × restriction combo.
#   This is the "missing context" from the SHR forest plot.
# ══════════════════════════════════════════════════════════════════════════════

p2_orig <- medians %>%
  filter(Cohort == "original") %>%
  mutate(
    Label = factor(Label, levels = restriction_order),
    Pct_label = paste0(round(Pct_Eligible, 1), "%")
  ) %>%
  ggplot(aes(x = Pct_Eligible, y = Label, fill = Criteria)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.6, alpha = 0.85) +
  geom_text(aes(label = Pct_label),
            position = position_dodge(width = 0.7),
            hjust = -0.1, size = 2.8) +
  facet_wrap(~Criteria, ncol = 1) +
  scale_fill_manual(values = crit_colors, guide = "none") +
  scale_x_continuous(limits = c(0, 105), breaks = seq(0, 100, 20)) +
  labs(
    title    = "Eligibility Cascade: Absolute % Eligible Under Each Definition",
    subtitle = "Original cohort (IMV ≥4h) | N = 17,309 encounter blocks",
    x = "% Encounter Blocks Ever Eligible",
    y = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    strip.text       = element_text(face = "bold", size = 10),
    panel.grid.minor = element_blank(),
    panel.grid.major.y = element_blank(),
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(color = "gray40", size = 10),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(file.path(fig_dir, "eligibility_cascade_original.png"),
       p2_orig, width = 10, height = 8, dpi = 300)
cat("Saved: eligibility_cascade_original.png\n")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 3: Combined — SHR + Absolute Eligibility side by side
#   Left panel: SHR forest (relative). Right panel: eligibility % (absolute).
#   Grouped by restriction, colored by criteria. Both cohorts.
# ══════════════════════════════════════════════════════════════════════════════

# Merge SHR with medians to get pct_eligible on the same rows
shr_with_pct <- shr_df %>%
  left_join(
    medians %>% select(Criteria, Cohort, Sensitivity, Pct_Eligible, Median_Hours),
    by = c("Criteria", "Cohort", "Comparison" = "Sensitivity")
  )

# Add the reference rows (SHR = 1 by definition)
ref_rows <- medians %>%
  filter(grepl("1h_anyday", Sensitivity)) %>%
  mutate(
    Restriction = "1h Any Day",
    SHR = 1.0, CI_Lower = 1.0, CI_Upper = 1.0,
    SHR_label = "1.00 (ref)"
  )

combined_orig <- bind_rows(
  shr_with_pct %>% filter(Cohort == "original"),
  ref_rows %>% filter(Cohort == "original") %>%
    select(Criteria, Cohort, Restriction, SHR, CI_Lower, CI_Upper,
           SHR_label, Pct_Eligible, Median_Hours)
) %>%
  mutate(
    Restriction = factor(Restriction, levels = rev(c("1h Any Day",
                                                      restriction_clean))),
    Criteria = factor(Criteria, levels = crit_order)
  )

# Left panel: SHR
p_left <- combined_orig %>%
  ggplot(aes(x = SHR, y = Restriction, color = Criteria)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "gray60") +
  geom_pointrange(aes(xmin = CI_Lower, xmax = CI_Upper),
                  position = position_dodge(width = 0.5), size = 0.5) +
  scale_color_manual(values = crit_colors) +
  scale_x_continuous(limits = c(0.35, 1.1), breaks = seq(0.4, 1.0, 0.1)) +
  labs(x = "SHR (95% CI)", y = NULL, title = "Relative: SHR vs 1h Any Day") +
  theme_minimal(base_size = 10) +
  theme(
    legend.position  = "bottom",
    legend.title     = element_blank(),
    panel.grid.minor = element_blank(),
    plot.title       = element_text(face = "bold", size = 11, hjust = 0.5),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

# Right panel: Absolute eligibility %
p_right <- combined_orig %>%
  ggplot(aes(x = Pct_Eligible, y = Restriction, color = Criteria)) +
  geom_point(position = position_dodge(width = 0.5), size = 3) +
  geom_text(aes(label = paste0(round(Pct_Eligible, 0), "%")),
            position = position_dodge(width = 0.5),
            hjust = -0.3, size = 2.7, show.legend = FALSE) +
  scale_color_manual(values = crit_colors) +
  scale_x_continuous(limits = c(70, 102), breaks = seq(70, 100, 10)) +
  labs(x = "% Encounter Blocks Eligible", y = NULL,
       title = "Absolute: Eligibility Rate") +
  theme_minimal(base_size = 10) +
  theme(
    legend.position  = "none",
    panel.grid.minor = element_blank(),
    plot.title       = element_text(face = "bold", size = 11, hjust = 0.5),
    axis.text.y      = element_blank(),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

p3_combined <- p_left + p_right +
  plot_layout(widths = c(1.3, 1)) +
  plot_annotation(
    title    = "Impact of Structural Restrictions on Mobilization Eligibility",
    subtitle = "Original cohort (IMV ≥4h) | N = 17,309 encounter blocks",
    theme = theme(
      plot.title    = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(color = "gray40", size = 10),
      plot.background = element_rect(fill = "white", color = NA)
    )
  )

ggsave(file.path(fig_dir, "shr_combined_forest.png"),
       p3_combined, width = 13, height = 5, dpi = 300)
cat("Saved: shr_combined_forest.png\n")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 4: Median Time Heatmap
#   Rows = criteria, columns = structural restriction. Cell = median hours.
#   Color intensity shows how restrictive.
# ══════════════════════════════════════════════════════════════════════════════

heat_orig <- medians %>%
  filter(Cohort == "original") %>%
  mutate(
    Criteria = factor(Criteria, levels = rev(crit_order)),
    Label    = factor(Label, levels = restriction_order),
    cell_text = paste0(Median_Hours, "h\n(", Q1_Hours, "-", Q3_Hours, ")")
  )

p4 <- heat_orig %>%
  ggplot(aes(x = Label, y = Criteria, fill = Median_Hours)) +
  geom_tile(color = "white", linewidth = 1.5) +
  geom_text(aes(label = cell_text), size = 3.5, lineheight = 0.85) +
  scale_fill_gradient(low = "#E8F5E9", high = "#B71C1C",
                       name = "Median hours\nto eligibility") +
  labs(
    title    = "Median Time to First Eligibility (hours from intubation)",
    subtitle = "Original cohort | cell = median (Q1-Q3)",
    x = "Structural Restriction", y = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    axis.text.x      = element_text(angle = 20, hjust = 1),
    panel.grid       = element_blank(),
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(color = "gray40", size = 10),
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

ggsave(file.path(fig_dir, "sensitivity_median_heatmap.png"),
       p4, width = 10, height = 4.5, dpi = 300)
cat("Saved: sensitivity_median_heatmap.png\n")

cat("\nAll figures generated successfully.\n")
