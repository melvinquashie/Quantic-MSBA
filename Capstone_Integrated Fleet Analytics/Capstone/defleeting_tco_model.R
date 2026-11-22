# ==============================================================================
# Fleet Management Capstone - Week 8: TCO & Optimal De-Fleeting Optimizer
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Environment Reset & Dependencies
# ------------------------------------------------------------------------------
rm(list = ls())
gc()

required_pkgs <- c("tidyverse", "scales", "broom")
new_pkgs <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if (length(new_pkgs) > 0) install.packages(new_pkgs)

library(tidyverse)
library(scales)
library(broom)

set.seed(42)

# ------------------------------------------------------------------------------
# 2. Ingest Data & Hygiene Filtering
# ------------------------------------------------------------------------------
df_raw <- read_csv("defleeting_tco_summary.csv")

# Filter out zero-mileage records and calculate fundamental cost per km metrics
df_tco <- df_raw %>%
  filter(
    purchase_cost > 5000,
    lifetime_odometer_km >= 5000,
    service_life_years > 0.25
  ) %>%
  mutate(
    # Total Cost of Ownership Incurred
    total_tco_incurred = net_depreciation_incurred + cumulative_maint_spend,
    
    # Unitized Cost Rates per Kilometer (USD / km)
    depr_cost_per_km   = net_depreciation_incurred / lifetime_odometer_km,
    maint_cost_per_km  = cumulative_maint_spend / lifetime_odometer_km,
    tco_per_km         = total_tco_incurred / lifetime_odometer_km,
    
    # Unitized Cost Rates per Year (USD / Year)
    maint_cost_per_year = cumulative_maint_spend / service_life_years,
    depr_cost_per_year  = net_depreciation_incurred / service_life_years,
    tco_per_year        = total_tco_incurred / service_life_years
  )

cat("\n--- Cleaned Asset Portfolio for Lifecycle Analysis (N =", nrow(df_tco), ") ---\n")
print(table(df_tco$current_status))

# ------------------------------------------------------------------------------
# 3. Parametric Lifecycle Modeling (Fitting TCO Curves by Mileage)
# ------------------------------------------------------------------------------
# Fit Non-linear Power/Log Curves to estimate cumulative maintenance as a function of km:
# Maint(km) = a * (km)^b
maint_model <- nls(
  cumulative_maint_spend ~ a * (lifetime_odometer_km)^b,
  data = df_tco,
  start = list(a = 0.05, b = 1.05)
)

params <- coef(maint_model)
a_param <- params["a"]
b_param <- params["b"]

cat("\n================ MAINTENANCE ACCELERATION FUNCTION ================\n")
cat("Cumulative Maint(km) =", round(a_param, 6), "* (km) ^", round(b_param, 4), "\n")

# Mean fleet-wide purchase cost and residual floor (20% salvage)
mean_purchase <- mean(df_tco$purchase_cost, na.rm = TRUE)
residual_pct  <- 0.20
capital_depreciable <- mean_purchase * (1 - residual_pct)

# ------------------------------------------------------------------------------
# 4. Numerical Optimization: Finding the Minimum TCO / km
# ------------------------------------------------------------------------------
# Define continuous mileage grid from 10,000 km to 250,000 km
km_grid <- tibble(km = seq(10000, 250000, by = 1000)) %>%
  mutate(
    # Cumulative depreciation modeled as standard linear-to-asymptotic fleet curve
    cum_depr = capital_depreciable * (1 - exp(-km / 75000)),
    cum_maint = a_param * (km)^b_param,
    cum_tco = cum_depr + cum_maint,
    
    # Cost per km curves
    depr_per_km  = cum_depr / km,
    maint_per_km = cum_maint / km,
    tco_per_km   = cum_tco / km
  )

# Extract Global Minimum TCO / km
optimal_row <- km_grid %>%
  slice_min(tco_per_km, n = 1)

optimal_km <- optimal_row$km
min_cost_per_km <- optimal_row$tco_per_km

cat("\n================ EMPIRICAL OPTIMAL RETIREMENT THRESHOLD ================\n")
cat("Fleet Optimal De-Fleeting Odometer :", comma(optimal_km), "km\n")
cat("Minimum Total Cost of Ownership    : $", round(min_cost_per_km, 3), "/ km\n")
cat("Depreciation Rate at Exit          : $", round(optimal_row$depr_per_km, 3), "/ km\n")
cat("Maintenance Rate at Exit           : $", round(optimal_row$maint_per_km, 3), "/ km\n")

# ------------------------------------------------------------------------------
# 5. Visual Exhibits for Capstone Report
# ------------------------------------------------------------------------------
# Exhibit 1: The Classic U-Shaped TCO Economic Curve
tco_curve_plot <- ggplot(km_grid, aes(x = km)) +
  geom_line(aes(y = depr_per_km, color = "Depreciation / km"), linewidth = 1.1, linetype = "dashed") +
  geom_line(aes(y = maint_per_km, color = "Maintenance / km"), linewidth = 1.1, linetype = "dashed") +
  geom_line(aes(y = tco_per_km, color = "Total Cost of Ownership (TCO) / km"), linewidth = 1.4) +
  geom_vline(xintercept = optimal_km, linetype = "dotted", color = "#D95F02", linewidth = 1) +
  annotate(
    "text", x = optimal_km + 8000, y = min_cost_per_km * 1.3,
    label = paste0("Optimal Retirement Point\n", comma(optimal_km), " km ($", round(min_cost_per_km, 2), "/km)"),
    hjust = 0, size = 4, fontface = "bold", color = "#D95F02"
  ) +
  scale_x_continuous(labels = comma, breaks = seq(0, 250000, by = 50000)) +
  scale_y_continuous(labels = dollar_format(accuracy = 0.05), limits = c(0, max(km_grid$tco_per_km[km_grid$km > 20000]) * 1.1)) +
  scale_color_manual(
    name = "Cost Component",
    values = c(
      "Depreciation / km" = "#1F77B4",
      "Maintenance / km" = "#E41A1C",
      "Total Cost of Ownership (TCO) / km" = "#2CA02C"
    )
  ) +
  labs(
    title = "Fleet Total Cost of Ownership (TCO) Curve & De-Fleeting Inflection",
    subtitle = "Trade-off between amortized capital depreciation and escalating repair costs",
    x = "Cumulative Vehicle Distance (Kilometers)",
    y = "Unit Cost (USD / km)"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom")

print(tco_curve_plot)
ggsave("defleeting_tco_economic_curve.png", tco_curve_plot, width = 8, height = 5.5, dpi = 300)

# Exhibit 2: Active Fleet De-Fleeting Urgency Matrix (76 Active Assets)
active_eval <- df_tco %>%
  filter(current_status != "De-fleeted / Sold") %>%
  mutate(
    odometer_gap_to_optimal = lifetime_odometer_km - optimal_km,
    retirement_status = case_when(
      lifetime_odometer_km >= (optimal_km * 1.15) ~ "Overdue for Disposal (Sub-economic)",
      lifetime_odometer_km >= optimal_km          ~ "In Optimal Retirement Window",
      lifetime_odometer_km >= (optimal_km * 0.80) ~ "Approaching Retirement (Plan Disposal)",
      TRUE                                        ~ "Prime Economic Operating Window"
    )
  )

retirement_summary_plot <- ggplot(active_eval, aes(x = lifetime_odometer_km, y = cumulative_maint_spend, color = retirement_status)) +
  geom_point(aes(size = purchase_cost), alpha = 0.8) +
  geom_vline(xintercept = optimal_km, linetype = "dashed", color = "darkred") +
  scale_x_continuous(labels = comma) +
  scale_y_continuous(labels = dollar) +
  scale_size_continuous(labels = dollar, name = "Purchase Cost") +
  scale_color_manual(
    name = "Asset Lifecycle Action",
    values = c(
      "Overdue for Disposal (Sub-economic)" = "#E41A1C",
      "In Optimal Retirement Window" = "#FF7F00",
      "Approaching Retirement (Plan Disposal)" = "#377EB8",
      "Prime Economic Operating Window" = "#4DAF4A"
    )
  ) +
  labs(
    title = "Active Fleet Asset Lifecycle Staging",
    subtitle = paste0("Evaluation of 76 Active Units Against ", comma(optimal_km), " km TCO Benchmark"),
    x = "Current Odometer (km)",
    y = "Cumulative Maintenance Spend (USD)"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom", legend.box = "vertical")

print(retirement_summary_plot)
ggsave("active_fleet_defleeting_urgency.png", retirement_summary_plot, width = 8.5, height = 6, dpi = 300)

# ------------------------------------------------------------------------------
# 6. Export Active Fleet De-Fleeting Action Register
# ------------------------------------------------------------------------------
active_defleet_register <- active_eval %>%
  select(
    vehicle_id, make, model, model_year, category,
    lifetime_odometer_km, service_life_years, purchase_cost,
    cumulative_maint_spend, tco_per_km, retirement_status
  ) %>%
  arrange(desc(lifetime_odometer_km))

write_csv(active_defleet_register, "active_fleet_defleeting_recommendations.csv")

cat("\n================ RETIREMENT ACTION BREAKDOWN (ACTIVE FLEET) ================\n")
print(table(active_eval$retirement_status))

cat("\n--- Top 10 Priority Candidates for Capital De-Fleeting ---\n")
print(head(active_defleet_register %>% filter(grepl("Disposal|Optimal", retirement_status)), 10))
