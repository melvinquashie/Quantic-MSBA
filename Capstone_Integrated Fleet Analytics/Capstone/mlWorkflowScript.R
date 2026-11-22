# ==============================================================================
# Fleet Management Capstone - Phase 3: Predictive Maintenance Pipeline
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Environment Reset & Dependencies
# ------------------------------------------------------------------------------
rm(list = ls())
gc()

required_pkgs <- c("tidyverse", "tidymodels", "ranger")
new_pkgs <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if (length(new_pkgs) > 0) install.packages(new_pkgs)

library(tidyverse)
library(tidymodels)
library(ranger)

set.seed(42)

# ------------------------------------------------------------------------------
# 2. Ingest Data & Feature Selection (Leakage Prevention)
# ------------------------------------------------------------------------------
df_raw <- read_csv("predictive_maintenance_features.csv")

# Cleanly structure target and retain only valid pre-failure predictors
df_model <- df_raw %>%
  mutate(
    target_high_failure_risk = factor(
      if_else(as.numeric(target_high_failure_risk) == 1, "High_Risk", "Low_Risk"),
      levels = c("High_Risk", "Low_Risk")
    )
  ) %>%
  select(
    target_high_failure_risk,
    operational_age_days,
    total_completed_trips,
    total_on_rent_days,
    avg_trip_length_days,
    historical_utilization_pct,
    total_service_events,
    pm_event_count,
    latest_odometer_km,
    days_since_last_service,
    unscheduled_failure_rate_pct
  ) %>%
  mutate(across(where(is.numeric), ~replace_na(., median(., na.rm = TRUE))))

cat("\n--- Full Fleet Risk Profile (N = 76) ---\n")
print(table(df_model$target_high_failure_risk))

# ------------------------------------------------------------------------------
# 3. Stratified Partitioning (70% Train / 30% Test)
# ------------------------------------------------------------------------------
data_split <- initial_split(df_model, prop = 0.70, strata = target_high_failure_risk)
train_data <- training(data_split)
test_data  <- testing(data_split)

cat("\n--- Holdout Test Distribution (N = 24) ---\n")
print(table(test_data$target_high_failure_risk))

# ------------------------------------------------------------------------------
# 4. Recipes & Model Workflows
# ------------------------------------------------------------------------------
maint_recipe <- recipe(target_high_failure_risk ~ ., data = train_data) %>%
  step_normalize(all_numeric_predictors()) %>%
  step_zv(all_predictors())

# 1. Baseline: Logistic Regression (Interpretable Odds Ratios)
logit_spec <- logistic_reg() %>%
  set_engine("glm") %>%
  set_mode("classification")

logit_wf <- workflow() %>%
  add_recipe(maint_recipe) %>%
  add_model(logit_spec)

# 2. Production Engine: Random Forest Classifier
rf_spec <- rand_forest(trees = 500, min_n = 5) %>%
  set_engine("ranger", importance = "impurity") %>%
  set_mode("classification")

rf_wf <- workflow() %>%
  add_recipe(maint_recipe) %>%
  add_model(rf_spec)

# ------------------------------------------------------------------------------
# 5. Model Training & Holdout Test Scoring
# ------------------------------------------------------------------------------
logit_fit <- fit(logit_wf, data = train_data)
rf_fit    <- fit(rf_wf, data = train_data)

# Test Predictions: Logistic Regression
results_logit <- test_data %>%
  select(target_high_failure_risk) %>%
  bind_cols(predict(logit_fit, test_data, type = "prob")) %>%
  bind_cols(predict(logit_fit, test_data, type = "class")) %>%
  mutate(model = "Logistic Regression")

# Test Predictions: Random Forest
results_rf <- test_data %>%
  select(target_high_failure_risk) %>%
  bind_cols(predict(rf_fit, test_data, type = "prob")) %>%
  bind_cols(predict(rf_fit, test_data, type = "class")) %>%
  mutate(model = "Random Forest")

# ------------------------------------------------------------------------------
# 6. Evaluation Metrics & Confusion Matrices
# ------------------------------------------------------------------------------
acc_rf  <- accuracy(results_rf, truth = target_high_failure_risk, estimate = .pred_class)
auc_rf  <- roc_auc(results_rf, truth = target_high_failure_risk, .pred_High_Risk, event_level = "first")
sens_rf <- sens(results_rf, truth = target_high_failure_risk, estimate = .pred_class, event_level = "first")
spec_rf <- spec(results_rf, truth = target_high_failure_risk, estimate = .pred_class, event_level = "first")

rf_performance <- bind_rows(acc_rf, auc_rf, sens_rf, spec_rf) %>%
  mutate(model = "Random Forest")

cat("\n=================== RANDOM FOREST TEST PERFORMANCE ===================\n")
print(rf_performance)

cat("\n================ RANDOM FOREST CONFUSION MATRIX ============\n")
print(conf_mat(results_rf, truth = target_high_failure_risk, estimate = .pred_class))

# ------------------------------------------------------------------------------
# 7. Visual Exhibits for Capstone Report
# ------------------------------------------------------------------------------
# Exhibit 1: ROC Curve
rf_roc <- results_rf %>%
  roc_curve(truth = target_high_failure_risk, .pred_High_Risk, event_level = "first")

roc_plot <- autoplot(rf_roc) +
  labs(
    title = "Predictive Maintenance ROC Curve (Random Forest)",
    subtitle = paste0(
      "Holdout Evaluation (N = 24) | ROC-AUC: ",
      round(auc_rf$.estimate, 3)
    ),
    x = "False Positive Rate (1 - Specificity)",
    y = "True Positive Rate (Sensitivity)"
  ) +
  theme_minimal(base_size = 12)

print(roc_plot)
ggsave("predictive_maint_roc_curves.png", roc_plot, width = 7, height = 5, dpi = 300)

# Exhibit 2: Gini Impurity Variable Importance
rf_obj <- extract_fit_parsnip(rf_fit)$fit
imp_vals <- rf_obj$variable.importance

var_imp_df <- tibble(
  Variable = names(imp_vals),
  Importance = as.numeric(imp_vals)
) %>%
  arrange(desc(Importance)) %>%
  slice_head(n = 8)

vip_plot <- ggplot(var_imp_df, aes(x = reorder(Variable, Importance), y = Importance)) +
  geom_col(fill = "#1F77B4", width = 0.7) +
  coord_flip() +
  labs(
    title = "Key Predictors of Mechanical Breakdown Risk",
    subtitle = "Gini Impurity Metric from Random Forest",
    x = "Operational Feature",
    y = "Variable Importance"
  ) +
  theme_minimal(base_size = 12)

print(vip_plot)
ggsave("predictive_maint_feature_importance.png", vip_plot, width = 7, height = 5, dpi = 300)

# ------------------------------------------------------------------------------
# 8. Active Fleet Failure Risk Scoring & Prioritization
# ------------------------------------------------------------------------------
active_scored <- df_raw %>%
  bind_cols(predict(rf_fit, df_raw, type = "prob")) %>%
  rename(failure_risk_probability = .pred_High_Risk) %>%
  mutate(
    risk_tier = case_when(
      failure_risk_probability >= 0.70 ~ "Critical Risk (Inspect Immediately)",
      failure_risk_probability >= 0.40 ~ "Elevated Risk (Monitor Next Service)",
      TRUE ~ "Low / Stable"
    )
  ) %>%
  select(
    vehicle_id, make, model, model_year, current_status,
    failure_risk_probability, risk_tier, operational_age_days,
    latest_odometer_km, total_service_events
  ) %>%
  arrange(desc(failure_risk_probability))

write_csv(active_scored, "active_fleet_risk_scored.csv")

cat("\n================ TOP 10 ASSETS BY PREDICTED RISK ============\n")
print(head(active_scored, 10))