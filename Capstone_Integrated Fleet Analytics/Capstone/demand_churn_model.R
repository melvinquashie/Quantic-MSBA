# ==============================================================================
# Fleet Management Capstone - Week 7: Demand Leakage & No-Show Engine
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
# 2. Ingest Data & Preprocessing Setup
# ------------------------------------------------------------------------------
# Ensure "demand_churn_features.csv" is in your R working directory
df_raw <- read_csv("demand_churn_features.csv")

# Clean target factor and define predictor matrix
df_model <- df_raw %>%
  mutate(
    target_churn = factor(
      if_else(as.numeric(target_churn) == 1, "Churn", "Fulfilled"),
      levels = c("Churn", "Fulfilled")
    ),
    pickup_location = as.factor(pickup_location),
    vehicle_class   = as.factor(vehicle_class),
    booked_by_channel = as.factor(booked_by_channel),
    pickup_day_of_week = as.factor(pickup_day_of_week),
    pickup_month = as.factor(pickup_month)
  ) %>%
  select(
    target_churn,
    pickup_location,
    is_same_location_return,
    vehicle_class,
    booked_by_channel,
    scheduled_duration_days,
    deposit_amount,
    pickup_day_of_week,
    pickup_hour,
    pickup_month,
    is_weekend_pickup
  )

cat("\n--- Full Reservation Portfolio Churn Distribution ---\n")
print(table(df_model$target_churn))

# ------------------------------------------------------------------------------
# 3. Stratified Partitioning (75% Train / 25% Test)
# ------------------------------------------------------------------------------
data_split <- initial_split(df_model, prop = 0.75, strata = target_churn)
train_data <- training(data_split)
test_data  <- testing(data_split)

cat("\n--- Holdout Test Distribution (N ~ 9,430) ---\n")
print(table(test_data$target_churn))

# ------------------------------------------------------------------------------
# 4. Recipe & High-Cardinality Pooling
# ------------------------------------------------------------------------------
churn_recipe <- recipe(target_churn ~ ., data = train_data) %>%
  step_other(booked_by_channel, threshold = 0.02, other = "Other_Agent") %>%
  step_other(vehicle_class, threshold = 0.02, other = "Other_Class") %>%
  step_other(pickup_location, threshold = 0.01, other = "Other_Depot") %>%
  step_dummy(all_nominal_predictors()) %>%
  step_normalize(all_numeric_predictors()) %>%
  step_zv(all_predictors())

# 1. Baseline: Logistic Regression
logit_spec <- logistic_reg() %>%
  set_engine("glm") %>%
  set_mode("classification")

logit_wf <- workflow() %>%
  add_recipe(churn_recipe) %>%
  add_model(logit_spec)

# 2. Production Engine: Random Forest
rf_spec <- rand_forest(trees = 300, min_n = 10) %>%
  set_engine("ranger", importance = "impurity") %>%
  set_mode("classification")

rf_wf <- workflow() %>%
  add_recipe(churn_recipe) %>%
  add_model(rf_spec)

# ------------------------------------------------------------------------------
# 5. Model Training & Test Set Prediction
# ------------------------------------------------------------------------------
cat("\nTraining Logistic Regression and Random Forest models on ~28,300 reservations...\n")
logit_fit <- fit(logit_wf, data = train_data)
rf_fit    <- fit(rf_wf, data = train_data)

# Score Logistic Regression
results_logit <- test_data %>%
  select(target_churn) %>%
  bind_cols(predict(logit_fit, test_data, type = "prob")) %>%
  bind_cols(predict(logit_fit, test_data, type = "class")) %>%
  mutate(model = "Logistic Regression")

# Score Random Forest
results_rf <- test_data %>%
  select(target_churn) %>%
  bind_cols(predict(rf_fit, test_data, type = "prob")) %>%
  bind_cols(predict(rf_fit, test_data, type = "class")) %>%
  mutate(model = "Random Forest")

# ------------------------------------------------------------------------------
# 6. Evaluation Metrics & Confusion Matrices
# ------------------------------------------------------------------------------
acc_rf  <- accuracy(results_rf, truth = target_churn, estimate = .pred_class)
auc_rf  <- roc_auc(results_rf, truth = target_churn, .pred_Churn, event_level = "first")
sens_rf <- sens(results_rf, truth = target_churn, estimate = .pred_class, event_level = "first")
spec_rf <- spec(results_rf, truth = target_churn, estimate = .pred_class, event_level = "first")

rf_perf <- bind_rows(acc_rf, auc_rf, sens_rf, spec_rf) %>%
  mutate(model = "Random Forest")

cat("\n================ RANDOM FOREST TEST PERFORMANCE ================\n")
print(rf_perf)

cat("\n================ RANDOM FOREST CONFUSION MATRIX ================\n")
print(conf_mat(results_rf, truth = target_churn, estimate = .pred_class))

# ------------------------------------------------------------------------------
# 7. Visual Exhibits for Capstone Report
# ------------------------------------------------------------------------------
# Exhibit 1: ROC Curve
rf_roc <- results_rf %>%
  roc_curve(truth = target_churn, .pred_Churn, event_level = "first")

roc_plot <- autoplot(rf_roc) +
  labs(
    title = "Demand Leakage & Churn ROC Curve (Random Forest)",
    subtitle = paste0("Test Set Holdout | ROC-AUC: ", round(auc_rf$.estimate, 4)),
    x = "False Positive Rate (1 - Specificity)",
    y = "True Positive Rate (Sensitivity)"
  ) +
  theme_minimal(base_size = 12)

print(roc_plot)
ggsave("demand_leakage_roc_curve.png", roc_plot, width = 7, height = 5, dpi = 300)

# Exhibit 2: Gini Impurity Feature Importance
rf_obj <- extract_fit_parsnip(rf_fit)$fit
imp_vals <- rf_obj$variable.importance

var_imp_df <- tibble(
  Variable = names(imp_vals),
  Importance = as.numeric(imp_vals)
) %>%
  arrange(desc(Importance)) %>%
  slice_head(n = 8)

vip_plot <- ggplot(var_imp_df, aes(x = reorder(Variable, Importance), y = Importance)) +
  geom_col(fill = "#2CA02C", width = 0.7) +
  coord_flip() +
  labs(
    title = "Primary Drivers of Reservation Churn & No-Shows",
    subtitle = "Gini Impurity Metric from Random Forest",
    x = "Reservation Signal",
    y = "Variable Importance"
  ) +
  theme_minimal(base_size = 12)

print(vip_plot)
ggsave("demand_leakage_feature_importance.png", vip_plot, width = 7, height = 5, dpi = 300)

# ------------------------------------------------------------------------------
# 8. Prescriptive Overbooking Multiplier by Depot
# ------------------------------------------------------------------------------
# Calculate operational overbooking buffers to protect revenue
overbooking_strategy <- df_raw %>%
  group_by(pickup_location) %>%
  summarise(
    total_bookings = n(),
    no_show_cancellations = sum(target_churn),
    historical_churn_rate = round(mean(target_churn) * 100, 2),
    # Buffer formula: 1 / (1 - Churn Rate) capped conservatively
    recommended_buffer_pct = round((mean(target_churn) / (1 - mean(target_churn))) * 100, 1),
    recommended_overbooking_multiplier = round(1 / (1 - mean(target_churn)), 2)
  ) %>%
  filter(total_bookings >= 100) %>%
  arrange(desc(historical_churn_rate))

write_csv(overbooking_strategy, "depot_overbooking_recommendations.csv")

cat("\n================ PRESCRIPTIVE OVERBOOKING MATRIX BY DEPOT ================\n")
print(overbooking_strategy)