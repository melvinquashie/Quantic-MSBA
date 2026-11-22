# ==============================================================================
# Fleet Management Capstone - Week 9: Publication Figure & Table Pack
# File: export_capstone_exhibits.R
# ==============================================================================

rm(list = ls())
gc()

library(tidyverse)
library(scales)

cat("Generating Capstone Publication Exhibits...\n")

# ------------------------------------------------------------------------------
# Exhibit 1: Star Schema Warehouse Baselines (Table 1 for Chapter 3)
# ------------------------------------------------------------------------------
table1_warehouse_summary <- tibble(
  `Database Entity` = c("dim_vehicles", "fact_reservations", "fact_maintenance_logs", "Active Operating Fleet"),
  `Record Count` = c("488 Assets", "37,737 Agreements", "8,804 Work Orders", "76 Commercial Units"),
  `Financial Scope` = c("$14.2M Fleet Asset Base", "$3.37M Revenue at Risk", "$695.2k Vendor Spend", "100% Star-Schema Validated"),
  `Primary Function` = c(
    "Asset master, purchase prices & depreciation schedules",
    "Rental transactions, station demand, duration & no-shows",
    "Repairs, parts vs. labor split & odometer tracking",
    "Current active deployment subject to predictive scoring"
  )
)

write_csv(table1_warehouse_summary, "report_table1_warehouse_summary.csv")

# ------------------------------------------------------------------------------
# Exhibit 2: Predictive Model Performance Benchmark (Table 2 for Chapter 4)
# ------------------------------------------------------------------------------
table2_model_benchmark <- tibble(
  `Engine Focus` = c(
    "Predictive Maintenance (Week 6)",
    "Demand Churn & No-Show (Week 7)",
    "TCO De-Fleeting Optimizer (Week 8)"
  ),
  `Algorithm Architecture` = c(
    "Random Forest (ranger) vs Logistic Regression",
    "Random Forest (ranger, 300 trees)",
    "Non-linear Parametric Least Squares (nls)"
  ),
  `Target Variable` = c(
    "target_high_failure_risk (Binary)",
    "target_churn (Cancelled or No-Show)",
    "Optimal Retirement km (arg min TCO/km)"
  ),
  `Holdout Test Metric` = c(
    "ROC-AUC: 1.000 | Accuracy: 100%",
    "ROC-AUC: 0.993 | Accuracy: 96.8% | Precision: 93.7%",
    "Optimal Threshold: 250,000 km"
  ),
  `Operational Implementation` = c(
    "Pre-dispatch mechanical inspection gating",
    "Station-specific dynamic overbooking multipliers",
    "Immediate disposal of sub-economic units (>250k km)"
  )
)

write_csv(table2_model_benchmark, "report_table2_model_benchmark.csv")

# ------------------------------------------------------------------------------
# Exhibit 3: Prescriptive Overbooking Policy (Table 3 for Chapter 5)
# ------------------------------------------------------------------------------
table3_overbooking_policy <- tibble(
  `Depot Station` = c("Kempinski Hotel (KEMP)", "Headquarters (HDQTR)", "La Palm Beach (LAPAM)"),
  `Booking Volume` = c("13,364", "23,732", "640"),
  `Historical Churn Rate` = c("0.96%", "16.74%", "58.12%"),
  `Recommended Buffer` = c("+1.0%", "+20.1%", "+138.8%"),
  `Overbooking Multiplier` = c("1.01x", "1.20x", "2.39x (Policy Restricted)"),
  `Mandated Policy Guideline` = c(
    "Strict 1:1 inventory reservation. Preserve zero-failure VIP service levels.",
    "Dynamic 1.20x overbooking permitted on standard sedan and SUV inventory.",
    "Do NOT overbook. Mandate credit card prepayment or 24-hr reconfirmation."
  )
)

write_csv(table3_overbooking_policy, "report_table3_overbooking_policy.csv")

# ------------------------------------------------------------------------------
# Exhibit 4: Active Fleet De-Fleeting Action Register (Table 4 for Chapter 5)
# ------------------------------------------------------------------------------
table4_defleet_register <- tibble(
  `Lifecycle Action Category` = c(
    "Overdue for Disposal (Sub-economic)",
    "In Optimal Retirement Window",
    "Approaching Retirement (Plan Disposal)",
    "Prime Economic Operating Window"
  ),
  `Odometer Threshold (km)` = c(
    "> 287,500 km (> 1.15x ceiling)",
    "250,000 – 287,500 km",
    "200,000 – 249,999 km",
    "< 200,000 km"
  ),
  `Active Vehicle Count` = c("1 Unit (~400k km)", "0 Units", "3 Units", "72 Units"),
  `Prescriptive Fleet Action` = c(
    "Immediate salvage / auction disposal. High acute breakdown risk.",
    "De-fleet upon completion of current contractual cycle.",
    "Restrict to short-haul urban rentals; schedule replacement capital.",
    "Unrestricted deployment across all commercial rental operations."
  )
)

write_csv(table4_defleet_register, "report_table4_defleet_register.csv")

cat("All 4 Capstone Report Tables successfully exported as clean CSVs!\n")
