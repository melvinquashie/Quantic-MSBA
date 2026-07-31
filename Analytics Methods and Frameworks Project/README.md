# NovaBank Lead Scoring & Predictive Analytics Project
**Author:** Melvin Quashie  
**Course:** Analytics Methods and Frameworks  
**Date:** August 2026  

---

## 📌 Project Overview
This repository contains the end-to-end analytical workflow for optimizing NovaBank's direct telemarketing operations. Using binomial logistic regression models built in R, this project establishes a pre-call lead qualification framework that transitions NovaBank away from blind cold-calling and toward targeted lead scoring.

---

## 🛠 Repository Contents
1. `novabank_analysis.R`: Fully executable R script containing data loading, EDA, train-test splitting (80/20), GLM model training (In-Call vs. Pre-Call), evaluation metrics, and export workflows.
2. `bank_marketing.csv`: Dataset containing customer financial, demographic, and historical campaign records.
3. `app.R`: Interactive R-Shiny web application (`shinyapp_precall`) for real-time lead tier calculation.
4. `outputs/`: High-resolution plot exports generated during EDA.

---

## 🚀 How to Reproduce the Analysis

### Prerequisites
Ensure you have **R (>= 4.0)** and **RStudio** installed.

### Required R Packages
Install the required packages by running the following command in RStudio:

```R
install.packages(c("tidyverse", "caTools", "caret", "shiny", "shinydashboard"))
