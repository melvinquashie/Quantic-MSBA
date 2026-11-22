# ==============================================================================
# Fleet Management Capstone - Week 9: Executive BI Decision Support System
# File: app.R
# ==============================================================================

# 1. Load Required Libraries
required_pkgs <- c("shiny", "shinydashboard", "tidyverse", "DT", "scales", "plotly")
new_pkgs <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if (length(new_pkgs) > 0) install.packages(new_pkgs)

library(shiny)
library(shinydashboard)
library(tidyverse)
library(DT)
library(scales)
library(plotly)

# 2. Data Loading & Defensive Handling
load_data <- function() {
  # Scored Active Fleet from Week 6
  if (file.exists("active_fleet_risk_scored.csv")) {
    df_risk <- read_csv("active_fleet_risk_scored.csv")
  } else {
    df_risk <- tibble(
      vehicle_id = paste0("AV0", 100:175),
      make = "TOYOTA", model = "COROLLA", model_year = 2022,
      current_status = "Available", failure_risk_probability = runif(76, 0.05, 0.85),
      risk_tier = if_else(failure_risk_probability >= 0.70, "Critical Risk (Inspect Immediately)",
                          if_else(failure_risk_probability >= 0.40, "Elevated Risk (Monitor Next Service)", "Low / Stable")),
      operational_age_days = 800, latest_odometer_km = 65000, total_service_events = 12
    )
  }
  
  # Overbooking Recommendations from Week 7
  if (file.exists("depot_overbooking_recommendations.csv")) {
    df_overbook <- read_csv("depot_overbooking_recommendations.csv")
  } else {
    df_overbook <- tibble(
      pickup_location = c("LAPAM", "HDQTR", "KEMP"),
      total_bookings = c(640, 23732, 13364),
      no_show_cancellations = c(372, 3972, 128),
      historical_churn_rate = c(58.1, 16.7, 0.96),
      recommended_buffer_pct = c(138.8, 20.1, 1.0),
      recommended_overbooking_multiplier = c(2.39, 1.20, 1.01)
    )
  }
  
  # De-fleeting Staging from Week 8
  if (file.exists("active_fleet_defleeting_recommendations.csv")) {
    df_defleet <- read_csv("active_fleet_defleeting_recommendations.csv")
    if (!"lifetime_odometer_km" %in% names(df_defleet) && "latest_odometer_km" %in% names(df_defleet)) {
      df_defleet <- df_defleet %>% rename(lifetime_odometer_km = latest_odometer_km)
    }
  } else {
    df_defleet <- df_risk %>%
      mutate(
        purchase_cost = 32000,
        lifetime_odometer_km = latest_odometer_km,
        cumulative_maint_spend = latest_odometer_km * 0.045,
        tco_per_km = 0.22,
        retirement_status = case_when(
          lifetime_odometer_km >= 287500 ~ "Overdue for Disposal (Sub-economic)",
          lifetime_odometer_km >= 250000 ~ "In Optimal Retirement Window",
          lifetime_odometer_km >= 200000 ~ "Approaching Retirement (Plan Disposal)",
          TRUE                         ~ "Prime Economic Operating Window"
        )
      )
  }
  
  
  list(risk = df_risk, overbook = df_overbook, defleet = df_defleet)
}

fleet_data <- load_data()

# 3. User Interface (UI)
ui <- dashboardPage(
  skin = "black",
  dashboardHeader(title = "Fleet Analytics Command"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Executive Overview", tabName = "overview", icon = icon("dashboard")),
      menuItem("Predictive Maintenance", tabName = "maint", icon = icon("wrench")),
      menuItem("Lifecycle & De-Fleeting", tabName = "lifecycle", icon = icon("car-crash")),
      menuItem("Demand Protection", tabName = "demand", icon = icon("calendar-check"))
    )
  ),
  dashboardBody(
    tags$head(
      tags$style(HTML("
        .content-wrapper, .right-side { background-color: #f7f9fa; }
        .box { border-top: 3px solid #1F77B4; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
      "))
    ),
    tabItems(
      # Tab 1: Executive Overview
      tabItem(
        tabName = "overview",
        fluidRow(
          valueBox(nrow(fleet_data$risk), "Active Commercial Units", icon = icon("truck"), color = "blue", width = 3),
          valueBox("11.85%", "Baseline Demand Leakage", icon = icon("exclamation-circle"), color = "yellow", width = 3),
          valueBox("$3.37M", "At-Risk Lost Revenue", icon = icon("dollar-sign"), color = "red", width = 3),
          valueBox("250k km", "Optimal TCO Retirement Point", icon = icon("tachometer-alt"), color = "green", width = 3)
        ),
        fluidRow(
          box(
            title = "Station Demand Protection & Buffer Policy", width = 7, status = "primary", solidHeader = TRUE,
            tableOutput("tbl_station_policy")
          ),
          box(
            title = "Active Fleet Failure Risk Composition", width = 5, status = "warning", solidHeader = TRUE,
            plotlyOutput("plot_risk_pie", height = "280px")
          )
        )
      ),
      
      # Tab 2: Predictive Maintenance Dispatch
      tabItem(
        tabName = "maint",
        fluidRow(
          box(
            title = "Operational Dispatch Triage (76 Active Vehicles)", width = 12, status = "primary",
            p("Triage active vehicles by failure probability before dispatching on long-distance contracts."),
            DTOutput("dt_active_risk")
          )
        )
      ),
      
      # Tab 3: Lifecycle & De-Fleeting
      tabItem(
        tabName = "lifecycle",
        fluidRow(
          box(
            title = "Active Fleet Lifecycle Staging vs 250,000 km Economic Ceiling", width = 8, status = "primary",
            plotlyOutput("plot_lifecycle_interactive", height = "400px")
          ),
          box(
            title = "Asset Retirement Staging Counts", width = 4, status = "info",
            tableOutput("tbl_defleet_counts")
          )
        )
      ),
      
      # Tab 4: Demand Protection Matrix
      tabItem(
        tabName = "demand",
        fluidRow(
          box(
            title = "Historical Demand Attrition by Depot (37,737 Bookings)", width = 12, status = "success",
            DTOutput("dt_overbooking_detail")
          )
        )
      )
    )
  )
)

# 4. Server Logic
server <- function(input, output, session) {
  
  # Tab 1: Station Policy Table
  output$tbl_station_policy <- renderTable({
    fleet_data$overbook %>%
      transmute(
        `Station / Depot` = pickup_location,
        `Total Bookings` = comma(total_bookings),
        `Historical Churn` = paste0(historical_churn_rate, "%"),
        `Recommended Buffer` = paste0("+", recommended_buffer_pct, "%"),
        `Overbooking Multiplier` = paste0(recommended_overbooking_multiplier, "x")
      )
  }, striped = TRUE, hover = TRUE, bordered = TRUE)
  
  # Tab 1: Risk Pie Chart
  output$plot_risk_pie <- renderPlotly({
    p_data <- fleet_data$risk %>%
      count(risk_tier)
    
    plot_ly(p_data, labels = ~risk_tier, values = ~n, type = 'pie',
            marker = list(colors = c('#E41A1C', '#FF7F00', '#4DAF4A'))) %>%
      layout(margin = list(l = 20, r = 20, t = 10, b = 10), legend = list(orientation = "h"))
  })
  
  # Tab 2: Predictive Maintenance Interactive Table
  output$dt_active_risk <- renderDT({
    datatable(
      fleet_data$risk %>%
        select(vehicle_id, make, model, model_year, current_status,
               failure_risk_probability, risk_tier, latest_odometer_km, total_service_events),
      options = list(pageLength = 10, order = list(list(6, 'desc'))),
      rownames = FALSE
    ) %>%
      formatPercentage("failure_risk_probability", digits = 1) %>%
      formatRound("latest_odometer_km", digits = 0) %>%
      formatStyle(
        "risk_tier",
        backgroundColor = styleEqual(
          c("Critical Risk (Inspect Immediately)", "Elevated Risk (Monitor Next Service)", "Low / Stable"),
          c("#FFCDD2", "#FFE0B2", "#C8E6C9")
        )
      )
  })
  
  # Tab 3: Interactive Staging Plot
  output$plot_lifecycle_interactive <- renderPlotly({
    req(fleet_data$defleet)
    
    p <- ggplot(
      fleet_data$defleet, 
      aes(
        x = lifetime_odometer_km, 
        y = cumulative_maint_spend, 
        color = retirement_status, 
        text = paste0(
          "Vehicle: ", vehicle_id, 
          "<br>Model: ", make, " ", model,
          "<br>Odometer: ", comma(lifetime_odometer_km), " km",
          "<br>Maint Spend: $", comma(cumulative_maint_spend)
        )
      )
    ) +
      geom_point(size = 3, alpha = 0.8) +
      geom_vline(xintercept = 250000, linetype = "dashed", color = "darkred") +
      scale_x_continuous(labels = comma) +
      scale_y_continuous(labels = dollar) +
      scale_color_manual(values = c(
        "Overdue for Disposal (Sub-economic)" = "#E41A1C",
        "In Optimal Retirement Window" = "#FF7F00",
        "Approaching Retirement (Plan Disposal)" = "#377EB8",
        "Prime Economic Operating Window" = "#4DAF4A"
      )) +
      labs(x = "Lifetime Odometer (km)", y = "Cumulative Maintenance ($)") +
      theme_minimal()
    
    ggplotly(p, tooltip = "text")
  })
  
  
  # Tab 3: De-fleeting Counts
  output$tbl_defleet_counts <- renderTable({
    fleet_data$defleet %>%
      count(`Retirement Status` = retirement_status, name = "Vehicle Count")
  }, striped = TRUE, hover = TRUE, bordered = TRUE)
  
  # Tab 4: Demand Table
  output$dt_overbooking_detail <- renderDT({
    datatable(
      fleet_data$overbook,
      options = list(dom = 't'),
      rownames = FALSE
    )
  })
}

# Run the Application
shinyApp(ui = ui, server = server)
