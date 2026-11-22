-- Creating tables in database
-- 1. DROP EXISTING TABLES TO START COMPLETELY FRESH
DROP TABLE IF EXISTS fact_maintenance_logs CASCADE;
DROP TABLE IF EXISTS fact_reservations CASCADE;
DROP TABLE IF EXISTS dim_vehicles CASCADE;

-- 2. CREATE DIM_VEHICLES (EXACT 22 COLUMNS MATCHING YOUR CSV)
CREATE TABLE dim_vehicles (
    vehicle_id                  TEXT PRIMARY KEY,
    fleet_number                TEXT,
    anonymized_vin              TEXT,
    license_plate               TEXT,
    make                        TEXT,
    model                       TEXT,
    model_year                  TEXT,
    category                    TEXT,
    fuel_type                   TEXT,
    transmission                TEXT,
    color                       TEXT,
    acquisition_date            TEXT,
    in_service_date             TEXT,
    defleet_date                TEXT,
    current_status              TEXT,
    current_location            TEXT,
    current_odometer            TEXT,
    target_retirement_odometer  TEXT,
    purchase_cost               TEXT,
    target_residual_value       TEXT,
    accumulated_depreciation    TEXT,
    sale_price                  TEXT
);

-- 3. CREATE FACT_RESERVATIONS (EXACT 16 COLUMNS MATCHING YOUR CSV)
CREATE TABLE fact_reservations (
    reservation_id              TEXT,
    booking_status              TEXT,
    unit_id                     TEXT,
    vehicle_class               TEXT,
    model                       TEXT,
    model_year                  TEXT,
    pickup_datetime             TEXT,
    return_datetime             TEXT,
    date_cancelled              TEXT,
    rental_days                 TEXT,
    pickup_location             TEXT,
    return_location             TEXT,
    rate                        TEXT,
    deposit                     TEXT,
    miles_out                   TEXT,
    booked_by                   TEXT
);

-- 4. CREATE FACT_MAINTENANCE_LOGS (EXACT 13 COLUMNS MATCHING YOUR CSV)
CREATE TABLE fact_maintenance_logs (
    ro_number                   TEXT,
    vehicle_id                  TEXT,
    service_date                TEXT,
    completion_date             TEXT,
    downtime_days               TEXT,
    odometer                    TEXT,
    service_type                TEXT,
    parts_cost                  TEXT,
    labor_cost                  TEXT,
    total_cost                  TEXT,
    repair_facility             TEXT,
    notes                       TEXT,
    location                    TEXT
);

--Import done using pgAdmin GUI import tool
-- Verification of import

SELECT 'dim_vehicles' AS table_name, COUNT(*) AS total_rows FROM dim_vehicles
UNION ALL
SELECT 'fact_reservations', COUNT(*) FROM fact_reservations
UNION ALL
SELECT 'fact_maintenance_logs', COUNT(*) FROM fact_maintenance_logs;