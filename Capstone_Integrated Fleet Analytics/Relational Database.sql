-- Clean up existing tables if rebuilding
DROP TABLE IF EXISTS telematics_pings CASCADE;
DROP TABLE IF EXISTS turnaround_events CASCADE;
DROP TABLE IF EXISTS maintenance_logs CASCADE;
DROP TABLE IF EXISTS reservations CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS locations CASCADE;

-- ============================================================================
-- Avis Fleet & Operations Data Warehouse DDL Schema
-- Database: PostgreSQL 14+ / pgAdmin
-- ============================================================================

-- Clean up existing tables if rebuilding
DROP TABLE IF EXISTS fact_maintenance_logs CASCADE;
DROP TABLE IF EXISTS fact_reservations CASCADE;
DROP TABLE IF EXISTS dim_vehicles CASCADE;

-- ----------------------------------------------------------------------------
-- 1. DIMENSION TABLE: dim_vehicles (Fleet Asset Master)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_vehicles (
    vehicle_id                  VARCHAR(30) PRIMARY KEY,
    fleet_number                VARCHAR(30),
    anonymized_vin              VARCHAR(50),
    license_plate               VARCHAR(30),
    make                        VARCHAR(50) NOT NULL,
    model                       VARCHAR(50) NOT NULL,
    model_year                  INTEGER,
    category                    VARCHAR(50),
    fuel_type                   VARCHAR(30),
    transmission                VARCHAR(30),
    color                       VARCHAR(30),
    acquisition_date            DATE,
    in_service_date             DATE,
    defleet_date                DATE,
    current_status              VARCHAR(50) NOT NULL,
    current_location            VARCHAR(100),
    current_odometer            INTEGER DEFAULT 0,
    target_retirement_odometer  INTEGER,
    purchase_cost               NUMERIC(12,2) DEFAULT 0.00,
    target_residual_value       NUMERIC(12,2) DEFAULT 0.00,
    accumulated_depreciation    NUMERIC(12,2) DEFAULT 0.00,
    sale_price                  NUMERIC(12,2) DEFAULT 0.00,
    acquisition_year            INTEGER,
    cost_imputed_flag           VARCHAR(50),
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert fallback record to preserve Referential Integrity for unassigned bookings
INSERT INTO dim_vehicles (vehicle_id, make, model, current_status)
VALUES ('UNASSIGNED', 'UNKNOWN', 'UNKNOWN', 'System Pipeline Placeholder');

-- ----------------------------------------------------------------------------
-- 2. FACT TABLE: fact_reservations (Demand, Trips, Cancellations, No-Shows)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_reservations (
    reservation_id              VARCHAR(50) PRIMARY KEY,
    booking_status              VARCHAR(30) NOT NULL, -- 'Completed', 'Active', 'Cancelled', 'No-Show'
    vehicle_id                  VARCHAR(30) REFERENCES dim_vehicles(vehicle_id) ON UPDATE CASCADE,
    vehicle_class               VARCHAR(20),
    model                       VARCHAR(50),
    model_year                  VARCHAR(20),
    pickup_datetime             TIMESTAMP,
    return_datetime             TIMESTAMP,
    date_cancelled              TIMESTAMP,
    rental_days                 NUMERIC(8,2),
    pickup_location             VARCHAR(50),
    return_location             VARCHAR(50),
    rate                        VARCHAR(50),
    deposit                     NUMERIC(10,2) DEFAULT 0.00,
    miles_out                   INTEGER,
    booked_by                   VARCHAR(100),
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. FACT TABLE: fact_maintenance_logs (RO History, Downtime, Workshop Costs)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_maintenance_logs (
    maintenance_id              BIGSERIAL PRIMARY KEY,
    ro_number                   VARCHAR(50) NOT NULL,
    vehicle_id                  VARCHAR(30) REFERENCES dim_vehicles(vehicle_id) ON UPDATE CASCADE,
    service_date                DATE,
    completion_date             DATE,
    downtime_days               NUMERIC(6,1),
    odometer                    INTEGER,
    service_type                VARCHAR(100),
    parts_cost                  NUMERIC(10,2) DEFAULT 0.00,
    labor_cost                  NUMERIC(10,2) DEFAULT 0.00,
    total_cost                  NUMERIC(10,2) DEFAULT 0.00,
    repair_facility             VARCHAR(150),
    notes                       TEXT,
    location                    VARCHAR(100),
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 4. PERFORMANCE INDEXES (Optimized for EDA & Capstone Analytical Queries)
-- ----------------------------------------------------------------------------
CREATE INDEX idx_veh_status ON dim_vehicles(current_status);
CREATE INDEX idx_veh_make_model ON dim_vehicles(make, model);

CREATE INDEX idx_res_vehicle ON fact_reservations(vehicle_id);
CREATE INDEX idx_res_status ON fact_reservations(booking_status);
CREATE INDEX idx_res_pickup_dt ON fact_reservations(pickup_datetime);
CREATE INDEX idx_res_pickup_loc ON fact_reservations(pickup_location);

CREATE INDEX idx_maint_vehicle ON fact_maintenance_logs(vehicle_id);
CREATE INDEX idx_maint_ro ON fact_maintenance_logs(ro_number);
CREATE INDEX idx_maint_date ON fact_maintenance_logs(service_date);
CREATE INDEX idx_maint_facility ON fact_maintenance_logs(repair_facility);