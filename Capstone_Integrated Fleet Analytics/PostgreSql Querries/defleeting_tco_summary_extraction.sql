DROP TABLE IF EXISTS ml_defleeting_tco_summary;

CREATE TABLE ml_defleeting_tco_summary AS
WITH maint_agg AS (
    SELECT 
        vehicle_id,
        COUNT(ro_number) AS total_ro_events,
        SUM(COALESCE(NULLIF(REPLACE(parts_cost, ',', ''), '')::numeric, 0)) AS total_parts_cost,
        SUM(COALESCE(NULLIF(REPLACE(labor_cost, ',', ''), '')::numeric, 0)) AS total_labor_cost,
        SUM(COALESCE(NULLIF(REPLACE(total_cost, ',', ''), '')::numeric, 0)) AS total_maint_cost,
        MAX(COALESCE(NULLIF(REPLACE(odometer, ',', ''), '')::numeric, 0)) AS max_service_odometer
    FROM fact_maintenance_logs
    GROUP BY vehicle_id
),
res_agg AS (
    SELECT 
        unit_id,
        COUNT(reservation_id) AS total_rentals,
        SUM(
            GREATEST(
                EXTRACT(EPOCH FROM (return_datetime::timestamp - pickup_datetime::timestamp)) / 86400.0,
                0
            )
        ) AS total_days_on_rent
    FROM fact_reservations
    WHERE booking_status = 'Completed'
      AND pickup_datetime ~ '^\d'
      AND return_datetime ~ '^\d'
    GROUP BY unit_id
)
SELECT 
    v.vehicle_id,
    v.make,
    v.model,
    v.model_year,
    v.category,
    v.current_status,
    -- Acquisition & Disposal Dates
    v.acquisition_date::date AS acq_date,
    v.defleet_date::date AS exit_date,
    ROUND(
        (COALESCE(v.defleet_date::date, '2026-09-01'::date) - v.acquisition_date::date)::numeric / 365.25, 
        2
    ) AS service_life_years,
    -- Financial Parameters
    COALESCE(NULLIF(REPLACE(v.purchase_cost, ',', ''), '')::numeric, 0) AS purchase_cost,
    COALESCE(NULLIF(REPLACE(v.sale_price, ',', ''), '')::numeric, 0) AS actual_sale_price,
    COALESCE(NULLIF(REPLACE(v.target_residual_value, ',', ''), '')::numeric, 0) AS target_residual_value,
    -- Actual Capital Depreciation Incurred
    CASE 
        WHEN v.current_status = 'De-fleeted / Sold' AND COALESCE(NULLIF(REPLACE(v.sale_price, ',', ''), '')::numeric, 0) > 0 
            THEN COALESCE(NULLIF(REPLACE(v.purchase_cost, ',', ''), '')::numeric, 0) - COALESCE(NULLIF(REPLACE(v.sale_price, ',', ''), '')::numeric, 0)
        ELSE COALESCE(NULLIF(REPLACE(v.accumulated_depreciation, ',', ''), '')::numeric, 
                      COALESCE(NULLIF(REPLACE(v.purchase_cost, ',', ''), '')::numeric, 0) * 0.80)
    END AS net_depreciation_incurred,
    -- Utilization & Cumulative Maintenance
    COALESCE(r.total_days_on_rent, 0)::numeric(10, 1) AS cumulative_rental_days,
    COALESCE(m.total_ro_events, 0) AS lifetime_service_count,
    COALESCE(m.total_maint_cost, 0) AS cumulative_maint_spend,
    GREATEST(
        COALESCE(NULLIF(REPLACE(v.current_odometer, ',', ''), '')::numeric, 0),
        COALESCE(m.max_service_odometer, 0)
    ) AS lifetime_odometer_km
FROM dim_vehicles v
LEFT JOIN maint_agg m ON v.vehicle_id = m.vehicle_id
LEFT JOIN res_agg r ON v.vehicle_id = r.unit_id
WHERE v.acquisition_date IS NOT NULL 
  AND v.acquisition_date ~ '^\d{4}';



SELECT * FROM ml_defleeting_tco_summary;
