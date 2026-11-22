-- Build the dataset for Option 1: Predicting Unscheduled Breakdown Risk
-- ml_predictive_maintenance_features
CREATE TABLE ml_predictive_maintenance_features AS
WITH vehicle_age_base AS (
    SELECT 
        v.vehicle_id,
        v.make,
        v.model,
        v.model_year,
        v.current_status,
        -- Operating lifespan up to extraction date (Sep 1, 2026)
        GREATEST(
            DATE '2026-09-01' - NULLIF(v.acquisition_date, '')::date, 
            1
        ) AS operational_age_days
    FROM dim_vehicles v
    WHERE v.current_status != 'De-fleeted / Sold'
),
reservation_stress AS (
    SELECT 
        r.unit_id AS vehicle_id,
        COUNT(r.reservation_id) AS total_completed_trips,
        ROUND(
            COALESCE(
                SUM(
                    CASE 
                        WHEN r.pickup_datetime ~ '^\d' AND r.return_datetime ~ '^\d' 
                        THEN EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0 
                        ELSE 0 
                    END
                ), 
                0
            )::numeric, 
            1
        ) AS total_on_rent_days,
        ROUND(
            AVG(
                CASE 
                    WHEN r.pickup_datetime ~ '^\d' AND r.return_datetime ~ '^\d' 
                    THEN EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0 
                    ELSE NULL 
                END
            )::numeric, 
            2
        ) AS avg_trip_length_days
    FROM fact_reservations r
    WHERE r.booking_status = 'Completed'
      AND r.reservation_id != 'R/A #'
    GROUP BY r.unit_id
),
maintenance_profile AS (
    SELECT 
        m.vehicle_id,
        COUNT(m.ro_number) AS total_service_events,
        -- Preventive Maintenance (PM) counts
        COUNT(CASE 
            WHEN UPPER(m.service_type) ~ '(OIL|FILTER|PLUG|SERVICE)' THEN 1 
        END) AS pm_event_count,
        -- Unscheduled component failures
        COUNT(CASE 
            WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' THEN 1 
        END) AS unscheduled_event_count,
        -- Unscheduled repair expenditure
        COALESCE(
            SUM(
                CASE 
                    WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' 
                    THEN NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric 
                    ELSE 0 
                END
            ), 
            0
        ) AS unscheduled_spend,
        -- Total maintenance expenditure
        COALESCE(
            SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 
            0
        ) AS total_maint_spend,
        -- Highest recorded odometer reading
        MAX(NULLIF(REPLACE(m.odometer, ',', ''), '')::numeric) AS max_recorded_odometer,
        -- Days elapsed since the most recent service relative to extraction cutoff
        DATE '2026-09-01' - MAX(
            CASE 
                WHEN m.service_date ~ '^\d' THEN m.service_date::date 
                ELSE NULL 
            END
        ) AS days_since_last_service
    FROM fact_maintenance_logs m
    WHERE m.service_date IS NOT NULL
      AND m.service_date ~ '^\d'
    GROUP BY m.vehicle_id
)
SELECT 
    b.vehicle_id,
    b.make,
    b.model,
    b.model_year,
    b.current_status,
    b.operational_age_days,
    -- Reservation Stress Features
    COALESCE(r.total_completed_trips, 0) AS total_completed_trips,
    COALESCE(r.total_on_rent_days, 0) AS total_on_rent_days,
    COALESCE(r.avg_trip_length_days, 0) AS avg_trip_length_days,
    ROUND(
        (COALESCE(r.total_on_rent_days, 0) * 100.0 / NULLIF(b.operational_age_days, 0))::numeric, 
        2
    ) AS historical_utilization_pct,
    -- Maintenance Features
    COALESCE(m.total_service_events, 0) AS total_service_events,
    COALESCE(m.pm_event_count, 0) AS pm_event_count,
    COALESCE(m.unscheduled_event_count, 0) AS unscheduled_event_count,
    ROUND(COALESCE(m.unscheduled_spend, 0)::numeric, 2) AS unscheduled_spend,
    ROUND(COALESCE(m.total_maint_spend, 0)::numeric, 2) AS total_maint_spend,
    COALESCE(m.max_recorded_odometer, 0) AS latest_odometer_km,
    COALESCE(m.days_since_last_service, b.operational_age_days) AS days_since_last_service,
    -- Unscheduled Failure Ratio
    ROUND(
        (COALESCE(m.unscheduled_event_count, 0) * 100.0 / NULLIF(COALESCE(m.total_service_events, 0), 0))::numeric, 
        2
    ) AS unscheduled_failure_rate_pct,
    -- Target Variable Y (1 = High Failure Risk Asset, 0 = Low Risk)
    CASE 
        WHEN COALESCE(m.unscheduled_spend, 0) > 1500 
          OR COALESCE(m.unscheduled_event_count, 0) >= 15 THEN 1 
        ELSE 0 
    END AS target_high_failure_risk
FROM vehicle_age_base b
LEFT JOIN reservation_stress r 
    ON b.vehicle_id = r.vehicle_id
LEFT JOIN maintenance_profile m 
    ON b.vehicle_id = m.vehicle_id
ORDER BY target_high_failure_risk DESC, unscheduled_spend DESC;