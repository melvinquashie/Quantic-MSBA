-- Execute this query to materialize the table ml_demand_leakage_features:

DROP TABLE IF EXISTS ml_demand_leakage_features;

CREATE TABLE ml_demand_leakage_features AS
SELECT 
    r.reservation_id,
    -- Target Variable: 1 = Lost Demand (Cancelled or No-Show), 0 = Fulfilled (Completed)
    CASE 
        WHEN r.booking_status IN ('Cancelled', 'No-Show') THEN 1 
        ELSE 0 
    END AS target_churn,
    r.booking_status,
    -- Operational & Depot Features
    COALESCE(r.pickup_location, 'UNKNOWN') AS pickup_location,
    COALESCE(r.return_location, 'UNKNOWN') AS return_location,
    CASE 
        WHEN r.pickup_location = r.return_location THEN 1 
        ELSE 0 
    END AS is_same_location_return,
    COALESCE(r.vehicle_class, 'UNKNOWN') AS vehicle_class,
    COALESCE(r.booked_by, 'UNKNOWN') AS booked_by_channel,
    -- Rental Duration & Financial Commitment
    ROUND(
        GREATEST(
            EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0,
            0.1
        )::numeric, 
        2
    ) AS scheduled_duration_days,
    COALESCE(NULLIF(REPLACE(r.deposit, ',', ''), '')::numeric, 0) AS deposit_amount,
    -- Temporal Scheduling Signals
    EXTRACT(DOW FROM r.pickup_datetime::timestamp)::int AS pickup_day_of_week,
    EXTRACT(HOUR FROM r.pickup_datetime::timestamp)::int AS pickup_hour,
    EXTRACT(MONTH FROM r.pickup_datetime::timestamp)::int AS pickup_month,
    CASE 
        WHEN EXTRACT(DOW FROM r.pickup_datetime::timestamp) IN (0, 6) THEN 1 
        ELSE 0 
    END AS is_weekend_pickup
FROM fact_reservations r
WHERE r.reservation_id != 'R/A #'
  AND r.pickup_datetime ~ '^\d'
  AND r.return_datetime ~ '^\d'
  AND r.booking_status IN ('Completed', 'Cancelled', 'No-Show');



  SELECT * FROM ml_demand_leakage_features;