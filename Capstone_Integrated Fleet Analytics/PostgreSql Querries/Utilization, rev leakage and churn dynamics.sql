-- Query 1: Overall Demand & Churn Profile
SELECT 
    booking_status,
    COUNT(*) AS total_reservations,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_share
FROM fact_reservations
WHERE reservation_id != 'R/A #'
GROUP BY booking_status
ORDER BY total_reservations DESC;

--Query 2: Location Fulfillment & Leakage Matrix
SELECT 
    COALESCE(pickup_location, 'UNKNOWN') AS pickup_location,
    COUNT(*) AS total_bookings,
    COUNT(CASE WHEN booking_status = 'Completed' THEN 1 END) AS completed_count,
    COUNT(CASE WHEN booking_status = 'No-Show' THEN 1 END) AS noshow_count,
    COUNT(CASE WHEN booking_status = 'Cancelled' THEN 1 END) AS cancelled_count,
    ROUND(
        (COUNT(CASE WHEN booking_status IN ('No-Show', 'Cancelled') THEN 1 END) * 100.0) / COUNT(*), 
        2
    ) AS leakage_rate_pct
FROM fact_reservations
WHERE reservation_id != 'R/A #'
GROUP BY pickup_location
ORDER BY total_bookings DESC;

-- Query 3: Cancellation Lead Time Bins
SELECT 
    CASE 
        WHEN (pickup_datetime::timestamp - date_cancelled::timestamp) < INTERVAL '1 day' THEN '1. Under 24 Hours (Same Day)'
        WHEN (pickup_datetime::timestamp - date_cancelled::timestamp) BETWEEN INTERVAL '1 day' AND INTERVAL '3 days' THEN '2. 1 to 3 Days Notice'
        WHEN (pickup_datetime::timestamp - date_cancelled::timestamp) BETWEEN INTERVAL '3 days' AND INTERVAL '7 days' THEN '3. 3 to 7 Days Notice'
        ELSE '4. Over 7 Days Notice'
    END AS cancellation_notice_window,
    COUNT(*) AS total_cancellations,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_cancellations
FROM fact_reservations
WHERE booking_status = 'Cancelled'
  AND pickup_datetime IS NOT NULL 
  AND date_cancelled IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- Query 4: Top 10 Vehicle Classes by Lost Demand Volume
SELECT 
    vehicle_class,
    COUNT(CASE WHEN booking_status = 'No-Show' THEN 1 END) AS noshow_count,
    COUNT(CASE WHEN booking_status = 'Cancelled' THEN 1 END) AS cancelled_count,
    COUNT(*) AS total_lost_bookings,
    ROUND(SUM(NULLIF(REPLACE(rental_days, ',', ''), '')::numeric), 1) AS total_lost_rental_days
FROM fact_reservations
WHERE booking_status IN ('No-Show', 'Cancelled')
GROUP BY vehicle_class
ORDER BY total_lost_bookings DESC
LIMIT 10;


-- Query 5: Actively used models utilization as of 1.09.26
WITH vehicle_metrics AS (
    SELECT 
        v.vehicle_id,
        v.model,
        -- Calculate individual active lifespan up to September 1, 2026
        GREATEST(
            DATE '2026-09-01' - NULLIF(v.acquisition_date, '')::date, 
            1
        ) AS operating_lifespan_days,
        -- Count completed trips per vehicle
        COUNT(r.reservation_id) AS completed_trips,
        -- Sum total days on rent per vehicle
        COALESCE(
            SUM(
                CASE 
                    WHEN r.pickup_datetime IS NOT NULL AND r.return_datetime IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0 
                    ELSE 0 
                END
            ), 
            0
        ) AS vehicle_on_rent_days
    FROM dim_vehicles v
    LEFT JOIN fact_reservations r 
        ON v.vehicle_id = r.unit_id 
       AND r.booking_status = 'Completed'
    WHERE v.current_status != 'De-fleeted / Sold'
    GROUP BY v.vehicle_id, v.model, v.acquisition_date
)
SELECT 
    model,
    COUNT(vehicle_id) AS active_units,
    SUM(completed_trips) AS total_completed_trips,
    ROUND(SUM(vehicle_on_rent_days)::numeric, 1) AS total_on_rent_days,
    ROUND(SUM(operating_lifespan_days)::numeric, 0) AS total_operating_lifespan_days,
    ROUND(
        (SUM(vehicle_on_rent_days) * 100.0 / NULLIF(SUM(operating_lifespan_days), 0))::numeric, 
        2
    ) AS true_utilization_pct
FROM vehicle_metrics
GROUP BY model
ORDER BY active_units DESC, total_on_rent_days DESC;


-- Query 6: Actively used vehicles utilization as of 1.09.26
-- To prevent paperwork lags from pushing utilization past 100%
SELECT 
    v.vehicle_id,
    v.make,
    v.model,
    v.model_year,
    v.current_status,
    COUNT(r.reservation_id) AS total_completed_trips,
    ROUND(
        COALESCE(
            SUM(
                CASE 
                    WHEN r.pickup_datetime IS NOT NULL AND r.return_datetime IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0 
                    ELSE 0 
                END
            ), 
            0
        )::numeric, 
        1
    ) AS total_on_rent_days,
    -- Takes the earlier of acquisition_date or first reservation date
    GREATEST(
        DATE '2026-09-01' - LEAST(
            NULLIF(v.acquisition_date, '')::date,
            MIN(r.pickup_datetime::date)
        ), 
        1
    ) AS operating_lifespan_days,
    -- Cap at 100.00% to reflect perfect continuous utilization
    LEAST(
        ROUND(
            (
                COALESCE(
                    SUM(
                        CASE 
                            WHEN r.pickup_datetime IS NOT NULL AND r.return_datetime IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (r.return_datetime::timestamp - r.pickup_datetime::timestamp)) / 86400.0 
                            ELSE 0 
                        END
                    ), 
                    0
                ) * 100.0 / 
                NULLIF(
                    GREATEST(
                        DATE '2026-09-01' - LEAST(
                            NULLIF(v.acquisition_date, '')::date,
                            MIN(r.pickup_datetime::date)
                        ), 
                        1
                    ), 
                    0
                )
            )::numeric, 
            2
        ),
        100.00
    ) AS true_utilization_pct
FROM dim_vehicles v
LEFT JOIN fact_reservations r 
    ON v.vehicle_id = r.unit_id 
   AND r.booking_status = 'Completed'
WHERE v.current_status != 'De-fleeted / Sold'
GROUP BY 
    v.vehicle_id, 
    v.make, 
    v.model, 
    v.model_year, 
    v.current_status, 
    v.acquisition_date
ORDER BY true_utilization_pct DESC;
