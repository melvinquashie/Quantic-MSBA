-- Maintenance & Downtime Baselines.

-- Querry 1: Top maintenance expense categories
SELECT 
    COALESCE(service_type, 'UNSPECIFIED') AS service_type,
    COUNT(*) AS total_work_orders,
    ROUND(SUM(NULLIF(REPLACE(parts_cost, ',', ''), '')::numeric), 2) AS total_parts_cost,
    ROUND(SUM(NULLIF(REPLACE(labor_cost, ',', ''), '')::numeric), 2) AS total_labor_cost,
    ROUND(SUM(NULLIF(REPLACE(total_cost, ',', ''), '')::numeric), 2) AS total_spend,
    ROUND(AVG(NULLIF(REPLACE(total_cost, ',', ''), '')::numeric), 2) AS avg_cost_per_service
FROM fact_maintenance_logs
GROUP BY service_type
ORDER BY total_spend DESC
LIMIT 10

-- Querry 2: Active Fleet Maintenance Profile by Model
SELECT 
    v.model,
    COUNT(DISTINCT v.vehicle_id) AS active_vehicles,
    COUNT(m.ro_number) AS total_work_orders,
    ROUND(COALESCE(SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 0), 2) AS total_maintenance_spend,
    ROUND(
        COALESCE(
            AVG(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 
            0
        ), 
        2
    ) AS avg_cost_per_order,
    ROUND(
        COUNT(m.ro_number)::numeric / NULLIF(COUNT(DISTINCT v.vehicle_id), 0), 
        1
    ) AS avg_orders_per_vehicle,
    ROUND(
        COALESCE(SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 0) / 
        NULLIF(COUNT(DISTINCT v.vehicle_id), 0), 
        2
    ) AS avg_spend_per_vehicle
FROM dim_vehicles v
LEFT JOIN fact_maintenance_logs m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.current_status != 'De-fleeted / Sold'
GROUP BY v.model
ORDER BY total_maintenance_spend DESC;

-- Querry 3: Active Fleet Maintenance Profile per vehicle
SELECT 
    v.vehicle_id,
    v.make,
    v.model,
    v.model_year,
    v.current_status,
    COUNT(m.ro_number) AS total_work_orders,
    ROUND(COALESCE(SUM(NULLIF(REPLACE(m.parts_cost, ',', ''), '')::numeric), 0), 2) AS total_parts_spend,
    ROUND(COALESCE(SUM(NULLIF(REPLACE(m.labor_cost, ',', ''), '')::numeric), 0), 2) AS total_labor_spend,
    ROUND(COALESCE(SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 0), 2) AS total_maintenance_spend,
    ROUND(
        COALESCE(
            AVG(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 
            0
        ), 
        2
    ) AS avg_cost_per_order
FROM dim_vehicles v
LEFT JOIN fact_maintenance_logs m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.current_status != 'De-fleeted / Sold'
GROUP BY 
    v.vehicle_id, 
    v.make, 
    v.model, 
    v.model_year, 
    v.current_status
ORDER BY total_maintenance_spend DESC;

-- Query 4: Unscheduled Failures vs. PM Breakdown per model
SELECT 
    v.model,
    COUNT(CASE 
        WHEN UPPER(m.service_type) ~ '(OIL|FILTER|PLUG|SERVICE)' THEN 1 
    END) AS pm_orders_count,
    COUNT(CASE 
        WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' THEN 1 
    END) AS unscheduled_orders_count,
    ROUND(
        SUM(
            CASE 
                WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' 
                THEN NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric 
                ELSE 0 
            END
        ), 
        2
    ) AS unscheduled_repair_spend,
    ROUND(
        AVG(NULLIF(REPLACE(m.downtime_days, ',', ''), '')::numeric), 
        2
    ) AS avg_downtime_days
FROM dim_vehicles v
JOIN fact_maintenance_logs m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.current_status != 'De-fleeted / Sold'
GROUP BY v.model
ORDER BY unscheduled_repair_spend DESC;

-- Query 5: Unscheduled Failures vs. PM Breakdown for each active vehicle
SELECT 
    v.vehicle_id,
    v.make,
    v.model,
    v.model_year,
    v.current_status,
    -- Preventive Maintenance Counts & Spend (Oil, Filters, Plugs, Routine Servicing)
    COUNT(CASE 
        WHEN UPPER(m.service_type) ~ '(OIL|FILTER|PLUG|SERVICE)' THEN 1 
    END) AS pm_orders_count,
    ROUND(
        COALESCE(
            SUM(
                CASE 
                    WHEN UPPER(m.service_type) ~ '(OIL|FILTER|PLUG|SERVICE)' 
                    THEN NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric 
                    ELSE 0 
                END
            ), 
            0
        ), 
        2
    ) AS pm_spend,
    -- Unscheduled / Reactive Component Failures (Tyres, Brakes, A/C, Battery, Suspension, Belts)
    COUNT(CASE 
        WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' THEN 1 
    END) AS unscheduled_orders_count,
    ROUND(
        COALESCE(
            SUM(
                CASE 
                    WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' 
                    THEN NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric 
                    ELSE 0 
                END
            ), 
            0
        ), 
        2
    ) AS unscheduled_repair_spend,
    -- Total Lifetime Spend for the Active Asset
    ROUND(
        COALESCE(SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 0), 
        2
    ) AS total_maintenance_spend,
    -- Ratio of Spend Consumed by Reactive Failures
    ROUND(
        (
            COALESCE(
                SUM(
                    CASE 
                        WHEN UPPER(m.service_type) ~ '(TYRE|TIRE|BRAKE|A/C|BATTERY|SUSPENSION|CLUTCH|BELT)' 
                        THEN NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric 
                        ELSE 0 
                    END
                ), 
                0
            ) * 100.0 / 
            NULLIF(SUM(NULLIF(REPLACE(m.total_cost, ',', ''), '')::numeric), 0)
        )::numeric, 
        2
    ) AS unscheduled_spend_pct,
    -- Average Workshop Out-of-Service Turnaround (Days)
    ROUND(
        AVG(NULLIF(REPLACE(m.downtime_days, ',', ''), '')::numeric), 
        2
    ) AS avg_downtime_days
FROM dim_vehicles v
LEFT JOIN fact_maintenance_logs m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.current_status != 'De-fleeted / Sold'
GROUP BY 
    v.vehicle_id, 
    v.make, 
    v.model, 
    v.model_year, 
    v.current_status
ORDER BY unscheduled_repair_spend DESC;