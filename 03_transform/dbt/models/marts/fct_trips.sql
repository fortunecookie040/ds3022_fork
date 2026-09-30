-- Fact table: one row per trip, with derived metrics computed once
-- here so every downstream model/query reuses the same definitions.

with trips as (
    select * from {{ ref('stg_yellow_tripdata') }}
),

derived as (
    select
        *,
        date_diff('second', pickup_at, dropoff_at) / 60.0   as trip_duration_minutes,
        trip_distance_miles
            / nullif(date_diff('second', pickup_at, dropoff_at) / 3600.0, 0)
                                                             as avg_speed_mph,
        case
            when fare_amount > 0 then tip_amount / fare_amount
            else null
        end                                                  as tip_pct,
        date_trunc('day', pickup_at)                        as pickup_date
    from trips
)

select *
from derived
-- guard against timestamp glitches producing absurd durations
where trip_duration_minutes between 1 and 180
