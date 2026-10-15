-- Livability per region and measurement year, with the national percentile
-- among regions of the same level (buurt / wijk / gemeente) and the change
-- since the previous measurement and since 2014 (first year with dimensions).
with scores as (
    select * from {{ ref('stg_leefbaarometer_scores') }}
),

ranked as (
    select
        *,
        round(100 * percent_rank() over (
            partition by region_level, year order by livability_score
        ), 1)                                                   as national_percentile,
        livability_score - lag(livability_score) over (
            partition by region_code order by year
        )                                                       as change_since_previous,
        lag(year) over (partition by region_code order by year) as previous_year,
        livability_score - max(case when year = 2014 then livability_score end) over (
            partition by region_code
        )                                                       as change_since_2014,
        year = max(year) over ()                                as is_latest
    from scores
)

select * from ranked
