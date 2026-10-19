-- Freshness check: fails when the most recent value of a date/timestamp column
-- is older than max_age_days. Used on ingestion_date (did the pipeline run?)
-- and on the measurement date itself (is the source still publishing?).
{% test expect_recent(model, column_name, max_age_days) %}

select
    max({{ column_name }})                                   as most_recent,
    current_date - max({{ column_name }})::date              as age_days
from {{ model }}
having max({{ column_name }}) is null
    or current_date - max({{ column_name }})::date > {{ max_age_days }}

{% endtest %}
