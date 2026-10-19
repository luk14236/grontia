-- Fails when the model has fewer than min_rows rows (catches a silently
-- empty or truncated load that every column test would happily pass).
{% test expect_min_row_count(model, min_rows) %}

select n_rows
from (select count(*) as n_rows from {{ model }})
where n_rows < {{ min_rows }}

{% endtest %}
