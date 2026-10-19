-- on-run-end hook: appends one row per executed node (model or test) to
-- <schema>_OBSERVABILITY.dbt_run_results, so data-quality history survives
-- across runs and can be queried next to the marts.
{% macro log_run_results(results) %}
    {% if not execute or results | length == 0 %}
        {{ return('') }}
    {% endif %}

    {% set relation = target.schema ~ '_OBSERVABILITY.dbt_run_results' %}

    {% set ddl %}
        create schema if not exists {{ target.schema }}_OBSERVABILITY;
        create table if not exists {{ relation }} (
            invocation_id    varchar,
            run_started_at   timestamp,
            target_name      varchar,
            unique_id        varchar,
            resource_type    varchar,
            node_name        varchar,
            status           varchar,
            failures         bigint,
            execution_time_s double,
            message          varchar
        );
    {% endset %}
    {% do run_query(ddl) %}

    {% set rows = [] %}
    {% for res in results %}
        {% set message = (res.message or '') | string | replace("'", "''") %}
        {% do rows.append(
            "('" ~ invocation_id ~ "', '" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "', '"
            ~ target.name ~ "', '" ~ res.node.unique_id ~ "', '" ~ res.node.resource_type ~ "', '"
            ~ res.node.name ~ "', '" ~ res.status ~ "', "
            ~ (res.failures if res.failures is not none else 'null') ~ ", "
            ~ (res.execution_time | round(3)) ~ ", '" ~ message[:1000] ~ "')"
        ) %}
    {% endfor %}

    {% do run_query('insert into ' ~ relation ~ ' values ' ~ rows | join(',\n')) %}
    {{ log('Logged ' ~ rows | length ~ ' run results to ' ~ relation, info=true) }}
{% endmacro %}
