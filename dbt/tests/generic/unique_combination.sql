-- Grain test: fails with one row per duplicated key combination.
-- Written here rather than pulled from dbt_utils so the project needs no packages.
{% test unique_combination(model, columns) %}

select {{ columns | join(', ') }}, count(*) as n_rows
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
