-- SCD Type 2 over the ticker universe.
--
-- Why history matters here: on 2026-09-16 ANSS left the universe (Ansys was
-- acquired by Synopsys; SNPS was already tracked). A Type 1 dimension would
-- overwrite that, and "which tickers were we tracking on 2026-08-01?" would
-- have no answer. The snapshot records a validity window per version of each
-- row instead.
--
-- ANSS stays in the seed with is_active=false rather than being deleted: a
-- changed row is a new version; a deleted row needs hard-delete handling,
-- which is easier to get wrong.
{% snapshot ticker_universe_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker',
        strategy='check',
        check_cols=['is_active'],
    )
}}

select ticker, is_active from {{ ref('ticker_universe') }}

{% endsnapshot %}
