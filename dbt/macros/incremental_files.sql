{# Each mart owns its checkpoint: a failed model cannot acknowledge another model's work. #}
{% macro cdc_file_ledger() %}
    {{ return(api.Relation.create(database=target.database, schema='marts', identifier='processed_files')) }}
{% endmacro %}

{% macro cdc_initialize_ledger() %}
    -- Run once before dbt workers start, avoiding concurrent table creation.
    create schema if not exists marts;
    create table if not exists {{ cdc_file_ledger() }} (
        model_name varchar(128) not null,
        source_file varchar(2048) not null
    );
{% endmacro %}

{% macro cdc_temp(suffix) %}
    {{ return(adapter.quote(this.identifier ~ '__' ~ suffix)) }}
{% endmacro %}

{% macro cdc_changed_keys(table, key) %}
    select distinct {{ key }} from {{ source('raw', table) }}
    where _source_file in (select source_file from {{ cdc_temp('pending') }})
{% endmacro %}

{% macro cdc_prepare(table, key, include_details=false) %}
    -- Acquire before reading checkpoints; hold through model writes and commit.
    -- Mart transactions take turns, even when dbt has multiple worker threads.
    lock table {{ cdc_file_ledger() }};
    {% if not is_incremental() %}
        -- First build/full refresh bootstraps the checkpoint in the same transaction.
        delete from {{ cdc_file_ledger() }} where model_name = '{{ this.identifier }}';
    {% endif %}
    drop table if exists {{ cdc_temp('pending') }};
    create temporary table {{ cdc_temp('pending') }} as
        select source_file from {{ source('raw', 'loaded_files') }} f
        where not exists (
            select 1 from {{ cdc_file_ledger() }} p
            where p.model_name = '{{ this.identifier }}' and p.source_file = f.source_file
        );
    {% if is_incremental() %}
        drop table if exists {{ cdc_temp('keys') }};
        create temporary table {{ cdc_temp('keys') }} as
            {{ cdc_changed_keys(table, key) }}
            {% if include_details %}
                {% for detail, detail_key in [('order_items', 'order_item_key'), ('order_payments', 'payment_key')] %}
                union
                -- Include every observed parent of changed details, including deleted rows.
                select distinct order_id from {{ source('raw', detail) }}
                where {{ detail_key }} in ({{ cdc_changed_keys(detail, detail_key) }})
                {% endfor %}
            {% endif %}
        ;
        -- delete+insert alone cannot remove a deleted entity with no replacement row,
        -- or history versions made obsolete by a late event. Replace the whole entity.
        delete from {{ this }} where {{ key }} in (select {{ key }} from {{ cdc_temp('keys') }});
    {% endif %}
{% endmacro %}

{% macro cdc_filter(key) %}
    {% if is_incremental() %}
        where {{ key }} in (select {{ key.split('.')[-1] }} from {{ cdc_temp('keys') }})
    {% endif %}
{% endmacro %}

{% macro cdc_acknowledge() %}
    -- Only acknowledge the frozen input list, never files arriving during the build.
    insert into {{ cdc_file_ledger() }} (model_name, source_file)
        select '{{ this.identifier }}', source_file from {{ cdc_temp('pending') }};
{% endmacro %}
