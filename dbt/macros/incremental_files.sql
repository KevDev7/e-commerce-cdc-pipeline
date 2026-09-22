{# Each mart owns its checkpoint: a failed model cannot acknowledge another model's work. #}
{% macro cdc_file_ledger() %}
    {{ return(this.incorporate(path={'identifier': this.identifier ~ '__files'})) }}
{% endmacro %}

{% macro cdc_temp(suffix) %}
    {{ return(adapter.quote(this.identifier ~ '__' ~ suffix)) }}
{% endmacro %}

{% macro cdc_changed_keys(table, key) %}
    select distinct {{ key }} from {{ source('raw', table) }}
    where _source_file in (select source_file from {{ cdc_temp('pending') }})
{% endmacro %}

{% macro cdc_prepare(table, key, include_details=false) %}
    create table if not exists {{ cdc_file_ledger() }} (
        source_file varchar(2048) not null
    );
    {% if not is_incremental() %}
        -- First build/full refresh bootstraps the checkpoint in the same transaction.
        delete from {{ cdc_file_ledger() }};
    {% endif %}
    drop table if exists {{ cdc_temp('pending') }};
    create temporary table {{ cdc_temp('pending') }} as
        select source_file from {{ source('raw', 'loaded_files') }} f
        where not exists (
            select 1 from {{ cdc_file_ledger() }} p where p.source_file = f.source_file
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
                union
                -- Late customer history can change an existing order's version assignment.
                -- Include old observed customer links as well as the current link.
                select distinct order_id from {{ source('raw', 'orders') }}
                where customer_id in ({{ cdc_changed_keys('customers', 'customer_id') }})
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
    insert into {{ cdc_file_ledger() }} (source_file)
        select source_file from {{ cdc_temp('pending') }};
{% endmacro %}
