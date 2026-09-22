-- Track observed attribute changes, including tombstones. This cannot reconstruct
-- patient attributes from before the initial snapshot.
-- Snapshot timestamps are transfer times, not source commit times. If CDC has
-- already begun for this patient, that snapshot cannot establish an earlier
-- history version: it might already contain one of the later changes. Retain
-- it in raw/current-state processing, but start history with the captured CDC.
with timed_events as (
    select *, min(case when not _is_snapshot then _commit_at end)
        over (partition by patient_id) as first_cdc_at
    from {{ ref('stg_patients') }}
), history_events as (
    select * from timed_events
    where not _is_snapshot or first_cdc_at is null or _commit_at < first_cdc_at
), fingerprints as (
    select *, md5(
        {% for column in ['birth_date', 'death_date', 'gender', 'city', 'state', 'postal_code'] %}
        coalesce(cast(length(cast({{ column }} as varchar)) as varchar) || ':' || cast({{ column }} as varchar), '-1:') ||
        {% endfor %}
        case when _op = 'D' then 'deleted' else 'present' end
    ) as attribute_hash
    from history_events
), previous as (
    select *, lag(attribute_hash) over (partition by patient_id order by _source_order) as previous_hash
    from fingerprints
), changes as (
    select * from previous where previous_hash is null or previous_hash <> attribute_hash
)
select md5(patient_id || ':' || cast(_source_order as varchar(35))) as patient_version_id,
       patient_id, birth_date, death_date, gender, city, state, postal_code,
       _commit_at as observed_from,
       lead(_commit_at) over (partition by patient_id order by _source_order) as observed_to,
       _source_order as source_order_from,
       lead(_source_order) over (partition by patient_id order by _source_order) as source_order_to,
       _op = 'D' as is_deleted,
       _is_snapshot as is_initial_snapshot
from changes
