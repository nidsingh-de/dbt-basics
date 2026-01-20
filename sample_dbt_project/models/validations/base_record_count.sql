{{
     config(
            materialized='ephemeral'
        )
}}

with file_trailer_rc as (
       select processed_file_name,processed_ts,
             CAST(REPLACE(metadata,'TLR','') AS INT64) as tlr_record_count
       from {{ ref('base_transaction') }} raw
       where raw.metadata like 'TLR%'
       {% if is_incremental() %}
           and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
       {% endif %}
     ),
file_content_rc as (
       select processed_file_name,processed_ts,
              count(*) as content_record_count
       from {{ ref('base_transaction')}} raw
       where raw.metadata is null
       {% if is_incremental() %}
           and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
       {% endif %}
       group by raw.processed_file_name, raw.processed_ts
)

select distinct(concat(ftr.processed_file_name,ftr.processed_ts)) as file_pk,
          ftr.processed_file_name, ftr.processed_ts,
          'FAILED' as validation_status,
          'Invalid record_count.' as validation_reason
   from file_content_rc fcr join file_trailer_rc ftr
   on fcr.processed_file_name = ftr.processed_file_name
      and fcr.processed_ts = ftr.processed_ts
   where content_record_count != tlr_record_count
   {% if is_incremental() %}
       and ftr.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
   {% endif %}
