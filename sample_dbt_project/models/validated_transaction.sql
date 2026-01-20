{{
    config(
        materialized='incremental',
        unique_key = 'txn_id'
    )
}}


with file_metadata as (
    select SUBSTR(metadata, LENGTH(metadata) - 8) as FILE_DATE,
            processed_ts,
            processed_file_name
    from {{ ref('base_transaction') }} raw
    where raw.metadata like 'HDR%'
    {% if is_incremental() %}
        and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
    {% endif %}
),
rule_invalid_currency_code as (
    select distinct(raw.currency_code) as currency_code,
           'FAILED' as validation_status,
           'Invalid Currency_code.' as validation_reason
      from {{ ref('base_transaction') }} raw
      left join {{ source('reference_set','currency_reference')}} ref_currency
      on raw.currency_code = ref_currency.code
    where ref_currency.code is null and raw.metadata is null
    {% if is_incremental() %}
        and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
    {% endif %}
),
rule_incorrect_principal_amount as (
     select distinct(concat(transaction_code,portfolio_id,loan_id,processed_file_name)) as loan_pk,
               'FAILED' as validation_status,
               'Invalid principal_amount.' as validation_reason
     from {{ ref('base_transaction') }} raw
     where SAFE_CAST(raw.principal_amount AS NUMERIC) is null
     {% if is_incremental() %}
         and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
     {% endif %}
)

select
    txn_id,
    md.FILE_DATE as file_date,
    transaction_code,
    portfolio_id,
    loan_id,
    raw.currency_code,
    principal_amount,
    bank,

CASE when rule_invalid_currency_code.validation_status = 'FAILED'
         or rule_incorrect_principal_amount.validation_status = 'FAILED'
         or rule_invalid_recordCount.validation_status = 'FAILED'
     then 'FAILED'
     else 'PASSED' end as validation_status,
CONCAT( IFNULL(rule_invalid_currency_code.validation_reason,''),
        IFNULL(rule_incorrect_principal_amount.validation_reason,''),
        IFNULL(rule_invalid_recordCount.validation_reason,'')
      ) as validation_reason,
      raw.processed_file_name,
      CURRENT_TIMESTAMP as processed_ts
from {{  ref('base_transaction')  }} raw
left join file_metadata md
    on raw.processed_file_name = md.processed_file_name
        and raw.processed_ts = md.processed_ts
left join rule_incorrect_principal_amount
      on rule_incorrect_principal_amount.loan_pk = CONCAT(raw.transaction_code,raw.portfolio_id,raw.loan_id,raw.processed_file_name)
left join rule_invalid_currency_code
    on rule_invalid_currency_code.currency_code = raw.currency_code
left join {{ ref('base_record_count') }} rule_invalid_recordCount
    on rule_invalid_recordCount.processed_file_name = raw.processed_file_name
where raw.metadata is null

{% if is_incremental() %}
    and raw.processed_ts > ( SELECT max(processed_ts) from {{ this }} )
{% endif %}

order by md.FILE_DATE