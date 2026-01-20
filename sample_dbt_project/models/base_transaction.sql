{{
    config(
        materialized='incremental',
        unique_key= 'txn_id'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['metadata','transaction_code','portfolio_id','loan_id','processed_file_name'])}} as txn_id,
    metadata,
    transaction_code,
    portfolio_id,
    loan_id,
    currency_code,
    principal_amount,
    {{ format_object('bank_details','=') }}  as bank,
    processed_file_name,
    CURRENT_TIMESTAMP as processed_ts
from {{ source('input_set','raw_transaction')}}
