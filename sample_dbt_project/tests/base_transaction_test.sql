{{ config(tags=['unit-test']) }}

{% set options = {"include_missing_columns": true, "input_format":"sql"} %}

{% call dbt_unit_testing.test('base_transaction') %}

  {% call dbt_unit_testing.mock_source('input_set','raw_transaction',options) %}
   select  'a' as transaction_code, 'b' as portfolio_id, 'c' as loan_id,'ABC' as currency_code, '134fgh' as principal_amount, 'xyz' as processed_file_name
      {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select  'a' as transaction_code, 'b' as portfolio_id, 'c' as loan_id,'ABC' as currency_code, '134fgh' as principal_amount, 'xyz' as processed_file_name
  {% endcall %}

{% endcall %}