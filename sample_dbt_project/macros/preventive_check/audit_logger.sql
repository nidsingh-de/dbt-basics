{% macro generate_control_json_logs(rule_execution_context) %}

     {% set pass_count = rule_execution_context.get('total_count') | int - rule_execution_context.get('error_count') | int %}
     {% set log_message = {   "event_id": rule_execution_context.get('event_id') ,
                              "control_code": rule_execution_context.get('control_code') ,
                              "control_timestamp": rule_execution_context.get('control_timestamp'),
                              "pass_count": pass_count | int,
                              "fail_count": rule_execution_context.get('error_count') | int,
                              "file": rule_execution_context.get('file_name')
                           } %}

     {% set json_log_message = tojson(log_message) %}
     {% do log(json_log_message, info=True) %}

{% endmacro %}