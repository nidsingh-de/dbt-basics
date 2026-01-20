{% macro run_validation() %}
    {% set run_validation = env_var('RUN_PREVENTIVE_CHECKS', 'true') %}
    {% if run_validation == 'true' %}
        {% set rule_config = var('config') %}
        {% set entity_rules = rule_config.get('rules') %}
        {% set batch_run_id = invocation_id %}
        {% set execution_ts = modules.datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ") %}
        {% set batch_run_id = invocation_id %}
        {% set error_list = [ ] %}
        {% set glob = {"control_status":"pass"} %}
        {% for entity_rule in entity_rules.items() %}
            {% set is_enabled = entity_rule[1].get('enabled') %}
            {% if is_enabled %}
                    {% set record_count_query = format_sql(entity_rule[1].get('total_record_count_sql'),"") %}
                    {% if execute %}
                          {% set res = run_query(record_count_query) %}
                    {% endif %}
                    {% set __= glob.update({"control_status":"pass"}) %}
                    {% for i in range(res.rows|length) %}
                        {% set total_record_count = res[i][1] %}
                        {% set file_name = res[i][0] %}
                        {% set control_query = "select count(*) as errors, GENERATE_UUID() as validation_output_id from ("+entity_rule[1].get('sql_expr')+") as check" %}
                        {% set rule_execution_context = {
                                                "validation_query": entity_rule[1].get('sql_expr'),
                                                "control_code": entity_rule[1].get('audit_code'),
                                                "description": entity_rule[1].get('control_number_description'),
                                                "total_count": total_record_count|int,
                                                "entity" : entity_rule[1].get('sub_entity'),
                                                "file_name": file_name,
                                               "control_timestamp": res[i][2]|string+" 00:00:00"
                                             }
                                        %}
                        {% if execute %}
                            {% set results = run_query(format_sql(control_query,file_name)) %}
                        {% endif %}
                        {% if results[0][0] != 0 %}
                              {% set __= glob.update({"control_status":"fail"}) %}
                        {% endif %}

                        {% do rule_execution_context.update({
                                                    "error_count": results[0][0]|int,
                                                    "event_id": results[0][1]
                                                    }) %}
                        {{ generate_control_json_logs(rule_execution_context)}}

                    {% endfor %}

                    {% if glob.get("control_status") == "fail" %}
                        {% set __ = error_list.append(1) %}
                    {% endif %}

            {% endif %}
        {% endfor %}

        {% set error_count = error_list | length %}

        {% if error_count != 0 %}
           {% do log("Total Failed rules: " ~ error_count, info=True) %}
        {% else %}
           {% do log("All rules passed.", info=True) %}
        {% endif %}

    {% endif %}
{% endmacro %}


{% macro format_sql(sql_expr,file_name,last_successful_check_time) %}
     {{ sql_expr | replace('{project}', target.project) | replace('{dataset}', target.dataset) | replace('{file_name}',"'"+file_name+"'") }}
{% endmacro%}