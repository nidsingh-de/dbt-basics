{% macro format_object(column, assignment_operator) %}

    PARSE_JSON(REPLACE(
                    REPLACE(
                            REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE({{column}},', ',','),
                                        '"',''),
                                    "{",'{"'),
                            "}",'"}' ),
                    "{{assignment_operator}}",'":"'),
                 ',','","')
                )


{% endmacro %}