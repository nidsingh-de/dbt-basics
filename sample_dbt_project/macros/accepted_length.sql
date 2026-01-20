{% test accepted_length(model, column_name,length) %}

with validation_errors as (

    select
        {{ column_name }} as even_field

    from {{ model }}
    where length({{column_name}}) != {{length}}

)
select *
from validation_errors

{% endtest %}