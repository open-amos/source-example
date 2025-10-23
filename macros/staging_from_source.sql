{% macro staging_from_source(source_name, table_name) %}
{%- set source_relation = source(source_name, table_name) -%}

{# Find the source metadata from the graph - only during execution #}
{%- if execute -%}
    {%- set source_meta = none -%}
    {%- for source_node in graph.sources.values() -%}
        {%- if source_node.source_name == source_name and source_node.name == table_name -%}
            {%- set source_meta = source_node -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if not source_meta -%}
        {{ exceptions.raise_compiler_error("Source '" ~ source_name ~ "." ~ table_name ~ "' not found in project") }}
    {%- endif -%}

    {# Extract metadata configuration #}
    {%- set add_metadata = source_meta.meta.get('add_metadata', false) -%}
    {%- set column_transformations = source_meta.meta.get('column_transformations', {}) -%}
    {%- set column_mappings = source_meta.meta.get('column_mappings', {}) -%}

    {# Get columns from the source relation #}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}

    select
        {%- for column in columns %}
        {%- set column_name = column.name | lower -%}
        {%- set target_column_name = column_mappings.get(column_name, column_name) -%}
        
        {%- if column_name in column_transformations %}
        {{ column_transformations[column_name] }} as {{ target_column_name }}
        {%- else %}
        {{ column_name }} as {{ target_column_name }}
        {%- endif -%}
        
        {%- if not loop.last or add_metadata %},{% endif %}
        {%- endfor %}
        
        {%- if add_metadata %}
        '{{ source_name | upper }}' as _source_system,
        current_timestamp as _source_loaded_at
        {%- endif %}

    from {{ source_relation }}
{%- else -%}
    {# During parse phase, return a placeholder query #}
    select
        *,
        '{{ source_name | upper }}' as _source_system,
        current_timestamp as _source_loaded_at
    from {{ source_relation }}
{%- endif -%}

{% endmacro %}
