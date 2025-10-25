with companies_resolved as (
    select
        company_id,
        source_system,
        source_id
    from {{ ref('int_companies_resolved') }}
),

-- Extract share class information from PM investments
-- Note: PM investments don't explicitly have share class data in the seed,
-- but we can infer share classes from investment types and liquidation preferences
pm_share_classes as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['cr.company_id', 'coalesce(inv.liquidation_preference, \'COMMON\')']) }} as share_class_id,
        cr.company_id,
        case
            when inv.liquidation_preference is not null and inv.liquidation_preference != '' then
                case
                    -- Use descriptive names that reflect the actual preference structure
                    -- Since we don't have series info, use the preference type as the distinguisher
                    when inv.liquidation_preference like '%Participating%' then 'Participating Preferred'
                    when inv.liquidation_preference like '%Non-Participating%' then 'Non-Participating Preferred'
                    else 'Preferred Stock'
                end
            else 'Common Stock'
        end as name,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from {{ ref('stg_pm__investments') }} inv
    inner join companies_resolved cr
        on cr.source_system = 'PM'
        and cr.source_id = inv.company_id
    where inv.company_id is not null
)

select * from pm_share_classes
