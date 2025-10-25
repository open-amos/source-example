with companies_resolved as (
    select
        company_id,
        source_system,
        source_id
    from {{ ref('int_companies_resolved') }}
),

funds_resolved as (
    select
        fund_id,
        source_system,
        source_id
    from {{ ref('int_funds_resolved') }}
),

share_classes as (
    select
        share_class_id,
        company_id,
        name
    from {{ ref('int_share_classes_curated') }}
),

-- Get fund xref for PM source (since PM investments reference fund_id directly)
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

-- Extract shareholder information from PM investments
-- We can infer fund shareholders from investment ownership
pm_fund_shareholders as (
    select
        {{ dbt_utils.generate_surrogate_key(['cr.company_id', 'coalesce(fx.fund_id, inv.fund_id)']) }} as shareholder_id,
        cr.company_id,
        funds.name as shareholder_name,
        'FUND' as type,
        -- Calculate number of shares based on ownership percentage
        -- This is a placeholder calculation since we don't have actual share counts
        null as number_of_shares,
        sc.share_class_id,
        true as affiliated_entity,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from {{ ref('stg_pm__investments') }} inv
    inner join companies_resolved cr
        on cr.source_system = 'PM'
        and cr.source_id = inv.company_id
    -- Try to resolve fund_id through xref, otherwise use direct fund_id
    left join fund_xref fx
        on fx.source_system = 'PM'
        and fx.source_id = inv.fund_id
    inner join {{ ref('int_funds_unified') }} funds
        on coalesce(fx.fund_id, inv.fund_id) = funds.fund_id
    left join share_classes sc
        on cr.company_id = sc.company_id
        -- Match to the appropriate share class based on liquidation preference
        and (
            (inv.liquidation_preference is not null and sc.name like 'Preferred%')
            or (inv.liquidation_preference is null and sc.name = 'Common')
        )
    where inv.company_id is not null
        and inv.fund_id is not null
)

select * from pm_fund_shareholders
