-- Facility lenders (syndicate participants) curated from PM or fund admin sources
-- Note: Currently no facility lender source data exists in seeds
-- This model will return empty results until facility lender data is added

with pm_facility_lenders as (
    -- Placeholder: Extract facility lenders from PM system when available
    -- Expected columns: facility_id, counterparty_id, fund_id, syndicate_role,
    -- commitment_amount, allocation_pct, primary_flag
    -- Note: Exactly one of counterparty_id or fund_id must be non-null (XOR constraint)
    select
        cast(null as varchar(64)) as source_facility_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as source_counterparty_id,
        cast(null as varchar(64)) as source_fund_id,
        cast(null as varchar(64)) as syndicate_role,
        cast(null as numeric(24,2)) as commitment_amount,
        cast(null as decimal(7,4)) as allocation_pct,
        cast(null as boolean) as primary_flag,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

fund_admin_facility_lenders as (
    -- Placeholder: Extract facility lenders from fund admin system when available
    select
        cast(null as varchar(64)) as source_facility_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as source_counterparty_id,
        cast(null as varchar(64)) as source_fund_id,
        cast(null as varchar(64)) as syndicate_role,
        cast(null as numeric(24,2)) as commitment_amount,
        cast(null as decimal(7,4)) as allocation_pct,
        cast(null as boolean) as primary_flag,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

all_sources as (
    select * from pm_facility_lenders
    union all
    select * from fund_admin_facility_lenders
),

-- Resolve facility_id through xref
facility_xref as (
    select
        source_system,
        source_id,
        canonical_id as facility_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FACILITY'
),

-- Resolve counterparty_id through xref
counterparty_xref as (
    select
        source_system,
        source_id,
        canonical_id as counterparty_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COUNTERPARTY'
),

-- Resolve fund_id through xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

facility_lenders_resolved as (
    select
        facility_xref.facility_id,
        counterparty_xref.counterparty_id,
        fund_xref.fund_id,
        all_sources.syndicate_role,
        all_sources.commitment_amount,
        all_sources.allocation_pct,
        all_sources.primary_flag,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join facility_xref
        on all_sources.source_system = facility_xref.source_system
        and all_sources.source_facility_id = facility_xref.source_id
    left join counterparty_xref
        on all_sources.source_system = counterparty_xref.source_system
        and all_sources.source_counterparty_id = counterparty_xref.source_id
    left join fund_xref
        on all_sources.source_system = fund_xref.source_system
        and all_sources.source_fund_id = fund_xref.source_id
    -- Enforce XOR constraint: exactly one of counterparty_id or fund_id must be non-null
    where (counterparty_xref.counterparty_id is not null and fund_xref.fund_id is null)
       or (counterparty_xref.counterparty_id is null and fund_xref.fund_id is not null)
),

-- Map syndicate_role to canonical enum
-- Based on canonical DBML: 'AGENT', 'ARRANGER', 'LENDER', 'CO_LENDER', 'ADMIN_AGENT'
facility_lenders_typed as (
    select
        facility_id,
        counterparty_id,
        fund_id,
        case
            when upper(syndicate_role) = 'AGENT' then 'AGENT'
            when upper(syndicate_role) = 'ARRANGER' then 'ARRANGER'
            when upper(syndicate_role) = 'LENDER' then 'LENDER'
            when upper(syndicate_role) in ('CO_LENDER', 'CO-LENDER', 'CO LENDER') then 'CO_LENDER'
            when upper(syndicate_role) in ('ADMIN_AGENT', 'ADMIN AGENT', 'ADMINISTRATIVE AGENT') then 'ADMIN_AGENT'
            else null  -- syndicate_role is optional
        end as syndicate_role,
        commitment_amount,
        allocation_pct,
        primary_flag,
        created_date as created_at,
        last_modified_date as updated_at
    from facility_lenders_resolved
)

select * from facility_lenders_typed
