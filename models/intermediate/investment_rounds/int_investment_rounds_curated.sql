-- Investment rounds represent discrete equity financing events for a company
-- Business Logic: One instrument (equity position) can have MULTIPLE investment rounds
--   - Initial investment (Series A, B, C, etc.)
--   - Follow-on investments in subsequent rounds
--   - Each round captures: date, stake acquired, share class, number of shares
--
-- Implementation demonstrates multiple rounds per instrument:
--   - PM source provides detailed round-by-round data (pm_investment_rounds)
--   - Multiple rounds for same instrument are supported via surrogate key (instrument_id + date)
--   - Examples: TechFlow has Series A + Series B, CloudSoft has Series B + Series C, BioTech has Series A + A-1

with pm_rounds as (
    select
        round_id,
        investment_code,
        round_name,
        round_date,
        round_amount,
        round_currency,
        stake_acquired,
        share_class,
        number_of_shares,
        round_description,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__investment_rounds') }}
    where round_id is not null
),

-- Resolve instrument_id through xref (using investment_code)
instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTMENT'
),

-- Resolve share_class_id through xref if available
share_class_xref as (
    select
        source_system,
        source_id,
        canonical_id as share_class_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'SHARE_CLASS'
),

investment_rounds as (
    select
        -- Surrogate key supports multiple rounds per instrument (instrument_id + date)
        -- This allows multiple rounds with different dates to coexist for same instrument
        {{ dbt_utils.generate_surrogate_key(['instrument_xref.instrument_id', 'pm_rounds.round_date']) }} as investment_round_id,
        instrument_xref.instrument_id,
        pm_rounds.round_date as date,
        pm_rounds.round_description as description,
        pm_rounds.number_of_shares as number_of_shares_acquired,
        share_class_xref.share_class_id,  -- Will be null if no xref mapping exists
        pm_rounds.stake_acquired as acquired_stake,
        pm_rounds.created_date as created_at,
        pm_rounds.last_modified_date as updated_at
    from pm_rounds
    inner join instrument_xref
        on pm_rounds.investment_code = instrument_xref.source_id
        and instrument_xref.source_system = 'PM'
    left join share_class_xref
        on pm_rounds.share_class = share_class_xref.source_id
        and share_class_xref.source_system = 'PM'
)

select * from investment_rounds
