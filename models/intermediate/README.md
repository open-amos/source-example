# Intermediate Layer

This folder contains intermediate models that apply business logic, entity resolution, and data enrichment.

## Architecture Decision: Reference Data

Reference dimensions (industries, investor_types, stages) **do not** have intermediate models because:

1. **No entity deduplication needed**: Each reference entity comes from a single authoritative source
2. **No business logic required**: Reference data is already clean after mechanical staging transformations
3. **No cross-reference resolution**: Reference entities don't need xref_entities mapping
4. **No enrichment needed**: Reference data is complete as-is

### Direct Staging-to-Marts Flow

The following entities skip the intermediate layer and marts select directly from staging:

- `dim_industries` → `stg_ref__industries`
- `dim_investor_types` → `stg_ref__investor_types`
- `dim_stages` → `stg_crm__stages`

In this project, intermediate models are for business logic and entity resolution - not needed for simple reference data.

## Entities with Intermediate Models

The following entities **do** require intermediate models:

- **companies**: Deduplication from CRM + PM sources, industry mapping
- **funds**: Entity resolution via xref_entities
- **investors**: Entity resolution via xref_entities, investor type mapping
- **instruments**: Entity resolution, country/industry allocations
- **transactions**: Classification, type mapping, enrichment
- **facilities**: Curation from multiple sources
- **loans**: Linking to facilities and instruments
- **counterparties**: Entity resolution
- **share_classes**: Curation and validation
- **commitments**: Aggregation and snapshot generation
- **investment_rounds**: Enrichment with share class data
- **opportunities**: Country and industry mapping
- **shareholders**: Curation and validation
- **snapshots**: Aggregation from multiple sources
- **cashflows**: Calculation and classification
