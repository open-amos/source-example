### AMOS Source Example (dbt project)

AMOS helps investment managers unify data across CRM, portfolio management, fund administration, and finance to produce a consistent analytics layer. This example project ships realistic sample data and source‑aligned models that demonstrate how to connect your systems to AMOS Core.

### Related Projects

- **[AMOS Core](../amos_core/README.md)** - Canonical dimensional model
- **[AMOS Starter](../starter)** – Orchestrator and entry point

### The problem this addresses

- Source schemas vary by vendor and implementation, making joins brittle
- Key business entities (funds, portfolios, deals, entities) lack stable IDs
- Recreating transformations per team causes drift and slows delivery

### How it works in this repo

1. Seeds provide realistic CSVs under `seeds/`
2. Staging models align raw sources to a common shape (`models/staging`)
3. Intermediate models normalize and prepare for AMOS Core (`models/intermediate`)
4. AMOS Core consumes these to build curated marts and metrics

### Quickstart

```bash
cd amos_source_example
dbt deps
dbt seed
dbt build
```

### What’s inside

- **Seeds**: realistic sample CSVs under `seeds/`
- **Staging**: source‑aligned models in `models/staging`
- **Intermediate**: normalized transforms in `models/intermediate`

### When to use this project

- To understand the source‑to‑core mapping patterns
- To prototype your own adapters before connecting production sources

### Docs

See the full documentation and guides at [docs.amos.tech](https://docs.amos.tech).


