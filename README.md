# AMOS

[Overview](https://github.com/open-amos/) | [Starter](https://github.com/open-amos/starter) | [Core](https://github.com/open-amos/core) | **Source Example** | [Dashboard](https://github.com/open-amos/dashboard-example)

---

# AMOS Source Example

AMOS Source Example is a collection of sample data and connectors (source‑aligned models) that demonstrate how to map operational systems to AMOS Core. It is used by [AMOS Starter](../starter) to build the end-to-end pipeline—from source systems to analytics-ready data.

![image](https://img.shields.io/badge/version-0.1.0-blue?style=for-the-badge) ![image](https://img.shields.io/badge/status-proof--of--concept-yellow?style=for-the-badge) ![image](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)

## Quick Start

Install and run [Amos Starter](https://github.com/open-amos/starter) (recommended) or add as a dependency to your dbt project.

## Contents

- **Data Seeds**: Sample data for common operational systems: CRM, fund admin, portfolio management, and accounting systems.
- **Models**: Source-aligned *staging* and *intermediate* models that demonstrate how to map operational systems to AMOS Core.
- **Documentation and tests** for the example models.

### Data Seeds

Seeds are stored in the `seeds` directory. The directory is organized by system and contains the following tables:

- **CRM**: Companies, opportunities, stages, countries.
- **Fund Admin**: Funds, investors, capital calls, distributions, expenses, fees, NAV fund, and NAV investment.
- **Portfolio Management**: Instruments, valuations, facilities, company financials, countries, industries.
- **Accounting**: Accounting journal entries.
- **Reference**: Reference data for industry and investor type categories. Also includes the cross-reference entities table—the table that maps various source system IDs to canonical IDs, e.g. to reconcile companies from the CRM and PM systems.

### Staging Models

Staging models are stored in the `models/staging` directory. Staging models are used for lightweight transformations and validation of the source data. They are organized by system and map to the source tables.

### Intermediate Models

Intermediate models are stored in the `models/intermediate` directory. They are used for transformations mapping source data to AMOS Core entities. They also conduct entity resolution via the cross-reference entities table.

Intermediate models are organized by entity and include companies, funds, investors, instruments, transactions, facilities, loans, share_classes, commitments, opportunities, snapshots, and cashflows.

## Customization

Create a new dbt project, add AMOS Core as a dependency, and use AMOS Source Example as a template to create connectors for your own source systems in a new package.

## Contributing

AMOS is open source and welcomes contributions. Report bugs, suggest features, add integration patterns, or submit pull requests.

## Support

- **Documentation**: [docs.amos.tech](https://docs.amos.tech)
- **Issues**: GitHub Issues

## Related Projects
- **[AMOS Starter](../starter)** - Coordinator and entry point
- **[AMOS Core](../core)** - Canonical dimensional model
- **[AMOS Dashboard](../dashboard-example)** - Example analytics and KPI dashboards built on AMOS Core

## Licensing

This subproject is part of the AMOS public preview. Licensing terms will be finalized before version 1.0.
For now, the code is shared for evaluation and feedback only. Commercial or production use requires written permission from the maintainers.
