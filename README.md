# Tourism ESG & Carbon Emissions Analytics Pipeline

An end-to-end data engineering project built on **Databricks** to analyze the environmental impact of global tourism, with a focus on carbon emissions, transportation patterns, and sustainable travel choices.

The project implements a **Medallion Architecture (Bronze, Silver, Gold)** using Lakeflow Spark Declarative Pipelines, incremental ingestion, Change Data Capture (CDC), Slowly Changing Dimensions (SCD Type 2), data quality expectations, workflow orchestration, and CI/CD automation.

## Project Overview

The main objective is to build an automated and governed data pipeline that transforms raw tourism data into analytics-ready datasets for ESG reporting and business intelligence.

**Business Question:**

How do carbon emissions vary across transportation modes, travel destinations, and eco-friendly travel choices?

The project explores:
- Carbon emissions associated with tourism activities.
- Emissions per traveler and per traveler-night.
- Transportation modes and their ESG classifications.
- Tourism spending and environmental impact.
- Sustainable travel behavior across destinations.

## Dataset

The project uses the **Global Tourism Travel Trends** dataset, containing approximately 10,000 travel records and 33 attributes.

Relevant fields include:

- `trip_id`
- `year` and `month`
- `origin_country` and `destination_country`
- `transport_mode`
- `accommodation_type`
- `num_travelers`
- `carbon_footprint_kg_co2`
- `eco_friendly_choices`
- `total_trip_spend_usd`

The original dataset is ingested from a Unity Catalog Volume.

A separate synthetic CDC dataset is used to simulate changes in transportation ESG classifications. These classifications are created for demonstration purposes and should not be interpreted as verified environmental ratings.

## Architecture

```text
                    DATA SOURCES
                         |
          +--------------+--------------+
          |                             |
     Tourism CSV                 Transport CDC CSV
          |                             |
          v                             v
     BRONZE TRIPS                BRONZE TRANSPORT
          |                             |
          v                             v
     SILVER TRIPS                SILVER TRANSPORT
     Cleaning                    AUTO CDC / SCD2
     Expectations                Historical Versions
     Derived Metrics                    |
          |                             |
          +--------------+--------------+
                         |
                         v
                  GOLD ESG DATASET
               Materialized View
                         |
                         v
                  ESG ANALYTICS
                 AI/BI Dashboard
```

The pipeline is orchestrated using **Lakeflow Jobs** and deployed through **Databricks Asset Bundles**.

GitHub Actions manages automated validation, deployment, and execution across development and production environments.

## Data Processing Layers

### Bronze — Raw Data Ingestion

The Bronze layer ingests two data sources using Databricks Auto Loader:

**Tourism Trips**
- Original tourism travel records.
- Incremental ingestion from a Unity Catalog Volume.
- Ingestion metadata for traceability.

**Transport CDC**
- Synthetic events for transportation classifications.
- INSERT, UPDATE, and DELETE operations.
- Incremental processing of CDC batches.

### Silver — Data Transformation and CDC

The Silver layer prepares reliable datasets for analytics.

**Tourism Trips**
- Data cleaning and string standardization.
- Data quality expectations with warning and row-dropping behaviors.
- Derived carbon emission and tourism spending metrics.
- Emission segmentation for downstream analytics.

**Transport Dimension**
- CDC processing using `dp.create_auto_cdc_flow()`.
- Business key: `transport_mode`.
- SCD Type 2 historical tracking.
- Handling of updates and logical deletions.
- Historical version management using `__START_AT` and `__END_AT`.

### Gold — ESG Analytics

The Gold layer combines cleaned tourism records with the transportation dimension through a Materialized View.

The analytical dataset includes:

- Carbon emissions.
- Emissions per traveler.
- Emissions per traveler-night.
- Tourism spending.
- Spending per traveler.
- Transportation ESG categories.
- Destination and accommodation attributes.
- Eco-friendly travel choices.

A left join is used to preserve tourism records even when a transportation classification is not currently active.

## Data Quality

The declarative pipeline includes data quality expectations for:

- Missing trip identifiers.
- Invalid or missing traveler information.
- Carbon emission validation.

The rules demonstrate different quality management behaviors, including warning and dropping invalid records.

## Workflow Orchestration

A Lakeflow Job orchestrates the main pipeline and downstream validation tasks.

The workflow includes:

1. **Run ESG Pipeline:** Executes the declarative pipeline.
2. **Gold Validation:** Validates the number of records in the Gold dataset.
3. **Conditional Check:** Compares the record count against a configurable threshold.
4. **Dashboard Task:** Runs when the validation condition is satisfied.
5. **Validation Failure Task:** Handles unsuccessful validation outcomes.

The Job also defines environment parameters and email notifications for successful and failed executions.

## CI/CD with GitHub Actions

The project uses GitHub Actions and Databricks Asset Bundles for automated deployment.

Two Git branches represent the development and production release flow:

| Branch | Bundle Target | Unity Catalog Schema |
|---|---|---|
| `dev` | `development` | `tourism-esg-dev` |
| `main` | `production` | `tourism-esg-prod` |

Both targets use the `tourism-esg` catalog within the same Databricks workspace.

**Development workflow**

- Pull Request to `dev`: Bundle validation.
- Push or merge to `dev`: Validation, deployment, and Job execution.

**Production workflow**

- Pull Request to `main`: Bundle validation.
- Push or merge to `main`: Production deployment and Job execution.

Authentication for GitHub Actions is configured through OAuth machine-to-machine authentication using a Databricks Service Principal.

Credentials are managed through GitHub Actions Secrets and must never be committed to the repository.

## Repository Structure

```text
Tourism-ESG-Databricks/
|
|-- .github/
|   |-- workflows/
|       |-- dev.yml
|       |-- prod.yml
|
|-- resources/
|   |-- pipeline.yml
|   |-- jobs.yml
|
|-- src/
|   |-- pipeline/
|       |-- trips_bronze_jbolanos.py
|       |-- trips_silver_jbolanos.py
|       |-- transport_cdc_bronze_jbolanos.py
|       |-- transport_cdc_silver_jbolanos.py
|       |-- tourism_esg_gold_jbolanos.py
|
|-- README.md
|-- databricks.yml
```

Additional validation notebooks are referenced by the Lakeflow Job configuration.

## Deployment Instructions

### Prerequisites

- Databricks workspace with Unity Catalog.
- Databricks CLI installed and authenticated.
- Access to the required catalog and schemas.
- Unity Catalog Volumes for source datasets.
- GitHub repository and configured CI/CD credentials for automated deployment.

### 1. Clone the Repository

```bash
git clone https://github.com/Jdbc2003/Tourism-ESG-Databricks.git
cd Tourism-ESG-Databricks
```

### 2. Authenticate to Databricks

For local development:

```bash
databricks auth login --host https://YOUR-WORKSPACE-URL
```

### 3. Prepare Source Data

Upload the original tourism CSV to the appropriate environment's `raw_data` Volume.

Upload only the first synthetic transport CDC batch to the `cdc_files` Volume before the initial CDC test.

The current pipeline expects:

```text
/Volumes/<catalog>/<schema>/raw_data/
/Volumes/<catalog>/<schema>/cdc_files/
```

Source files must be prepared separately for development and production.

### 4. Validate the Bundle

```bash
databricks bundle validate -t development
databricks bundle validate -t production
```

### 5. Deploy to Development

```bash
databricks bundle deploy -t development
```

### 6. Execute the Development Job

```bash
databricks bundle run Tourism_ESG_Orchestrator_jbolanos -t development
```

### 7. Test Incremental CDC

After recording the initial dimension state, upload the second transport CDC batch and run the pipeline again.

Verify:

- INSERT creates a new dimension record.
- UPDATE creates a new historical version.
- DELETE closes the active version.
- Previous versions remain available for historical queries.

Do not use a full refresh when demonstrating incremental CDC behavior.

### 8. Deploy to Production

After validating development and promoting the changes through GitHub:

```bash
databricks bundle deploy -t production
databricks bundle run Tourism_ESG_Orchestrator_jbolanos -t production
```

## Analytics and Governance

The project supports ESG analytics and dashboard consumption through Databricks AI/BI.

Unity Catalog is used to separate development and production datasets and manage access to the analytical resources.

The intended governance model exposes only the necessary production analytical outputs to business consumers, without granting access to internal Bronze and Silver processing tables.

## Technology Stack

- **Databricks**
- **Apache Spark / PySpark**
- **Delta Lake**
- **Lakeflow Spark Declarative Pipelines**
- **Auto Loader**
- **AUTO CDC / SCD Type 2**
- **Lakeflow Jobs**
- **Unity Catalog**
- **Databricks AI/BI**
- **Databricks Asset Bundles**
- **GitHub Actions**
- **Git**

## Project Scope

This project demonstrates the design of an end-to-end governed data engineering workflow, including incremental ingestion, historical dimension tracking, transformation, analytics, orchestration, and automated deployment.

Carbon emissions are analyzed using the values recorded in the source dataset. Synthetic ESG classifications and CDC events are included to demonstrate data engineering functionality rather than provide official environmental certifications or audited emissions measurements.

---

**Academic Project:** Data Engineering with Databricks — Universidad Latina de Costa Rica.