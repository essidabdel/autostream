# AutoStream Data Catalog

## Purpose
Local Data Lake layout (bronze/silver/gold) with dated partitions and parquet outputs for analytics.

## Storage Zones

### Bronze (raw)
- Path: data/bronze/run_date=YYYY-MM-DD/
- Formats: JSON, CSV, SQLite
- Description: Raw ingested data, stored as-is for traceability.

### Silver (clean)
- Path: data/silver/telemetry_parquet/run_date=YYYY-MM-DD/event_date=YYYY-MM-DD/
- Format: Parquet
- Description: Cleaned telemetry with standardized types and dates.

### Gold (analytics)
- Path: data/gold/parquet/run_date=YYYY-MM-DD/reporting_final.parquet
- Format: Parquet
- Description: Feature-enriched dataset with predictions and statuses.

### Aggregations
- Path: data/gold/aggregations/
- Files:
  - pannes_par_jour_YYYY-MM-DD.parquet
  - pannes_par_semaine_YYYY-MM-DD.parquet
  - pannes_par_mois_YYYY-MM-DD.parquet

### Quality Reports
- Path: data/quality/
- Files:
  - silver_quality_YYYY-MM-DD.csv
  - gold_quality_YYYY-MM-DD.csv

## Schemas (expected)

### Gold - reporting_final.parquet
- vin: string
- modele: string
- temp_moyenne: double
- km_actuel: double
- pression_huile: double
- regime_moteur: double
- voltage_batterie: double
- km_depuis_revis: double
- score_risque: double
- statut: string
- type_panne_predit: int
- prob_panne: double

### Aggregations (daily/weekly/monthly)
- event_date / week_start / month_start: date
- vehicules_actifs: long
- pannes_predites: long
- prob_panne_moy: double
- temp_moy: double
- voltage_moy: double

## Notes
- The dashboard reads the latest run_date parquet in data/gold/parquet.
- Quality reports include missing columns, null counts, null percentages, and schema mismatches.
