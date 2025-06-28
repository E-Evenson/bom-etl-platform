## Database Model

This lays out SQL Schema used for the ETL Pipeline. It includes staging, transformation views, and final materialized tables for reporting and downstream consumption.

It is intended to live in a named schema (e.g. department_schema.example_bom_staging)

### Schema

| Table Name                     | Description                                                  |
|--------------------------------|--------------------------------------------------------------|
| `example_bom_staging`          | Cleaned and transformed data from BOMs                       |
| `vw_example_bom_with_item_ids` | BOMs with item_ids mapped to pieces                          |
| `example_bom_final`            | Materialized from view, overwritten during full scrape       |
| `example_bom_final_history`    | Append only table, captures all BOM uploads over time        |
| `vw_example_bom_final_current` | Identifies most recent BOM per PON from final_history        |
| `example_bom_final_current`    | Materialized from current view                               |
| `item_id_reference`            | Lookup table for item ids with material types and dimensions |
