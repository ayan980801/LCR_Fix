# LCR ETL Pipeline

This repository contains the scripts used to load Lead Custody Repository data into Snowflake.

## Historical Load

Run the ingestion pipeline once with `historical_load=True` to perform a clean full load of the staging tables:

```
python ingest.py  # ensure `historical_load=True` in `main()` or when calling `process_table`
```

This truncates all `STG_LCR_*` tables before inserting the data. After the initial back-fill completes, set `historical_load=False` for daily incremental runs.
