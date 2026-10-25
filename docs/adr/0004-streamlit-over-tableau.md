# 0004. Streamlit over Tableau

Status: Accepted

## Context and problem

The marts need a front end that anyone can run and review from the repository.

## Options

- Tableau (licence, desktop tool, workbooks that do not diff).
- Streamlit (Python, code in the repo, runs in Docker).

## Decision

Use Streamlit. The app queries the marts through Athena with the same AWS profile.

## Consequences

- The app is reviewed like other code and can run in Compose.
- The app lives in `app/` (`main.py`, `data.py`, `loaders.py`, `queries.py`, `views/`) and has seven views. It reads `mart_daily_zone_revenue`, `mart_hourly_borough`, `mart_od_pairs`, `mart_month_quality`, `dq_suspect_trips` and `mart_dq_summary`, and `fct_trips` for one heatmap.
- Run it with `uv run --group app streamlit run app/main.py`. `docker/streamlit.Dockerfile` is ready for Compose, but the Compose service is commented out.
- Each query the app sends to Athena costs scanned bytes. The app caches results for an hour and clears the cache when a new dbt build lands (`app/freshness.py`).
