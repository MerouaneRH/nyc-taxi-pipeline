# 0004. Streamlit over Tableau

Status: Accepted

## Context and problem

The mart needs a front end that can be run and reviewed from the repository.

## Options

- Tableau (licence, desktop tool, workbook not diffable).
- Streamlit (Python, code in the repo, runs in Docker).

## Decision

Use Streamlit, reading `mart_daily_zone_revenue` through Athena with the same AWS profile.

## Consequences

- The app is reviewed like other code and can run in Compose.
- State: the app exists in `app/` (`main.py`, `data.py`). Run it with `uv run --group app streamlit run app/main.py`. The compose service is commented out; `docker/streamlit.Dockerfile` is there for it.
- Each query the app sends to Athena costs scanned bytes. Caching is the app's responsibility.
