# 0006. CI without AWS secrets (parse only)

Status: Accepted

## Context and problem

Pull-request CI that holds AWS keys adds credential exposure and cost risk on a personal AWS account.

## Options

- CI with AWS credentials running `dbt build`.
- CI with no credentials: lint, unit tests, DAG import, `dbt parse`.

## Decision

`.github/workflows/ci.yml` runs on pull requests with `permissions: contents: read` and no secrets. Jobs:

- `lint-test`: ruff and pytest, excluding `tests/dags`.
- `dag-tests`: Airflow 2.10.5 with its constraint file, running `tests/dags`. Separate job because project dependencies conflict with the constraint file.
- `dbt-parse`: `dbt deps` and `dbt parse` with placeholder environment values.

## Consequences

- Broken YAML, refs, config and Jinja are caught. SQL errors, type errors and failing data tests are not.
- Model correctness is checked only by manual runs against Athena.
- A later option is OIDC role assumption with a restricted workgroup and a scan limit. Not implemented.
