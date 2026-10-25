# 0006. CI without AWS secrets on pull requests

Status: Accepted

## Context and problem

Pull-request CI that holds AWS keys exposes credentials and risks cost on a personal AWS account.

## Options

- CI with AWS credentials that runs `dbt build` on every pull request.
- Pull-request CI with no credentials (lint, unit tests, DAG import, `dbt parse`), plus a separate `dbt build` workflow that runs only on `main` and on manual dispatch, using OIDC.

## Decision

`.github/workflows/ci.yml` runs on pull requests with `permissions: contents: read` and no secrets. Its jobs:

- `lint-test`: ruff and pytest, without `tests/dags`.
- `dag-tests`: Airflow 2.10.5 with its constraint file, running `tests/dags`. This is a separate job because the project's dependencies conflict with the constraint file.
- `dbt-parse`: `dbt deps` and `dbt parse` with placeholder environment values.

`.github/workflows/dbt-build.yml` runs `dbt build` on Athena for pushes to `main` that change `dbt/**`, and for manual dispatch. It has `id-token: write`, assumes the role in the repository variable `AWS_DBT_ROLE_ARN` through GitHub OIDC (no stored keys), and uses the `ci` target in `dbt/profiles.yml`. A concurrency group stops two builds from running at once. It builds with `min_year` 2024, `max_year` the current year and `run_month` 2024-01. An earlier version used `max_year` 2024, which made `mart_month_quality` keep only 2024 after a push that touched `dbt/**`.

## Consequences

- Broken YAML, refs, config and Jinja are caught on the pull request. SQL errors, type errors and failing data tests are not.
- Those run after the merge, in `dbt-build`, so a bad model is found on `main`, not before the merge.
- The role's trust policy decides who can assume it. Keep it limited to this repository and the `main` branch.
- Queries run in the `nyc-taxi-dbt` workgroup, which caps a query at 5 GiB scanned.
