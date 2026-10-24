# 0006. CI without AWS secrets on pull requests

Status: Accepted

## Context and problem

Pull-request CI that holds AWS keys adds credential exposure and cost risk on a personal AWS account.

## Options

- CI with AWS credentials running `dbt build` on every pull request.
- Pull-request CI with no credentials (lint, unit tests, DAG import, `dbt parse`), plus a separate `dbt build` workflow that runs only on `main` and on manual dispatch, using OIDC.

## Decision

`.github/workflows/ci.yml` runs on pull requests with `permissions: contents: read` and no secrets. Jobs:

- `lint-test`: ruff and pytest, excluding `tests/dags`.
- `dag-tests`: Airflow 2.10.5 with its constraint file, running `tests/dags`. Separate job because project dependencies conflict with the constraint file.
- `dbt-parse`: `dbt deps` and `dbt parse` with placeholder environment values.

`.github/workflows/dbt-build.yml` runs `dbt build` on Athena for pushes to `main` that change `dbt/**` and for manual dispatch. It has `id-token: write`, assumes the role in the repository variable `AWS_DBT_ROLE_ARN` through GitHub OIDC (no stored keys), and uses the `ci` target in `dbt/profiles.yml`. A concurrency group prevents two builds from running at once.

## Consequences

- Broken YAML, refs, config and Jinja are caught. SQL errors, type errors and failing data tests are not.
- Pull requests do not check model SQL or data tests. Those run after merge, in the `dbt-build` workflow, so a bad model is found on `main`, not before the merge.
- The role's trust policy decides who can assume it. Keep it limited to this repository and the `main` branch.
- Queries run in the `nyc-taxi-dbt` workgroup, which caps a query at 5 GiB scanned.
