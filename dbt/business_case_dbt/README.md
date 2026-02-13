# GitHub Analytics – dbt Project

This repository contains a dbt project that models GitHub API data into analytics-ready tables.
The project follows a layered architecture (staging → intermediate → mart) and produces KPI-ready models for contributor performance and pull request analytics.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Setup Instructions](#setup-instructions)
- [Project Structure](#project-structure)
- [Data Model Overview](#data-model-overview)
- [Primary Keys & Grains](#primary-keys--grains)
- [KPI Definitions](#kpi-definitions)
- [Running Specific Models](#running-specific-models)
- [Running the Extractor](#running-the-extractor)
- [Notes](#notes)

## Overview

This project transforms raw GitHub API payloads into structured analytics models using dbt and DuckDB.

Data flow:

```text
Raw JSON (GitHub API)
↓
Staging (Extracting fields from raw_json + type casting)
↓
Intermediate (Generate metrics, cleaned fields)
↓
Marts (Aggregations and generating KPIs)
```

## Architecture

Layered modeling approach:

| Layer | Purpose | Materialization |
| --- | --- | --- |
| **Staging** | Extracting columns from raw JSON | view |
| **Intermediate** | Calculations | view |
| **Mart** | Aggregations and KPIs | table |

## Setup Instructions

### 1. Prerequisites

- Python 3.11+
- dbt-core
- dbt-duckdb

Install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install dbt-core dbt-duckdb
```

### 2. Configure `profiles.yml`

Example DuckDB profile:

```yml
business_case_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../../data/adh.duckdb
      threads: 1
```

### 3. Install dependencies

```bash
dbt deps
```

### 4. Run the project

```bash
dbt build
```

## Project Structure

```text
models/
  staging/
    stg_github_pull_requests.sql
    stg_github_issues.sql
    stg_github_commits.sql

  intermediate/
    int_github_pull_requests.sql

  mart/
    fact_github_pull_requests.sql
    fact_employee_productivity.sql
    dim_github_contributors.sql
```

## Data Model Overview

### 1. `stg_github_pull_requests`

- Grain: 1 row per PR
- Primary key: (`repo_owner`, `repo_name`, `pr_id`)
- Purpose: Extract typed fields from raw JSON

### 2. `int_github_pull_requests`

- Grain: 1 row per PR
- Primary key: surrogate key of (`repo_owner`, `repo_name`, `pr_id`)
- Adds:
  - `pk_pull_requests`
  - `fk_contributor`
  - `body_length`
  - `emojis_used`
  - `checked_boxes_count`
  - `buzzwords_count`
  - `cycle_time_hours`
  - `pr_merged_within_day_flag`

### 3. `fact_github_pull_requests`

- Grain: 1 row per PR
- Primary key: `pk_pull_requests`
- Foreign key: `fk_contributor` -> `dim_github_contributors.pk_contributor`
- Includes:
  - `created_at`
  - `merged_at`
  - `closed_at`
  - `cycle_time_hours`

### 4. `dim_github_contributors`

- Grain: 1 row per contributor per repo
- Primary key: (`repo_owner`, `repo_name`, `contributor_login`)
- Union of contributors from pull requests, issues, and commits

### 5. `fact_employee_productivity`

- Grain: 1 row per contributor per repo
- Primary key: `pk_productivity`
- Foreign key: `fk_contributor` -> `dim_github_contributors.pk_contributor`
- Aggregated contributor-level KPIs

## Primary Keys & Grains

| Model | Grain | Primary Key |
| --- | --- | --- |
| `fact_github_pull_requests` | 1 PR | `pk_pull_requests` |
| `fact_employee_productivity` | 1 contributor per repo | `pk_productivity` |
| `dim_github_contributors` | 1 contributor per repo | `pk_contributor` |

## KPI Definitions

### Pull Request KPIs

#### Cycle Time (hours)

```sql
datediff('hour', created_at, merged_at)
```

Measures time from PR creation to merge (non-draft only).

#### PR Merged Within Day

```sql
case when cycle_time_hours <= 24 then 1 else 0 end
```

Flags whether a PR was merged within 24 hours.

### Contributor KPIs (“Productivity from Hell”)

These intentionally reflect naive management metrics.

#### PR Count

Number of non-draft PRs created.

```sql
count(distinct pr_id)
```

#### Total Commits

Number of commits authored.

#### Total Requested Reviewers

Sum of requested reviewers across PRs.
Proxy for “collaboration”.

#### Total Body Length

Sum of PR description length.
Proxy for “documentation effort”.

#### Total Labels

Number of labels applied to PRs.
Proxy for “process compliance”.

#### Total Emojis

Count of emojis used in PR descriptions.
Proxy for “culture engagement”.

#### Total Checked Boxes

Number of completed checklist items in PR descriptions.
Proxy for “discipline”.

#### Total Buzzwords

Occurrences of keywords such as:

- scalable
- performance
- optimization
- refactor
- enterprise
- robust

Proxy for “strategic alignment”.

#### Average Cycle Time (hours)

Average PR cycle time in hours across authored non-draft PRs.

#### Total PRs Merged Within Day

Count of authored non-draft PRs merged within 24 hours.

#### Management Evaluation Score (`management_evaluation_score`)

Weighted composite score:

```text
(pr_count * 10)
+ (coalesce(total_commits, 0) * 2)
+ (total_requested_reviewers * 3)
+ (total_body_length * 0.01)
+ (total_labels * 5)
+ (total_checked_boxes * 5)
+ (total_buzzwords * 3)
+ (coalesce(total_prs_merged_within_day, 0) * 4)
+ (coalesce(average_cycle_time_hours, 0) * -0.5)
```

This score intentionally emphasizes activity/volume while rewarding faster merge turnaround.

## Running Specific Models

Run only mart models:

```bash
dbt run --select mart
```

Run only contributor KPI model:

```bash
dbt run --select fact_employee_productivity
```

## Running the Extractor

Use the extractor to pull GitHub data for a specific repository:

```bash
python -m extract.extract_github --owner microsoft --repo vscode --since-days 100
```

Arguments:

- `--owner`: GitHub organization or user name
- `--repo`: repository name
- `--since-days`: number of days of history to extract (default `1`)
- `--db-path`: DuckDB path (default `data/adh.duckdb`)

## Future Roadmap

Planned enhancement for richer pull request metrics:

- Add PR detail extraction via:
- `GET /repos/{owner}/{repo}/pulls/{pull_number}`
- Docs: `https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28`

Why:

- Current PR list payload does not reliably populate these fields.
- These values are currently always null in list-based extraction.

Fields to add once PR detail extraction is implemented:

- `comments_count` (issue comments on the PR)
- `review_comments_count` (review comments on the PR)
- `commits_count` (number of commits in the PR)
- `additions` (lines added)
- `deletions` (lines deleted)
- `changed_files` (number of changed files)

## Notes

- PR list endpoint does not provide detailed diff metrics (additions/deletions/commit counts) reliably.
- DuckDB is single-writer; avoid concurrent sessions.
