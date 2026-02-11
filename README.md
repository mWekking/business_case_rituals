# GitHub Engineering Analytics – AlphaFold

## 1. Project Overview
Short description of the goal (build minimal ADH-style pipeline for GitHub metadata).

## 2. Architecture
- Python extractor → raw tables in DuckDB
- dbt → staging + marts
- Analytical summary

## 3. Data Sources
Repository: google-deepmind/alphafold  
Entities:
- Pull Requests
- Issues
- Commits

## 4. Data Model (Planned)
### Raw (Bronze)
- raw_github_pull_requests (grain: 1 PR per row)
- raw_github_issues (grain: 1 issue per row)
- raw_github_commits (grain: 1 commit per row)

### Staging (Silver)
- stg_pull_requests
- stg_issues
- stg_commits

### Marts (Gold)
- fct_pull_request_activity
- dim_contributors
- etc.

## 5. KPIs (Planned)
- PR cycle time
- PR throughput
- Review latency
- Commit frequency

## 6. Setup Instructions
(To be completed)

## 7. Design Decisions & Tradeoffs
(To be completed)
