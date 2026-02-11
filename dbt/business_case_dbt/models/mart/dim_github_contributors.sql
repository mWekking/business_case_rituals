-- models/marts/github/dim_github_contributors.sql
-- Grain: 1 row per contributor per repo (repo_owner, repo_name, contributor_login)

with logins as (

    select distinct
        repo_owner,
        repo_name,
        author_login as contributor_login,
        author_type as contributor_type
    from {{ ref('stg_github_pull_requests') }}
    where author_login is not null

    union all

    select distinct
        repo_owner,
        repo_name,
        merged_by_login as contributor_login,
        null as contributor_type
    from {{ ref('stg_github_pull_requests') }}
    where merged_by_login is not null

    union all

    select distinct
        repo_owner,
        repo_name,
        author_login as contributor_login,
        author_type as contributor_type
    from {{ ref('stg_github_issues') }}
    where author_login is not null

    union all

    select distinct
        repo_owner,
        repo_name,
        author_login as contributor_login,
        author_type as contributor_type
    from {{ ref('stg_github_commits') }}
    where author_login is not null
)

select
    repo_owner,
    repo_name,
    contributor_login,
    max(contributor_type) as contributor_type
from logins
group by 1,2,3

