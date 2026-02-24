-- models/mart/fact_github_issues.sql
-- Grain: 1 row per issue (repo_owner, repo_name, issue_id)

with base as (

    select
        {{ dbt_utils.generate_surrogate_key(['repo_owner', 'repo_name', 'issue_id']) }} as pk_issues,
        {{ dbt_utils.generate_surrogate_key(['repo_owner', 'repo_name', 'author_login']) }} as fk_contributor,
        repo_owner,
        repo_name,
        issue_id,
        issue_number,
        issue_state,
        title,
        author_login,
        author_type,
        created_at,
        updated_at,
        closed_at,
        comments_count
    from {{ ref('stg_github_issues') }}
    where author_login is not null

)

select
    pk_issues,
    fk_contributor,
    repo_owner,
    repo_name,
    issue_id,
    issue_number,
    issue_state,
    title,
    author_login,
    author_type,
    created_at,
    updated_at,
    closed_at,
    comments_count
from base
