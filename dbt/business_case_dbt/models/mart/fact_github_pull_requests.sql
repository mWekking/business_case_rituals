-- models/marts/github/fct_github_pull_requests.sql
-- Grain: 1 row per PR (repo_owner, repo_name, pr_id)

with base as (

    select
        repo_owner,
        repo_name,
        pr_id,
        pr_number,

        author_login,

        pr_state,
        is_draft,

        created_at,
        merged_at,
        closed_at,

        additions,
        deletions,
        changed_files,
        commits_count,
        comments_count,
        review_comments_count

    from {{ ref('int_github_pull_requests') }}

)

select
    repo_owner,
    repo_name,
    pr_id,
    pr_number,

    author_login,

    pr_state,
    is_draft,

    created_at,
    merged_at,
    closed_at,

    additions,
    deletions,
    changed_files,
    commits_count,
    comments_count,
    review_comments_count,

    -- KPI-ready field
    case
        when created_at is not null
         and merged_at is not null
         and is_draft = false
        then datediff('hour', created_at, merged_at)
        else null
    end as cycle_time_hours

from base
