-- models/mart/fact_github_pull_requests.sql
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

        cycle_time_hours

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

    cycle_time_hours


from base
