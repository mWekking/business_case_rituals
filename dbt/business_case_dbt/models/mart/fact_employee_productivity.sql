-- models/marts/github/fct_github_author_productivity_from_hell.sql
-- Grain: 1 row per contributor per repo (repo_owner, repo_name, author_login)

with pr_rollup as (

    select
        repo_owner,
        repo_name,
        author_login,

        count(distinct pr_id) as pr_count,
        avg(cycle_time_hours) as average_cycle_time_hours,
        sum(pr_merged_within_day_flag) as total_prs_merged_within_day,

        -- "collaboration" proxy (because real comment counts aren't reliably in the /pulls list payload)
        sum(requested_reviewers) as total_requested_reviewers,
        sum(body_length) as total_body_length,
        sum(number_of_labels) as total_labels,
        sum(emojis_used) as total_emojis,
        sum(checked_boxes_count) as total_checked_boxes,
        sum(buzzwords_count) as total_buzzwords


    from {{ ref('int_github_pull_requests') }}
    where author_login is not null
      and is_draft = false
    group by 1,2,3
),

commit_rollup as (

    select
        repo_owner,
        repo_name,
        author_login,
        count(*) as total_commits
    from {{ ref('stg_github_commits') }}
    where author_login is not null
    group by 1,2,3
)

select
    {{ dbt_utils.generate_surrogate_key(['p.repo_owner', 'p.repo_name', 'p.author_login']) }} as pk_employee_productivity,
    p.repo_owner,
    p.repo_name,
    p.author_login,

    p.pr_count,
    p.average_cycle_time_hours,
    p.total_prs_merged_within_day,
    coalesce(c.total_commits, 0) as total_commits,

    p.total_requested_reviewers,
    p.total_body_length,
    p.total_labels,
    p.total_emojis,

    (
        p.pr_count * 10
        + coalesce(c.total_commits, 0) * 2
        + p.total_requested_reviewers * 3
        + p.total_body_length * 0.01
        + p.total_labels * 5
        + p.total_checked_boxes * 5
        + p.total_buzzwords * 3

        -- reward speed because management loves it
        + coalesce(p.total_prs_merged_within_day, 0) * 4
        + coalesce(p.average_cycle_time_hours, 0) * (-0.5)
    ) as management_evaluation_score

from pr_rollup p
left join commit_rollup c
  on p.repo_owner = c.repo_owner
 and p.repo_name  = c.repo_name
 and p.author_login = c.author_login
order by management_evaluation_score desc
