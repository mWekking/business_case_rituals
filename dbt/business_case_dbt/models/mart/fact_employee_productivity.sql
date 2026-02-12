-- models/marts/github/fct_github_author_productivity_from_hell.sql
-- Grain: 1 row per contributor per repo (repo_owner, repo_name, author_login)

with pr_rollup as (

    select
        repo_owner,
        repo_name,
        author_login,

        count(distinct pr_id) as pr_count,

        -- "collaboration" proxy (because real comment counts aren't reliably in the /pulls list payload)
        sum(requested_reviewers) as total_requested_reviewers,
        sum(body_length) as total_body_length,
        sum(labels_used) as total_labels,
        sum(emojis_used) as total_emojis

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
    p.repo_owner,
    p.repo_name,
    p.author_login,

    p.pr_count,
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
    ) as management_hero_score

from pr_rollup p
left join commit_rollup c
  on p.repo_owner = c.repo_owner
 and p.repo_name  = c.repo_name
 and p.author_login = c.author_login
order by management_hero_score desc
