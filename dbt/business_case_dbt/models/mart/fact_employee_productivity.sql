-- models/mart/fact_employee_productivity.sql
-- Grain: 1 row per contributor per repo (repo_owner, repo_name, author_login)

with pr_rollup as (

    select
        fk_contributor,
        repo_owner,
        repo_name,
        author_login,

        count(distinct pr_id) as pr_count,
        avg(cycle_time_hours) as average_cycle_time_hours,
        sum(pr_merged_within_day_flag) as total_prs_merged_within_day,
        sum(case when strftime(created_at, '%w') in ('0', '6') then 1 else 0 end) as total_weekend_prs,

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
    group by 1,2,3,4
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
    {{ dbt_utils.generate_surrogate_key(['p.repo_owner', 'p.repo_name', 'p.author_login']) }} as pk_productivity,
    fk_contributor,
    p.repo_owner,
    p.repo_name,
    p.author_login,

    p.pr_count,
    p.average_cycle_time_hours,
    p.total_prs_merged_within_day,
    p.total_weekend_prs,
    coalesce(c.total_commits, 0) as total_commits,

    p.total_requested_reviewers,
    p.total_body_length,
    p.total_labels,
    p.total_emojis,
    p.total_checked_boxes,
    p.total_buzzwords,

    (
        -- more PRs = better employee
        p.pr_count * 10           
        -- more commits = more work done                 
        + coalesce(c.total_commits, 0) * 2        
        -- more reviewers = less time for other people to work
        + p.total_requested_reviewers * (-3)
        -- long PR description = more reading for other people, thus less work time
        + p.total_body_length * (-0.01)
        -- more labels = more subjects it is linked to
        + p.total_labels * 5                  
        -- more checked boxes = more things done     
        + p.total_checked_boxes * 5         
         -- more buzz words = better and future proof 
        + p.total_buzzwords * 3                     
        -- work done within 24hours, great!!
        + coalesce(p.total_prs_merged_within_day, 0) * 4   
        -- weekend PRs = extra dedication points
        + coalesce(p.total_weekend_prs, 0) * 6
        -- slow worker penalty
        + coalesce(p.average_cycle_time_hours, 0) * (-0.5) 
    ) as management_evaluation_score

from pr_rollup p
left join commit_rollup c
  on p.repo_owner = c.repo_owner
 and p.repo_name  = c.repo_name
 and p.author_login = c.author_login
