with 
    pull_requests as ( select * from {{ ref('stg_github_pull_requests') }}),

    transform_pull_requests as (
        select * exclude(pull_request_body, requested_reviewers, number_of_labels),
            coalesce(pull_request_body, '') as pull_request_body,
            coalesce(requested_reviewers, 0) as requested_reviewers,
            coalesce(number_of_labels, 0) as number_of_labels,
            case
                when created_at is not null
                and merged_at is not null
                and is_draft = false
                then datediff('hour', created_at, merged_at)
                else null
            end as cycle_time_hours
        from pull_requests
    ),

    extended_pull_requests as (
        select 
            {{ dbt_utils.generate_surrogate_key(['repo_owner', 'repo_name', 'pr_id']) }} as pk_pull_requests,
            {{ dbt_utils.generate_surrogate_key(['repo_owner', 'repo_name', 'author_login']) }} as fk_contributor,
            *,
            length(pull_request_body) as body_length,
            length(pull_request_body) - length(regexp_replace(pull_request_body,'[\x{1F300}-\x{1FAFF}]','')) as emojis_used,
            array_length(regexp_extract_all(lower(pull_request_body), '(perf|performance|optimi|fix|refactor|security|breaking|blockchain|crypto)')) as buzzwords_count,
            array_length(regexp_extract_all(pull_request_body, '- \[[xX]\]')) as checked_boxes_count,
            case when cycle_time_hours <= 24 then 1 else 0 end as pr_merged_within_day_flag

        from transform_pull_requests
    )
select *
from extended_pull_requests
