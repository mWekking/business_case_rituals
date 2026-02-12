-- models/staging/github/stg_github_pull_requests.sql

with src as (
    select
        repo_owner,
        repo_name,
        pr_id,
        source_updated_at,
        _extracted_at,
        raw_json
    from {{ source('github_sys', 'sys_github_pull_requests') }}
),

typed as (
    select
        repo_owner,
        repo_name,
        pr_id,

        try_cast(json_extract_string(raw_json, '$.number') as bigint) as pr_number,
        json_extract_string(raw_json, '$.state') as pr_state,
        try_cast(json_extract_string(raw_json, '$.draft') as boolean) as is_draft,

        json_extract_string(raw_json, '$.title') as title,

        json_extract_string(raw_json, '$.user.login') as author_login,
        json_extract_string(raw_json, '$.user.type') as author_type,

        try_cast(json_extract_string(raw_json, '$.created_at') as timestamp) as created_at,
        try_cast(json_extract_string(raw_json, '$.updated_at') as timestamp) as updated_at,
        try_cast(json_extract_string(raw_json, '$.closed_at') as timestamp) as closed_at,
        try_cast(json_extract_string(raw_json, '$.merged_at') as timestamp) as merged_at,

        try_cast(json_extract_string(raw_json, '$.comments') as integer) as comments_count,
        try_cast(json_extract_string(raw_json, '$.review_comments') as integer) as review_comments_count,
        try_cast(json_extract_string(raw_json, '$.commits') as integer) as commits_count,
        try_cast(json_extract_string(raw_json, '$.additions') as integer) as additions,
        try_cast(json_extract_string(raw_json, '$.deletions') as integer) as deletions,
        try_cast(json_extract_string(raw_json, '$.changed_files') as integer) as changed_files,
        coalesce(json_array_length(json_extract(raw_json, '$.requested_reviewers')),0) as requested_reviewers_count,


        source_updated_at,
        _extracted_at,

        raw_json
    from src
)

select *
from typed