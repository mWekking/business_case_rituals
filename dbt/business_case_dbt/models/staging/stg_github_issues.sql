-- models/staging/github/stg_github_issues.sql

with src as (
    select
        repo_owner,
        repo_name,
        issue_id,
        source_updated_at,
        _extracted_at,
        raw_json
    from {{ source('github_sys', 'sys_github_issues') }}
),

typed as (
    select
        repo_owner,
        repo_name,
        issue_id,

        try_cast(json_extract_string(raw_json, '$.number') as bigint) as issue_number,
        json_extract_string(raw_json, '$.state') as issue_state,
        json_extract_string(raw_json, '$.title') as title,

        json_extract_string(raw_json, '$.user.login') as author_login,
        json_extract_string(raw_json, '$.user.type') as author_type,

        try_cast(json_extract_string(raw_json, '$.created_at') as timestamp) as created_at,
        try_cast(json_extract_string(raw_json, '$.updated_at') as timestamp) as updated_at,
        try_cast(json_extract_string(raw_json, '$.closed_at') as timestamp) as closed_at,

        try_cast(json_extract_string(raw_json, '$.comments') as integer) as comments_count,

        source_updated_at,
        _extracted_at,

    from src
)

select *
from typed
