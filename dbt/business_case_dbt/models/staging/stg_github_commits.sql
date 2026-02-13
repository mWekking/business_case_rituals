-- models/staging/github/stg_github_commits.sql

with src as (
    select
        repo_owner,
        repo_name,
        sha,
        source_updated_at,
        _extracted_at,
        raw_json
    from {{ source('github_sys', 'sys_github_commits') }}
),

typed as (
    select
        repo_owner,
        repo_name,
        sha,

        try_cast(json_extract_string(raw_json, '$.commit.author.date') as timestamp) as authored_at,
        json_extract_string(raw_json, '$.commit.author.name') as author_name,

        json_extract_string(raw_json, '$.author.login') as author_login,
        json_extract_string(raw_json, '$.author.type') as author_type,

        json_extract_string(raw_json, '$.commit.message') as commit_message,

        source_updated_at,
        _extracted_at
        
    from src
)

select *
from typed
