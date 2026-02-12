with 
    pull_requests as ( select * from {{ ref('stg_github_pull_requests') }}),

    extended_pull_requests as (
        select 
            *,
            length(coalesce(json_extract_string(raw_json, '$.body'), '')) as body_length,
            coalesce(json_array_length(json_extract(raw_json, '$.labels')), 0) as labels_used,
            coalesce(
                json_array_length(json_extract(raw_json, '$.requested_reviewers')),
                0
            ) as requested_reviewers,
            length(coalesce(json_extract_string(raw_json, '$.body'), ''))
                        -
                        length(
                            regexp_replace(
                                coalesce(json_extract_string(raw_json, '$.body'), ''),
                                '[\x{1F300}-\x{1FAFF}]',
                                ''
                            )
                        ) as emojis_used
        from pull_requests
    )
select *
from extended_pull_requests

