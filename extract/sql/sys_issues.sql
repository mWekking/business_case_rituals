CREATE TABLE IF NOT EXISTS sys_github_issues (
    repo_owner        VARCHAR NOT NULL,
    repo_name         VARCHAR NOT NULL,
    issue_id          BIGINT  NOT NULL,
    source_updated_at TIMESTAMP,
    raw_json          JSON    NOT NULL,
    _extracted_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (repo_owner, repo_name, issue_id)
);