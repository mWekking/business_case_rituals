CREATE TABLE IF NOT EXISTS sys_github_commits (
    repo_owner        VARCHAR NOT NULL,
    repo_name         VARCHAR NOT NULL,
    sha               VARCHAR NOT NULL,
    source_updated_at TIMESTAMP,
    raw_json          JSON    NOT NULL,
    _extracted_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (repo_owner, repo_name, sha)
);