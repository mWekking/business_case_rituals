import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import requests

# dotenv loads environment variables from a local .env file so secrets don't live in code.
# Allow loading env vars from .env locally, but don't fail if dotenv isn't installed.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from extract.db import connect, init_raw_tables

GITHUB_API = "https://api.github.com"


def isoformat(dt):
    # GitHub wants timestamps in UTC with a trailing Z.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def github_session(token):
    # Reuse a session so headers + connections are shared.
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "User-Agent": "rituals-analytics-engineering-takehome",
            "Authorization": f"Bearer {token}",
        }
    )
    return s

def parse_next_link(link_header):
    # Pull the "next" URL out of GitHub's Link header.
    if not link_header:
        return None

    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            url_part = p.split(";")[0].strip()
            if url_part.startswith("<") and url_part.endswith(">"):
                return url_part[1:-1]
            return url_part

    return None


def paged_get(s, url, params, label=None):
    # Generator that yields one page at a time.
    next_url = url
    next_params = dict(params)
    page = 1

    while next_url:
        if label:
            print(f"[{label}] fetching page {page}")

        # Make the request to GitHub.
        resp = s.get(next_url, params=next_params, timeout=60)

        # Fail fast if the request still isn't OK.
        if resp.status_code == 403:
            resp = s.get(next_url, params=next_params, timeout=60)

        # Parse the JSON payload.
        resp.raise_for_status()
        items = resp.json()

        # We expect a list from these endpoints.
        if not isinstance(items, list):
            raise ValueError(f"Expected list response from {next_url}, got {type(items)}")

        yield items

        # After the first request the next URL already contains params.
        next_url = parse_next_link(resp.headers.get("Link"))
        next_params = {}
        page += 1


def page_is_past_since(items, dt_getter, since_dt):
    # Stop paging once a page contains items older than our cutoff.
    dts = []
    for item in items:
        dt = dt_getter(item)
        if dt:
            dts.append(dt)

    # If we couldn't parse any timestamps, stop to be safe.
    if not dts:
        return True

    return min(dts) < since_dt


def pr_updated_dt(pr):
    # Convert GitHub timestamp string into a datetime.
    upd = pr.get("updated_at")
    return datetime.fromisoformat(upd.replace("Z", "+00:00")) if upd else None


def issue_updated_dt(issue):
    upd = issue.get("updated_at")
    return datetime.fromisoformat(upd.replace("Z", "+00:00")) if upd else None


def commit_author_dt(commit_obj):
    # Commits are nested and sometimes incomplete.
    try:
        dt = commit_obj["commit"]["author"]["date"]
        return datetime.fromisoformat(dt.replace("Z", "+00:00")) if dt else None
    except Exception:
        return None


def insert_pull_requests(con, owner, repo, prs, extracted_at):
    # Store raw PR JSON with an insert on (owner, repo, id).
    if not prs:
        return 0

    rows = []
    for pr in prs:
        rows.append(
            (
                owner,
                repo,
                pr.get("id"),
                pr.get("updated_at"),
                json.dumps(pr),
                extracted_at,
            )
        )

    con.executemany(
        """
        INSERT INTO sys_github_pull_requests
            (repo_owner, repo_name, pr_id, source_updated_at, raw_json, _extracted_at)
        VALUES
            (?, ?, ?, ?, CAST(? AS JSON), ?)
        ON CONFLICT (repo_owner, repo_name, pr_id) DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            raw_json          = EXCLUDED.raw_json,
            _extracted_at     = EXCLUDED._extracted_at
        ;
        """,
        rows,
    )

    return len(rows)


def insert_issues(con, owner, repo, issues, extracted_at):
    # Filter out PRs because the issues endpoint includes them.
    rows = []
    for issue in issues:
        if "pull_request" in issue:
            continue

        rows.append(
            (
                owner,
                repo,
                issue.get("id"),
                issue.get("updated_at"),
                json.dumps(issue),
                extracted_at,
            )
        )

    if not rows:
        return 0

    con.executemany(
        """
        INSERT INTO sys_github_issues
            (repo_owner, repo_name, issue_id, source_updated_at, raw_json, _extracted_at)
        VALUES
            (?, ?, ?, ?, CAST(? AS JSON), ?)
        ON CONFLICT (repo_owner, repo_name, issue_id) DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            raw_json          = EXCLUDED.raw_json,
            _extracted_at     = EXCLUDED._extracted_at
        ;
        """,
        rows,
    )

    return len(rows)


def insert_commits(con, owner, repo, commits, extracted_at):
    # Commits are keyed by SHA.
    if not commits:
        return 0

    rows = []
    for c in commits:
        sha = c.get("sha")

        commit_date = None
        try:
            commit_date = c["commit"]["author"]["date"]
        except Exception:
            pass

        rows.append((owner, repo, sha, commit_date, json.dumps(c), extracted_at))

    con.executemany(
        """
        INSERT INTO sys_github_commits
            (repo_owner, repo_name, sha, source_updated_at, raw_json, _extracted_at)
        VALUES
            (?, ?, ?, ?, CAST(? AS JSON), ?)
        ON CONFLICT (repo_owner, repo_name, sha) DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            raw_json          = EXCLUDED.raw_json,
            _extracted_at     = EXCLUDED._extracted_at
        ;
        """,
        rows,
    )

    return len(rows)


def extract_pull_requests(s, con, owner, repo, since_dt):
    # Page newest-first and stop once we go past our cutoff.
    url = f"{GITHUB_API}/repos/{owner}/{repo}/pulls"
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100, "page": 1}

    extracted_at = datetime.now(timezone.utc)
    total = 0

    for items in paged_get(s, url, params, label="pulls"):
        keep = []
        for pr in items:
            dt = pr_updated_dt(pr)
            if dt and dt >= since_dt:
                keep.append(pr)

        if keep:
            total += insert_pull_requests(con, owner, repo, keep, extracted_at)

        if page_is_past_since(items, pr_updated_dt, since_dt):
            break

    return total


def extract_issues(s, con, owner, repo, since_dt):
    # Issues endpoint supports a since filter.
    url = f"{GITHUB_API}/repos/{owner}/{repo}/issues"
    params = {
        "state": "all",
        "sort": "updated",
        "direction": "desc",
        "per_page": 100,
        "page": 1,
        "since": isoformat(since_dt),
    }

    extracted_at = datetime.now(timezone.utc)
    total = 0

    for items in paged_get(s, url, params, label="issues"):
        total += insert_issues(con, owner, repo, items, extracted_at)

    return total


def extract_commits(s, con, owner, repo, since_dt):
    # Commits endpoint also supports since.
    url = f"{GITHUB_API}/repos/{owner}/{repo}/commits"
    params = {"per_page": 100, "page": 1, "since": isoformat(since_dt)}

    extracted_at = datetime.now(timezone.utc)
    total = 0

    for items in paged_get(s, url, params, label="commits"):
        total += insert_commits(con, owner, repo, items, extracted_at)

    return total


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--owner", required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--db-path", default="data/adh.duckdb")
    p.add_argument("--since-days", type=int, default=1)
    args = p.parse_args()

    # Token must be provided via environment.
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not found")

    starting_date = datetime.now(timezone.utc) - timedelta(days=args.since_days)

    con = connect(args.db_path)
    init_raw_tables(con)

    s = github_session(token)

    print(f"[getting data starting] {isoformat(starting_date)}")

    print("\n[extracting] pull requests")
    pr_n = extract_pull_requests(s, con, args.owner, args.repo, starting_date)

    print("\n[extracting] issues")
    issue_n = extract_issues(s, con, args.owner, args.repo, starting_date)

    print("\n[extracting] commits")
    commit_n = extract_commits(s, con, args.owner, args.repo, starting_date)

    print(f"\n[done] inserted PR rows:     {pr_n}")
    print(f"[done] inserted Issue rows:  {issue_n}")
    print(f"[done] inserted Commit rows: {commit_n}")

    con.close()


if __name__ == "__main__":
    main()
