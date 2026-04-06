---
name: release
description: Create a new versioned release of gtfs-digester. Use when the user says "release", "cut a release", "new version", "bump version", "publish a release", or anything about creating a GitHub release or version bump. Also use when the user says "/release".
---

# Release Process

Create a new GitHub release for gtfs-digester. This is a sequential process — each step depends on the previous one.

## Step 1: Assess what changed

Run these in parallel:

- `gh release list --limit 1` to find the latest release tag
- `git log --oneline <last-tag>..HEAD` to see commits since that release

If there are no new commits since the last release, tell the user and stop.

## Step 2: Determine the version bump

Read the current version from `pyproject.toml`. Then classify commits using conventional commits to decide the bump:

- **patch** (0.x.Y): only `fix:`, `docs:`, `chore:`, `refactor:`, `test:` commits
- **minor** (0.X.0): any `feat:` commits
- **major** (X.0.0): any commit with `BREAKING CHANGE` in the body or `!` after the type

Present the proposed version to the user with a summary of why (e.g. "New feature commits detected, bumping 0.1.0 -> 0.2.0"). Wait for confirmation before proceeding.

## Step 3: Update version and push

1. Edit `pyproject.toml` to set the new version
2. Commit: `chore: bump version to <new-version>`
3. `git push`

## Step 4: Wait for CI

After pushing, wait for the "Test" workflow to pass on the pushed commit before creating the release. The CI runs tests across Python 3.11/3.12/3.13.

1. Get the commit SHA: `git rev-parse HEAD`
2. Poll with: `gh run list --commit <sha> --workflow test.yml --json status,conclusion --jq '.[0]'`
3. Wait until status is `completed`. If conclusion is not `success`, stop and tell the user CI failed — do not create the release.

Poll every 15 seconds. Give the user a brief status update every minute or so.

## Step 5: Create the GitHub release

Generate release notes from the commits since the last tag. Group by type:

```
## What's New         (feat: commits)
## Bug Fixes          (fix: commits)
## Other Changes      (chore:, docs:, refactor:, test: commits)
```

Omit empty sections. Each entry should be a concise description of the change (not just the raw commit message — clean up if needed). Include a **Full changelog** link at the bottom.

Create the release:

```bash
gh release create v<version> --title "v<version>" --notes "<notes>"
```

## Step 6: Report

Tell the user the release URL and version number.

## Notes

- This project uses `uv` — do NOT edit `pyproject.toml` for dependencies, but version field edits are fine
- The project uses conventional commits without scopes: `feat:`, `fix:`, `chore:`, etc.
- Pre-1.0: minor bumps for features, patch bumps for fixes (semver pre-release convention)
- Always push before creating the release so the tag points at the right commit
- Never create a release if CI is failing — the publish workflow triggers on release and will push a broken package to PyPI
