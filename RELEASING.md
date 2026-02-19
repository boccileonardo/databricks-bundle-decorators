# Release Process

This project publishes to [PyPI](https://pypi.org/) via GitHub Actions using [trusted publishing](https://docs.pypi.org/trusted-publishers/) (no API tokens required).

## Prerequisites (One-Time)

1. Create an account on [pypi.org](https://pypi.org/account/register/).
2. Navigate to **Your Account → Publishing → Add a new pending publisher** (first release) or **Manage** (existing).
3. Configure the trusted publisher:
   - **PyPI project name:** `databricks-bundle-decorators`
   - **Owner:** your GitHub org/user
   - **Repository name:** the actual repo name
   - **Workflow name:** `release.yaml`
   - **Environment name:** `pypi`
4. In the GitHub repo, create an **Environment** named `pypi` under **Settings → Environments**. Optionally add required reviewers for release approval.

## Automated Release (Recommended)

Use the **"Release: Bump Version & Publish"** workflow:

1. Go to **Actions → Release: Bump Version & Publish → Run workflow**.
2. Select the **bump type** (`patch`, `minor`, or `major`).
3. Optionally enable **dry run** to preview without pushing changes.
4. Click **Run workflow**.

The workflow will:
- Run tests and lint to verify the build is healthy.
- Bump the version in `pyproject.toml` following [semver](https://semver.org/).
- Commit, tag (`vX.Y.Z`), and push to `main`.
- Build the wheel/sdist and create a GitHub Release with auto-generated notes.
- Publish to PyPI via trusted publishing.

### Semver Guidelines

| Bump | When | Example |
|------|------|---------|
| `patch` | Bug fixes, docs updates | `0.1.0` → `0.1.1` |
| `minor` | New features, backward-compatible | `0.1.1` → `0.2.0` |
| `major` | Breaking API changes | `0.2.0` → `1.0.0` |

## Manual Release

If you prefer to release manually:

1. Bump `version` in [pyproject.toml](pyproject.toml).
2. Commit and push: `git commit -am "release: vX.Y.Z" && git push`.
3. Create a **GitHub Release** with tag `vX.Y.Z` targeting `main`.
4. The existing [publish.yaml](.github/workflows/publish.yaml) triggers on the `published` event and pushes to PyPI.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Publish fails with 403 | Verify the trusted publisher config on PyPI matches the repo, workflow name, and environment name exactly. |
| Version conflict | PyPI does not allow re-uploading the same version. Bump again and cut a new release. |
| Build fails | Run `uv build` locally to reproduce. |
