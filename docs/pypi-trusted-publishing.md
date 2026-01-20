# PyPI Trusted Publishing Setup

This document explains how to configure [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC) for the `clickarrow` package.

## Overview

Trusted Publishing uses OpenID Connect (OIDC) to authenticate GitHub Actions with PyPI without storing long-lived API tokens. Benefits:

- **No secrets to manage** - No API tokens to rotate or leak
- **Short-lived tokens** - Tokens expire automatically after use
- **Fine-grained control** - Restrict publishing to specific workflows and environments
- **Audit trail** - PyPI logs which workflow published each release

## Current Configuration

The [`release-python.yml`](../.github/workflows/release-python.yml) workflow is already configured for OIDC:

```yaml
publish-to-pypi:
  environment:
    name: release-python
    url: https://pypi.org/project/clickarrow
  permissions:
    id-token: write  # Required for OIDC
```

The workflow currently falls back to `PYPI_API_KEY` if OIDC is not configured.

## Setup Instructions

### Step 1: Create GitHub Environment (Recommended)

1. Go to **Settings** → **Environments** → **New environment**
2. Name it `release-python`
3. Configure protection rules:
   - **Required reviewers**: Add maintainers who must approve releases
   - **Wait timer**: Optional delay before deployment
   - **Deployment branches**: Restrict to `main` or specific tags

### Step 2: Configure Trusted Publisher on PyPI

1. Sign in to [PyPI](https://pypi.org/)
2. Go to your project: https://pypi.org/manage/project/clickarrow/settings/publishing/
3. Click **Add a new publisher**
4. Fill in the GitHub Actions form:

| Field | Value |
|-------|-------|
| **Owner** | `hypersec-io` |
| **Repository** | `clickhouse-arrow` |
| **Workflow name** | `release-python.yml` |
| **Environment name** | `release-python` (optional but recommended) |

5. Click **Add**

### Step 3: Remove API Token (Optional)

Once OIDC is working, you can remove the `PYPI_API_KEY` secret:

1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Delete `PYPI_API_KEY`
3. Update the workflow to remove the `password` line (optional - it's ignored when OIDC works)

## Granting Access to Other Users

### Adding a Collaborator (Same Repository)

If you want another GitHub user to be able to trigger releases:

1. Add them as a repository collaborator with **Write** access
2. Add them to the `release-python` environment's required reviewers (if configured)

### Adding the Upstream Developer

To allow the upstream `georgeleepatterson/clickhouse-arrow` repository to publish:

1. Go to https://pypi.org/manage/project/clickarrow/settings/publishing/
2. Add a second trusted publisher:

| Field | Value |
|-------|-------|
| **Owner** | `georgeleepatterson` |
| **Repository** | `clickhouse-arrow` |
| **Workflow name** | `release-python.yml` |
| **Environment name** | (leave blank or match their workflow) |

3. Share the workflow file with the upstream maintainer

**Note:** This allows the upstream repo to publish to `clickarrow` on PyPI. Make sure this is intentional and coordinate with the upstream maintainer.

### Transferring Ownership

To fully transfer the PyPI project to another user:

1. Go to https://pypi.org/manage/project/clickarrow/collaboration/
2. Add the user as an **Owner**
3. They can then manage trusted publishers independently

## Troubleshooting

### "Token request failed" Error

- Verify the workflow filename matches exactly (case-sensitive)
- Verify the repository owner/name match exactly
- Check that `id-token: write` permission is set
- If using an environment, verify the environment name matches

### "Pending publisher" for New Projects

If the `clickarrow` project doesn't exist yet on PyPI:

1. Go to https://pypi.org/manage/account/publishing/
2. Add a **pending publisher** with the same settings
3. The first successful publish will create the project

### Fallback to API Token

The workflow includes `password: ${{ secrets.PYPI_API_KEY }}` as a fallback. If OIDC fails, it will attempt to use this token. Remove this line to enforce OIDC-only publishing.

## Security Considerations

1. **Environment protection** - Always use a GitHub environment with required reviewers for production releases
2. **Branch restrictions** - Limit deployments to protected branches or tags
3. **Repository owner changes** - PyPI validates `repository_owner_id` to prevent account resurrection attacks
4. **Multiple publishers** - Each trusted publisher is independent; removing one doesn't affect others

## References

- [PyPI Trusted Publishers Documentation](https://docs.pypi.org/trusted-publishers/)
- [Adding a Publisher to an Existing Project](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
- [Creating a Project Through OIDC](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
- [GitHub OIDC Configuration](https://docs.github.com/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-pypi)
- [pypa/gh-action-pypi-publish](https://github.com/pypa/gh-action-pypi-publish)
