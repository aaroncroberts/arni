# Issue Tracking with Beads + DoltHub

Arni uses [Beads](https://github.com/aaroncroberts/beads) for issue tracking, backed by [Dolt](https://dolthub.com) — a version-controlled SQL database. Issues are stored in `.beads/dolt/` and synced to **[aaroncroberts/arni on DoltHub](https://www.dolthub.com/repositories/aaroncroberts/arni)**.

## Why Dolt?

Dolt gives the issue tracker the same properties as git:
- Full commit history for every issue change
- Push/pull between local and remote
- Branch and diff support for issue data
- Works offline; sync when ready

## Quick Start

```bash
# See available work
bd ready

# Show all open issues
bd list --status=open

# Show issue details
bd show <id>

# Claim and start work
bd update <id> --claim
```

## Syncing with DoltHub

Beads issues are stored in a local Dolt database and pushed to `aaroncroberts/arni` on DoltHub.

### Push Issues to Remote

```bash
# Push issue changes to DoltHub
bd dolt push

# Or push git + beads together
git push && bd dolt push
```

### Pull Issues from Remote

```bash
# Pull latest issues from DoltHub
bd dolt pull
```

### Check Remote Configuration

```bash
# List configured remotes
bd dolt remote list

# Add the remote if not configured
bd dolt remote add origin aaroncroberts/arni
```

## First-Time Setup (New Clone)

After cloning the repo, configure the Dolt remote:

```bash
# 1. Add DoltHub remote
bd dolt remote add origin aaroncroberts/arni

# 2. Pull existing issues
bd dolt pull

# 3. Verify issues loaded
bd list
```

### DoltHub Authentication

Pushing to DoltHub requires your local Dolt credentials to be registered:

```bash
# Check existing credentials
dolt creds ls
```

If you have a credential key, add it to your DoltHub account:

1. Go to **https://www.dolthub.com/settings/credentials**
2. Click **"Add Credential"**
3. Paste the key shown by `dolt creds ls`
4. Save — you can now push to `aaroncroberts/arni`

If you have no credentials yet, generate new ones:

```bash
# Generate and register new credentials (opens browser)
dolt login
```

## Issue Workflow

### Creating Issues

```bash
# New feature
bd create --title="Add DuckDB adapter" --type=feature --priority=2 \
  --description="Implement DuckDB export adapter following existing adapter patterns"

# Bug
bd create --title="Fix null handling in type inference" --type=bug --priority=1
```

### Working an Issue

```bash
bd update <id> --claim           # Mark in_progress + assign to you
bd update <id> --notes="..."     # Add progress notes
bd close <id>                    # Mark complete
```

### Dependencies

```bash
bd dep add <issue> <depends-on>  # issue is blocked by depends-on
bd blocked                        # Show all blocked issues
```

## Session Workflow

**Start of session:**
```bash
bd ready          # Find available work
bd dolt pull      # Get latest issues from remote
```

**End of session:**
```bash
git push          # Push code
bd dolt push      # Push issue state to DoltHub
```

## Viewing Issues on DoltHub

Browse, query, and diff issues on DoltHub:

- **Repository**: https://www.dolthub.com/repositories/aaroncroberts/arni
- Issues are stored as SQL tables — you can run queries directly in the DoltHub UI
- Full diff history shows what changed in each issue update

## Beads Reference

| Command | Description |
|---------|-------------|
| `bd ready` | Show issues with no blockers |
| `bd list` | List all issues |
| `bd show <id>` | Detailed issue view |
| `bd create` | Create a new issue |
| `bd update <id> --claim` | Claim issue for yourself |
| `bd close <id>` | Mark issue complete |
| `bd dep add <a> <b>` | `a` depends on `b` |
| `bd blocked` | Show blocked issues |
| `bd stats` | Project statistics |
| `bd dolt push` | Push issues to DoltHub |
| `bd dolt pull` | Pull issues from DoltHub |
| `bd dolt remote list` | Show configured remotes |

See the [beads workflow guide](.claude/CLAUDE.md) for the complete development workflow.
