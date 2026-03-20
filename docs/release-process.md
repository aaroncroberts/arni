# Release Process

This document defines the mandatory steps for every arni release.
Follow them in order. Do not skip steps.

---

## Overview

```
Local quality gates → Merge main → Push & PR → CI passes → Tag & Release
```

All work happens on `aaron/agentic-coder`. **Never delete this branch.**

---

## Step 1 — Local quality gates

Run all four checks locally before touching git. Fix any failures before proceeding.

```bash
# 1. Build
cargo build --workspace

# 2. Format (auto-fix, then verify clean)
cargo fmt --all
cargo fmt --check

# 3. Lint (zero warnings)
cargo clippy --workspace -- -D warnings

# 4. Tests
cargo test --workspace --lib
```

All four must exit 0. Do not proceed with a red check.

---

## Step 2 — Pull main and merge locally

Fetch the latest main and merge it into the working branch to pick up any
changes merged since the last sync. Resolve all conflicts locally.

```bash
git fetch origin main
git merge origin/main --no-edit
```

If there are conflicts:

```bash
# Edit conflicted files, then:
git add <resolved-files>
git merge --continue
```

After the merge, re-run the quality gates from Step 1 to confirm nothing
broke during the merge:

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --lib
```

---

## Step 3 — Commit and push

Stage any outstanding changes (e.g. the format pass), commit, and push.

```bash
git add -u
git commit -m "style: apply rustfmt across all crates"   # only if fmt changed files
git push origin aaron/agentic-coder
```

---

## Step 4 — Open the pull request

Create the PR targeting `main`. Use a title that matches the version bump.

```bash
gh pr create \
  --base main \
  --title "feat: vX.Y.Z — <brief summary>" \
  --body "$(cat <<'EOF'
## Summary
- Bullet 1
- Bullet 2

## Test plan
- [ ] cargo test --workspace --lib passes
- [ ] cargo clippy --workspace -- -D warnings clean
- [ ] cargo fmt --check clean
- [ ] CI passes
- [ ] After merge: tag vX.Y.Z to trigger release workflow

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Step 5 — Wait for CI

Monitor the CI run triggered by the PR push:

```bash
gh run list --branch aaron/agentic-coder --limit 5
gh run watch <run-id>
```

**Do not proceed to Step 6 until CI is green.** If CI fails, fix the issue
on `aaron/agentic-coder`, push the fix, and wait for CI to re-run.

---

## Step 6 — Merge the PR

Once CI is green, merge via the GitHub UI or:

```bash
gh pr merge <pr-number> --squash --delete-branch=false
```

`--delete-branch=false` ensures `aaron/agentic-coder` is preserved on the remote.

---

## Step 7 — Tag the release

After the PR is merged, tag `main` with the version. This triggers the
automated release workflow (`.github/workflows/release.yml`) which builds
binaries, publishes to crates.io, and attaches artifacts.

```bash
git checkout main
git pull origin main
git tag v0.4.0
git push origin v0.4.0
```

Verify the release workflow started:

```bash
gh run list --branch main --limit 3
```

---

## Step 8 — Verify the release

Once the release workflow completes:

```bash
# Check the release was created with artifacts
gh release view v0.4.0

# Verify crates.io publish (allow ~5 minutes)
cargo search arni
```

Confirm:
- GitHub release exists with the correct tag
- Release notes match the CHANGELOG `[X.Y.Z]` section
- Binaries are attached as release assets
- `arni` version on crates.io matches the tag

---

## Step 9 — Return to aaron/agentic-coder

Switch back to the working branch and sync it with the newly merged main:

```bash
git checkout aaron/agentic-coder
git fetch origin main
git merge origin/main --no-edit
git push origin aaron/agentic-coder
```

The branch is now in sync with the release commit and ready for the next
development cycle.

---

## Branch rules

| Branch | Purpose | Delete after release? |
| :--- | :--- | :--- |
| `main` | Release branch — always green, always releasable | Never |
| `aaron/agentic-coder` | Primary working branch | **Never** |
| `feature/*`, `fix/*` | Short-lived topic branches | Yes, after merge |

---

## Quick reference card

```bash
# Step 1 — Quality gates
cargo fmt --all && cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --lib

# Step 2 — Merge main
git fetch origin main && git merge origin/main --no-edit

# Step 3 — Push
git add -u && git push origin aaron/agentic-coder

# Step 4 — PR
gh pr create --base main --title "feat: vX.Y.Z — ..."

# Step 5 — Watch CI
gh run list --branch aaron/agentic-coder --limit 5

# Step 6 — Merge
gh pr merge <pr-number> --squash --delete-branch=false

# Step 7 — Tag
git checkout main && git pull origin main
git tag vX.Y.Z && git push origin vX.Y.Z

# Step 8 — Verify
gh release view vX.Y.Z

# Step 9 — Sync working branch
git checkout aaron/agentic-coder
git fetch origin main && git merge origin/main --no-edit
git push origin aaron/agentic-coder
```
