# Release Process

This document defines the mandatory steps for every arni release.
Follow them in order. Do not skip steps.

---

## Philosophy

Work is visible from day one. The draft PR is opened at the **start** of the
release cycle, not the end. Commits accumulate on `aaron/agentic-coder` and
are pushed continuously. CI runs on every push. Contributors can participate
via the open PR. The PR stays open until CI is fully green, then it is merged,
tagged, and released.

```
Open draft PR → commit & push loop → CI passes → mark ready → merge → tag → release
```

All work happens on `aaron/agentic-coder`. **Never delete this branch.**

---

## Step 1 — Open the draft PR

At the start of any release cycle, open a draft PR from `aaron/agentic-coder`
targeting `main`. Do this before writing any code.

```bash
gh pr create \
  --base main \
  --draft \
  --title "feat: vX.Y.Z — <brief summary of the release>" \
  --body "$(cat <<'EOF'
## Summary
<!-- Fill in as work progresses -->

## Changes
<!-- Updated as commits land -->

## Test plan
- [ ] cargo test --workspace --lib passes
- [ ] cargo clippy --workspace -- -D warnings clean
- [ ] cargo fmt --check clean
- [ ] All CI jobs green
- [ ] CHANGELOG updated
- [ ] Version bumped in Cargo.toml files

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

The PR number is your workspace. Reference it in commits if useful
(`gh pr view` to retrieve it later).

---

## Step 2 — Development loop

Work proceeds in normal commit cycles on `aaron/agentic-coder`. After each
logical unit of work:

```bash
# Stage and commit
git add <files>
git commit -m "feat|fix|chore|docs: description"

# Push — this triggers a CI run on the open PR
git push origin aaron/agentic-coder
```

**Picking up main's changes** (do this whenever main advances):

```bash
git fetch origin main
git merge origin/main --no-edit
# Resolve any conflicts locally, then:
git push origin aaron/agentic-coder
```

Never rebase a branch with an open PR — merge only. This preserves commit
history and keeps collaborator forks in sync.

---

## Step 3 — Local quality gates (before marking ready)

When all planned work is complete, run the full quality gate locally. Fix
every failure before proceeding.

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

All four must exit 0. Fix any failures and push before marking the PR ready.

---

## Step 4 — Mark the PR ready for review

Convert the draft PR to ready-for-review:

```bash
gh pr ready <pr-number>
```

Update the PR body to reflect the final state — fill in the Summary and
Changes sections if not already done. CI will run automatically on the
ready-for-review transition.

---

## Step 5 — Wait for CI

Monitor the CI run:

```bash
gh run list --branch aaron/agentic-coder --limit 5
gh run watch <run-id>
```

**Do not proceed to Step 6 until all CI jobs are green.** If CI fails, fix
the issue on `aaron/agentic-coder`, push the fix, and wait for CI to re-run.
The PR must not be merged while any CI job is red.

---

## Step 6 — Merge the PR

Once CI is fully green:

```bash
gh pr merge <pr-number> --squash --delete-branch=false
```

`--delete-branch=false` ensures `aaron/agentic-coder` is preserved on the
remote. Never use `--delete-branch` or `--delete-branch=true`.

---

## Step 7 — Tag the release

After the PR is merged, check out main, pull, and tag with the version.
This triggers the automated release workflow (`.github/workflows/release.yml`)
which creates a GitHub Release with notes extracted from the matching
CHANGELOG section. Source archives (zip, tar.gz) are attached automatically.

```bash
git checkout main
git pull origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

Verify the release workflow started:

```bash
gh run list --branch main --limit 3
```

---

## Step 8 — Verify the release

Once the release workflow completes:

```bash
# Check the GitHub Release was created
gh release view vX.Y.Z
```

Confirm:
- GitHub release exists with the correct tag
- Release notes match the CHANGELOG `[X.Y.Z]` section
- Source archives (zip, tar.gz) are attached automatically by GitHub

---

## Step 9 — Return to aaron/agentic-coder

Switch back to the working branch and sync it with the newly merged main.
This is the base for the next release cycle.

```bash
git checkout aaron/agentic-coder
git fetch origin main
git merge origin/main --no-edit
git push origin aaron/agentic-coder
```

---

## Branch rules

| Branch | Purpose | Delete after release? |
| :--- | :--- | :--- |
| `main` | Release branch — always green, always releasable | Never |
| `aaron/agentic-coder` | Primary working branch | **Never** |

Short-lived topic branches are not used in this workflow. All work lands
directly on `aaron/agentic-coder` via commits.

---

## Conflict resolution

When `main` advances (another PR merges while this one is open):

```bash
git fetch origin main
git merge origin/main --no-edit   # merge, never rebase
# Fix any conflicts, re-run quality gates, then:
git push origin aaron/agentic-coder
```

CI re-runs automatically after the push. GitHub will show the PR as
conflict-free once the merge commit is pushed.

---

## Quick reference card

```bash
# Start of cycle — open draft PR
gh pr create --base main --draft --title "feat: vX.Y.Z — ..."

# Development loop
git commit -m "..." && git push origin aaron/agentic-coder

# Pick up main changes
git fetch origin main && git merge origin/main --no-edit && git push

# Pre-merge quality gates
cargo fmt --all && cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --lib

# Mark ready
gh pr ready <pr-number>

# Watch CI
gh run list --branch aaron/agentic-coder --limit 5

# Merge (only when CI green)
gh pr merge <pr-number> --squash --delete-branch=false

# Tag
git checkout main && git pull origin main
git tag vX.Y.Z && git push origin vX.Y.Z

# Verify
gh release view vX.Y.Z

# Sync working branch
git checkout aaron/agentic-coder
git fetch origin main && git merge origin/main --no-edit
git push origin aaron/agentic-coder
```
