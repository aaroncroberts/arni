# Draft PR Workflow

A development workflow where the pull request is opened at the **start** of the
release cycle, not the end. Work accumulates on a persistent working branch.
CI runs continuously. Others can contribute throughout. The PR merges only when
everything is green.

---

## Philosophy

> Open early. Commit often. Merge only when CI says so.

Traditional workflows open a PR when work is "done." This workflow opens a
draft PR before writing the first line of code. The PR is a collaboration
surface, not a delivery mechanism.

**Core rules:**
- The working branch is **never deleted** — it is the permanent home for
  in-progress work
- Conflicts are resolved locally via merge, never rebase — this keeps
  contributor forks intact and avoids force-push surprises
- The PR stays open until CI is fully green — not until you think it's ready
- Release artifacts (tags, binaries, published packages) are created
  **after** the merge, triggered by the tag, not by the PR

---

## Roles

| Term | Meaning |
| :--- | :--- |
| `<working-branch>` | Your permanent development branch (e.g. `dev`, `next`, `aaron/agentic-coder`) |
| `main` | The release branch — always green, always releasable |
| `<version>` | The version being released (e.g. `v1.2.0`) |

---

## Step 1 — Open the draft PR

At the start of any release cycle, open a draft PR before writing any code.

```bash
gh pr create \
  --base main \
  --draft \
  --title "feat: <version> — <brief summary>" \
  --body "$(cat <<'EOF'
## Summary
<!-- Fill in as work progresses -->

## Changes
<!-- Updated as commits land -->

## Test plan
- [ ] Tests pass
- [ ] Lint clean
- [ ] Format clean
- [ ] All CI jobs green
- [ ] CHANGELOG updated
- [ ] Version bumped
EOF
)"
```

The PR is now your workspace. CI will run on every push from this point forward.

---

## Step 2 — Development loop

Commit and push normally throughout development. Each push triggers a CI run
on the open PR, giving you continuous feedback.

```bash
git add <files>
git commit -m "type: description"
git push origin <working-branch>
```

**Picking up changes from main** (do this whenever main advances):

```bash
git fetch origin main
git merge origin/main --no-edit    # always merge, never rebase
# Resolve any conflicts locally, then:
git push origin <working-branch>
```

GitHub will show the PR as conflict-free once the merge commit is pushed.
CI re-runs automatically.

---

## Step 3 — Local quality gates

When all planned work is complete, run your full quality suite locally and
fix every failure before proceeding.

```bash
# Replace with your project's actual quality commands
<run tests>
<run linter>
<check formatting>
<run type checker / build>
```

All checks must pass. Do not mark the PR ready with a failing local check.

---

## Step 4 — Mark the PR ready for review

```bash
gh pr ready <pr-number>
```

Update the PR body with the final summary and change list. CI runs again on
the ready-for-review transition.

---

## Step 5 — Wait for CI

**Do not merge until every CI job is green.** Monitor progress:

```bash
gh run list --branch <working-branch> --limit 5
gh run watch <run-id>
```

If CI fails: fix on `<working-branch>`, push, wait for CI to re-run. Repeat
until green.

---

## Step 6 — Merge

```bash
gh pr merge <pr-number> --squash --delete-branch=false
```

`--delete-branch=false` preserves the working branch on the remote.
Do not use `--delete-branch` or `--delete-branch=true`.

---

## Step 7 — Tag and release

After the merge, tag `main` to trigger your release automation (binary
builds, package publishes, artifact uploads, etc.).

```bash
git checkout main
git pull origin main
git tag <version>
git push origin <version>
```

Verify the release workflow started:

```bash
gh run list --branch main --limit 3
```

---

## Step 8 — Verify the release

Once the release workflow completes:

```bash
gh release view <version>
```

Confirm:
- GitHub release exists with the correct tag
- Release notes match the CHANGELOG entry for `<version>`
- Expected artifacts are attached

---

## Step 9 — Sync the working branch

Merge the release commit back into the working branch so the next cycle
starts from the correct base.

```bash
git checkout <working-branch>
git fetch origin main
git merge origin/main --no-edit
git push origin <working-branch>
```

---

## Branch rules

| Branch | Purpose | Delete after release? |
| :--- | :--- | :--- |
| `main` | Release branch — always green | Never |
| `<working-branch>` | Permanent development branch | **Never** |

Short-lived topic branches are not used in this workflow. All work lands
directly on `<working-branch>`.

---

## Why merge instead of rebase?

Rebasing rewrites commit SHAs. If collaborators have checked out
`<working-branch>` or based work on it, a rebase forces them to recover
manually. Merging preserves history and keeps all forks valid. The resulting
merge commit is a clear record of when main's changes were integrated.

---

## Why open the draft PR first?

| Late PR (traditional) | Draft PR (this workflow) |
| :--- | :--- |
| CI runs once, at the end | CI runs on every push |
| Surprises at review time | Issues surface early |
| Others can't contribute easily | PR is open for collaboration from day one |
| PR is a gate | PR is a workspace |
| Work invisible until "ready" | Work visible throughout |

---

## Quick reference card

```bash
# Start of cycle
gh pr create --base main --draft --title "feat: <version> — ..."

# Development loop
git commit -m "..." && git push origin <working-branch>

# Pick up main changes
git fetch origin main && git merge origin/main --no-edit && git push

# Pre-merge quality gates
<run your quality checks>

# Mark ready
gh pr ready <pr-number>

# Watch CI
gh run list --branch <working-branch> --limit 5

# Merge (only when CI is green)
gh pr merge <pr-number> --squash --delete-branch=false

# Tag
git checkout main && git pull origin main
git tag <version> && git push origin <version>

# Verify
gh release view <version>

# Sync working branch
git checkout <working-branch>
git fetch origin main && git merge origin/main --no-edit
git push origin <working-branch>
```
