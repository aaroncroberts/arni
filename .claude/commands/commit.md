# Commit Command

Stage all changes and create a commit with a concise, impact-focused message.

## What This Command Does

1. **Stage all changes**: `git add -A`
2. **Analyze changes**: Review what was modified
3. **Generate commit message**: Create impact-focused message
4. **Commit**: Execute `git commit` with the generated message

## Commit Message Format

```
# [Brief title: what changed and why it matters]

## Summary
[1-2 sentences: what you did, why it matters]

## Impact

**[Category 1]:**
- [Observable change with quantification]

**[Category 2]:**
- [Observable change with quantification]
```

## Impact Categories

Choose 2-3 categories that describe observable changes:

- **Data Quality** - Fixed calculations, corrected inconsistencies, improved accuracy
- **Features** - New capabilities, enhanced functionality, improved UX
- **Bugs** - Eliminated errors, fixed broken behavior
- **Performance** - Reduced time/memory/resources (with numbers)
- **Automation** - Reduced manual work, prevented future errors
- **Security** - Fixed vulnerabilities, strengthened protection
- **Developer Experience** - Better debugging, clearer code, improved tooling
- **Process** - Streamlined workflows, better collaboration

## Key Principles

- NO file lists (git diff shows this)
- NO testing commands/output (noise)
- NO "next steps" sections
- YES observable impact with numbers
- YES 2-3 focused impact categories
- YES concise summaries

## Workflow

When `/commit` is invoked:

1. Run `git status` to see what changed
2. Run `git diff --stat HEAD` to understand scope
3. Stage all changes with `git add -A`
4. Generate an impact-focused commit message
5. Execute `git commit -m "$(cat <<'EOF' ... EOF)"` with the message
6. Show the result of the commit

## Example Output

```bash
$ git add -A
$ git commit -m "..."
[main abc1234] Fix payment timeout errors (27% failure rate → 0%)
 3 files changed, 45 insertions(+), 12 deletions(-)
```

## Usage

Say `/commit` and I will:
1. Stage all changes
2. Generate an impact-focused commit message
3. Execute the commit
4. Show you the result
