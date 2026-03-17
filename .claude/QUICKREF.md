# Quick Reference: Arni Workflow

> **ALWAYS follow the two-phase approach: PLAN → EXECUTE**

## Phase 1: PLAN (ALWAYS FIRST)

```bash
# Use: .claude/commands/task-generate.md
```

**Do this when:**
- User provides high-level request
- Starting any new feature or significant work
- Need to break down complex work

**Process:**
1. Analyze user request
2. Detect work item type (Epic/Feature/Task/Bug/Chore)
3. Apply correct description template
4. Create WBS with hierarchy and dependencies
5. Create issues with `bd create`
6. Sync with `bd sync`

**Output:** Structured WBS with all issues created and synced

## Phase 2: EXECUTE (ONLY AFTER PLANNING)

```bash
# Use: .claude/commands/task-execute.md
```

**Do this when:**
- WBS is created and synced
- Ready to implement specific work item

**Process:**
1. **Start**: `bd ready` → `bd update <id> --status=in_progress` → `bd sync`
2. **Context**: Read ancestors, predecessors, successors
3. **Execute**: Follow type-specific workflow:
   - Epic: Execute Features sequentially
   - Feature: Execute Tasks sequentially + human verification
   - Task: Context-aware with INCOMING/OUTGOING
   - Bug: TDD (RED → GREEN → REFACTOR)
   - Chore: Maintenance + side effects
4. **Update**: `bd update <id> --notes="..."` → `bd sync` (frequently!)
5. **Complete**: Tests + lint + `bd close <id>` → `bd sync`

**Output:** Working code, tests passing, issues closed with notes

## Type-Specific Quick Guide

### Epic
- Execute child Features sequentially
- Update notes after each Feature completes
- Create human verification task at end
- Sync frequently

### Feature
- Execute child Tasks/Bugs/Chores sequentially
- Verify each child updated their notes
- Map to Acceptance Criteria
- Create human verification task at end
- Sync frequently

### Task
- Read ancestor context (WHY)
- Read predecessor notes (INCOMING)
- Check successor expectations (OUTGOING)
- Execute scoped work
- Document deliverables in notes
- Sync frequently

### Bug (TDD MANDATORY)
- Phase 0: Reproduction
- Phase 1 (RED): Write failing test
- Phase 2 (GREEN): Implement fix
- Phase 3 (REFACTOR): Clean up
- Phase 4: Complete with root cause
- Sync after each phase

### Chore
- Like Task but for maintenance
- Document side effects explicitly
- Verify goal achieved

## State Management (OBSESSIVE)

```bash
# Starting work
bd update <id> --status=in_progress
bd sync

# After progress
bd update <id> --notes="Completed X, next: Y"
bd sync

# When blocked
bd update <id> --notes="BLOCKED: reason"
bd sync

# Completing
bd update <id> --notes="Final summary"
bd close <id> --reason="Details"
bd sync
```

**Sync frequency:** Every 5-10 minutes minimum, after every phase

## Quality Gates (BEFORE CLOSING)

```bash
cargo test                 # All tests pass
cargo clippy -- -D warnings # Lint clean
cargo fmt --check          # Format clean
cargo tarpaulin            # Coverage ≥80%
```

## Critical Rules

1. **Never start code without planning** - Use task-generate.md first
2. **Never execute without context** - Read ancestors/predecessors
3. **Never close without verification** - Tests + lint must pass
4. **Never skip state updates** - `bd update` + `bd sync` frequently
5. **Never close with open children** - Check descendants first

## Example Flow

```bash
# User: "Add comprehensive testing to adapters"

# PHASE 1: PLAN (task-generate.md)
# → Create Epic with 4 Features and 13 Tasks
# → Set dependencies (critical path, blockers)
# → bd sync

# PHASE 2: EXECUTE (task-execute.md)
bd ready                                    # Shows first task
bd update arni-bcq.1.1 --status=in_progress
bd sync

# Work on Task 1.1
bd update arni-bcq.1.1 --notes="Added testcontainers-rs..."
bd sync

cargo test && cargo clippy -- -D warnings
bd close arni-bcq.1.1 --reason="Dependency added, tests pass"
bd sync

# Continue with next tasks...
```

## Reference Documents

- **Complete workflow**: [`WORKFLOW.md`](../../WORKFLOW.md)
- **Task generation**: [`commands/task-generate.md`](commands/task-generate.md)
- **Task execution**: [`commands/task-execute.md`](commands/task-execute.md)
- **Project guide**: [`CLAUDE.md`](CLAUDE.md)
- **Quick reference**: [`../../AGENTS.md`](../../AGENTS.md)

---

**Remember:** Planning and execution are separate, sequential phases. Never skip either one.
