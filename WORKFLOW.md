# Arni Workflow Guide

> **CRITICAL**: This document defines the MANDATORY workflow for planning and executing work in the Arni project. All work MUST follow this two-phase approach.

## 🎯 The Two-Phase Workflow

Arni uses a strict two-phase workflow for all development work:

### Phase 1: Planning with `task-generate.md`

**ALWAYS start here.** Before writing any code, use the task generation process to create a structured Work Breakdown Structure (WBS).

**Location**: `.claude/commands/task-generate.md`

**When to use:**
- Starting any new feature or significant work
- User provides a high-level request or goal
- Need to break down complex work into manageable pieces
- Creating Epic → Feature → Task hierarchy

**Process:**
1. Analyze the user's request
2. Detect appropriate work item type (Epic, Feature, Task, Bug, Chore)
3. Apply the correct description template
4. Establish hierarchy and dependencies
5. Create issues in `bd` (beads issue tracker)
6. Sync with `bd sync`

**Output:** A complete WBS with:
- Clear hierarchy (Epic → Features → Tasks)
- Blocking dependencies where needed
- Detailed descriptions with acceptance criteria
- Priority levels assigned
- All issues created and synced

### Phase 2: Execution with `task-execute.md`

**Execute work ONLY after planning is complete.** Use the task execution workflows to implement each work item.

**Location**: `.claude/commands/task-execute.md`

**When to use:**
- After WBS is created and synced
- Ready to implement a specific task/feature/bug
- Need to follow TDD workflow
- Closing completed work

**Process:**
1. Claim work: `bd ready` → `bd update <id> --status=in_progress`
2. Understand context (ancestors, predecessors)
3. Execute with appropriate workflow:
   - **Epic**: Execute child Features sequentially
   - **Feature**: Execute child Tasks sequentially, require human verification
   - **Task**: Context-aware execution with INCOMING/OUTGOING deliverables
   - **Bug**: TDD workflow (RED → GREEN → REFACTOR)
   - **Chore**: Maintenance work with side-effect documentation
4. Update notes frequently: `bd update <id> --notes="..."`
5. Sync frequently: `bd sync`
6. Complete work: `bd close <id> --reason="..."` → `bd sync`

**Output:**
- Implemented code/fixes
- Tests passing (≥80% coverage)
- Lint/format clean
- Documentation updated
- Issues closed with detailed notes
- All changes committed and pushed

## 🚫 Anti-Patterns (DO NOT DO)

### ❌ Starting code without planning
**Wrong:**
```
User: "Add DuckDB adapter"
Agent: [immediately starts writing code]
```

**Right:**
```
User: "Add DuckDB adapter"
Agent: [uses task-generate.md to create Epic/Feature/Task WBS]
Agent: [then uses task-execute.md to implement]
```

### ❌ Creating issues without templates
**Wrong:**
```bash
bd create "Do the thing" --type=task
```

**Right:**
```bash
bd create "Add logging to PostgreSQL adapter" --type=task \
  --parent=feature-id \
  --description="**INCOMING**: Container infrastructure ready
**OUTGOING**: PostgreSQL adapter with tracing

## Description
Add tracing instrumentation to PostgreSQL adapter...

## Acceptance Criteria
- [ ] Connection methods instrumented
- [ ] Query execution logged with context
..."
```

### ❌ Executing without context
**Wrong:**
```bash
bd update task-123 --status=in_progress
[starts coding without reading ancestor/predecessor notes]
```

**Right:**
```bash
bd update task-123 --status=in_progress
bd show parent-feature-id  # Understand WHY
bd show predecessor-id     # Read INCOMING context
bd dep list task-123       # Check successors
[execute with full context]
```

### ❌ Closing without verification
**Wrong:**
```bash
[makes changes]
bd close task-123 --reason="Done"
```

**Right:**
```bash
[makes changes]
cargo test                 # All tests pass
cargo clippy -- -D warnings # Lint clean
cargo fmt --check          # Format clean
bd close task-123 --reason="Completed X, Y, Z. Tests passing, coverage maintained."
bd sync
```

## 📋 Quick Reference

### Planning Commands
```bash
# See task-generate.md for full details
# Creates Epic → Features → Tasks with dependencies
```

### Execution Commands
```bash
# Find available work
bd ready

# Claim task
bd update <id> --status=in_progress
bd sync

# Check context
bd show <parent-id>        # Understand WHY
bd show <predecessor-id>   # INCOMING context
bd dep list <id>           # Check dependencies

# Update frequently
bd update <id> --notes="Progress: completed X, next: Y"
bd sync

# Complete work
cargo test                 # Verify tests
cargo clippy -- -D warnings # Lint check
bd close <id> --reason="Detailed completion message"
bd sync
```

### TDD Cycle (for Bugs/Features)
```bash
# RED: Write failing test
cargo test                 # MUST FAIL
bd update <id> --notes="RED: Test written"
bd sync

# GREEN: Implement fix
cargo test                 # MUST PASS
bd update <id> --notes="GREEN: Implementation complete"
bd sync

# REFACTOR: Clean up
cargo test                 # Still passes
cargo clippy -- -D warnings
bd update <id> --notes="REFACTOR: Code cleaned"
bd sync

# COMPLETE
bd close <id> --reason="TDD complete: test coverage added"
bd sync
```

## 📚 Required Reading

Before starting any work, read these documents:

1. **`.claude/commands/task-generate.md`** - Learn the planning workflow
2. **`.claude/commands/task-execute.md`** - Learn the execution workflows
3. **`AGENTS.md`** - Quick reference for bd commands
4. **`.claude/CLAUDE.md`** - Arni-specific development guide

## 🎓 Workflow Examples

### Example 1: New Feature Request

```
User: "Add comprehensive testing to all adapters"

Step 1: PLAN (task-generate.md)
→ Create Epic: "Adapter Testing & Observability"
→ Create Feature 1: "Container Test Infrastructure" [P0 - Critical Path]
  → Task 1.1: Add testcontainers-rs dependency
  → Task 1.2: Create shared utilities
  → Task 1.3: Configure CI/CD
→ Create Feature 2: "Adapter Logging Integration" [P1, blocked by F1]
  → Task 2.1: Add logging to PostgreSQL/MySQL
  → Task 2.2: Add logging to MongoDB
  → Task 2.3: Add logging to Oracle/SQL Server
  → Task 2.4: Add logging to SQLite/DuckDB
→ Create Feature 3: "Error Handling Enhancement" [P1, blocked by F1]
  → Task 3.1: Audit error patterns
  → Task 3.2: Implement improvements
→ Create Feature 4: "Integration Test Suite" [P1, blocked by F1,F2,F3]
  → Task 4.1: Tests for PostgreSQL/MySQL
  → Task 4.2: Tests for MongoDB
  → Task 4.3: Tests for Oracle/SQL Server
  → Task 4.4: Tests for SQLite/DuckDB

Step 2: EXECUTE (task-execute.md)
→ bd ready  # Shows Task 1.1 (critical path)
→ Execute Task 1.1 (with context awareness)
→ Execute Task 1.2 (reads 1.1's OUTGOING notes)
→ Execute Task 1.3 (reads 1.2's OUTGOING notes)
→ Feature 1 complete → Creates human verification task
→ Tasks 2.1-2.4 become available (F2 unblocked)
→ Tasks 3.1-3.2 become available (F3 unblocked)
→ Continue until all Features complete
→ Epic complete → Creates human verification task
```

### Example 2: Bug Fix

```
User: "Login fails with 500 error"

Step 1: PLAN (task-generate.md)
→ Search for related Feature (e.g., "User Authentication")
→ Create Bug: "Login fails with 500 error"
  --parent=auth-feature-id
  --type=bug
  --priority=1
→ Use Bug template with Steps to Reproduce

Step 2: EXECUTE (task-execute.md)
→ bd update bug-id --status=in_progress
→ Phase 0: Attempt reproduction
→ Phase 1 (RED): Write failing test
→ Phase 2 (GREEN): Implement fix
→ Phase 3 (REFACTOR): Clean up code
→ Phase 4: Complete and document root cause
→ bd close bug-id --reason="Fixed with test coverage"
```

## 🔄 State Management (CRITICAL)

**ALWAYS sync state throughout execution:**

```bash
# Start work
bd update <id> --status=in_progress
bd sync

# After each phase/milestone
bd update <id> --notes="Completed X, discovered Y, next: Z"
bd sync

# When blocked
bd update <id> --notes="BLOCKED: Cannot proceed until..."
bd sync

# When discovering new work
bd create "New discovered task" --type=task
bd dep add new-id --depends-on current-id --type=discovered-from
bd sync

# Before completion
bd update <id> --notes="Final summary: Delivered A, B, C"
bd close <id> --reason="Complete with full details"
bd sync
```

**Why this matters:**
- Session interruptions preserve progress
- Team visibility into discoveries and blockers
- Failed executions leave debugging breadcrumbs
- Dependency tracking maintains context

## 🎯 Success Criteria

Work is complete when:

- [ ] WBS created using task-generate.md
- [ ] All tasks executed using task-execute.md
- [ ] Each task has detailed notes about deliverables
- [ ] All tests pass (`cargo test`)
- [ ] Lint clean (`cargo clippy -- -D warnings`)
- [ ] Format clean (`cargo fmt --check`)
- [ ] Coverage ≥80% maintained
- [ ] All issues closed with detailed reasons
- [ ] Changes committed and pushed
- [ ] `bd sync` completed

---

**Remember**: Task generation and task execution are separate, sequential phases. Never skip planning. Never execute without context.
