---
description: Execute a beads task with TDD workflow, tests, and quality gates
argument-hint: <issue-id or "ready" to pick from available work>
tools:
  - Bash(bd:*)
  - Bash(npm:*)
  - Bash(git:*)
  - Read
  - Edit
  - Write
  - Glob
  - Grep
---

# Creating Beady Issues (Beads)

Beads is an installed CLI for persistent issue tracking across sessions. Use for strategic work, dependencies, and multi-session projects.

## When to Use Beads vs TodoWrite

- **Beads (`bd create`)**: Multi-session work, dependencies, blocking issues, discovered tasks, strategic planning
- **TodoWrite**: Single-session execution, simple task lists, temporary tracking

## Issue Types

```bash
--type=feature    # New feature or functionality
--type=epic       # Large feature with multiple dependencies
--type=task       # Work item or implementation task
--type=bug        # Bug fix or defect
--type=refactor   # Code improvement without new features
--type=docs       # Documentation or README updates
--type=test       # Testing or test infrastructure
--type=chore      # Maintenance, dependencies, configuration
```

## Priority Levels

```bash
--priority=0      # P0 (Critical - block everything)
--priority=1      # P1 (High - urgent)
--priority=2      # P2 (Medium - standard)
--priority=3      # P3 (Low - nice to have)
--priority=4      # P4 (Backlog - future)
```
⚠️ **DO NOT use**: "high", "medium", "low" (use numeric values only)

## Creating Issues

### Basic Issue
```bash
bd create --title="Implement user authentication" --type=feature --priority=2
```

### Full-Featured Issue
```bash
bd create \
  --title="Add error handling to API" \
  --type=task \
  --priority=1 \
  --description="Handle network failures gracefully" \
  --assignee=username
```

## Status Management

```bash
bd update <id> --status=in_progress    # Claim work
bd update <id> --status=open           # Return to backlog
bd close <id> --reason="Completed"     # Mark complete
bd close <id1> <id2> <id3>             # Batch close (more efficient)
```

## Dependencies & Relationships

### Dependency Types

**Blocking Dependencies** (prevent progress):
```bash
bd dep add <issue> --depends-on <blocker>
# OR
bd dep add <issue> <blocker>           # Default: --type=blocks
# OR
bd dep <blocker> --blocks <issue>
# issue is blocked and cannot start until blocker completes
# ✅ Use for: prerequisites, critical path, sequential work
```

**Hierarchy & Structure**:
```bash
bd dep add <child> --depends-on <parent> --type=parent-child
# parent-child: Parent task must complete before child can progress
# ✅ Use for: epic → feature → task breakdown, hierarchical decomposition
```

**Relationship Types** (different purposes):
```bash
# TRACKING - issue monitors/references another
bd dep add <tracker> --depends-on <tracked> --type=tracks
# ✅ Use for: story points tracked in another issue, sync points

# VALIDATION - issue validates/verifies another
bd dep add <test> --depends-on <feature> --type=validates
# ✅ Use for: testing depends on feature, QA verification

# DISCOVERY - issue was discovered while working on another
bd dep add <discovered> --depends-on <source> --type=discovered-from
# ✅ Use for: tech debt found during implementation, scope expansion

# CAUSATION - one issue caused/created another
bd dep add <follow-up> --depends-on <root> --type=caused-by
# ✅ Use for: bug fixes, followup work, issue chains

# TIME-BASED - work until another issue is done
bd dep add <work> --depends-on <deadline> --type=until
# ✅ Use for: temporary workarounds, deadline-based dependencies

# SUPERSEDING - one issue replaces/makes another obsolete
bd dep add <new> --depends-on <old> --type=supersedes
# ✅ Use for: version upgrades, refactoring replacements
```

**Loose Relationships** (no blocking, for knowledge graphs):
```bash
bd dep relate <issue1> <issue2>        # Bidirectional "see also"
# Related issues referenced each other
# ✅ Use for: similar features, related concerns, cross-references (non-blocking)
```

### Querying Dependencies

```bash
bd show <id>                           # View issue with all relationships
bd dep list <id>                       # List dependencies of an issue
bd dep tree <id>                       # Show dependency tree/graph
bd blocked                             # Show all currently blocked issues
bd dep cycles                          # Detect circular dependencies
```

### Example: Multi-Level Feature with Dependencies

```bash
# Epic (high-level feature)
bd create --title="Implement cache layer" --type=epic --priority=1
# Returns: beads-epic-1

# Design task (must complete first)
bd create --title="Design cache schema" --type=task --priority=1
# Returns: beads-001

# Make epic depend on design (blocks epic until design is done)
bd dep add beads-epic-1 beads-001 --type=parent-child

# Implementation task (depends on design)
bd create --title="Implement cache storage" --type=task --priority=1
# Returns: beads-002
bd dep add beads-002 beads-001         # Default: --type=blocks

# Testing task (validates implementation)
bd create --title="Test cache performance" --type=test --priority=1
# Returns: beads-003
bd dep add beads-003 beads-002 --type=validates

# Related optimization work (see also, non-blocking)
bd create --title="Optimize query patterns" --type=task --priority=3
# Returns: beads-004
bd dep relate beads-004 beads-003

# View the dependency structure
bd dep tree beads-epic-1               # Shows all dependencies
bd show beads-epic-1                   # Shows what's blocking it
bd blocked                             # Shows beads-002 and beads-003 are blocked
```

## Useful Commands

```bash
bd ready                        # Issues ready to work (no blockers)
bd list --status=open          # All open issues
bd list --status=in_progress   # Your active work
bd list --status=closed        # Completed issues

bd show <id>                    # Detailed view (dependencies, assignees, etc.)
bd epic <name>                  # Epic management
bd stats                        # Project statistics

bd sync                         # Push changes to remote
bd doctor                       # Check for sync issues
```

## Session Workflow

### Starting Work
```bash
bd ready                                    # Find available work
bd show <id>                                # Review details
bd update <id> --status=in_progress        # Claim it
```

### Completing Work

**Before closing ANY issue**, verify:
```bash
npm run lab:test                           # Unit tests pass (NO E2E - keep fast)
npm run lab:clean                          # Lint + typecheck pass (MANDATORY)
```

⚠️ **Work is NOT done until lint errors are fixed.** Do not close issues with lint failures.

#### 🚨 CRITICAL: Check for Open Descendants Before Closing

**NEVER close an issue that has open children/descendants.** Before closing ANY issue:

```bash
# Check for children (issues that have this as their parent)
bd dep list <id> --direction=up -t parent-child

# If ANY children are returned, check their status
bd show <child-id>                         # Verify status is 'closed' or 'tombstoned'
```

**Closure Rules:**
- ✅ **Can close**: All children/descendants are `closed` or `tombstoned`
- ❌ **Cannot close**: ANY child/descendant has status `open`, `in_progress`, or `blocked`

If descendants are still open:
1. Complete the outstanding child tasks first
2. OR close/tombstone the children if no longer needed
3. OR move children to a different parent if work should continue separately

**Example validation:**
```bash
# Check if issue has children
bd dep list todd-lab-xyz --direction=up -t parent-child
# Output: todd-lab-abc (open), todd-lab-def (closed)

# todd-lab-abc is still OPEN - cannot close parent!
# First, either:
bd close todd-lab-abc --reason="Completed"  # Close the child
# OR
bd update todd-lab-abc --status=tombstoned  # Tombstone if abandoned

# Now parent can be closed
bd close todd-lab-xyz --reason="All children complete"
```

Then close and sync:
```bash
bd close <id1> <id2> ...                   # Close completed issues
bd sync                                    # Push to remote
```

### Cleanup (IMPORTANT)
**Before ending a session**, clean up any orphaned test processes:
```bash
pkill -f vitest 2>/dev/null || true        # Kill orphaned vitest workers
```

⚠️ Vitest workers can accumulate and cause system pressure if tests are interrupted or run in watch mode. Always clean up at session end.

---

# 🔄 State Management Obsession (ALL TYPES)

**CRITICAL**: Beads enables persistent task state across sessions. You **MUST** obsessively update beads throughout execution to maintain continuity.

## State Update Rules

**Immediately when starting ANY issue:**
```bash
bd update <id> --status=in_progress
bd sync
```

**After EVERY significant step (discovery, decision, blocker):**
```bash
bd update <id> --notes="Completed X, discovered Y, next: Z"
bd sync
```

**When blocked or encountering issues:**
```bash
bd update <id> --notes="BLOCKED: Cannot proceed until X is resolved"
bd sync
```

**When creating new discovered work:**
```bash
bd create --title="..." --type=task
bd dep add <new-id> --depends-on <current-id> --type=discovered-from
bd sync
```

**Before completing work:**
```bash
bd update <id> --notes="Final summary: Completed A, B, C. Tests passing."
bd close <id> --reason="..."
bd sync
```

## Why This Matters

- **Session Interruption**: If execution stops (timeout, error, user interruption), all progress is preserved
- **Collaboration**: Team members can see what you discovered and where you left off
- **Debugging**: Failed executions leave breadcrumbs for troubleshooting
- **Dependency Tracking**: Discovered work is linked to its source context

## Pattern Applied to All Types

Every type-specific workflow below integrates these state management touchpoints. Treat `bd update` and `bd sync` as first-class execution steps, not afterthoughts.

**Sync Frequency Guidance:**
- After every phase completion (TDD phases, workflow steps)
- After creating/closing issues
- Before any pause or context switch
- Minimum: Every 5-10 minutes of active work

---

# Type-Specific Execution Workflows

The following sections describe how to execute different issue types. All workflows integrate the State Management Obsession pattern above.

## Epic Execution Workflow

**Intent**: Epics are large features with multiple child Features. Execute Features in succession, creating persistent checkpoints after each.

**Process**:

1. **Claim and understand scope:**
   ```bash
   bd update <epic-id> --status=in_progress
   bd show <epic-id>                              # Read description, Success Criteria
   bd sync
   ```

2. **Discover child Features:**
   ```bash
   bd dep list <epic-id> --direction=up -t parent-child
   bd dep tree <epic-id>                          # Understand dependency order
   ```

3. **Execute Features in succession:**
   ```bash
   # For each Feature (in dependency order):
   /task-execute <feature-id>                     # Execute the Feature
   bd update <epic-id> --notes="Completed Feature: <feature-id>. Next: <next-feature-id>"
   bd sync
   ```

4. **Create Human Verification task at completion:**
   ```bash
   bd create \
     --title="Human Verification: [Epic Name]" \
     --type=task \
     --priority=0 \
     --description="## Verification Summary

   Epic: <epic-id> - [Epic Name]
   Completed Features: <list feature-ids>

   ## Artifacts to Verify
   - [ ] [Artifact 1 location]
   - [ ] [Artifact 2 location]

   ## Verification Steps
   - [ ] Step 1: [Action] → Expected: [Outcome]
   - [ ] Step 2: [Action] → Expected: [Outcome]

   ⚠️ **NON-AGENTIC**: This task is for human verification only. Do NOT execute with /task-execute."

   # Link verification task to Epic
   bd dep add <verification-task-id> --depends-on <epic-id> --type=validates
   ```

5. **Complete the Epic:**
   ```bash
   bd update <epic-id> --notes="All Features complete. Human Verification task: <task-id>"
   bd close <epic-id> --reason="All child Features executed successfully"
   bd sync
   ```

**Key Principles:**
- Execute Features sequentially (not in parallel) to maintain clear progress
- Update Epic notes after EACH Feature completes
- Sync frequently to preserve progress across potential interruptions
- Human Verification is REQUIRED—do not skip this step
- Verification task is type=task (not agentic), explicitly marked for human review

## Feature Execution Workflow

**Intent**: Features are complete units of functionality with multiple child Tasks/Chores/Bugs. Execute children in succession, verify implementation, require human validation.

**Process**:

1. **Claim and understand scope:**
   ```bash
   bd update <feature-id> --status=in_progress
   bd show <feature-id>                           # Read description, Acceptance Criteria
   bd sync
   ```

2. **Discover child work items:**
   ```bash
   bd dep list <feature-id> --direction=up -t parent-child
   bd dep tree <feature-id>                       # Understand dependency order
   ```

3. **Execute children in succession:**
   ```bash
   # For each child Task/Chore/Bug (in dependency order):
   /task-execute <child-id>                       # Execute the child

   # Verify child updated its notes:
   bd show <child-id>                             # Check notes field for execution summary

   # Update Feature with progress:
   bd update <feature-id> --notes="Completed <child-id>: [summary]. Next: <next-child-id>"
   bd sync
   ```

4. **Verify Acceptance Criteria:**
   ```bash
   # Review original Acceptance Criteria from step 1
   # Ensure all criteria are met by completed children
   bd update <feature-id> --notes="All children complete. Acceptance Criteria verified: [list criteria]"
   bd sync
   ```

5. **Create Human Verification task at completion:**
   ```bash
   bd create \
     --title="Human Verification: [Feature Name]" \
     --type=task \
     --priority=0 \
     --description="## Verification Summary

   Feature: <feature-id> - [Feature Name]
   Completed Work: <list child-ids>

   ## Acceptance Criteria to Verify
   - [ ] Criterion 1: [How to verify] → Expected: [Result]
   - [ ] Criterion 2: [How to verify] → Expected: [Result]

   ## Artifacts to Verify
   - [ ] Code changes in [file paths]
   - [ ] Tests in [test file paths]
   - [ ] Documentation in [doc paths]

   ## Manual Testing Steps
   - [ ] Step 1: [Action] → Expected: [Outcome]
   - [ ] Step 2: [Action] → Expected: [Outcome]

   ⚠️ **NON-AGENTIC**: This task is for human verification only. Do NOT execute with /task-execute."

   # Link verification task to Feature
   bd dep add <verification-task-id> --depends-on <feature-id> --type=validates
   ```

6. **Complete the Feature:**
   ```bash
   bd update <feature-id> --notes="All children executed. Acceptance Criteria met. Human Verification: <task-id>"
   bd close <feature-id> --reason="Feature complete with verification task created"
   bd sync
   ```

**Key Principles:**
- Execute children sequentially to maintain coherent state
- **Verify each child updated its notes**—this ensures execution context is preserved
- Map completed children back to Acceptance Criteria before closing
- Human Verification is REQUIRED for all Features
- Verification task must include specific, testable steps (not vague "check if it works")
- Sync after every child completion to preserve progress

## Task Execution Workflow

**Intent**: Tasks are scoped work items that fit within Features. They are context-aware: understand ancestor goals, receive handoffs from predecessors, and deliver to successors.

**Process**:

1. **Claim and understand the immediate scope:**
   ```bash
   bd update <task-id> --status=in_progress
   bd show <task-id>                              # Read description, Acceptance Criteria
   bd sync
   ```

2. **Understand ancestor context (WHY this task exists):**
   ```bash
   # Find parent Feature or Epic
   bd dep list <task-id> -t parent-child

   # Read ancestor for broader context
   bd show <parent-id>                            # Understand Feature goals, Success Criteria
   ```

3. **Read predecessor notes (INCOMING context/deliverables):**
   ```bash
   # Find what blocks this task (what must complete first)
   bd dep list <task-id> --type=blocks

   # Read predecessor notes for handoff information
   bd show <predecessor-id>                       # Check notes field for OUTGOING deliverables
   ```

4. **Understand successor expectations (OUTGOING deliverables):**
   ```bash
   # Find what depends on this task
   bd dep list <task-id> --direction=up --type=blocks

   # Read successor descriptions to understand what they expect
   bd show <successor-id>                         # Check description for INCOMING expectations
   ```

5. **Execute the scoped work:**
   ```bash
   # Implement, write tests, run validation
   npm run lab:test                               # Unit tests pass
   npm run lab:clean                              # Lint + typecheck pass

   # Update notes after significant milestones
   bd update <task-id> --notes="Completed implementation of X. Tests passing."
   bd sync
   ```

6. **Document deliverables in notes (OUTGOING):**
   ```bash
   bd update <task-id> --notes="COMPLETED:
   - Implemented [feature/fix/change]
   - Files modified: [paths]
   - Tests added: [test paths]
   - Decisions: [key decisions made]
   - OUTGOING for successors: [deliverables/artifacts]
   - Discovered issues: [any new work found]"
   ```

7. **Complete the Task:**
   ```bash
   npm run lab:test && npm run lab:clean          # Final validation
   bd close <task-id> --reason="Scope complete. Deliverables documented in notes."
   bd sync
   ```

**Key Principles:**
- Tasks are NOT isolated—they inherit context from ancestors and pass context to successors
- **Always read predecessor notes**—they contain handoff information you need
- **Always document OUTGOING deliverables**—successors depend on this context
- Ancestor context provides the "why" behind the work
- Successor expectations inform what deliverables matter
- State updates should include discovered issues and key decisions
- Sync frequently, especially after documenting deliverables

## Chore Execution Workflow

**Intent**: Chores are maintenance, configuration, and operational tasks. Like Tasks, they are context-aware but focus on system health rather than feature delivery.

**Process**:

1. **Claim and understand the scope:**
   ```bash
   bd update <chore-id> --status=in_progress
   bd show <chore-id>                             # Read description, Completion Criteria
   bd sync
   ```

2. **Understand ancestor context (WHY this chore is needed):**
   ```bash
   # Find parent Task/Feature to understand motivation
   bd dep list <chore-id> -t parent-child

   # Read ancestor to understand system context
   bd show <parent-id>                            # Why does this maintenance matter?
   ```

3. **Read predecessor notes (INCOMING context):**
   ```bash
   # Find what blocks this chore
   bd dep list <chore-id> --type=blocks

   # Read predecessor notes
   bd show <predecessor-id>                       # Check for setup or dependencies
   ```

4. **Understand successor expectations (OUTGOING deliverables):**
   ```bash
   # Find what depends on this chore
   bd dep list <chore-id> --direction=up --type=blocks

   # Read successor descriptions
   bd show <successor-id>                         # What do they need from this chore?
   ```

5. **Execute the maintenance work:**
   ```bash
   # Perform cleanup, updates, configuration, etc.
   # For code changes, run validation
   npm run lab:test                               # If code-related
   npm run lab:clean                              # If code-related

   # Update notes after significant steps
   bd update <chore-id> --notes="Completed [step]. Status: [current state]"
   bd sync
   ```

6. **Document work and side effects:**
   ```bash
   bd update <chore-id> --notes="CHORE COMPLETE:
   - Work performed: [description]
   - Files/configs modified: [paths]
   - Side effects: [any changes that affect other systems]
   - Verification: [how you confirmed success]
   - OUTGOING for successors: [deliverables]"
   ```

7. **Complete the Chore:**
   ```bash
   bd close <chore-id> --reason="Maintenance complete. Changes documented in notes."
   bd sync
   ```

**Key Principles:**
- Chores are NOT isolated—they exist to support other work (ancestors provide motivation)
- Document side effects explicitly—maintenance can have unexpected impacts
- Verification is critical—confirm the chore achieved its goal
- Like Tasks, read predecessor notes and document OUTGOING deliverables for successors
- State updates should include what was changed and why
- Sync frequently, especially after system-level changes

---

## Bug Execution Workflow (TDD MANDATORY)

**Intent**: Bugs are defects in existing functionality. Follow TDD to ensure the fix is correct and regression-tested. Context from ancestors helps understand system behavior.

**Process**:

### Phase 0: Context & Reproduction (BEFORE TDD)

1. **Claim and understand the bug:**
   ```bash
   bd update <bug-id> --status=in_progress
   bd show <bug-id>                               # Read Steps to Reproduce, Expected, Actual
   bd sync
   ```

2. **Understand ancestor context:**
   ```bash
   # Find parent Feature to understand system context
   bd dep list <bug-id> -t parent-child

   # Read ancestor for system knowledge
   bd show <parent-id>                            # Understand Feature behavior
   ```

3. **Attempt to reproduce the bug:**
   ```bash
   # Follow Steps to Reproduce exactly as documented
   # Observe actual vs expected behavior

   # Document reproduction result:
   bd update <bug-id> --notes="Reproduction attempt: [SUCCESS/FAILED]
   - Followed steps: [list steps]
   - Observed: [actual behavior]
   - Expected: [expected behavior from issue]
   - Environment: [relevant context]"
   bd sync
   ```

   **If reproduction FAILS:**
   - Update notes with failure details
   - Request clarification from reporter
   - Do NOT proceed until reproduction succeeds

### 🔴 Phase 1: RED - Write Failing Test

⚠️ **Unit tests ONLY** - Do NOT run E2E tests during development. Keep test cycles fast.

**After successful reproduction:**

1. Write a test that captures the bug
2. Run `npm run lab:test` and **verify the test FAILS**
3. The failing test proves you understand the bug

```bash
# Write test in relevant .test.ts file
npm run lab:test                           # MUST see test FAIL

bd update <bug-id> --notes="RED phase: Failing test written in [test-file-path]"
bd sync
```

⚠️ **DO NOT proceed to implementation until you have a failing test!**

### 🟢 Phase 2: GREEN - Implement the Fix

**Only after you have a failing test:**

1. Implement the minimum fix needed
2. Run `npm run lab:test` frequently
3. Stop when the test passes

```bash
# Implement fix
npm run lab:test                           # Verify test now PASSES

bd update <bug-id> --notes="GREEN phase: Fix implemented in [file-paths]. Test passing."
bd sync
```

### 🔵 Phase 3: REFACTOR - Clean Up

1. Refactor if needed (improve code quality)
2. Run tests after each change
3. Ensure all tests still pass

```bash
npm run lab:test                           # All tests still pass
npm run lab:clean                          # Lint + typecheck pass

bd update <bug-id> --notes="REFACTOR phase: Code cleaned. All tests passing."
bd sync
```

### ✅ Phase 4: Complete & Document

```bash
bd update <bug-id> --notes="BUG FIX COMPLETE:
- Root cause: [explanation]
- Reproduction: [successful/documented in notes]
- Test file: [path to test]
- Fix files: [paths to changed files]
- Verification: All tests passing, lint clean"

bd close <bug-id> --reason="Fixed with test coverage and root cause documented"
bd sync
```

**Key Principles:**
- **Always attempt manual reproduction before writing tests**—this validates the bug is real
- Ancestor context helps understand system behavior and edge cases
- Document reproduction results even if they fail—helps clarify requirements
- Failing test is mandatory before implementation—proves you understand the bug
- Root cause analysis in final notes helps prevent similar bugs
- State updates after each TDD phase create persistent checkpoints

### TDD Checklist for Bugs

Before closing a bug, verify:
- [ ] **Reproduction attempted and documented** (Phase 0)
- [ ] Failing test written that reproduces the bug (RED phase)
- [ ] Test failed before fix (RED phase verified)
- [ ] Test passes after fix (GREEN phase verified)
- [ ] `npm run lab:test` - all unit tests pass (NO E2E)
- [ ] `npm run lab:clean` - lint and typecheck pass (MANDATORY - zero errors)
- [ ] Root cause documented in notes
- [ ] No regressions introduced
- [ ] `pkill -f vitest` - cleanup orphaned test processes

---

# Human Verification Task Template

Epics and Features **MUST** create a Human Verification task upon completion. This task is **NON-AGENTIC** and serves as a checklist for humans to manually verify the work.

## Template Structure

```bash
bd create \
  --title="Human Verification: [Epic/Feature Name]" \
  --type=task \
  --priority=0 \
  --description="## Verification Summary

[Epic/Feature]: <issue-id> - [Name]
Completed Work: <list of completed child issue-ids>

## Purpose
This task provides a structured checklist for human verification of completed work.
⚠️ **NON-AGENTIC**: Do NOT execute this task with /task-execute. This is for manual human verification only.

## Artifacts to Verify
- [ ] Code changes in [specific file paths]
- [ ] Tests in [specific test file paths]
- [ ] Documentation in [specific doc paths]
- [ ] Configuration in [specific config paths]

## Verification Steps
Perform these steps manually and check off as you complete them:

- [ ] Step 1: [Specific action to perform] → Expected: [Concrete expected outcome]
- [ ] Step 2: [Specific action to perform] → Expected: [Concrete expected outcome]
- [ ] Step 3: [Specific action to perform] → Expected: [Concrete expected outcome]

## Acceptance Criteria Review
Review original AC from parent Epic/Feature:

- [ ] AC 1: [How to verify this criterion] → Expected: [Result]
- [ ] AC 2: [How to verify this criterion] → Expected: [Result]

## Manual Testing Scenarios
- [ ] Scenario 1: [User action/context] → Expected: [System response]
- [ ] Scenario 2: [User action/context] → Expected: [System response]

## Sign-Off
Once all items above are verified:
- [ ] All verification steps completed successfully
- [ ] All acceptance criteria met
- [ ] No regressions observed
- [ ] Ready to close parent Epic/Feature"

# Link verification task to parent
bd dep add <verification-task-id> --depends-on <epic-or-feature-id> --type=validates
```

## Template Guidance

### Title Format
Always use: `"Human Verification: [Exact Epic/Feature Name]"`

### Type & Priority
- **Type**: Always `task` (not epic/feature—this ensures it's NOT treated as agentic work)
- **Priority**: Usually `0` (P0) to ensure it's addressed immediately

### Writing Good Verification Steps

**❌ Bad (vague):**
- "Check if the feature works"
- "Test the new functionality"
- "Verify everything is correct"

**✅ Good (specific, testable):**
- "Navigate to /dashboard → Expected: New 'Export' button visible in top-right"
- "Click 'Export' → CSV → Expected: Download starts, file contains headers"
- "Open downloaded CSV → Expected: Contains columns: id, name, created_at"

### Artifact Specificity

**❌ Bad:**
- "Code files"
- "Some tests"

**✅ Good:**
- "src/features/export/ExportButton.tsx"
- "src/features/export/__tests__/ExportButton.test.tsx"
- "docs/features/export.md"

### Expected Outcomes

Every step should have a concrete expected outcome that a human can verify:
- Visual changes: "Button appears", "Modal opens", "Error message displays"
- Data changes: "Database contains new row", "File has correct format"
- Behavior changes: "API returns 200", "Request completes in <2s"

### Non-Agentic Warning

**CRITICAL**: Always include this warning in the description:
```
⚠️ **NON-AGENTIC**: Do NOT execute this task with /task-execute.
This is for manual human verification only.
```

This prevents agents from attempting to auto-verify the work.

---

## Tips

- Use `bd create` in parallel for multiple related issues (e.g., via subagents)
- Batch close related issues with `bd close <id1> <id2> <id3>` (more efficient than individual closes)
- Check `bd ready` before planning new work (shows blockers)
- Run `bd sync` at session end to persist changes
- Add `--assignee=username` to assign work to team members

## Execution Summary Writeback

When task-execute runs, it automatically appends an execution summary to the original issue's notes. This provides persistent tracking of execution attempts.

### Summary Format

```markdown
---
## Execution Summary
**Timestamp**: 20260119T163111Z
**Mode**: apply|preview
**Status**: ✅ Tests passed | ❌ Tests failed | ℹ️ Preview only
**Logfile**: [execution log](.claude/tmp/task-execute-20260119T163111Z.txt)

**Created Issues**:
- Feature: beads-xxx
- Task: beads-yyy
```

### Summary Contents

| Mode | Status | Created Issues |
|------|--------|----------------|
| `preview` | ℹ️ Preview only | Not shown |
| `apply` (tests pass) | ✅ Tests passed | Feature + Task IDs |
| `apply` (tests fail) | ❌ Tests failed | Feature + Task IDs |

### Logfile Links

- Links use relative paths from repository root
- Format: `.claude/tmp/task-execute-{timestamp}.txt`
- Clickable in issue viewers that support markdown links

### Multiple Executions

Each execution appends a new summary, separated by blank lines. This creates a history of all execution attempts on an issue, useful for debugging failed attempts or tracking iterative progress.

---

For full documentation:
```bash
bd --help
bd <command> --help
```