# MVP Cleanup & Quality Audit Command

Perform comprehensive cleanup and quality improvement for an MVP feature or codebase area. Creates an Epic with detailed WBS (Work Breakdown Structure) including documentation standardization, code quality audits, and third-party component reviews.

## Usage

```
/mvp-cleanup [feature-name-or-scope]
```

**Parameters:**
- `feature-name-or-scope` - Feature name or codebase area to audit (e.g., "Beady Kanban", "Auth System", "Dashboard")

**Examples:**
```
/mvp-cleanup "Beady Kanban prototype"
/mvp-cleanup "Authentication system"
/mvp-cleanup "Dashboard components"
/mvp-cleanup "Data access layer"
```

## What This Does

This command creates a systematic cleanup workflow:

1. **Explore System** - Uses Explore agent to comprehensively map the target area
2. **Create Epic** - Creates beads epic for tracking all cleanup work
3. **Generate CHILD Tasks** - Creates 15-20 tasks organized by category:
   - Documentation (README standardization, architecture docs)
   - Code Quality (duplicate patterns, type safety, error handling)
   - Dependencies (third-party library audits, version reviews)
   - Testing (coverage gaps, E2E completeness, accessibility)
4. **Link Tasks** - Establishes parent-child relationships to Epic
5. **Document Findings** - Each task includes research requirements and expected deliverables
   - May create additional Chores, Tasks, and/or Features (and associated breakdown) based on findings and recommendations.
   - May create .md files (references by Beads) to provide more detail.

## Task Categories

### Documentation Tasks (CHORE)
- README hierarchy and navigation (2-3 level file trees)
- Cross-references between docs
- Architecture decision documentation
- API documentation completeness

### Code Quality Tasks (CHORE)
- Duplicate code pattern detection
- Code smell identification (complexity, long functions)
- Type safety improvements (eliminate `any`)
- Error handling consistency
- Performance anti-patterns

### Third-Party Dependencies (CHORE)
- Version compatibility audit
- Best practices verification
- Security vulnerability check
- Alternative library evaluation
- Migration path documentation

### Testing Tasks (TASK)
- Unit test coverage gaps
- E2E test completeness
- Edge case coverage
- Accessibility compliance (WCAG AA)
- Performance benchmarking

## Task Structure

Each task includes:
- **Current State:** Description of what exists now
- **Research Requirements:**
  - Official documentation to review
  - Best practices to investigate
  - Alternative approaches to evaluate
- **Deliverables:**
  - Findings document (current smell/issue)
  - Recommendation with justification
  - 2+ alternatives with tradeoffs (complexity, performance, migration cost)
  - Implementation effort estimate

## Output

Upon completion, provides:
1. **Epic ID** - todd-lab-XXX
2. **Total Tasks** - Count of created tasks (typically 15-20)
3. **Task Breakdown** - Organized by category
4. **Next Steps** - Recommended execution order
5. **Verification Command** - `todd-carl show [epic-id]` to see all children

## Example Output

```markdown
## Beady Kanban MVP Cleanup Epic

**Epic ID:** todd-lab-133
**Total Tasks:** 16
**Priority:** P2

### Task Breakdown

**Documentation (4 tasks):**
- todd-lab-mlr: Standardize README hierarchy
- todd-lab-XXX: Document architecture patterns
- todd-lab-XXX: Add API documentation
- todd-lab-XXX: Create design decision records

**Code Quality (6 tasks):**
- todd-lab-onf: Audit mutation hooks for duplicates
- todd-lab-qbm: Analyze database architecture smells
- todd-lab-18d: Review error handling patterns
- todd-lab-2i1: Evaluate TypeScript safety
- todd-lab-irc: Audit component patterns
- todd-lab-1hh: Assess performance bottlenecks

**Dependencies (4 tasks):**
- todd-lab-vi2: Review react-flow-renderer
- todd-lab-cq6: Audit @dnd-kit/core usage
- todd-lab-oj8: Review markdown libraries
- todd-lab-XXX: Evaluate testing frameworks

**Testing (2 tasks):**
- todd-lab-3x5: Audit test coverage gaps
- todd-lab-l1b: Review accessibility compliance

### Next Steps
1. Start with documentation tasks (low risk)
2. Tackle code quality audits (research phase)
3. Review dependencies (upgrade planning)
4. Address testing gaps
```

## Instructions Reference

All instructions are contained in this skill file. Follow the workflow above.

## Key Principles

- **Use Explore agent** for codebase understanding (not manual Grep/Glob)
- **Create 15-20 tasks** for comprehensive coverage
- **Use parent-child relationships** (NOT blocking dependencies) - see Dependency Commands below
- **Focus on research + documentation** not immediate fixes
- **Each task = audit + recommendation** with tradeoff analysis
- **Tradeoff analysis is mandatory** for every recommendation

## Dependency Commands

**CRITICAL: Use `parent-child` type for epic children, NOT `blocks`**

```bash
# CORRECT: Create parent-child relationship (child belongs to parent epic)
todd-carl dep add <child-id> <epic-id> --type parent-child

# WRONG: Creates blocking dependency (use only for critical path dependencies)
todd-carl dep add <child-id> <epic-id>  # Defaults to --type blocks
```

**When to use each type:**
- `--type parent-child`: Epic → children (containment/hierarchy)
- `--type blocks`: Critical path dependencies (B blocks A means A cannot start until B is done)
- `--type discovered-from`: Investigation task found follow-up work

**Example workflow:**
```bash
# 1. Create epic
todd-carl create --title="MVP Cleanup" --type=epic --priority=2

# 2. Create child tasks
todd-carl create --title="Audit dead code" --type=chore --priority=2

# 3. Link as parent-child (NOT blocks!)
todd-carl dep add todd-lab-xyz todd-lab-epic --type parent-child
```

## Skill Integration

This command is implemented as a Claude Code skill that:
1. Parses the feature/scope argument
2. Launches Explore agent to map the system
3. Creates Epic and tasks in beads
4. Links all tasks with parent-child relationships
5. Returns summary with Epic ID and next actions

## Success Criteria

The cleanup is properly structured when:
- [x] Epic created with clear description
- [x] 15-20 tasks created covering all categories
- [x] Each task has research requirements and deliverables
- [x] All tasks linked to epic (parent-child)
- [x] Tasks organized by category
- [x] Each task requires tradeoff analysis
- [x] Epic structure verified with `todd-carl show`
