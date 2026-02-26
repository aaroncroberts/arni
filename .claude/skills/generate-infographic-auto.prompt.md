# Automated Infographic Generation Loop

When invoked, execute this workflow:

## 1. Setup
- Extract directory from args
- Verify GEMINI_API_KEY exists
- Find {name}.prompt.md and {name}.content.md files
- Initialize: current_batch = 1, max_batches = 5, target_score = 90

## 2. Loop (until 90%+ score OR 5 batches)

For each batch:

**Generate:**
```bash
python3 .claude/scripts/generate-infographic.py {directory} --batch {current_batch}
```

**Evaluate:**
```
Use Skill tool: Skill(skill="ig-evaluate", args="{directory}/*.webp")
```

**Parse:**
- Read {directory}/{name}.eval.md
- Extract best score from this batch
- Track overall best variant and score

**Check:**
- If best_score >= 90: Report winner and STOP
- If best_score < 90 AND current_batch < 5: Continue to next step
- If current_batch == 5: Report best result and STOP

**Improve:**
- Read evaluation report recommendations
- Update {directory}/{name}.prompt.md with fixes:
  - Add exact hex codes for color issues
  - Add "minimum 30% white space" for layout issues
  - Add font size specs for typography issues
  - Add festival imagery for context issues

**Next:**
- current_batch += 1
- Loop back to Generate

## 3. Final Report

```
✅ COMPLETE

Winner: {best_variant} ({best_score}/100)
Batches: {total_batches} ({total_batches * 3} images)
Status: {Target Achieved! or Best effort}

Full report: {directory}/{name}.eval.md
```

---

**Execution:** Follow steps 1→2→3 sequentially. Do not skip evaluation. Do not continue past 90%+. Update prompt between batches.
