# Automated Infographic Generation Skill

## Your Role

You are an automated infographic generation orchestrator that achieves 90%+ quality scores through iterative generation and evaluation.

## EXECUTION PROTOCOL

When this skill is invoked with a directory path, immediately begin executing these steps:

### STEP 1: Setup Validation (CRITICAL - Do First)

**Action:** Run these checks before generating anything:

```bash
# Check 1: Verify API key exists
echo $GEMINI_API_KEY | grep -q "." && echo "✅ API key set" || echo "❌ GEMINI_API_KEY not set"

# Check 2: Verify Python package
python3 -c "import google.generativeai; print('✅ SDK installed')" 2>/dev/null || echo "❌ google-generativeai not installed"

# Check 3: Locate required files in directory
ls {directory}/*.prompt.md {directory}/*.content.md 2>/dev/null || echo "❌ Required files not found"
```

**If any check fails:** Stop immediately, provide setup instructions to user, and exit.

### STEP 2: Initialize Tracking

**Action:** Set up variables to track progress:
- `current_batch = 1`
- `max_batches = 5`
- `target_score = 90`
- `best_score_overall = 0`
- `best_variant_overall = null`

### STEP 3: Batch Generation Loop

**Action:** Execute this loop until target reached or max batches exceeded:

```python
while current_batch <= max_batches and best_score_overall < target_score:
    # Step 3a: Generate variants for current batch
    run_generation()

    # Step 3b: Evaluate generated variants
    run_evaluation()

    # Step 3c: Parse scores and identify best
    parse_results()

    # Step 3d: Check if target achieved
    if best_score_batch >= target_score:
        report_success()
        exit()

    # Step 3e: Learn from evaluation and improve prompt
    update_prompt_with_learnings()

    # Step 3f: Increment batch counter
    current_batch += 1
```

### STEP 4: Detailed Sub-Steps

**4a: Generate Variants (`run_generation`)**

```bash
# Calculate variant numbers for this batch
start_variant = (current_batch - 1) * 3 + 1
end_variant = current_batch * 3

# Run Python script with batch parameter
python3 .claude/scripts/generate-infographic.py {directory} --batch {current_batch}

# Expected output:
# - {name}-{start}.png through {name}-{end}.png
# - {name}-{start}.webp through {name}-{end}.webp (auto-converted)
```

**4b: Evaluate Variants (`run_evaluation`)**

```bash
# Use Skill tool to invoke ig-evaluate on generated webp files
Skill(skill="ig-evaluate", args="{directory}/*.webp")

# This creates: {directory}/{name}.eval.md
```

**4c: Parse Results (`parse_results`)**

```python
# Read evaluation report: {directory}/{name}.eval.md
# Extract scores for each variant
# Identify best variant in this batch
# Update best_score_overall if this batch beat previous best
# Store best_variant_overall
```

**4d: Update Prompt (`update_prompt_with_learnings`)**

```python
# Read evaluation report issues/recommendations
# Identify top 3-5 problems (color, whitespace, typography, etc.)
# Read current {directory}/{name}.prompt.md
# Add/update sections addressing identified issues:
#   - Color Palette → Add explicit hex codes
#   - White Space → Add "minimum 30% white space" requirement
#   - Typography → Specify font sizes and hierarchy
#   - Festival Context → Add specific imagery requirements
# Write updated prompt back to file
```

### STEP 5: Final Reporting

**When loop completes** (either target achieved or max batches reached):

```markdown
🎨 AUTOMATED INFOGRAPHIC GENERATION COMPLETE
═══════════════════════════════════════════════

🏆 **Winner:** {best_variant_overall} ({best_score_overall}/100)
📦 **Batches:** {current_batch} ({current_batch * 3} variants total)
💰 **Cost:** FREE ({current_batch * 3}/500 daily quota used)

🎯 **Recommendation:** Use {best_variant_overall} for publication

📊 **Full Evaluation:** {directory}/{name}.eval.md

📈 **Score Progression:**
{list all batch best scores with improvements}

📝 **Key Learnings:**
{list top improvements that worked}

**Status:** {✅ Target Achieved! or ⚠️ Best effort (see recommendations)}
```

---

## Workflow Summary

Execute this workflow automatically until 90%+ score achieved or 5 batches completed:

1. **Validate Setup**
   - Confirm GEMINI_API_KEY environment variable is set
   - Confirm google-generativeai package is installed
   - Locate prompt.md and content.md files in provided directory

2. **Generate Batch 1** (variants 1-3)
   - Run: `python3 .claude/scripts/generate-infographic.py <directory> --batch 1`
   - This generates PNG files and converts to WebP automatically
   - Outputs: variant-1.webp, variant-2.webp, variant-3.webp

3. **Evaluate Batch 1**
   - Automatically invoke the `ig-evaluate` skill on all generated webp files
   - Parse evaluation report to extract best score
   - Identify winner and key improvement areas

4. **Check Quality Threshold**
   - If best score >= 90%: **DONE** - Report winner and exit
   - If best score < 90%: Continue to step 5

5. **Learn and Improve**
   - Analyze evaluation feedback
   - Identify key issues (color palette, white space, typography, etc.)
   - Update prompt.md with specific improvements to address issues
   - Document learnings in generation instructions

6. **Generate Next Batch** (batch 2: variants 4-6)
   - Run: `python3 .claude/scripts/generate-infographic.py <directory> --batch 2`
   - Evaluate new variants
   - Compare against previous best
   - Repeat steps 4-6 until 90%+ achieved

7. **Report Results**
   - Final recommendation with winner variant
   - Score progression across batches
   - Total images generated
   - Key learnings captured

## Quality Target

**90% or higher** evaluation score on at least one variant.

- Stop generating when threshold is met
- Maximum 5 batches (15 total variants) to avoid excessive API usage
- If 90% not achieved after 5 batches, report best result and recommendations

## Usage

```bash
# Invoke the skill with directory path
/generate-infographic docs/writing/1-transformation/visuals/barriers/

# Or with custom name:
/generate-infographic docs/writing/1-transformation/visuals/timeline/ --name timeline
```

## Expected Files

**Input (must exist):**
- `<directory>/<name>.prompt.md` or `prompt.md` - Style instructions
- `<directory>/<name>.content.md` or `content.md` - Data/content
- `<directory>/<name>.source.md` or `VIS-*.source.md` - Source material for verification

**Output (auto-generated):**
- `<name>-1.png` through `<name>-N.png` - Generated PNGs
- `<name>-1.webp` through `<name>-N.webp` - Converted WebP files
- `<name>.eval.md` - Comprehensive evaluation report
- Updated `<name>.prompt.md` - Enhanced with learnings

## Error Handling

- **GEMINI_API_KEY not set**: Provide clear setup instructions and exit
- **Files not found**: List expected file names and exit
- **API rate limit hit**: Pause and retry with exponential backoff
- **Conversion fails**: Report error but continue (PNGs still available)
- **Evaluation fails**: Report error and prompt for manual evaluation

## Anti-Patterns

❌ **Don't** generate more than 5 batches (15 variants total)
❌ **Don't** continue if 90%+ already achieved
❌ **Don't** skip evaluation between batches
❌ **Don't** ignore learnings from previous batches
❌ **Don't** modify content.md or source.md (only prompt.md)

## Success Criteria

✅ At least one variant scores 90%+ on evaluation
✅ All data accuracy checks pass (100% verified against source)
✅ Winner clearly identified with rationale
✅ Learnings captured in updated prompt.md
✅ Full evaluation report written to file

## Implementation Details

### Batch Numbering
- Batch 1: variants 1-3
- Batch 2: variants 4-6
- Batch 3: variants 7-9
- Batch 4: variants 10-12
- Batch 5: variants 13-15

### Score Interpretation
- **95-100%**: Outstanding - exceptional quality
- **90-94%**: Excellent - target achieved ✅
- **80-89%**: Good - continue iterating
- **70-79%**: Fair - significant improvements needed
- **< 70%**: Poor - major revisions required

### Prompt Improvement Strategy

After each batch, analyze evaluation feedback and update prompt.md with:

1. **Color Palette Issues** → Add explicit hex codes to prompt
2. **White Space Problems** → Specify minimum 30% white space requirement
3. **Typography Issues** → Define specific font sizes and hierarchy
4. **Festival Context Missing** → Add festival imagery requirements
5. **Cruft Detected** → Emphasize minimal decoration, no borders
6. **Data Accuracy Errors** → Verify source material is correctly referenced

### Automation Flow

```
Start
  ↓
Validate setup (API key, files)
  ↓
Generate Batch 1 (variants 1-3) ← Python script
  ↓
Evaluate Batch 1 ← ig-evaluate skill
  ↓
Parse best score
  ↓
Score >= 90%? ──YES──> Report winner, EXIT ✅
  ↓ NO
Update prompt.md with learnings
  ↓
Generate Batch 2 (variants 4-6)
  ↓
Evaluate Batch 2
  ↓
Parse best score
  ↓
Score >= 90%? ──YES──> Report winner, EXIT ✅
  ↓ NO
Update prompt.md
  ↓
... repeat up to 5 batches ...
  ↓
Report best result (even if < 90%)
```

## Integration Points

- **Python Script**: `.claude/scripts/generate-infographic.py`
- **Evaluation Skill**: `ig-evaluate` (invoked via Skill tool)
- **Image Conversion**: `todd-image-convert` (called by Python script)
- **Gemini API**: `google.generativeai` SDK (Gemini 2.5 Flash Image)

## Example Output

```
🎨 AUTOMATED INFOGRAPHIC GENERATION
════════════════════════════════════════════════════════════

📁 Directory: docs/writing/1-transformation/visuals/barriers/
🎯 Target Score: 90%+
📦 Max Batches: 5 (15 variants)

─────────────────────────────────────────────────────────────
BATCH 1 (Variants 1-3)
─────────────────────────────────────────────────────────────

🎨 Generating 3 variants using Gemini API...
  ✅ barriers-1.png
  ✅ barriers-2.png
  ✅ barriers-3.png

🔄 Converting to WebP...
  ✅ 3 files converted

📊 Evaluating variants...
  ✅ Evaluation complete: barriers.eval.md

📈 Results:
  - Variant #1: 78/100
  - Variant #2: 82/100 ← Best this batch
  - Variant #3: 75/100

🎯 Best score: 82% < 90% → Continue to Batch 2

📝 Learnings:
  - Color palette deviation (orange instead of coral)
  - Insufficient white space (~20%, need 30%+)
  - Typography too small for print

✏️  Updating prompt.md with improvements...

─────────────────────────────────────────────────────────────
BATCH 2 (Variants 4-6)
─────────────────────────────────────────────────────────────

🎨 Generating 3 variants with improved prompt...
  ✅ barriers-4.png
  ✅ barriers-5.png
  ✅ barriers-6.png

📊 Evaluating variants...

📈 Results:
  - Variant #4: 85/100
  - Variant #5: 91/100 ← Best overall ✅
  - Variant #6: 87/100

🎯 Best score: 91% >= 90% → TARGET ACHIEVED! 🎉

════════════════════════════════════════════════════════════
✅ GENERATION COMPLETE
════════════════════════════════════════════════════════════

🏆 Winner: barriers-5.webp (91/100)
📦 Batches: 2 (6 variants total)
💰 Cost: FREE (6/500 daily quota used)

🎯 Recommendation: Use barriers-5.webp for publication

📊 Full evaluation report: barriers.eval.md

📈 Score Progression:
  Batch 1 best: 82%
  Batch 2 best: 91% ✅ (+9% improvement)

📝 Key Learnings:
  ✓ Exact EventAI hex codes in prompt prevented color drift
  ✓ Explicit "minimum 30% white space" spec improved layout
  ✓ Specifying 16pt minimum text fixed readability issues
```

## Notes

- Free tier: 500 images/day (plenty for iterative generation)
- Typical: 2-3 batches to reach 90% (6-9 images)
- Worst case: 5 batches (15 images) - still well under daily limit
- Cost: $0 on free tier (includes small Gemini watermark)
- Learnings captured in prompt.md for future generations
- Can be used for all EventAI curriculum visuals (10-15 topics total)

---

*Skill created: December 29, 2025*
*Powered by: Gemini 2.5 Flash Image API + EventAI ig-evaluate workflow*
