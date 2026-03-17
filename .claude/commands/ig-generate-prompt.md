# Generate Infographic Prompt (ig-generate-prompt)

**Purpose:** Create properly-scoped infographic prompts that match density tier expectations, using CLI validation to verify before generation.

**Usage:**
```
/ig-generate-prompt <topic> <tier> [--output=path]
```

**Arguments:**
- `<topic>`: What the infographic shows (e.g., "consent-spectrum", "cost-breakdown")
- `<tier>`: Density tier (concise|standard|detailed)
- `[--output]`: Optional output path (defaults to current visual directory)

---

## 🚨 CRITICAL: Tier-Specific Text Patterns

**The #1 rule for density tiers is TEXT PER CONCEPT, not concept count.**

### Concise Tier: Labels Only (3-5 words max)

**Pattern:**
```markdown
✅ CORRECT (label-only or brief label-value):
- Mandatory facial recognition (3 words)
- Bundled consent (2 words)
- Tickets: $750,000 (2 words - label-value OK)
- Default opt-in (2 words)

✅ ACCEPTABLE (brief drilldown, ≤5 words total):
- Facial recognition → instant (3 words total)
- Opt-in required (2 words)

❌ WRONG (topic - long detail, >5 words total):
- Mandatory facial recognition - no alternatives (5 words, but explanatory)
- Bundled consent - accept all or entry denied (8 words)
- Pre-checked boxes - silence assumed as consent (7 words)
- Soft wall penalties - manual ID takes 2 hours vs. 3 min (13 words)
```

**Why long drilldown is problematic:** The `[topic - long detail]` pattern signals to the AI "add explanatory text", which:
1. Creates drilldown text on infographic (clutters design)
2. Reduces white space (AI adds paragraphs)
3. Inflates tier (Concise → Standard)

**Concise tier philosophy:** Trust the AI. "Mandatory facial recognition" or "Tickets: $750,000" is enough - the AI understands context. Don't hand-hold with multi-word explanations.

**Text limits:**
- **Total bullet: 3-5 words MAX** (including label + value/descriptor)
- Label-value pairs OK if ≤5 words total ("Tickets: $750,000" = 2 words ✅)
- Brief drilldown OK if ≤5 words total ("Topic → brief" = 3 words ✅)
- Long drilldown NOT OK if >5 words total ("Topic - long explanation" = 6+ words ❌)

### Standard Tier: Brief Descriptors (1 sentence max)

**Pattern:**
```markdown
✅ CORRECT (topic + brief detail):
- Mandatory facial recognition - no alternatives offered
- Bundled consent requires accepting all terms or entry denied
- Pre-checked boxes assume silence as consent

❌ WRONG (multi-sentence explanations):
- Mandatory facial recognition - no alternatives offered. Festival-goers must use facial recognition to enter. Paper tickets are not accepted.
```

**Why:** Standard tier can have **one level of detail** (topic + descriptor), but not multiple sentences or examples.

**Text limits:**
- Bullet text: 10-15 words MAX
- One descriptor per concept (topic - detail OR topic + sentence)
- No multi-sentence explanations
- No examples or reasoning

### Detailed Tier: Multi-Level Explanations

**Pattern:**
```markdown
✅ CORRECT (topic + detail + examples + reasoning):
- Mandatory facial recognition - no alternatives offered
  - Festival-goers must use facial recognition to enter
  - Paper tickets and manual ID checks not accepted
  - Violates GDPR Article 9 (special category data requires explicit consent)
  - Example: Coachella 2024 facial recognition mandate (EU complaint filed)
```

**Why:** Detailed tier expects **comprehensive specifications** with examples, reasoning, and edge cases.

**Text limits:**
- Bullet text: No limit
- Multi-level explanations expected
- Examples and reasoning required
- Edge cases and annotations encouraged

---

## Tier Ranges (Real-World Validated)

**After analyzing real prompts, the CLI now uses these ranges:**

| Tier | Concepts | Depth | Complexity | Text per Concept |
|------|----------|-------|------------|------------------|
| **Concise** | 5-16 | 1-2 | < 50 | 3-5 words MAX |
| **Standard (Breadth)** | 15-25 | 1-2 | 50-100 | 10-15 words MAX |
| **Standard (Depth)** | 8-15 | 3 | 50-100 | 1 sentence MAX |
| **Detailed** | 25+ | 4+ | 100+ | Multi-level explanations |

**Key insight:** Complexity = Concepts × Depth. A Concise tier prompt can have 16 concepts if each is shallow (1-2 levels). The text pattern determines depth, not just the bullet count.

---

## Prompt Generation Process

When `/ig-generate-prompt` is invoked:

### Step 1: Gather Requirements
Ask user (or infer from arguments):
1. **Topic:** What does the infographic show?
2. **Tier:** Concise, Standard, or Detailed?
3. **Context:** Embedded (textbook) or Standalone (social media)?
4. **Visual type:** Spectrum, timeline, breakdown, comparison, process?

### Step 2: Generate Prompt Structure

**Concise tier template:**
```markdown
# [Topic Title]

[1-2 sentence description of what the infographic shows]

## Data Points

- [Label 1] (3-5 words)
- [Label 2] (3-5 words)
- [Label 3] (3-5 words)
- [Label 4] (3-5 words)
- [Label 5] (3-5 words)

## Style

- Colors: [EventAI palette]
- White space: 40%+ composition
- Typography: 16pt minimum body text
- Context: [Embedded/Standalone], [festival context]
- Accessibility: [Icons + text + color triple encoding]

## Structure

[2-3 sentences describing layout and visual approach]
```

**Standard tier template:**
```markdown
# [Topic Title]

[2-3 sentence description with context]

## Data Points

**[Category 1]:**
- [Concept] - [brief descriptor] (10-15 words max)
- [Concept] - [brief descriptor]
- [Concept] - [brief descriptor]

**[Category 2]:**
- [Concept] - [brief descriptor]
- [Concept] - [brief descriptor]

**[Category 3]:**
- [Concept] - [brief descriptor]
- [Concept] - [brief descriptor]

## Style

[Standard style specifications]

## Structure

[3-4 sentences describing layout]
```

**Detailed tier template:**
```markdown
# [Topic Title]

[Comprehensive introduction, 4-5 sentences]

## Data Points

**[Category 1]:**
- [Concept] - [descriptor]
  - [Detail line 1]
  - [Detail line 2]
  - [Example or reasoning]
  - [Edge case or annotation]

**[Category 2]:**
- [Concept] - [descriptor]
  - [Detail line 1]
  - [Detail line 2]
  - [Example or reasoning]

## Style

[Detailed style specifications with reasoning]

## Structure

[Comprehensive layout description, 5+ sentences]
```

### Step 3: CLI Validation

Before writing the prompt file:

```bash
# Write to temporary file
temp_prompt=/tmp/prompt-validation-$$.md

# Validate with CLI
gemini-generate --validate-prompt $temp_prompt --density [tier]

# If validation fails:
# - Show CLI output (concepts, depth, complexity)
# - Identify issues (too many/few concepts, wrong depth)
# - Regenerate with adjustments
# - Re-validate until pass

# If validation passes:
# - Write to final output path
# - Report success with metrics
```

### Step 4: Write Prompt File

Write to output path with validation confirmation:

```markdown
<!-- VALIDATED: [tier] tier (concepts: X, depth: Y, complexity: Z) -->
<!-- CLI validation: ✅ PASS -->

[Generated prompt content]
```

---

## Text Pattern Enforcement

**Critical enforcement rules:**

### Concise Tier Enforcement

```python
def validate_concise_text_patterns(prompt_text):
    """
    Enforce 3-5 word MAX per bullet in Concise tier.

    Rules:
    - Total bullet: 3-5 words MAX
    - Label-value pairs OK if ≤5 words ("Tickets: $750,000" = 2 words ✅)
    - Brief drilldown OK if ≤5 words ("Topic → brief" = 3 words ✅)
    - Long drilldown NOT OK if >5 words ("Topic - long explanation" ❌)
    """
    content_bullets = extract_content_bullets(prompt_text)

    issues = []
    for bullet in content_bullets:
        # Check word count FIRST (3-5 words max total)
        words = bullet.split()
        if len(words) > 5:
            issues.append(f"❌ Too many words: '{bullet}' ({len(words)} words, max 5)")

            # If over 5 words AND has drilldown pattern, check descriptor length
            if ' - ' in bullet:
                parts = bullet.split(' - ', 1)
                if len(parts) == 2:
                    descriptor_words = parts[1].split()
                    if len(descriptor_words) > 3:
                        issues.append(f"⚠️  Long descriptor: '{parts[1]}' ({len(descriptor_words)} words, prefer ≤3)")

        # Check for multi-sentence
        if '. ' in bullet and not bullet.endswith('.'):
            issues.append(f"❌ Multi-sentence bullet: '{bullet}' (should be single label)")

    return issues
```

**Key insight:** The validation only flags drilldown patterns if the TOTAL exceeds 5 words. This means:
- "Tickets: $750,000" (2 words) → ✅ PASS (no drilldown check needed)
- "Default opt-in" (2 words) → ✅ PASS
- "Topic → brief" (3 words) → ✅ PASS
- "Topic - long explanation here" (5 words) → ❌ FAIL (word count) + ⚠️ WARNING (long descriptor)

### Standard Tier Enforcement

```python
def validate_standard_text_patterns(prompt_text):
    """
    Enforce 10-15 word MAX per bullet in Standard tier.

    Rules:
    - One level of detail allowed (topic - detail OR topic + sentence)
    - No multi-sentence explanations
    - Max 15 words per bullet
    """
    content_bullets = extract_content_bullets(prompt_text)

    issues = []
    for bullet in content_bullets:
        # Check word count
        words = bullet.split()
        if len(words) > 15:
            issues.append(f"⚠️  Too many words: '{bullet}' ({len(words)} words, max 15 for Standard)")

        # Check for multi-sentence (more than one period not at end)
        periods = bullet.count('. ')
        if periods > 1:
            issues.append(f"❌ Multi-sentence bullet: '{bullet}' (Standard tier max 1 sentence)")

    return issues
```

### Detailed Tier Enforcement

```python
def validate_detailed_text_patterns(prompt_text):
    """
    Verify Detailed tier has multi-level explanations.

    Rules:
    - Should have nested bullets (indented detail)
    - Should have examples or reasoning
    - Should have comprehensive annotations
    """
    # Check for nested bullets
    has_nested = re.search(r'^\s{2,}-', prompt_text, re.MULTILINE)
    if not has_nested:
        return ["⚠️  No nested bullets detected (Detailed tier should have multi-level structure)"]

    return []
```

---

## Integration with ig-generate

**Workflow:**

```bash
# 1. Generate prompt with tier validation
/ig-generate-prompt consent-spectrum concise --output=docs/writing/4-privacy/visuals/consent-spectrum/consent-spectrum.prompt.md

# Output:
# ✅ Generated Concise tier prompt
# ✅ CLI validation: PASS (concepts: 12, depth: 2, complexity: 24)
# ✅ Text patterns: PASS (all bullets 3-5 words)
# 📄 Written to: consent-spectrum.prompt.md

# 2. Generate infographic variants
/ig-generate docs/writing/4-privacy/visuals/consent-spectrum/consent-spectrum.prompt.md

# 3. Evaluate results
/ig-evaluate docs/writing/4-privacy/visuals/consent-spectrum/*.webp
```

---

## Examples

### Example 1: Concise Tier Prompt (Correct)

```markdown
# Festival Revenue Breakdown

Simple bar chart showing revenue by category.

## Data Points

- Tickets
- Food & Beverage
- Merchandise
- Sponsorships
- VIP Upgrades

## Style

- Colors: Deep purple (#6B46C1), electric coral (#FF6B6B)
- White space: 40%+ composition
- Typography: Clean sans-serif, 16pt minimum
- Context: Embedded (textbook curriculum)
- Accessibility: Large category labels, values directly on bars

## Structure

Vertical bar chart with 5 categories. Each bar labeled with category name and dollar amount.
```

**Validation:**
- ✅ Concepts: 5 (within 5-16)
- ✅ Depth: 1 level (labels only, within 1-2)
- ✅ Text pattern: All bullets 1-3 words ✅
- ✅ Complexity: 5 (very simple)

### Example 2: Concise Tier Prompt (WRONG - Over-Specified)

```markdown
# Festival Revenue Breakdown

Simple bar chart showing revenue by category.

## Data Points

- Tickets - primary revenue source at $750,000
- Food & Beverage - concessions and bar sales totaling $180,000
- Merchandise - t-shirts, posters, and branded items at $95,000
- Sponsorships - corporate partnerships worth $120,000
- VIP Upgrades - premium experiences generating $85,000

## Style
[...]
```

**Validation:**
- ❌ Concepts: 5 (within range)
- ❌ Depth: 3 levels (topic - detail - value, exceeds 1-2)
- ❌ Text pattern: All bullets 10-15 words (exceeds 3-5 max) ❌
- ❌ Complexity: 15 (pushes Standard tier)

**Issues:**
- `[topic - detail - value]` pattern creates 3-level hierarchy
- 10-15 words per bullet signals Standard tier
- AI will generate explanatory text on infographic

**Fix:** Remove descriptors and values, just use labels:
```markdown
- Tickets
- Food & Beverage
- Merchandise
- Sponsorships
- VIP Upgrades
```

---

## Command Implementation

When `/ig-generate-prompt` is invoked:

1. **Parse arguments:** topic, tier, output path
2. **Load best practices:** EventAI palette, festival context, accessibility
3. **Generate prompt structure:** Template based on tier
4. **Validate text patterns:** Enforce 3-5 words (Concise), 10-15 words (Standard), etc.
5. **CLI validation:** Run `gemini-generate --validate-prompt`
6. **Iterate if needed:** Adjust and re-validate until pass
7. **Write file:** Save with validation metadata
8. **Report:** Show metrics and readiness for ig-generate

---

## Quality Standards

**Prompt is READY when:**
- ✅ CLI validation passes for target tier
- ✅ Text patterns match tier expectations (3-5 words for Concise, etc.)
- ✅ No drilldown patterns in Concise tier (topic - detail)
- ✅ EventAI palette and festival context included
- ✅ Accessibility triple encoding specified (icon + text + color)
- ✅ Embedded context correct (no title on infographic if textbook visual)

**Prompt needs REVISION when:**
- ❌ CLI validation fails (wrong tier range)
- ❌ Text patterns wrong (>5 words in Concise tier bullets)
- ❌ Drilldown patterns detected (topic - detail in Concise)
- ❌ Missing style or structure sections
- ❌ Generic color palette (not EventAI)
- ❌ Missing festival context

---

*Command created: January 1, 2026*
*Purpose: Generate tier-appropriate infographic prompts with text pattern enforcement*
*Method: Template generation + CLI validation + text pattern analysis*
