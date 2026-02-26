# Infographic Evaluation Command

## Purpose

Comprehensively evaluate generated infographics (from NotebookLM, Nano Banana, or other tools) against EventAI style guidelines, infographic best practices, and source material accuracy to ensure professional quality, visual effectiveness, and factual correctness.

## Core Approach

You are a visual design critic and content verifier for academic/professional infographics. Analyze provided infographic files with extreme attention to:
1. **Style Adherence**: Compliance with EventAI Visual Identity Guide
2. **Best Practices**: Application of Tufte principles and professional infographic standards
3. **Data Accuracy**: Verification of all statistics and claims against source material
4. **Effectiveness**: Visual hierarchy, readability, and message clarity
5. **Density Tier Appropriateness**: Evaluate against intended information density tier

---

## Information Density Tiers

**CRITICAL: First identify which density tier the infographic targets, then evaluate appropriately.**

### The Three-Tier Framework

**1. Concise Tier** (Minimal Detail)
- Headlines + 3-5 key stats only
- 15-30 second comprehension
- 40%+ white space required
- **When to identify:** Extremely minimal text, no breakdowns, social media format

**2. Standard Tier** (Balanced Detail) **← DEFAULT ASSUMPTION**
- Key breakdowns with 3-4 components each
- 30-60 second comprehension
- 30% white space target
- **When to identify:** Most infographics fall here - balanced detail without overwhelming

**3. Detailed Tier** (Comprehensive)
- Explanatory annotations, case studies, multiple detail layers
- 2-3 minute close reading required
- 25%+ white space acceptable
- **When to identify:** Dense text, explanatory paragraphs, educational annotations

### Tier Identification Examples

**Standard tier indicators:**
- Stacked bars/charts show components with $ values but no explanatory text
- 3-4 bullet points per section (not paragraphs)
- Labels are brief (category name + value)
- Can understand main message in 30-60 seconds
- **Example:** Dynamic-pricing-2 (three sections, brief labels, readable breakdowns)

**Detailed tier indicators:**
- Each component has 2-3 sentences of explanation
- Year-by-year breakdowns or timelines with annotations
- Case study callout boxes with multiple statistics
- Requires close reading to absorb all information
- **Example:** Dynamic-pricing-1 and dynamic-pricing-3 (comprehensive text, educational annotations)

**Default assumption:** Unless evidence suggests otherwise, assume Standard tier

### Evaluation Criteria by Tier

**DO NOT penalize an infographic for:**
- ❌ "Too simple" if it's appropriately Concise tier
- ❌ "Too detailed" if it's appropriately Detailed tier

**DO penalize for:**
- ✅ Tier mismatch (e.g., claims to be Standard but has Detailed-level text density)
- ✅ Crossing tier boundaries inappropriately (Standard with occasional Detailed paragraphs = inconsistent)

**Scoring adjustments:**
- **Concise tier:** Penalize insufficient white space (<40%), penalize detail creep
- **Standard tier:** Penalize if too sparse OR too dense (target 30% white space, balanced detail)
- **Detailed tier:** Penalize if cognitive overload or accessibility fails (text too small, overwhelming)

---

## Required Context Files

### Primary References (Fixed Best Practices)

**1. EventAI Visual Identity Guide**
- Location: `docs/lemmy/style-guide/eventai-visual-identity.md`
- Purpose: Brand color palette, typography, layout standards
- Key elements: Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)

**2. Infographic Best Practices**
- Location: `docs/lemmy/research/infographics-best-practices.md`
- Purpose: Tufte principles, data-ink ratio, professional design standards
- Key elements: White space (30%+), minimal cruft, data integrity

**3. Source Material** (REQUIRED - will prompt if not provided)
- Location: Varies (e.g., `docs/writing/*/visuals/*/VIS-*.source.md`)
- Purpose: Verify data accuracy, ensure claims match authoritative sources
- Key elements: Statistics, percentages, dates, citations

### Optional Context (Can Override Defaults)

**4. Custom Evaluation Criteria**
- User can provide specific focus areas or override default best practices
- Example: "Prioritize print readability over web optimization"
- Example: "Evaluate for accessibility compliance (WCAG AAA)"

---

## Evaluation Process

### Phase 1: File Discovery & Context Gathering

**Step 1: Identify infographic files**
```bash
# User provides paths, or command searches for common patterns
/ig-evaluate docs/writing/1-transformation/visuals/eventai-timeline/*.webp

# If no paths provided, ask:
"Which infographic files should I evaluate? (Provide file paths or glob patterns)"
```

**Step 2: Identify presentation context (CRITICAL)**

**Determine if infographic is standalone or embedded:**

```markdown
## Context Identification

**File location analysis:**
- Path: docs/writing/2-education/visuals/academic-integration/academic-integration-3.webp
- Parent directory: `/writing/` (narrative content) ✅ Embedded context likely
- Visual directory: `/visuals/` (supporting narrative) ✅ Embedded context

**Prompt file analysis:**
- Check VIS-X.X-GENERATE-INSTRUCTIONS.md for use case
- Look for: "textbook", "curriculum", "figure", "embedded" → Embedded
- Look for: "social media", "standalone", "presentation" → Standalone

**Content location indicators:**
- In `/docs/writing/*/visuals/` → **Embedded** (supporting textbook/article)
- In `/docs/social/` or `/docs/marketing/` → **Standalone** (social/marketing use)
- Referenced as "Figure X.X" in narrative → **Embedded**

**Default for EventAI curriculum:**
Unless clearly indicated otherwise, visuals in `/docs/writing/*/visuals/` are **EMBEDDED**.

**Identified context: [STANDALONE | EMBEDDED]**

**Evaluation adjustments:**
- If EMBEDDED: ❌ DO NOT expect title on infographic (would be redundant)
- If EMBEDDED: ❌ DO NOT expect context statements (provided by narrative)
- If EMBEDDED: ✅ EXPECT minimal explanatory text (labels and data only)
- If STANDALONE: ✅ EXPECT title, context, self-contained design
```

**Context-Specific Evaluation Criteria:**

**EMBEDDED Infographics (Textbook, Articles, Curriculum):**
```markdown
✅ Expected elements:
- Data visualization (primary focus)
- Labels and values (what is shown)
- Legend if needed (understanding the visual)
- Source citation (can be minimal if in caption)
- Clean, focused design

❌ NOT expected (would be redundant):
- Title on the infographic itself (title is in figure caption or surrounding text)
- Context statements ("What this shows:", "Key takeaway:")
- Explanatory paragraphs (interpretation is in narrative)
- Self-contained design (relies on surrounding text)

⚠️ CRITICAL: DO NOT penalize embedded infographics for:
- "Missing title" (CORRECT to omit - title is in text/caption)
- "Needs more context" (CORRECT - context in narrative)
- "Too minimal" (CORRECT - should focus on data only)
```

**STANDALONE Infographics (Social, Presentations, Marketing):**
```markdown
✅ Expected elements:
- Clear title (part of visual design)
- Subtitle or context statement
- Complete labels and legends
- Source citations (visible on infographic)
- Self-contained design (understandable without external text)

❌ Penalize if missing:
- Title or clear main message
- Context (what is this about?)
- Self-contained explanation
```

**Step 3: Locate source material**
```
# Check for VIS-*.source.md in same directory
# If not found, ask:
"I couldn't locate source material for these infographics. Please provide:
 - Path to source document (VIS-*.source.md or similar)
 - Or confirm evaluation should proceed without data verification"
```

**Step 4: Load best practices**
```
# Automatically load:
- docs/lemmy/style-guide/eventai-visual-identity.md
- docs/lemmy/research/infographics-best-practices.md
- Apply context-aware evaluation (standalone vs. embedded)

# If user provides overrides:
"Using custom evaluation criteria: [list provided criteria]"
```

**Step 5: Validate Source Prompt (if available)**

**🔴 CRITICAL: Check prompt file for text pattern violations before evaluating infographic.**

```bash
# Locate prompt file
prompt_file=docs/writing/{topic}/visuals/{name}/{name}.prompt.md

# Run CLI validation
gemini-generate --validate-prompt $prompt_file --density {tier}
```

**Why this matters:**
- If the source prompt has text pattern violations (e.g., drilldown patterns in Concise tier), the infographic will inherit those issues
- Text pattern problems in the prompt → visual clutter on the infographic
- Validates prompt BEFORE judging infographic quality

**Prompt validation results feed into infographic evaluation:**

```markdown
## Prompt Validation Results

**File:** consent-spectrum.prompt.md
**Declared tier:** Concise

**CLI Validation:**
- Structural metrics: ✅ PASS (16 concepts, 2 depth)
- Text patterns: ❌ FAIL (12 drilldown violations)

**Text Pattern Issues:**
- ❌ Drilldown pattern: "Mandatory facial recognition - no alternatives" (should be label only)
- ❌ Too many words: "Bundled consent - accept all or entry denied" (8 words, max 5 for Concise)

**Impact on Infographic:**
- Expect explanatory text on infographic (AI generated drilldown content)
- Likely reduced white space (text blocks added)
- May inflate tier (Concise prompt → Standard infographic)

**Recommendation:** Fix prompt text patterns first, then regenerate infographic
```

**If prompt validation fails:**
1. Document issues in evaluation report
2. Note that infographic inherited prompt problems
3. Recommend prompt revision + regeneration
4. Do NOT penalize infographic for prompt-sourced issues (separate concern)

---

### Phase 2: Visual Analysis

**Step 1: EventAI Style Compliance**

Evaluate against Visual Identity Guide standards:

```markdown
## Color Palette Assessment
- [ ] Deep Festival Purple (#6B46C1) used for primary elements
- [ ] Electric Coral (#FF6B6B) used for key statistics/emphasis
- [ ] Sky Blue (#4299E1) used for data visualization
- [ ] Midnight Slate (#2D3748) used for body text
- [ ] Pure White (#FFFFFF) background
- [ ] No off-brand colors introduced

Score: _/10
Issues: [List any deviations]
```

```markdown
## Typography Assessment
- [ ] Clean modern sans-serif (Inter or similar) for headings
- [ ] Readable body font (Source Sans Pro or similar)
- [ ] Maximum 2 font families used
- [ ] Hero statistics 48-72pt
- [ ] Section headers 24-32pt
- [ ] Body text 14-16pt minimum
- [ ] Text legible at intended size (print or screen)

Score: _/10
Issues: [List any problems]
```

```markdown
## Layout & Composition

**Context-aware evaluation:**

**If EMBEDDED (textbook/article context):**
- [ ] Minimum 30% white space
- [ ] Clear visual hierarchy (data → supporting labels → source)
- [ ] ❌ Title on infographic (should NOT be present - redundant with caption)
- [ ] Left-aligned text (or justified alignment with purpose)
- [ ] Generous margins (48-64px minimum)
- [ ] Elements aligned to grid
- [ ] Breathing room between sections (24-48px)
- [ ] Minimal explanatory text (labels only, not paragraphs)

**If STANDALONE (social/presentation context):**
- [ ] Minimum 30% white space
- [ ] Clear visual hierarchy (title → data → supporting → source)
- [ ] ✅ Title on infographic (clear, prominent)
- [ ] Subtitle or context statement (what is this about?)
- [ ] Left-aligned text (or justified alignment with purpose)
- [ ] Generous margins (48-64px minimum)
- [ ] Elements aligned to grid
- [ ] Breathing room between sections (24-48px)
- [ ] Self-contained design (understandable without external text)

Score: _/10
Issues: [List any concerns - NOTE context when flagging title presence/absence]
```

```markdown
## Festival Context Integration
- [ ] Festival-relevant visual elements (stages, crowds, wristbands)
- [ ] NOT generic business imagery (suits, offices, handshakes)
- [ ] NOT clichéd AI imagery (robot overlords, circuit brains)
- [ ] Human festival-goers represented (if applicable)
- [ ] Cultural diversity shown (if people depicted)

Score: _/10
Issues: [List any missing context]
```

**Step 2: Best Practices Compliance**

Evaluate against Tufte principles and professional standards:

```markdown
## Data-Ink Ratio (Tufte)
- [ ] Minimal cruft (no unnecessary decoration)
- [ ] No decorative borders
- [ ] No excessive gradients or shadows
- [ ] No ornamental shapes without meaning
- [ ] Grid lines only if serving data readability
- [ ] Every element serves information or clarity

Score: _/10
Cruft identified: [List unnecessary elements]
```

```markdown
## Graphical Excellence (Tufte)
- [ ] Information-rich presentation
- [ ] Worth reading closely (rewards attention)
- [ ] Reveals data at multiple levels (overview → detail)
- [ ] Encourages eye to compare data
- [ ] Makes large dataset coherent (if applicable)

Score: _/10
Issues: [List any problems]
```

```markdown
## Graphical Integrity (Tufte)
- [ ] Proportions in graphic match proportions in data
- [ ] No truncated axes (unless justified and labeled)
- [ ] Clear labeling (axes, scales, units)
- [ ] No misleading visualizations
- [ ] Context provided (baselines, comparisons)

Score: _/10
Issues: [List any integrity concerns]
```

```markdown
## Professional + Whimsy Balance
- [ ] Professional foundation (clean structure, evidence-based)
- [ ] Whimsical accents (personality touches, unexpected colors)
- [ ] NOT too casual (childish illustrations, cartoon characters)
- [ ] NOT too sterile (all gray, rigid grids only)
- [ ] Appropriate for business/academic audience

Score: _/10
Balance assessment: [Too professional / Just right / Too whimsical]
```

**Step 3: Accessibility & Readability**

```markdown
## Accessibility Compliance
- [ ] Text-to-background contrast meets WCAG AA (4.5:1 minimum)
- [ ] Information not conveyed by color alone
- [ ] Icon + text + color for indicators (not color-only)
- [ ] Minimum 12pt text (10pt for micro-labels only)
- [ ] Readable for color-blind viewers (test with common CVD types)

Score: _/10
Accessibility issues: [List any barriers]
```

```markdown
## Print Readiness (if applicable)
- [ ] Resolution 300+ DPI for print (or 150+ for screen-only)
- [ ] Text remains crisp when zoomed
- [ ] Colors work in both RGB and CMYK (no extreme shifts)
- [ ] Safe zones respected (12-16px from edges minimum)

Score: _/10 (or N/A if screen-only)
Issues: [List any print concerns]
```

### Phase 3: Data Verification

**Step 1: Extract all quantitative claims from infographic**

```markdown
## Statistics Inventory (from infographic)

| Statistic | Value | Location in Image | Context |
|-----------|-------|------------------|---------|
| AI Adoption 2025 | 47% | Timeline start | Major festivals |
| AI Adoption 2028 | 67.5% | Phase 1 end | Major festivals |
| AI Adoption 2035 | 95% | Timeline end (hero stat) | Major festivals |
| Small festivals 2025 | 45% | Timeline start | Small venues |
| Market value 2023 | $1.8B | Callout box | AI event management sector |

Example extraction table - populate with actual values from infographic
```

**Step 2: Cross-reference against source material**

```markdown
## Data Accuracy Verification

| Claim (Infographic) | Source Material | Match? | Issue |
|---------------------|----------------|--------|-------|
| 47% adoption 2025 | Source: 47% ✅ | ✅ | - |
| 67.5% adoption 2028 | Source: 60-70% range ✅ | ✅ | Midpoint of range (acceptable) |
| 95% adoption 2035 | Source: 95%+ ✅ | ✅ | - |
| $1.8B market value | Source: $1.8 billion ✅ | ✅ | - |
| "EU AI Act 2025" | Source: February 2, 2025 ✅ | ✅ | Date detail omitted (acceptable for infographic) |

Overall accuracy: ✅ VERIFIED / ⚠️ MINOR DISCREPANCIES / ❌ ERRORS FOUND
```

**Step 3: Check for hallucinations or unsupported claims**

```markdown
## Unsupported Claims Check

Claims in infographic NOT found in source material:
- [List any statistics, dates, or facts not in source]
- [Flag for verification or removal]

If none: ✅ All claims supported by source material
```

### Phase 4: Comparative Evaluation (if multiple variants)

**For multiple infographic variants of same content:**

```markdown
## Variant Comparison Matrix

| Criterion | Variant #1 | Variant #2 | Variant #3 | Winner |
|-----------|-----------|-----------|-----------|--------|
| Style adherence | 7/10 | 8/10 | 9/10 | #3 ✅ |
| Data accuracy | 10/10 | 10/10 | 10/10 | Tie ✅ |
| White space | 6/10 | 7/10 | 8/10 | #3 ✅ |
| Festival context | 7/10 | 8/10 | 9/10 | #3 ✅ |
| Typography | 6/10 | 8/10 | 9/10 | #3 ✅ |
| Minimal cruft | 6/10 | 8/10 | 9/10 | #3 ✅ |
| Print readiness | 8/10 | 8/10 | 9/10 | #3 ✅ |
| **TOTAL** | **50/70** | **57/70** | **63/70** | **#3** ✅ |

Recommendation: Use Variant #3
Rationale: [Brief explanation of why this variant is strongest]
```

---

## Evaluation Report Format

### Executive Summary

```markdown
# Infographic Evaluation Report: [Infographic Name]

**⚠️ CONTEXT CHECK PERFORMED ⚠️**
**Presentation Context:** [STANDALONE | EMBEDDED]
- EMBEDDED: Infographic is part of textbook/article, surrounding text provides title and context
- STANDALONE: Infographic stands alone (social media, presentation), must be self-contained

**Context-based evaluation adjustments applied:** [Yes/No]

**Evaluation Date:** [Date]
**Evaluator:** Claude Sonnet 4.5
**Infographic(s):** [File paths]
**Source Material:** [Source document path]

## Overall Assessment

**Status:** ✅ APPROVED / ⚠️ APPROVED WITH REVISIONS / ❌ REQUIRES MAJOR REVISIONS

**Overall Score:** [Score]/100

**Recommendation:**
- ✅ **Ready for publication** - Meets all standards
- ⚠️ **Minor revisions recommended** - Good overall, improvements suggested
- ❌ **Major revisions required** - Significant issues must be addressed

## Score Breakdown

| Category | Score | Status |
|----------|-------|--------|
| EventAI Style Compliance | 85/100 | ✅ Strong |
| Best Practices Adherence | 78/100 | ⚠️ Good |
| Data Accuracy | 100/100 | ✅ Perfect |
| Accessibility | 72/100 | ⚠️ Needs work |
| Festival Context | 90/100 | ✅ Excellent |
| **TOTAL** | **425/500** | **85% - Strong** |
```

### Detailed Findings

#### 1. Strengths (What Works Well)

```markdown
## Strengths ✅

### Visual Design
- ✅ **Excellent color palette adherence**: Deep purple, electric coral, and sky blue perfectly match EventAI brand
- ✅ **Strong festival context**: Background crowd silhouette immediately signals festival industry (not generic business)
- ✅ **Clean typography**: Clear hierarchy, readable at intended size

### Information Design
- ✅ **High data-ink ratio**: Minimal decoration, every element serves purpose
- ✅ **Effective white space**: 30%+ composition, clean breathing room
- ✅ **Clear visual hierarchy**: Title → phases → data points → supporting text

### Data Accuracy
- ✅ **100% accuracy**: All statistics verified against source material
- ✅ **Proper emphasis**: 95% endpoint correctly highlighted as key statistic
- ✅ **Source citation**: Included at bottom (credibility)

[Continue listing all strengths identified]
```

#### 2. Weaknesses (Areas for Improvement)

```markdown
## Weaknesses ⚠️

### Minor Issues (Recommended Improvements)

**Typography Readability**
- Issue: Milestone text appears ~12-14pt, may be difficult to read in print
- Impact: Medium - Reduces accessibility for print use
- Recommendation: Increase to 16pt minimum for print readability
- Priority: 🟡 Medium

**White Space Distribution**
- Issue: Text callout boxes have minimal internal padding
- Impact: Low - Slightly reduces readability
- Recommendation: Add 8-12px padding inside callout boxes
- Priority: 🟢 Low

### Major Issues (Must Address)

**[None identified in this evaluation]**

[List any critical issues requiring immediate correction]
```

#### 3. EventAI Style Compliance Scorecard

```markdown
## EventAI Style Guide Compliance

### Color Palette: 9/10 ✅
- ✅ Deep purple (#6B46C1) used for primary elements
- ✅ Electric coral (#FF6B6B) used for 95% emphasis
- ✅ Sky blue (#4299E1) used for data lines
- ✅ Pure white background
- Minor: Gold/orange used for phase dividers (acceptable - warm sunlight #F6AD55)

### Typography: 8/10 ✅
- ✅ Clean sans-serif (appears to be Inter or similar)
- ✅ Maximum 2 font families
- ✅ Clear hierarchy (title bold, body regular)
- ⚠️ Minor: Some callout text could be larger (see weaknesses)

### Layout: 9/10 ✅
- ✅ Generous white space (estimated 35-40% of composition)
- ✅ Clear visual hierarchy
- ✅ Left-aligned text
- ✅ Proper margins (48px+ estimated)

### Festival Context: 10/10 ✅✅
- ✅✅ Exceptional crowd silhouette (strong festival atmosphere)
- ✅ Stage icons at data points
- ✅ RFID wristband symbols (if present - verify in image)
- ✅ No generic business imagery

### Professional + Whimsy: 9/10 ✅
- ✅ Professional foundation (structured timeline, evidence-based data)
- ✅ Whimsical accents (crowd silhouette, colorful phase dividers)
- ✅ Appropriate for academic/business audience
- Balance: Just right (credible yet memorable)

### Minimal Cruft: 9/10 ✅
- ✅ No decorative borders
- ✅ No excessive gradients
- ✅ No ornamental shapes
- ✅ Every element serves information
- Minor: Verify no unnecessary icon embellishments

**Overall EventAI Style Score: 54/60 (90%) - Excellent adherence**
```

#### 4. Best Practices Compliance Scorecard

```markdown
## Tufte Principles & Professional Standards

### Data-Ink Ratio: 9/10 ✅
- ✅ High ratio of data-serving ink to total ink
- ✅ Minimal decoration
- ✅ No cruft identified
- Minor: Could simplify further (if any redundant elements)

### Graphical Excellence: 8/10 ✅
- ✅ Information-rich presentation
- ✅ Rewards close attention (overview → detail)
- ✅ Encourages comparison (major vs small festivals)
- ⚠️ Could reveal more layers (e.g., milestone details)

### Graphical Integrity: 10/10 ✅
- ✅ Proportions in graphic match data proportions
- ✅ No truncated axes
- ✅ Clear labeling (years, percentages, phases)
- ✅ No misleading visualizations

### White Space: 9/10 ✅
- ✅ Estimated 35-40% white space (exceeds 30% minimum)
- ✅ Strategic use to group related elements
- ✅ Breathing room around key statistics
- Minor: Could add more padding in text boxes

### Accessibility: 7/10 ⚠️
- ✅ High contrast text (appears to meet WCAG AA)
- ✅ Not color-dependent (icons + text + color)
- ⚠️ Some text may be below 12pt minimum (verify)
- ⚠️ Color-blind simulation not yet performed

### Print Readiness: 8/10 ✅ (if print intended)
- ✅ Appears to be high resolution (1080p webp)
- ✅ Text appears crisp
- ⚠️ Verify 300+ DPI for print (1080p may be screen-optimized)
- ⚠️ Some text sizes may need increase for print

**Overall Best Practices Score: 51/60 (85%) - Strong compliance**
```

#### 5. Data Accuracy Verification

```markdown
## Source Material Cross-Reference

### Statistics Verified: 100% ✅

| Infographic Claim | Source Material | Status |
|------------------|----------------|--------|
| 47% baseline (2025) | Source: 47% | ✅ Exact match |
| 67.5% major (2028) | Source: 60-70% range | ✅ Midpoint (acceptable) |
| 30% small (2028) | Source: 25-35% range | ✅ Midpoint (acceptable) |
| 87.5% major (2032) | Source: 85-90% range | ✅ Midpoint (acceptable) |
| 55% small (2032) | Source: 50-60% range | ✅ Midpoint (acceptable) |
| 95% major (2035) | Source: 95%+ | ✅ Exact match |
| 72.5% small (2035) | Source: 70-75% range | ✅ Midpoint (acceptable) |
| EU AI Act 2025 | Source: Feb 2, 2025 | ✅ Year correct (date detail omitted) |
| DICE 40% sales | Source: 40-41% | ✅ Rounded (acceptable for infographic) |

### Unsupported Claims: None identified ✅

All data points traced to source material. No hallucinations detected.

**Data Accuracy Score: 10/10 (100%) - Perfect**
```

---

## Recommendations & Action Items

### Critical (Must Fix Before Publication)

```markdown
## Critical Issues: [None] ✅

[If any critical issues exist, list here with specific corrections needed]
```

### High Priority (Strongly Recommended)

```markdown
## High Priority Improvements

1. **Increase milestone text size for print readability**
   - Current: ~12-14pt estimated
   - Recommended: 16pt minimum
   - Impact: Improves print readability significantly
   - Implementation: Regenerate with larger text or edit in design tool

2. **Verify resolution for intended use**
   - Current: 1080p webp (~2MB)
   - For print: Confirm 300+ DPI or regenerate at higher resolution
   - For screen: Current resolution sufficient ✅
```

### Medium Priority (Nice to Have)

```markdown
## Medium Priority Enhancements

3. **Add padding to callout boxes**
   - Current: Minimal internal padding
   - Recommended: 8-12px padding inside text boxes
   - Impact: Improves readability slightly

4. **Color-blind simulation test**
   - Current: Not yet performed
   - Recommended: Test with deuteranopia, protanopia, tritanopia simulations
   - Impact: Ensures accessibility for color-blind viewers
```

### Low Priority (Optional)

```markdown
## Optional Polish

5. **Consider adding visual progression to stage icons**
   - Current: Stage icons appear similar across timeline
   - Optional: Increase complexity/detail from 2025 → 2035 (suggesting evolution)
   - Impact: Reinforces narrative of technological advancement
```

---

## Comparative Ranking (if multiple variants evaluated)

```markdown
# Variant Comparison: AI Adoption Timeline

**Evaluated:** 3 variants from NotebookLM generation

## Winner: Variant #3 ⭐

**Score: 85/100** - Best overall balance

### Why Variant #3 Wins

1. **Best EventAI style adherence (90%)**: Color palette, typography, layout all excellent
2. **Strongest festival context (10/10)**: Crowd silhouette perfectly integrated
3. **Optimal white space (9/10)**: Clean composition, breathing room
4. **Professional + whimsy balance (9/10)**: Credible yet memorable
5. **100% data accuracy**: All statistics verified

### Variant #1: 72/100
- ⚠️ Color palette deviation (orange instead of coral)
- ⚠️ Limited white space (text-heavy)
- ⚠️ Background beige instead of white
- ✅ Data accuracy perfect

### Variant #2: 79/100
- ✅ Good festival context (crowd silhouette)
- ✅ Color palette closer to standard
- ⚠️ Dark text boxes too heavy
- ⚠️ Could use more white space
- ✅ Data accuracy perfect

## Recommendation: Use Variant #3 for publication

Minor refinements suggested (see action items), but ready for use as-is if needed.
```

---

## Usage Instructions

### CRITICAL: Context Identification Required ⚠️

**ALWAYS identify presentation context BEFORE evaluation:**

```bash
# Step 0: Determine context
# Check file location and intended use

EMBEDDED context indicators:
- Location: docs/writing/*/visuals/
- Use: Textbook, article, curriculum
- Referenced as: "Figure X.X" in narrative
- Expect: NO title on infographic (title in caption/text)

STANDALONE context indicators:
- Location: docs/social/, docs/marketing/
- Use: Social media, presentations, marketing
- Referenced as: Independent visual
- Expect: Title on infographic (self-contained)

# For EventAI curriculum: Default to EMBEDDED unless explicitly standalone
```

**Evaluation adjustment example:**
```bash
# EMBEDDED context (most EventAI curriculum visuals)
/ig-evaluate docs/writing/2-education/visuals/academic-integration/*.webp
→ Evaluates as embedded (NO title expected on infographic)
→ Title presence would be penalized (redundant with caption)

# STANDALONE context (social media, presentations)
/ig-evaluate docs/social/linkedin-posts/*.webp
→ Evaluates as standalone (title REQUIRED on infographic)
→ Missing title would be penalized
```

### CRITICAL: Image Format Requirements ⚠️

**Always convert PNG to webp before evaluation:**

```bash
# Step 1: Convert PNGs to 1080p webp (REQUIRED)
todd-image-convert docs/writing/*/visuals/*/*.png --resolution 1080p --output-format webp

# Step 2: Evaluate the webp files (not the PNGs)
/ig-evaluate docs/writing/*/visuals/*/*.webp
```

**Why webp, not PNG?**
- Consistent resolution (1080p standardized)
- Smaller file sizes for web use
- Better compression without quality loss
- Standard format for EventAI visual assets

**DO NOT evaluate PNG files directly** - always convert first.

### Basic Evaluation (Single Infographic)

```bash
# Evaluate a single infographic with auto-detected source
/ig-evaluate docs/writing/1-transformation/visuals/eventai-timeline/transformation-infographic-adoption-timeline-3.webp

# Command will:
# 1. Read the webp file
# 2. Search for VIS-*.source.md in same directory
# 3. Load EventAI style guide and best practices
# 4. Generate comprehensive evaluation report
# 5. Write evaluation to [directory]/[name].eval.md
```

### Evaluation with Explicit Source Material

```bash
# Provide source material explicitly
/ig-evaluate docs/writing/1-transformation/visuals/eventai-timeline/transformation-infographic-adoption-timeline-3.webp --source=docs/writing/1-transformation/visuals/eventai-timeline/VIS-1.1-source.md

# Useful if source is in different location or has non-standard naming
```

### Comparative Evaluation (Multiple Variants)

```bash
# Evaluate multiple variants and compare
/ig-evaluate docs/writing/1-transformation/visuals/eventai-timeline/*.webp

# Command will:
# 1. Evaluate each variant individually
# 2. Create comparison matrix
# 3. Recommend best variant
# 4. Explain why winner was selected
# 5. Write comprehensive report to [name].eval.md
# 6. Update [name]-GENERATE-INSTRUCTIONS.md with learnings
```

### Custom Evaluation Criteria

```bash
# Override default criteria with specific focus
/ig-evaluate my-infographic.png --focus="accessibility,print-readiness"

# Or provide custom instructions
/ig-evaluate my-infographic.png --criteria="Prioritize color-blind accessibility (WCAG AAA), verify all text >14pt for classroom projection use"
```

### Evaluation Without Source Material

```bash
# Evaluate style and best practices only (skip data verification)
/ig-evaluate my-infographic.png --no-source

# Useful for:
# - Concept sketches before source material exists
# - Design-only review
# - Third-party infographics
```

---

## Evaluation Checklist

Use this checklist to ensure comprehensive evaluation:

### Pre-Evaluation
- [ ] Infographic file(s) identified and accessible
- [ ] Source material located (or confirmed not needed)
- [ ] EventAI style guide loaded
- [ ] Best practices documentation loaded
- [ ] Custom criteria noted (if any)

### EventAI Style Evaluation
- [ ] Color palette assessed (purple, coral, blue, white)
- [ ] Typography evaluated (fonts, sizes, hierarchy)
- [ ] Layout checked (white space, margins, alignment)
- [ ] Festival context verified (icons, imagery, metaphors)
- [ ] Professional + whimsy balance assessed
- [ ] Minimal cruft principle applied

### Best Practices Evaluation
- [ ] Data-ink ratio calculated
- [ ] Graphical excellence assessed
- [ ] Graphical integrity verified
- [ ] White space percentage estimated
- [ ] Accessibility tested (contrast, color-dependence)
- [ ] Print readiness checked (if applicable)

### Data Verification
- [ ] All statistics extracted from infographic
- [ ] Cross-referenced against source material
- [ ] Hallucinations/unsupported claims checked
- [ ] Numerical consistency verified
- [ ] Source citations confirmed

### Comparative Analysis (if applicable)
- [ ] Each variant scored individually
- [ ] Comparison matrix created
- [ ] Winner identified with rationale
- [ ] Strengths/weaknesses of each variant documented

### Report Generation
- [ ] Executive summary written
- [ ] Detailed findings documented
- [ ] Scorecards completed
- [ ] Recommendations prioritized (critical/high/medium/low)
- [ ] Action items clearly stated
- [ ] **Report written to file** (`[name].eval.md` in infographic directory)
- [ ] **GENERATE-INSTRUCTIONS updated** with findings to prevent recurring issues

---

## Special Evaluation Cases

### Case 1: Timeline Infographics

**Additional checks:**
- [ ] Timeline flows left-to-right (standard reading direction)
- [ ] Time intervals clearly marked
- [ ] Phase divisions visible
- [ ] Progression shown (ascending curves, increasing complexity)
- [ ] Endpoint emphasized (e.g., 95% adoption in 2035)

### Case 2: Comparison Infographics (Before/After)

**Additional checks:**
- [ ] Clear visual divider between before/after
- [ ] Before side muted (grayscale or desaturated)
- [ ] After side full color (EventAI palette)
- [ ] Equal space given to both sides
- [ ] Comparison elements aligned (easy to scan)

### Case 3: Process/Flow Diagrams

**Additional checks:**
- [ ] Flow direction clear (arrows, numbering)
- [ ] Decision points visible (if applicable)
- [ ] Steps sequentially numbered
- [ ] Connections not spaghetti (clean routing)
- [ ] Festival context integrated (not generic flowchart)

### Case 4: Statistical Charts/Graphs

**Additional checks:**
- [ ] Axes labeled clearly (units, scale)
- [ ] Data points directly labeled (not legend-only)
- [ ] No truncated axes (unless justified and labeled)
- [ ] Bars/lines proportional to data
- [ ] Legend only if direct labeling impossible

---

## Common Evaluation Patterns

### Pattern 0: Context Misidentification (CRITICAL)

**Problem:** Penalizing embedded infographic for missing title, or standalone for having one

**Detection:**
```
File: docs/writing/2-education/visuals/academic-integration/academic-integration-3.webp
Location: /writing/*/visuals/ → EMBEDDED context
Title on infographic: None
Evaluator flags: "Missing title" ❌ WRONG!
```

**Correct Evaluation:**
```
Context Identification: EMBEDDED (textbook visual)
Title presence: None (CORRECT ✅)
Rationale: Title is in figure caption/surrounding text. Having title on infographic would be redundant and break narrative flow.

Layout Score: 9/10 ✅
Strengths: 
- Correctly omits title (follows embedded context best practices)
- Clean, data-focused design
- Integrates seamlessly with narrative text

⚠️ CRITICAL: This is CORRECT design for embedded context. 
   DO NOT penalize for missing title - it SHOULD be absent.
```

**Wrong Evaluation:**
```
❌ "Missing title - needs clear heading" ← INCORRECT for embedded context
❌ "Needs context statement" ← INCORRECT - context is in narrative
❌ "Too minimal, needs more explanation" ← INCORRECT - narrative provides explanation
```

**Prevention:**
1. ALWAYS identify context (standalone vs. embedded) FIRST
2. Check file location (/writing/*/visuals/ = embedded)
3. Apply context-appropriate evaluation criteria
4. Document context identification in report

### Pattern 1: Color Palette Deviation

**Problem:** Infographic uses colors outside EventAI palette

**Detection:**
```
Expected: Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
Found: Orange (#FF8C42), teal (#00CED1), generic gray
```

**Evaluation:**
```
Color Palette Score: 5/10 ⚠️
Issue: Non-brand colors used (orange instead of coral, teal instead of blue)
Impact: Reduces brand consistency, doesn't match EventAI visual identity
Recommendation: Regenerate with exact hex codes specified in prompt
Priority: 🟡 High (brand consistency important)
```

### Pattern 2: Insufficient White Space

**Problem:** Infographic appears cramped, visually cluttered

**Detection:**
```
Estimated white space: ~15-20% of composition
Minimum required: 30%
```

**Evaluation:**
```
White Space Score: 4/10 ⚠️
Issue: Insufficient breathing room, elements too close together
Impact: Reduces readability, appears cluttered, eye doesn't know where to focus
Recommendation: Increase margins, reduce number of elements, or enlarge canvas
Priority: 🟡 High (significantly impacts quality perception)
```

### Pattern 3: Data Accuracy Mismatch

**Problem:** Statistics in infographic don't match source material

**Detection:**
```
Infographic: "50% adoption by 2028"
Source material: "60-70% adoption by 2028"
```

**Evaluation:**
```
Data Accuracy Score: 6/10 ❌
Issue: Statistic outside source material range
Impact: CRITICAL - Factual error, misinforms audience
Recommendation: Correct to match source (use midpoint 65% or state as range)
Priority: 🔴 Critical (must fix before publication)
```

### Pattern 4: Typography Hierarchy Issues

**Problem:** No clear visual hierarchy, all text similar size

**Detection:**
```
Title: 28pt
Section headers: 24pt
Body text: 22pt
Statistics: 26pt
```

**Evaluation:**
```
Typography Score: 5/10 ⚠️
Issue: Insufficient size differentiation, no clear hierarchy
Impact: Reader doesn't know what to focus on, key stats not emphasized
Recommendation: Increase title to 36-40pt, stats to 56-72pt, reduce body to 14-16pt
Priority: 🟡 High (significantly impacts effectiveness)
```

---

## Quality Standards

### Evaluation is COMPLETE when:

✅ **Visual analysis performed:**
- EventAI style compliance scored (color, typography, layout, context)
- Best practices adherence verified (Tufte principles, white space, accessibility)
- Print/screen readiness assessed

✅ **Data verification performed:**
- All statistics extracted and inventoried
- Cross-referenced against source material
- Hallucinations/unsupported claims checked
- Numerical accuracy confirmed

✅ **Report generated AND WRITTEN TO FILE:**
- Executive summary with overall score and recommendation
- Detailed findings (strengths and weaknesses)
- Scorecards for each evaluation category
- Prioritized action items (critical/high/medium/low)
- **Report file created:** `[name].eval.md` in infographic directory

✅ **Comparative analysis (if multiple variants):**
- Each variant scored individually
- Comparison matrix created
- Winner identified with clear rationale

✅ **GENERATE-INSTRUCTIONS updated:**
- Prompt improvements added based on evaluation findings
- Common issues flagged with ⚠️ or ❌ in AVOID section
- Critical requirements emphasized at top of prompt
- Learnings fed back into generation workflow

### Evaluation is INCOMPLETE if:

❌ Source material not reviewed (unless explicitly skipped)
❌ EventAI style guide not consulted
❌ Best practices not applied
❌ Data accuracy not verified
❌ No clear recommendation provided
❌ Issues identified but not prioritized
❌ Scores assigned without explanation
❌ **Report not written to file** (only in conversation)
❌ **GENERATE-INSTRUCTIONS not updated** with learnings

---

## Anti-Patterns (What NOT to Do)

❌ **Don't skip context identification:**
```
Bad: Evaluate all infographics as standalone, penalize embedded ones for "missing title"
Good: Identify context FIRST (standalone vs. embedded), apply appropriate criteria
```

**CRITICAL Example:**
```
❌ WRONG:
File: docs/writing/2-education/visuals/literacy-comparison/literacy-comparison-1.webp
Issue flagged: "Missing title - needs clear heading"
Problem: This is EMBEDDED context (textbook visual). Title SHOULD be absent!

✅ CORRECT:
File: docs/writing/2-education/visuals/literacy-comparison/literacy-comparison-1.webp
Context: EMBEDDED (textbook curriculum)
Title status: Correctly absent (title is in figure caption)
Evaluation: 9/10 - Proper embedded infographic design
```

❌ **Don't skip source material verification:**
```
Bad: "Looks good, numbers seem reasonable"
Good: Cross-reference every statistic against source document
```

❌ **Don't accept "close enough" on brand colors:**
```
Bad: "Orange is close to coral, good enough"
Good: Verify exact hex codes, flag deviations
```

❌ **Don't overlook accessibility:**
```
Bad: "Looks fine to me" (without checking contrast ratios or color-blind view)
Good: Test WCAG compliance, simulate color-blindness
```

❌ **Don't evaluate in isolation:**
```
Bad: Review infographic without context of intended use (print vs screen, audience, etc.)
Good: Consider use case, audience needs, distribution channel
```

❌ **Don't be vague in recommendations:**
```
Bad: "Could be better"
Good: "Increase milestone text from 12pt to 16pt for print readability"
```

---

## Output Format Preferences

### Concise Summary (default)
```markdown
# Evaluation: [Infographic Name]

**Score:** 85/100 ✅ Strong
**Status:** Approved with minor revisions

**Strengths:**
- Excellent EventAI style adherence (90%)
- Perfect data accuracy (100%)
- Strong festival context (10/10)

**Improvements:**
1. Increase milestone text to 16pt (print readability)
2. Verify 300+ DPI for print use

**Recommendation:** Use Variant #3 - ready for publication with minor refinements
```

### Detailed Report (use --verbose flag)
```markdown
[Full comprehensive report with all scorecards, detailed findings, comparative analysis]
```

### JSON Output (use --json flag)
```json
{
  "overall_score": 85,
  "status": "approved_with_revisions",
  "category_scores": {
    "style_compliance": 90,
    "best_practices": 85,
    "data_accuracy": 100,
    "accessibility": 72,
    "festival_context": 90
  },
  "critical_issues": [],
  "high_priority": [
    "Increase milestone text to 16pt",
    "Verify 300+ DPI for print"
  ],
  "recommendation": "Use Variant #3"
}
```

---

## Integration with Lemmy Workflow

### Typical Workflow

```bash
# 1. Generate infographic variations in NotebookLM (manual step)
#    - Follow VIS-X.X-GENERATE-INSTRUCTIONS.md
#    - Download PNG files from NotebookLM
#    - Save as transformation-infographic-{name}-{1,2,3}.png

# 2. Convert to webp (REQUIRED - do not skip!)
cd docs/writing/{topic}/visuals/{name}
todd-image-convert *.png --resolution 1080p --output-format webp

# 3. Evaluate all variants
/ig-evaluate docs/writing/{topic}/visuals/{name}/*.webp

# This command will:
# - Analyze all variants against EventAI style guide
# - Verify data accuracy against [name].source.md (or [name].content.md)
# - Create [name].eval.md with findings
# - Update [name].instructions.md with improvements
# - Recommend winning variant

# 4. Review evaluation report
cat [name].eval.md

# 5. Select winner and optionally regenerate if critical issues found
# - If score < 80%: Regenerate with improved prompt
# - If score 80-90%: Use as-is or apply minor refinements
# - If score > 90%: Approved for publication

# 6. Mark VIS-X.X as complete in VISUAL-CONTENT-PLAN.md
```

### Beads Integration

```bash
# Create evaluation task in beads
todd-carl create --title="Evaluate VIS-1.1 infographics" --type=task --priority=2

# Run evaluation
/ig-evaluate docs/writing/1-transformation/visuals/eventai-timeline/*.webp

# Document results in beads
todd-carl update beads-xxx --status=completed --reason="Variant #3 selected, scored 85/100, ready for publication"
```

---

## Final Notes

**Philosophy:** Visual quality and data accuracy are non-negotiable. Every infographic represents EventAI brand and academic credibility.

**Rigor over speed:** Thorough evaluation may take 15-30 minutes per infographic. This is time well spent to ensure professional quality.

**When in doubt, flag it:** If you cannot definitively verify a claim or assess a design choice, mark it for author review rather than making assumptions.

**Document everything:** The evaluation report should be detailed enough that anyone can understand your assessment and reproduce your findings.

**Balance:** Professional standards with practical constraints. Perfection is ideal, but "very good with minor improvements" is often publication-ready.

---

**Command maintained by:** Lemmy Content Generation System
**Last updated:** December 28, 2025
**Version:** 1.0
