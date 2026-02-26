# /generate-infographic: Automated Infographic Generation

**Purpose:** Fully automated infographic generation using Gemini web automation (Playwright)

**Workflow:**
1. Reads prompt.md and content.md from directory
2. Opens Gemini in Chrome browser with parallel tabs
3. Generates 3 variants simultaneously
4. Downloads PNG files
5. Converts to WebP using todd-image-convert
6. Ready for /ig-evaluate

**Method:** Browser automation via Playwright (not API - uses gemini.google.com directly)

---

## Context-Aware Generation (CRITICAL)

**ALWAYS identify whether the infographic is standalone or embedded before generating prompts.**

### Standalone vs. Embedded Context

**EMBEDDED (DEFAULT for EventAI curriculum):**
- **Location:** `/docs/writing/*/visuals/` (textbook/article content)
- **Usage:** Figure within narrative, referenced as "Figure X.X"
- **Title:** ❌ NO title on infographic (title is in figure caption or surrounding text)
- **Context:** ❌ NO context statements (provided by narrative paragraphs)
- **Design:** Clean, data-focused, minimal explanatory text (labels only)

**STANDALONE:**
- **Location:** `/docs/social/`, `/docs/marketing/`, presentations
- **Usage:** Independent visual (social media post, slide, standalone document)
- **Title:** ✅ Title REQUIRED on infographic (clear, prominent)
- **Context:** ✅ Context statement needed (subtitle, "What this shows:")
- **Design:** Self-contained, can be understood without surrounding text

### Prompt Adjustments by Context

**For EMBEDDED infographics (MOST EventAI CURRICULUM):**
```markdown
❌ OMIT from prompt:
- "Add a clear title to the infographic"
- "Include context statement explaining what this shows"
- "Self-contained design with full context"

✅ INCLUDE in prompt:
- "Data visualization only, no title (title will be in figure caption)"
- "Minimal text: labels and values only, no explanatory paragraphs"
- "Clean, focused design that integrates with surrounding narrative"
- "Assume viewer has read surrounding text for context"
```

**For STANDALONE infographics:**
```markdown
✅ INCLUDE in prompt:
- "Clear, prominent title at top of infographic"
- "Subtitle or context statement explaining significance"
- "Self-contained design understandable without external text"
- "Complete context so viewer needs no supporting material"
```

### Default Assumption

**Unless location/use case clearly indicates standalone:**
→ Default to EMBEDDED context (no title on infographic)

**EventAI curriculum visuals in `/docs/writing/*/visuals/`:**
→ Always EMBEDDED (title in caption, not on visual)

---

## Information Density Tiers

**CRITICAL: EventAI uses three density tiers. Default to Standard tier unless explicitly requested.**

### The Three-Tier Framework

**1. Concise Tier** (Minimal Detail)
- Headlines + 3-5 key stats
- 15-30 second comprehension
- 40%+ white space
- **Use:** Social media, quick pitches
- **Example:** Title + "13x ROI, 8-9 month payback, $6.87M net benefit" (no breakdowns)

**2. Standard Tier** (Balanced Detail) **← DEFAULT**
- Key breakdowns with 3-4 components each
- 30-60 second comprehension
- 30% white space
- **Use:** Conference slides, blog posts, reports (most common)
- **Example:** Title + Investment breakdown (4 components w/ values) + Benefits breakdown (4 components w/ values) + ROI summary

**3. Detailed Tier** (Comprehensive)
- Explanatory annotations, case studies, year-by-year detail
- 2-3 minute comprehension
- 25%+ white space
- **Use:** Textbooks, MBA cases, training materials
- **Example:** Everything in Standard + explanatory text for each component + year-by-year progression + case study callout

**Unless user specifies tier, ALWAYS generate Standard tier prompts.**

---

## Tier-Specific Prompt Generation Guidelines

**CRITICAL: Complexity = Concept Count × Information Hierarchy Depth**

### Concise Tier Prompts (Target: 5-8 concepts, 1-2 levels deep)

**Philosophy:** Few concepts, shallow depth. Labels without extensive explanations.

**Tier Definition:**
- **Concept count:** 5-8 distinct ideas/requirements
- **Hierarchy depth:** 1-2 levels (label, maybe brief descriptor)
- **Example:** "DEFAULT OPT-IN" (label only) = 1 level ✅
- **Too deep:** "DEFAULT OPT-IN - biometric on unless disabled" = 2 levels, pushing Standard ⚠️

**Required Elements:**
1. **Visual description** (1-2 sentences): What to show
2. **Key data points** (5-8 labels): Simple labels, minimal explanatory text
3. **Color palette** (1 line): EventAI hex codes only
4. **White space target** (1 line): "40%+ white space"
5. **Context** (1 line): Standalone vs. embedded, festival context

**Optional Elements (use sparingly):**
- Typography guidance (ONLY if critical, e.g., "16pt minimum body text")
- Accessibility notes (ONLY if non-standard, e.g., color-blind considerations)

**AVOID in Concise prompts:**
- ❌ Multi-level explanations (label + descriptor + detail = too deep)
- ❌ Exhaustive "CRITICAL" sections with 10+ requirements
- ❌ Detailed AVOID lists (trust AI to avoid obviously bad practices)
- ❌ Redundant specifications (e.g., "NOT X" when you've stated "Y")
- ❌ Multiple emoji flags (🚨) - use sparingly if at all
- ❌ Comprehensive style guides embedded in prompt

**Concise Prompt Template:**
```markdown
# [Visual Name]

[1-2 sentence description of what to visualize]

## Data Points
- [Key stat 1]
- [Key stat 2]
- [Key stat 3]

## Style
- Colors: [hex codes]
- White space: 40%+ composition
- Context: [embedded/standalone], [festival context note]
- [Any critical accessibility requirements]

## Structure
[2-3 sentences describing layout/organization]
```

**Example Concise Prompt (2,500 chars):**
```markdown
# Consent Architecture Spectrum

Horizontal spectrum showing coercive to voluntary consent designs for festival biometric systems.

## Data Points
- Coercive (left): Mandatory facial recognition, bundled consent, pre-checked boxes, hidden terms
- Problematic (middle): Default opt-in, soft penalties, confusing interfaces
- Voluntary (right): Clear opt-in, genuine alternatives, granular controls, plain language

## Style
- Colors: Red (#FF6B6B), Orange (#ED8936), Green (#48BB78), White background (#FFFFFF)
- White space: 40%+ composition, generous margins and padding
- Context: Embedded (textbook), festival entry/biometric scenarios
- Accessibility: Large icons (❌ ⚠️ ✅) for each zone, zone labels visible

## Structure
Left-to-right progression with three color zones. Each zone shows 3-4 examples with icons. Bottom includes GDPR Article 7 and 9 principles.
```

---

### Standard Tier Prompts (Expand ONE dimension: breadth OR depth)

**Philosophy:** Go wide OR go deep, but NOT BOTH.

**Tier Definition:**
- **Option A (Breadth):** 10-15 concepts at 1-2 levels deep (many labels, minimal explanations)
- **Option B (Depth):** 5-8 concepts at 3 levels deep (fewer labels, more detail per concept)
- **Complexity:** Medium (expand ONE dimension, not both)

**Required Elements:**
1. Visual description with purpose statement
2. Data structure (sections, categories, breakdowns)
3. Full color palette with application notes
4. Typography guidelines (sizes for key elements)
5. White space and layout requirements
6. Context and accessibility basics
7. AVOID list (top 5-7 critical items only)

**Standard Prompt Complexity:**
- Choose breadth (many concepts, shallow) OR depth (few concepts, detailed)
- 4-6 major sections
- 15-25 total bullet points
- 1-2 CRITICAL flags (not 5+)
- Focused AVOID list (5-7 items, not 15+)

**Example (Breadth):** 12 consent patterns, each with label + brief descriptor (2 levels)
**Example (Depth):** 6 consent patterns, each with label + descriptor + regulatory implications (3 levels)

---

### Detailed Tier Prompts (Both breadth AND depth)

**Philosophy:** Comprehensive specification - many concepts with deep detail.

**Tier Definition:**
- **Concept count:** 20-30+ distinct ideas/requirements
- **Hierarchy depth:** 4+ levels per concept (label + descriptor + detail + examples/reasoning)
- **Complexity:** High (BOTH dimensions expanded)

**Required Elements:**
- All Standard elements plus:
- Detailed examples with line-by-line specs
- Comprehensive style guide sections
- Extensive AVOID list with reasoning (10-15+ items)
- Multiple CRITICAL sections if needed
- Annotation and labeling guidelines
- Print production specifications

**Use Detailed tier ONLY for:**
- Complex multi-layer infographics
- Educational materials requiring precision
- High-stakes publication visuals

---

## Prompt Complexity Metrics

**Target Ranges:**
| Tier | Char Count | Sections | Bullets | CRITICAL Flags | AVOID Items |
|------|-----------|----------|---------|----------------|-------------|
| Concise | 2000-3000 | 3-4 | 8-12 | 0-1 | 0-3 |
| Standard | 4000-6000 | 4-6 | 15-25 | 1-2 | 5-7 |
| Detailed | 7000-10000 | 6-10 | 30-50 | 2-4 | 10-15 |

**Over-Specification Warning:**
If your prompt exceeds these ranges, you're likely over-specifying. Simplify by:
1. Removing redundant requirements
2. Trusting AI capabilities (don't specify obvious things)
3. Consolidating related requirements
4. Moving detailed specs to separate style guide document

---

## Quick Start

```bash
# The /ig-generate skill uses the gemini-generate CLI
gemini-generate \
  --content docs/writing/1-transformation/visuals/barriers/barriers.content.md \
  --prompt docs/writing/1-transformation/visuals/barriers/barriers.prompt.md \
  --output-dir docs/writing/1-transformation/visuals/barriers \
  --name barriers

# Generates 3 variants (typically at Standard tier density):
# - barriers-1.png, barriers-2.png, barriers-3.png
# - barriers-1.webp, barriers-2.webp, barriers-3.webp
# - Ready for /ig-evaluate
```

---

## Setup (One-Time)

### 1. Install CLI Tool

```bash
cd .todd/lib/py/todd-media
pip install -e .
```

This installs:
- `gemini-generate` CLI command (installed to `~/.local/bin/`)
- Playwright dependency for browser automation

### 2. Install Playwright Browsers

```bash
playwright install chromium
```

### 3. Verify Setup

```bash
which gemini-generate  # Should show ~/.local/bin/gemini-generate
gemini-generate --help # Should show usage
```

**Requirements:**
- Chrome browser with signed-in Google account
- Gemini access at gemini.google.com

---

## Usage

### Basic Generation

```bash
gemini-generate \
  --content path/to/content.md \
  --prompt path/to/prompt.md \
  --output-dir path/to/output \
  --name base-name

# Example:
gemini-generate \
  --content docs/writing/1-transformation/visuals/barriers/barriers.content.md \
  --prompt docs/writing/1-transformation/visuals/barriers/barriers.prompt.md \
  --output-dir docs/writing/1-transformation/visuals/barriers \
  --name barriers
```

**Required arguments:**
- `--content`: Path to content markdown file
- `--prompt`: Path to prompt/style instructions file
- `--output-dir`: Directory for generated files
- `--name`: Base filename for outputs

**Generated files:**
- `barriers-1.png` through `barriers-3.png`
- `barriers-1.webp` through `barriers-3.webp`

### Iterative Generation (Multiple Batches)

```bash
# First batch (generates #1-3)
gemini-generate --content barriers.content.md --prompt barriers.prompt.md \
  --output-dir ./barriers --name barriers --batch 1

# Second batch (generates #4-6)
gemini-generate --content barriers.content.md --prompt barriers.prompt.md \
  --output-dir ./barriers --name barriers --batch 2

# Each batch generates 3 new variants
```

### Optional Arguments

```bash
--variants 5          # Generate 5 instead of 3 (default: 3, max: 5)
--aspect-ratio portrait  # portrait, landscape (default), or square
--batch 2             # Batch number for iterative generation
--skip-webp           # Skip WebP conversion, keep PNGs only
--resolution 1080p    # WebP resolution (default: 1080p)
--chrome-profile ProfileName  # Use specific Chrome profile (default: Default)
```

---

## How It Works

### Step 1: Read Files

**CRITICAL DISTINCTION:**

```
barriers/
  ├── barriers.content.md  ← RAW DATA (unfiltered dump of information)
  ├── barriers.prompt.md   ← VISUAL SPECIFICATION (how to design/present it)
  └── (output files will be created here)
```

**content.md** = The raw, unfiltered information to visualize
- Source data, statistics, facts, quotes
- No formatting, no design guidance
- Just the content dump

**Example content.md:**
```markdown
Festival Revenue Breakdown (10,000 attendees):
- Ticket sales: $750,000
- Food & Beverage: $180,000
- Merchandise: $95,000
- Sponsorships: $120,000
- VIP Upgrades: $85,000
Total: $1.23M
```

**prompt.md** = The infographic specification (visual design)
- Layout, style, colors, typography
- How to present the data visually
- NO raw data (that's in content.md)

**Example prompt.md:**
```markdown
# Festival Revenue Breakdown

Create a vertical bar chart showing revenue by category.

Style:
- Colors: Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
- White space: 40%+ composition
- Typography: Clean sans-serif, 16pt minimum
- Context: Festival entry gates, crowd silhouettes

Structure:
Vertical bar chart with 5 categories. Each bar labeled with category name
and dollar amount. Total revenue displayed prominently at top.
```

### Step 2: Browser Automation

```
1. Launches Chrome browser with user profile (already logged in)
2. Opens 3 parallel tabs to gemini.google.com
3. Enables "Create images" mode in each tab
4. Fills combined prompt + content into each tab
5. Submits all prompts simultaneously
6. Waits for image generation (typically 40-50s per variant)
7. Downloads generated PNGs sequentially
```

**Advantages over API:**
- No API key needed (uses your Google account)
- No rate limits or quotas (free unlimited use)
- Same quality as NotebookLM (uses Gemini Imagen)
- Parallel generation speeds up workflow

### Step 3: Convert to WebP

```bash
todd-image-convert barriers-*.png \
  --resolution 1080p \
  --output-format webp \
  --no-replace
```

### Step 4: Ready for Evaluation

```bash
# Run evaluation manually
/ig-evaluate

# Or use the CLI directly
# Generates: barriers.eval.md with scores
```

---

## Iterative Improvement Loop

**Goal:** Achieve 90%+ score through iteration

### Round 1: Initial Generation

```bash
/generate-infographic barriers/

# Output:
# - barriers-1.png (Score: 75%)
# - barriers-2.png (Score: 82%)  ← Best
# - barriers-3.png (Score: 78%)
#
# Best score: 82% < 90% → Continue
```

### Round 2: Learn and Regenerate

```bash
# Script updates prompt.md with learnings from barriers.eval.md
# Generates barriers-4, 5, 6

/generate-infographic barriers/ --batch 2

# Output:
# - barriers-4.png (Score: 85%)
# - barriers-5.png (Score: 91%)  ← Best ✅
# - barriers-6.png (Score: 87%)
#
# Best score: 91% >= 90% → DONE!
```

**Result:** barriers-5.webp is the winner (91%)

---

## No Limits - Free Unlimited Use

**Browser Automation Benefits:**
- ✅ No daily quotas or rate limits
- ✅ No API key required
- ✅ Free unlimited use (via your Google account)
- ✅ No watermarks
- ✅ Same quality as NotebookLM/Gemini web interface

**Realistic Usage:**
- 3 variants per batch (~95 seconds total)
- ~5 batches max to reach 90% (15 images, ~8 minutes)
- Can generate unlimited infographics per day
- Perfect for EventAI curriculum (10-15 topics total)

**Cost:** $0 (completely free via browser automation)

---

## File Naming Convention

**Generated files follow EventAI standard:**

```
barriers/
  ├── barriers.prompt.md
  ├── barriers.content.md
  ├── barriers.instructions.md
  ├── barriers.eval.md              ← Auto-generated
  ├── barriers-1.png                ← Generated (batch 1)
  ├── barriers-2.png
  ├── barriers-3.png
  ├── barriers-1.webp               ← Converted
  ├── barriers-2.webp
  ├── barriers-3.webp
  └── [If needed: barriers-4.png...] ← Batch 2
```

---

## Integration with Workflow

### Current Manual Workflow

1. Write content.md ✍️
2. Write prompt.md ✍️
3. Open NotebookLM 🖱️
4. Upload files 🖱️
5. Click "Infographic" 🖱️
6. Download PNGs 🖱️
7. Rename files ✍️
8. Convert to WebP ⚙️
9. Run /ig-evaluate ⚙️
10. Review and select 👀

**Total time:** ~20-30 minutes

### New Automated Workflow

1. Write content.md ✍️
2. Write prompt.md ✍️
3. Run: `/generate-infographic barriers/` ⚙️ (automated!)
4. Review evaluation, select winner 👀

**Total time:** ~5 minutes (75% faster!)

---

## Error Handling

### "GEMINI_API_KEY not set"

```bash
export GEMINI_API_KEY='your-key-here'
# Or add to ~/.zshrc permanently
```

### "google-generativeai not installed"

```bash
pip install google-generativeai
```

### "File not found: prompt.md"

**Expected names:**
- `{directory-name}.prompt.md` (e.g., `barriers.prompt.md`)
- OR `prompt.md` (fallback)

### "todd-image-convert failed"

**Check environment variables:**
```bash
export PYTHONPATH="/Users/ja/Documents/CodeProjects/todd-lab/.todd/lib/py/todd-media/image-converter:$PYTHONPATH"
export DYLD_LIBRARY_PATH="/opt/homebrew/opt/vips/lib:$DYLD_LIBRARY_PATH"
```

### "Rate limit exceeded"

**Free tier limits:**
- 500/day
- ~10/minute

**Wait and retry:**
- Script auto-pauses 6 seconds between generations
- If hit daily limit, try next day

---

## Advanced Usage

### Generate More Variants

```bash
# Default: 3 variants
/generate-infographic barriers/

# Custom: 5 variants (uses more quota)
/generate-infographic barriers/ --variants 5
```

### Custom Output Name

```bash
/generate-infographic barriers/ --name custom-barriers
# Generates: custom-barriers-1.png, custom-barriers-2.png, etc.
```

### Skip Conversion

```bash
# Generate PNGs only, skip WebP conversion
python3 .claude/scripts/generate-infographic.py barriers/ --skip-convert
```

---

## Quality Targets

**Evaluation Scores:**
- **< 70%:** Poor - major issues
- **70-79%:** Fair - significant improvements needed
- **80-89%:** Good - minor refinements recommended
- **90-94%:** Excellent - publication-ready
- **95-100%:** Outstanding - exceptional quality

**Target:** 90%+ (Excellent)

**Iterative approach ensures we hit target without over-generating**

---

## Browser Automation Benefits

### Why gemini-generate vs Manual NotebookLM/Gemini?

✅ **Fully automated** - zero manual clicking
✅ **Parallel generation** - 3 variants simultaneously (~95s total)
✅ **Iterative** - generate batches until quality target met
✅ **Reproducible** - version-controlled prompts
✅ **Unlimited** - no API quotas or rate limits
✅ **Free** - no API costs, uses your Google account
✅ **Programmatic** - integrate with other tools
✅ **Direct filesystem writes** - no manual downloads

### Gemini Web Interface Equivalence

**Same underlying model:**
- gemini.google.com "Create images" = Gemini Imagen
- Same quality as NotebookLM infographic generation
- Browser automation gives programmatic access
- No watermarks (unlike API free tier)

**Proof:** Identical output quality to manual generation

---

## Troubleshooting

### Browser Doesn't Launch

**Check installation:**
```bash
which gemini-generate  # Should show ~/.local/bin/gemini-generate
playwright install chromium  # Install browsers if missing
```

### Chrome Profile Not Found

**Common issue:** Profile name mismatch

```bash
# Find your Chrome profile names
ls ~/Library/Application\ Support/Google/Chrome/

# Use correct profile name
gemini-generate --chrome-profile "Profile 1" ...
```

### Image Mode Activation Warning

**Message:** `⚠️ Image mode activation failed`

**This is OK!** The warning appears but generation continues. The script attempts to enable image mode but will work even if already enabled.

### Generation Timeout

**If variants timeout after 60s:**
1. Check Gemini is accessible at gemini.google.com
2. Ensure you're logged into Google account
3. Try with fewer variants (--variants 1)
4. Check prompt length (very long prompts may slow generation)

### Download Fails

**If downloads fail:**
- Ensure Downloads folder is accessible
- Close other Chrome windows/tabs that might conflict
- Try again with --batch parameter to resume

---

## Best Practices Integration

### Recommended Pre-Generation Workflow

**For optimal results, load infographic best practices BEFORE creating prompts:**

```bash
# 1. Load Tufte principles and EventAI design standards
/infographics-bestpractices

# 2. Create prompt.md with loaded guidance
"Generate a NotebookLM-style prompt for VIS-X.X following Tufte's principles"

# Claude will automatically include:
# - Data-ink ratio optimization (minimal cruft)
# - EventAI color palette (purple, coral, blue, white)
# - Professional + whimsy balance (80% credible, 20% playful)
# - White space targets (30%+ composition)
# - Typography rules (two-font maximum)
# - Festival context (stages, crowds, wristbands)
# - Accessibility requirements (4.5:1 contrast)

# 3. Write content.md with source data
"Create content.md from VIS-X.X-source.md"

# 4. Run automated generation
/generate-infographic docs/writing/.../visuals/name/

# 5. Evaluation automatically uses loaded best practices
# → Tufte principles applied to scoring
# → EventAI brand compliance checked
# → Accessibility verified
```

### Why Load Best Practices First?

**Without `/infographics-bestpractices`:**
- Generic prompt creation (no Tufte principles)
- Missing EventAI color palette specifications
- No white space or cruft guidance
- Evaluation uses basic criteria only

**With `/infographics-bestpractices`:**
- ✅ Prompts include data-ink ratio requirements
- ✅ EventAI palette hex codes specified
- ✅ Professional + whimsy balance articulated
- ✅ Minimal cruft explicitly stated
- ✅ Evaluation scores against Tufte standards
- ✅ Higher quality output on first generation

### Quality Improvement Example

**Before (without best practices loaded):**
```markdown
# barriers.prompt.md
Create an infographic showing barriers to AI adoption.
Use EventAI colors. Make it professional but fun.
```
→ **Result:** 72% score (vague guidance, generic design)

**After (with /infographics-bestpractices loaded):**
```markdown
# barriers.prompt.md
Create an information-dense infographic showing barriers to AI adoption.

CRITICAL REQUIREMENTS:
- Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
- Data-ink ratio: minimal decoration, every element serves information
- 30%+ white space for breathing room
- Festival context (academic setting with event-relevant metaphors)
- Professional foundation (clean structure) + whimsy accents (unexpected colors)
- No decorative borders, gradients, or ornamental shapes
- 4.5:1 minimum contrast ratio, non-color-dependent information
- Maximum 2 font families: clean sans-serif for body
```
→ **Result:** 89% score (first generation near target!)

---

## Practical Examples: Embedded vs. Standalone

### Example 1: EMBEDDED Infographic (EventAI Curriculum)

**Context:**
- Location: `docs/writing/2-education/visuals/academic-integration/`
- Usage: Figure 2.2 in textbook section on "Academic Integration Barriers"
- Surrounding text provides title, context, and interpretation

**Correct Prompt (NO TITLE):**
```markdown
Create an information-dense data visualization showing academic integration barriers.

VISUAL CONTENT ONLY (NO TITLE - title is in figure caption):
- Three barrier categories with percentage breakdowns
- Each category shows 3-4 specific obstacles with values
- Clean, focused design with labels and data only
- Minimal explanatory text (surrounding narrative provides context)

EventAI Style:
- Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
- 30%+ white space, clean composition
- Festival context where appropriate
- Professional foundation + playful accents
- NO decorative borders or unnecessary embellishments

Technical:
- High-resolution, print-ready quality
- Maximum data-ink ratio (minimal cruft)
```

**Narrative Integration:**
```markdown
## Academic Integration Barriers

Traditional academic institutions face three primary barriers when 
integrating AI into curricula: structural obstacles (42%), cultural 
resistance (35%), and resource constraints (23%).

**Figure 2.2: Academic Integration Barriers**

[INFOGRAPHIC HERE - NO TITLE ON IMAGE]

As Figure 2.2 illustrates, structural obstacles represent the largest 
impediment, comprising curriculum rigidity (18%), accreditation delays 
(12%), and departmental silos (12%)...
```

---

### Example 2: STANDALONE Infographic (Social Media)

**Context:**
- Location: `docs/social/linkedin-posts/`
- Usage: LinkedIn post graphic (standalone, no surrounding text)
- Must be self-contained and understandable alone

**Correct Prompt (WITH TITLE):**
```markdown
Create a self-contained infographic titled "Academic Integration Barriers" 
showing why universities struggle to adopt AI in curricula.

MUST INCLUDE:
- Clear, prominent title: "Academic Integration Barriers"
- Subtitle: "Why Universities Struggle with AI Curriculum Integration"
- Context statement: "42% cite structural obstacles as primary barrier"

Three barrier categories with breakdowns:
- Structural Obstacles (42%)
- Cultural Resistance (35%)
- Resource Constraints (23%)

EventAI Style:
- Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
- Professional + whimsy balance
- Festival context (academic setting with event metaphors)
- Self-contained design (understandable without article)

Include:
- Source citation at bottom
- EventAI branding (subtle)
```

**Social Media Post:**
```markdown
[INFOGRAPHIC WITH TITLE/SUBTITLE/CONTEXT]

Universities face three primary barriers to AI curriculum integration.

Structural obstacles (42%) lead the way, followed by cultural resistance 
(35%) and resource constraints (23%).

Learn more: [link to full article]
```

---

### Example 3: When to Regenerate Due to Context Mismatch

**Problem:** Generated embedded infographic incorrectly includes title

```markdown
[Narrative text...]

**Figure 2.2: Academic Integration Barriers**

[INFOGRAPHIC WITH "ACADEMIC INTEGRATION BARRIERS" TITLE]  ← WRONG!

[More narrative...]
```

**Issue:** Title appears twice (redundant), breaks narrative flow

**Solution:** Regenerate with corrected prompt (NO TITLE):
```bash
# Update prompt.md to explicitly state NO TITLE
# Add: "Data visualization only, no title (title in caption)"
# Regenerate using gemini-generate
```

---

## See Also

- **`/infographics-bestpractices`** - Load Tufte principles and EventAI design standards (USE BEFORE /ig-generate!)
- **`/ig-evaluate`** - Evaluation workflow (automatically uses loaded best practices)
- **`gemini-generate`** - The CLI tool (installed from `.todd/lib/py/todd-media/setup.py`)
- **`todd-image-convert`** - Image conversion to WebP
- **EventAI Visual Identity Guide** - Brand color palette and typography
- **Gemini Web Interface:** https://gemini.google.com

---

*Skill created: December 29, 2025*
*Powered by: Gemini Imagen via browser automation (Playwright)*
*Method: Parallel tab generation at gemini.google.com*
*CLI: gemini-generate (installed from .todd/lib/py/todd-media)*
*Integration: EventAI + Lemmy Content Generation System*
