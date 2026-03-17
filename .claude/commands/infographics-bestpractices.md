# Infographics Best Practices

## Purpose

Load comprehensive best practices for creating effective, information-dense, print-ready infographics into working memory to guide visual design, data visualization, and quality evaluation.

## Core Approach

This command doesn't perform actions—it loads authoritative, research-based visual design guidance into your working context. Use it when creating, evaluating, or refining infographics to ensure:
1. **Graphical Excellence:** Tufte's principles for information-dense yet clear visualization
2. **Professional + Whimsy Balance:** EventAI's distinctive credible-yet-memorable style
3. **Print Readiness:** 300-600 DPI, proper color profiles, technical specifications
4. **Minimal Cruft:** Every pixel serves the data, no unnecessary decoration
5. **Accessibility:** WCAG-compliant contrast, non-color-dependent information, inclusive design

---

## When to Use This Command

### Primary Use Cases

**✅ INVOKE `/infographics-bestpractices` when:**
- Creating new infographic prompts for NotebookLM or other generators
- Evaluating generated infographics for quality and compliance
- Refining infographic designs before publication
- Designing data visualizations for textbook integration
- Establishing visual identity and style guidelines
- Reviewing infographics for EventAI brand consistency
- Training AI generators with specific design requirements

**❌ DON'T invoke for:**
- Quick logo design or branding work (different principles)
- Photography or illustration (different visual domains)
- Web UI/UX design (different interaction paradigms)
- Presentation slides (different density and pacing)

---

## What Gets Loaded

When you invoke `/infographics-bestpractices`, the following knowledge areas become available in working memory:

### 0. Information Density Tiers (EventAI Framework)

**CRITICAL: EventAI uses three distinct density tiers. Always default to Standard tier unless explicitly requested otherwise.**

#### The Three-Tier Framework

**1. Concise Tier (Minimal Detail)**
- **Purpose:** Quick glance comprehension, social media, elevator pitches
- **Information density:** Headline stats only, minimal explanatory text
- **Comprehension time:** 15-30 seconds
- **White space target:** 40%+ composition
- **Use cases:**
  - Social media posts (LinkedIn, Twitter)
  - Conference slide backgrounds
  - Quick reference cards
  - Elevator pitch visuals
  - Executive summary graphics

**Example characteristics:**
- Title + 3-5 key statistics (large, prominent)
- Minimal labels (just enough to understand)
- No detailed breakdowns or sub-categories
- Maximum visual clarity, minimum text
- "At-a-glance" understanding

**2. Standard Tier (Balanced Detail) ← DEFAULT**
- **Purpose:** Conference presentations, blog posts, reports, general publication
- **Information density:** Key breakdowns visible, balanced detail without overwhelming
- **Comprehension time:** 30-60 seconds
- **White space target:** 30% composition
- **Use cases:**
  - Conference presentations (InfoComm, PLASA, industry events)
  - Blog posts and articles (EventAI Curriculum, Medium)
  - Executive briefings (C-suite, 2-minute pitch)
  - Sales decks (prospect meetings)
  - General publication (default choice)

**Example characteristics:**
- Title + key statistics + breakdown categories
- Each category shows 3-4 data points or components
- Brief labels/descriptions (not paragraphs)
- Stacked bars/charts show components with values
- Readable without requiring close study
- "Quick comprehension with enough detail to be useful"

**3. Detailed Tier (Comprehensive Information)**
- **Purpose:** Textbooks, MBA case studies, deep educational contexts, analyst reports
- **Information density:** Comprehensive annotations, explanatory text, maximum educational value
- **Comprehension time:** 2-3 minutes (requires close reading)
- **White space target:** 25% composition (acceptable)
- **Use cases:**
  - Textbook companion visuals (EventAI Curriculum deep-dive chapters)
  - MBA case study materials (business school coursework)
  - Detailed analyst reports (Forrester-style white papers)
  - Training materials (internal onboarding, sales training)
  - Research presentations (academic conferences)

**Example characteristics:**
- Title + statistics + breakdowns + explanatory annotations
- Each component has 2-3 sentences of explanation
- Year-by-year progressions or detailed timelines
- Case study callouts with multiple data points
- Comprehensive legends and footnotes
- "Rewards close study, provides deep understanding"

#### Default Behavior: Standard Tier

**Unless explicitly requested, ALWAYS target Standard tier:**
- ✅ "Generate an infographic for VIS-X.X" → Standard tier
- ✅ "Create a cost-benefit visualization" → Standard tier
- ✅ "Evaluate this infographic" → Assume Standard tier unless context suggests otherwise

**Only use other tiers when:**
- 🔷 User explicitly requests "Concise" or "minimal detail" → Concise tier
- 📘 User explicitly requests "Detailed" or "educational/textbook version" → Detailed tier
- 📋 Context clearly indicates tier (e.g., "for social media" → Concise, "for MBA course" → Detailed)

#### Evaluation Against Density Tiers

**When evaluating infographics, first identify the intended tier:**

**Concise tier evaluation:**
- ✅ Success: Headline stats prominent, minimal text, 40%+ white space, instant comprehension
- ❌ Failure: Too much detail (becomes Standard), insufficient white space, requires reading

**Standard tier evaluation:**
- ✅ Success: Balanced detail, key breakdowns visible, 30% white space, 30-60 second comprehension
- ❌ Failure: Too sparse (should be Concise) OR too dense (becomes Detailed)

**Detailed tier evaluation:**
- ✅ Success: Comprehensive annotations, educational value, rewards close study
- ❌ Failure: Cognitive overload (too dense even for Detailed), accessibility issues from small text

**CRITICAL:** Do not penalize an infographic for being "too simple" if it's appropriately Concise, or "too detailed" if it's appropriately Detailed. Match evaluation criteria to intended tier.

---

### 0.5. Context-Aware Design: Standalone vs. Embedded Infographics

**CRITICAL: Infographics serve different purposes depending on their presentation context. Evaluation criteria must match intended use.**

#### The Two Context Types

**1. Standalone Infographics**
- **Use cases:** Social media posts, conference slides, reports (as separate pages), marketing materials, email newsletters
- **Characteristics:** Self-contained, can be understood without surrounding text
- **Required elements:**
  - ✅ Title (clear, prominent)
  - ✅ Subtitle or context statement (what is this about?)
  - ✅ Complete labels and legends
  - ✅ Source citations
  - ✅ All context necessary to understand the data
- **Rationale:** Viewer may encounter infographic without any supporting text

**2. Embedded Infographics (In-Narrative Media)**
- **Use cases:** Textbook visuals, article illustrations, blog post graphics, academic papers, curriculum materials
- **Characteristics:** Integrated within text flow, referenced by surrounding paragraphs
- **Required elements:**
  - ❌ **NO title on the infographic itself** (title is in the narrative text or figure caption)
  - ❌ **NO subtitle/context statement** (context provided by surrounding paragraphs)
  - ✅ Labels and data only (what's shown, not why we're showing it)
  - ✅ Legend if needed (for understanding the visual)
  - ✅ Source citations (credibility, but can be in caption)
- **Rationale:** Surrounding text provides title, context, and interpretation

#### Why This Distinction Matters

**Problem: Redundancy and flow disruption**
```
[Narrative text discussing AI adoption trends...]

**Figure 2.1: AI Adoption Timeline 2025-2035**  ← Title in text

[Infographic with "AI ADOPTION TIMELINE 2025-2035" as title]  ← REDUNDANT!

[Narrative continues discussing what the timeline shows...]
```

**Solution: Context-appropriate design**
```
[Narrative text discussing AI adoption trends...]

**Figure 2.1: AI Adoption Timeline 2025-2035**  ← Title in text

[Infographic with data visualization only, no title]  ← Clean, integrated

[Narrative continues interpreting the data...]
```

#### Design Principles by Context

**Standalone Infographics:**
- Design as if infographic could be shared without article
- Include title as visual element (part of composition hierarchy)
- Add context statements ("What this shows:", "Key takeaway:")
- Complete legends, not relying on external explanation
- Footer with source, date, creator

**Embedded Infographics:**
- Design as visual data layer within text
- NO title on the infographic (would duplicate figure caption)
- NO explanatory text beyond labels (interpretation is in narrative)
- Minimal text overall (let the narrative do the explaining)
- Focus on visual clarity of data relationships
- Think "illustration" not "standalone document"

#### Evaluation Criteria Adjustments

**When evaluating standalone infographics:**
- ✅ Expect clear title as part of design
- ✅ Expect context statements or introductory text
- ✅ Penalize if missing self-contained context
- ✅ Full composition hierarchy (title → data → details → source)

**When evaluating embedded infographics:**
- ❌ **DO NOT penalize** for missing title (should not have one!)
- ❌ **DO NOT penalize** for missing context statements (provided by text)
- ✅ Expect minimal explanatory text (just labels and data)
- ✅ Simplified composition hierarchy (data → supporting labels → source)
- ✅ Penalize if title IS present (redundant with figure caption)

#### How to Identify Context

**Embedded infographic indicators:**
- Located within a document with surrounding text (e.g., `/docs/writing/*/drafts/*.md`)
- Referenced as "Figure X.X" or similar in narrative
- Part of a numbered sequence (Figure 2.1, 2.2, etc.)
- Stored in `/visuals/` directory alongside narrative content
- Prompt file indicates "for textbook" or "curriculum integration"

**Standalone infographic indicators:**
- Designed for social media, presentations, marketing
- Prompt includes "self-contained" or "standalone"
- Includes "share on LinkedIn/Twitter" in use case
- Stored in `/social/` or `/marketing/` directories
- Would make sense viewed in isolation (e.g., Pinterest pin)

#### EventAI Curriculum Specifics

**For EventAI textbook/curriculum visuals:**
- **Default context: Embedded** (unless explicitly noted otherwise)
- Infographics support narrative sections
- Figure captions provide titles and context
- Visuals illustrate concepts discussed in text
- Clean, data-focused design without redundant titling

**Example evaluation adjustment:**
```
Infographic: academic-integration-3.webp
Location: docs/writing/2-education/visuals/academic-integration/
Context: Embedded (textbook curriculum)

✅ Correct: No title on infographic (title in figure caption)
✅ Correct: Minimal explanatory text (narrative provides context)
✅ Correct: Focus on data visualization clarity

❌ Wrong: "Missing title" penalty (title should NOT be present!)
❌ Wrong: "Needs more context" (context is in surrounding text)
```

#### Default Assumptions

**Unless context clearly indicates otherwise:**
- EventAI curriculum visuals → **Embedded**
- Social media posts → **Standalone**
- Conference slides → **Standalone**
- Blog post graphics → **Embedded**
- Marketing materials → **Standalone**
- Academic paper figures → **Embedded**

**When in doubt:** Check the prompt file (`VIS-X.X-GENERATE-INSTRUCTIONS.md`) or ask about intended use.

---

### 1. Edward Tufte's Principles for Dense Information Display

**Tufte is the authority on data-rich visualization. His principles are essential for information-dense yet clear infographics.**

#### Graphical Excellence
- Well-designed presentation of interesting data
- Substance over style
- Many numbers in a small space
- Makes large datasets coherent
- Encourages eye to compare data
- Reveals data at multiple levels (overview → detail)
- **Tufte's ideal:** Graphic that rewards attention—viewer can scan quickly, then keep learning

#### Graphical Integrity
- Proportions in graphic = proportions in data
- Clear labeling (axes, scales, units)
- Show data variation, not design variation
- Number of information-carrying dimensions ≤ number of data dimensions
- Context matters (show baselines, comparisons)
- **Avoid:** Misleading scales, truncated axes, cherry-picked ranges

#### Data-Ink Ratio
**Formula:** Data-Ink Ratio = (Ink used for data) / (Total ink used)

**Maximize data ink, minimize everything else:**
- Remove unnecessary gridlines
- Eliminate decorative borders
- Simplify axis labels
- Remove redundant legends
- Every pixel should serve the data

**Caution:** Don't oversimplify to confusion—context elements (axes, labels) are necessary

#### Data Density
- As volume increases, data measures must shrink
- More data points → smaller dots (scatter plots)
- Busy time-series → thinner lines
- High-density displays require special care
- **High-performance graphics:** Maximum information in minimum space while remaining clear

#### Analytical Design
Charts should support serious analysis:
- **Comparisons:** Show relationships between variables
- **Multivariate thinking:** Display multiple dimensions simultaneously
- **Layered detail:** Overview + drill-down capability
- **Integration:** Text, numbers, and visuals work together

### 2. Print-Ready Technical Specifications

**Resolution requirements:**
- **Print (Paper/Posters):** 300-600 DPI minimum
  - High-quality prints: 600 DPI
  - Standard prints: 300 DPI
  - Export as high-resolution PDF or TIFF

**File format guidelines:**
- **Print:** PDF/X-4 or TIFF with CMYK color profile
- **Web:** PNG or WebP formats (sRGB color profile)
- **Both:** Export separate versions optimized for each medium

### 3. Core Design Principles

#### Typography (Two-Font Rule)
- **Font 1:** Title (can be distinctive, eye-catching)
- **Font 2:** Headings and body text (clean, simple, readable)

**✅ Do:**
- Keep body font simple and clean
- Ensure readability at small sizes
- Test legibility at print size

**❌ Don't:**
- Use decorative/script fonts for small text
- Mix more than 2-3 font families
- Use hard-to-read fonts for data/numbers

#### Layout and Flow
**Natural reading pattern:**
- Left-to-right, top-to-bottom (English)
- Information hierarchy matches reading flow
- Visual path guides the eye through content

**Composition hierarchy:**
1. Title/Main Message (immediate attention)
2. Key Statistics/Data (supporting evidence)
3. Detailed Information (for deep readers)
4. Citations/Sources (credibility footer)

#### White Space (Negative Space)
**Purpose:**
- Draw attention to key points
- Prevent visual clutter
- Create breathing room
- Establish hierarchy

**Critical principle:** White space is not wasted space—it's an active design element
- Use to separate distinct sections
- Don't fill every gap with content
- Strategic emptiness = visual power
- "Less is more" in information design

#### Focused and Purposeful Content
**Start with clear message:**
- What is the ONE main point?
- What should people remember?
- Is every element supporting that message?

**Content strategy:**
- Remove anything that doesn't serve core message
- Prioritize clarity over comprehensiveness
- One infographic = One focused topic
- Save secondary topics for separate visuals

### 4. Professional + Whimsy Balance (EventAI-Specific)

**How to achieve professional credibility + whimsical approachability:**

#### Whimsical Elements (Use Sparingly)
- Odd shapes and unexpected compositions
- Daring color schemes (bright accents)
- Playful illustrations or icons
- Personality-driven metaphors
- Soft, rounded typography for titles

#### Professional Elements (Foundation)
- Clean, structured layouts
- Sophisticated color palette (not childish)
- Readable, sans-serif body fonts
- Clear data visualization
- Proper alignment and spacing

#### The Balance Formula
- **Foundation:** Professional structure (80%)
- **Accent:** Whimsical personality touches (20%)
- **Result:** Memorable yet credible

#### Visual Approach Patterns

**Pattern 1: Soft Pastels + Crisp Lines**
- Playful pastel color scheme (approachability)
- Clean typography and uncluttered composition (professionalism)
- Result: Expressive character + aspiration

**Pattern 2: Bold Color + Minimalist Layout**
- Vibrant, daring colors (whimsy)
- Minimal elements, lots of white space (professionalism)
- Result: Eye-catching yet sophisticated

**Pattern 3: Quirky Icons + Serious Data**
- Illustrated, personality-rich icons (whimsy)
- Precise data visualization (professionalism)
- Result: Engaging yet trustworthy

#### Critical Success Factors
1. **Restraint:** Don't overdo whimsical elements (becomes unprofessional)
2. **Strategic placement:** Whimsy in accents, not foundational structure
3. **Clarity first:** Never sacrifice readability for personality
4. **Appropriate balance:** EventAI = professional audience, so lean toward credibility with whimsy accents

### 5. Minimal Cruft Principle (EventAI-Specific)

**What is "cruft"?**
- Decorative borders that serve no purpose
- Unnecessary gradients or shadows
- Generic stock imagery
- Excessive ornamentation
- Visual noise

**How to avoid:**
1. **Question every element:** "Does this help understanding?"
2. **Remove decoration:** If it's purely decorative, cut it
3. **Functional beauty:** Let data and structure create the aesthetic
4. **White space is good:** Don't fear emptiness

**Connection to Tufte:** Aligns perfectly with data-ink ratio principle

### 6. 2025 Design Trends

#### Dynamic Compositions
Traditional grid layouts → **Playful, dynamic compositions**
- Not rigidly structured
- Creative, flowing layouts
- Guide the eye naturally
- Organic, visual storytelling flows

#### Accessibility-First Design
**Critical trend in 2025:**
- Vision impairments (color blindness, low vision)
- Screen reader compatibility
- Alternative text descriptions
- High contrast ratios (WCAG standards)
- Non-color-dependent information (don't rely solely on color)

#### Interactive and Immersive Experiences
Infographics evolving beyond static visuals:
- Clickable elements
- Embedded videos
- Dynamic data visualizations
- Responsive to user interaction

**Note for EventAI:** While NotebookLM generates static infographics, consider this trend for digital versions

### 7. Ten Fundamental Visualization Principles

From scientific visualization research (applicable to all infographics):

1. **Know Your Audience:** Design for their expertise level
2. **Choose Appropriate Chart Types:** Match visualization to data type
3. **Simplify:** Remove non-essential elements
4. **Use Color Purposefully:** Color should convey information
5. **Label Clearly:** Everything should be immediately understandable
6. **Provide Context:** Show comparisons, baselines, benchmarks
7. **Tell a Story:** Visual should have beginning, middle, end
8. **Ensure Accessibility:** Work for color-blind, low-vision users
9. **Maintain Consistency:** Similar elements styled similarly
10. **Test and Iterate:** Show to real users, refine based on feedback

### 8. EventAI-Specific Applications

#### Distinctive Identity
**How to make EventAI infographics recognizable:**

1. **Consistent Color Palette** (defined in EventAI Visual Identity Guide):
   - Deep Festival Purple (#6B46C1)
   - Electric Coral (#FF6B6B)
   - Sky Blue (#4299E1)
   - Midnight Slate (#2D3748)
   - Warm Sunlight (#F6AD55)
   - Pure White (#FFFFFF)

2. **Signature Visual Elements:**
   - Consistent icon style
   - Repeating compositional patterns
   - Distinctive typography pairing

3. **Festival Context:**
   - Use festival-relevant metaphors (stages, crowds, wristbands, etc.)
   - Incorporate event industry visual language
   - Avoid generic tech/AI imagery
   - No business suits/handshakes/corporate clichés
   - No robot overlords/circuit brains/AI stereotypes

4. **Memorable Layout Patterns:**
   - Develop 3-4 layout templates
   - Consistent header/footer styling
   - Repeating structural elements

#### Dense Visual Integration with Text
**Strategy for EventAI Curriculum:**

Text Section (500-1000 words)
↓
**Infographic:** Visual decision framework
↓
Text Section: Case studies
↓
**Data Visualization:** Statistics
↓
Text Section: Recommendations
↓
**Infographic:** Quick-reference checklist

**Principle:** Every 500-1000 words, provide a visual break that adds information, not just decoration

**Visual types to rotate:**
- Process flowcharts
- Comparison tables (visual)
- Data charts/graphs
- Concept maps
- Timeline infographics
- Decision trees
- Quick-reference checklists

### 9. Quality Control Checklist

**After generating infographic:**

✅ **Clarity:**
- [ ] Main message immediately clear
- [ ] Text readable at intended size
- [ ] Data accurately represented
- [ ] No hallucinated statistics

✅ **Design Quality:**
- [ ] Aligns with EventAI style (professional + whimsy)
- [ ] Minimal cruft (no unnecessary decoration)
- [ ] Appropriate white space (30%+ composition)
- [ ] Visual hierarchy guides eye
- [ ] Typography follows two-font rule

✅ **Tufte Principles:**
- [ ] High data-ink ratio
- [ ] Graphical integrity (proportions match data)
- [ ] Graphical excellence (rewards attention)
- [ ] Analytical design (supports comparisons)

✅ **Technical:**
- [ ] Sufficient resolution for print (300-600 DPI if applicable)
- [ ] Colors work in both print (CMYK) and digital (RGB)
- [ ] Accessible (4.5:1 contrast, non-color-dependent info)
- [ ] Proper file format (PDF/TIFF for print, PNG/WebP for web)

✅ **Factual:**
- [ ] Data matches sources (AI can hallucinate)
- [ ] Labels accurate
- [ ] Statistics verifiable
- [ ] Source citations included

✅ **EventAI Brand:**
- [ ] Festival context (not generic business/tech imagery)
- [ ] Color palette adherence (purple, coral, blue, white)
- [ ] Professional + whimsy balance appropriate
- [ ] Distinctive EventAI identity recognizable

---

## How to Use After Invocation

Once `/infographics-bestpractices` loads the research into working memory, you can:

### Immediate Application

**For creating new infographic prompts:**
```
"Generate a NotebookLM prompt for an AI adoption timeline infographic following Tufte's principles."

→ Claude will include: High data density, minimal cruft, data-ink ratio optimization,
   professional + whimsy balance, EventAI color palette, festival context
```

**For evaluating generated infographics:**
```
"Evaluate this infographic against Tufte's principles and EventAI brand standards."

→ Claude will check: Graphical excellence, graphical integrity, data-ink ratio,
   white space (30%+), typography (two-font rule), color palette, minimal cruft
```

**For refining infographic designs:**
```
"This infographic feels cluttered. How can I improve it?"

→ Claude will apply: Data-ink ratio (remove decoration), white space strategy,
   visual hierarchy, minimal cruft principle
```

**For integration with /ig-generate:**
```
/infographics-bestpractices
/ig-generate VIS-2.1

→ Generation process automatically applies loaded best practices to prompt creation
→ Includes EventAI color palette, festival context, Tufte principles, minimal cruft
```

### Persistent Context

The loaded best practices remain in working memory throughout the conversation:

```
User: "Should I use a decorative border?"
→ Claude references minimal cruft principle: "No—decorative borders are cruft"

User: "How much white space should I target?"
→ Claude applies loaded guideline: "30%+ of composition for breathing room"

User: "Can I use color alone to show categories?"
→ Claude applies accessibility principle: "No—always provide additional cues (icons, labels)"
```

---

## Integration with /ig-generate Command

**When both commands are active:**

### Enhanced Prompt Generation
`/infographics-bestpractices` automatically enhances `/ig-generate` prompts with:

1. **Tufte Principles Embedded:**
   - "Data-focused, minimal decoration"
   - "High data density with clarity"
   - "No unnecessary borders or embellishments"
   - "Information-rich design"

2. **EventAI Brand Requirements:**
   - Exact hex codes for color palette
   - Festival-relevant visual metaphors specified
   - Professional + whimsy balance articulated
   - Minimal cruft explicitly stated

3. **Technical Specifications:**
   - "High-resolution, print-ready quality"
   - Resolution requirements stated
   - File format preferences noted

4. **Accessibility Criteria:**
   - "4.5:1 minimum contrast ratio"
   - "Information not conveyed by color alone"
   - "Icon + text + color for indicators"

### Quality Evaluation Standards
When `/ig-evaluate` uses loaded best practices:

**Scoring criteria automatically includes:**
- Tufte's graphical excellence (10 points)
- Data-ink ratio (10 points)
- Graphical integrity (10 points)
- White space (30%+ target, 10 points)
- Professional + whimsy balance (10 points)
- Minimal cruft (10 points)
- EventAI color palette adherence (10 points)
- Festival context integration (10 points)
- Typography (two-font rule, 10 points)
- Accessibility compliance (10 points)

**Total: 100 points**

### Workflow Integration Example

```bash
# 1. Load best practices
/infographics-bestpractices

# 2. Generate infographic with enhanced prompts
/ig-generate VIS-2.1
# → Prompt automatically includes Tufte principles, EventAI palette, minimal cruft

# 3. Evaluate result against loaded standards
/ig-evaluate docs/writing/2-education/visuals/academic-integration/*.webp
# → Evaluation automatically scores against Tufte, EventAI brand, accessibility

# 4. Refine based on evaluation
"The evaluation found insufficient white space (20%). How should I revise the prompt?"
# → Claude references loaded 30%+ target, suggests specific prompt modifications
```

---

## Source Material

**Loaded from:** `docs/lemmy/research/infographics-best-practices.md`

**Research synthesis includes:**
- **Tufte's principles:** *The Visual Display of Quantitative Information*, graphical excellence/integrity, data-ink ratio
- **Design fundamentals:** Venngage 2025 trends, Visme best practices, typography rules, layout principles
- **Professional resources:** EBSCO visualization guides, Hull University design principles, RMCAD effective infographics
- **Scientific visualization:** PMC principles of data visualization, Appnovation 12 principles
- **EventAI-specific:** Visual identity guide, festival context requirements, professional + whimsy balance

**Research completed:** December 28, 2025 | Task: eventai-6x7 | System: Lemmy

---

## Usage Examples

### Example 1: Creating Infographic Prompt for NotebookLM

```bash
# Load best practices
/infographics-bestpractices

# Request prompt generation
User: "Generate a NotebookLM prompt for VIS-2.1 (academic integration barriers infographic)."

# Claude applies loaded principles:
# - Tufte: "High data-ink ratio, minimal decoration, graphical integrity"
# - EventAI palette: "Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)"
# - Festival context: "Academic setting with festival-relevant metaphors (not generic classroom imagery)"
# - Professional + whimsy: "Clean structure with playful accents"
# - White space: "30%+ composition for breathing room"
# - Typography: "Maximum 2 font families, clean sans-serif for body"
# - Minimal cruft: "No decorative borders, every element serves information"
# - Accessibility: "4.5:1 minimum contrast, non-color-dependent information"
```

### Example 2: Evaluating Infographic Quality

```bash
/infographics-bestpractices

User: "Evaluate this AI adoption timeline infographic."

# Claude checks against loaded standards:
# - Graphical excellence (0-10): Does it reward attention? Multiple levels of detail?
# - Graphical integrity (0-10): Do proportions match data? Clear labeling?
# - Data-ink ratio (0-10): How much ink is cruft vs data?
# - White space (0-10): Is there 30%+ breathing room?
# - Professional + whimsy (0-10): Credible foundation with personality accents?
# - EventAI palette (0-10): Purple, coral, blue, white adherence?
# - Festival context (0-10): Event-relevant metaphors, not generic tech?
# - Typography (0-10): Two-font rule, readable at print size?
# - Minimal cruft (0-10): Decorative borders? Unnecessary shadows?
# - Accessibility (0-10): 4.5:1 contrast? Non-color-dependent info?

# Generates comprehensive scorecard with specific recommendations
```

### Example 3: Refining Cluttered Design

```bash
/infographics-bestpractices

User: "This infographic feels too busy. How can I simplify it?"

# Claude applies Tufte's data-ink ratio:
# 1. Identify cruft: Decorative borders, unnecessary gridlines, redundant legends
# 2. Maximize data ink: Keep only elements that convey information
# 3. Increase white space: Target 30%+ composition
# 4. Visual hierarchy: Ensure title → data → details flow is clear
# 5. Remove ornamentation: No shadows, gradients, or decorative shapes
# 6. Simplify typography: Reduce to 2 fonts maximum

# Specific recommendations:
# "Remove the decorative border (cruft)"
# "Eliminate background gradient (visual noise)"
# "Increase margins to 48px minimum (white space)"
# "Simplify axis labels (data-ink ratio)"
# "Use direct labels instead of legend (clarity)"
```

### Example 4: Establishing EventAI Visual Identity

```bash
/infographics-bestpractices

User: "How do I make infographics look distinctively EventAI?"

# Claude references loaded EventAI-specific guidance:
# 1. Color palette: Deep purple (#6B46C1), electric coral (#FF6B6B), sky blue (#4299E1)
# 2. Festival context: Stages, crowds, wristbands (not suits, offices, handshakes)
# 3. Professional + whimsy: 80% credible structure, 20% playful accents
# 4. Typography: Modern sans-serif (Inter/similar) with occasional rounded title font
# 5. Layout patterns: Dynamic compositions (not rigid grids)
# 6. Minimal cruft: Functional beauty, no decoration
# 7. Signature elements: Consistent icon style, repeating compositional patterns

# Recommends:
# "Use EventAI palette exclusively (no off-brand colors)"
# "Incorporate festival metaphors (crowd silhouettes, stage icons)"
# "Balance clean layouts with unexpected color pops"
# "Establish 3-4 reusable layout templates for consistency"
```

---

## What This Command Doesn't Do

**This command is a reference loader, not an action executor:**

❌ **Doesn't automatically generate infographics**
- You must use `/ig-generate` or request specific prompt creation

❌ **Doesn't automatically edit image files**
- You must request design revisions or regeneration

❌ **Doesn't score infographics unprompted**
- You must invoke `/ig-evaluate` or request assessment

**Instead, it:**
✅ Loads authoritative best practices into Claude's working memory
✅ Makes Tufte's principles and design research immediately available
✅ Enables contextually-aware responses about infographic design
✅ Provides a shared framework for visual quality discussions
✅ Enhances `/ig-generate` prompts with research-based criteria
✅ Enriches `/ig-evaluate` scoring with Tufte standards

---

## Integration with EventAI Workflow

### Typical Workflow

```bash
# 1. Load best practices before infographic work
/infographics-bestpractices

# 2. Generate infographic with enhanced prompts
/ig-generate VIS-2.1
# → Automatically includes Tufte principles, EventAI palette, minimal cruft

# 3. NotebookLM generates 3 variants (manual step)
# → Download PNGs from NotebookLM

# 4. Convert to webp (required)
todd-image-convert docs/writing/2-education/visuals/academic-integration/*.png --resolution 1080p --output-format webp

# 5. Evaluate all variants
/ig-evaluate docs/writing/2-education/visuals/academic-integration/*.webp
# → Scores against Tufte, EventAI brand, accessibility (loaded standards)

# 6. Review evaluation report
# → Identifies winner, flags issues, prioritizes improvements

# 7. Regenerate if needed (score < 80%)
# → Refine prompt based on evaluation findings
# → Update VIS-X.X-GENERATE-INSTRUCTIONS.md with learnings

# 8. Publish winner (score 80%+)
# → Mark VIS-X.X complete in VISUAL-CONTENT-PLAN.md
```

### Beads Integration

```bash
# Create infographic task
todd-carl create --title="Generate and evaluate VIS-2.1 infographics" --type=task --priority=2

# Load best practices (once per session)
/infographics-bestpractices

# Generate and evaluate
/ig-generate VIS-2.1
# (manual NotebookLM step, convert to webp)
/ig-evaluate docs/writing/2-education/visuals/academic-integration/*.webp

# Document results
todd-carl update beads-xxx --status=completed --reason="Variant #3 selected, scored 87/100, meets Tufte standards and EventAI brand"
```

---

## Quality Standards

**Best practices are SUCCESSFULLY LOADED when:**

✅ Claude can reference Tufte's principles without re-reading research
✅ Responses demonstrate awareness of data-ink ratio (maximize data, minimize decoration)
✅ Recommendations align with EventAI palette (purple, coral, blue, white)
✅ White space targets are specific (30%+ composition)
✅ Professional + whimsy balance is articulated (80% credible, 20% playful)
✅ Minimal cruft principle is applied (question every decorative element)
✅ Accessibility checks cover contrast (4.5:1), color-independence, labeling

**Best practices are NOT loaded if:**

❌ Claude recommends decorative borders without questioning necessity (cruft)
❌ White space suggestions are vague ("add some space")
❌ Color palette deviations accepted without EventAI brand justification
❌ Tufte's principles not referenced in design evaluations
❌ Accessibility overlooked (color-only information, low contrast)
❌ Professional + whimsy balance undefined or inconsistently applied

---

## Related Commands

- **`/ig-generate`** - Generate infographic prompts for NotebookLM using loaded best practices
- **`/ig-evaluate`** - Evaluate infographics against Tufte principles and EventAI standards
- **`/textbook-bestpractices`** - Load academic textbook writing guidance
- **`/validate`** - Verify factual accuracy of infographic data

**Complementary workflow:**
```bash
/infographics-bestpractices    # Load design guidance
/ig-generate VIS-X.X           # Create prompt with Tufte principles
# → (NotebookLM generation, convert to webp)
/ig-evaluate *.webp            # Score against loaded standards
/validate                      # Verify data accuracy (if needed)
```

---

## Notes on Context and Memory

**Context persistence:**
- Loaded best practices remain available throughout current conversation
- No need to re-invoke unless you start new conversation
- Claude will reference loaded guidelines automatically in subsequent responses
- Works seamlessly with `/ig-generate` and `/ig-evaluate`

**Memory management:**
- This command loads ~4,000 words of research into working memory
- Recommended for focused infographic design sessions
- For brief questions, you may not need full context load

**When to re-invoke:**
- Start of new infographic work session
- After conversation compaction/summarization
- Before running `/ig-generate` (ensures prompts include best practices)
- Before running `/ig-evaluate` (ensures comprehensive scoring criteria)

---

## Final Philosophy

**Tufte's Wisdom:** "Graphical excellence is that which gives to the viewer the greatest number of ideas in the shortest time with the least ink in the smallest space."

**EventAI Application:** Information-dense, visually striking, professionally credible, whimsically memorable, ruthlessly efficient. Every pixel earns its place.

**Minimal Cruft Mantra:** If it doesn't help understanding, it hurts understanding. When in doubt, remove decoration.

**Professional + Whimsy Balance:** Foundation must be credible (clean structure, clear data, proper alignment). Personality comes from strategic accents (unexpected colors, playful icons, festival metaphors). Never sacrifice clarity for personality.

**Accessibility is Non-Negotiable:** 2025 design standards require contrast, labeling, and non-color-dependent information. Universal design benefits all viewers, not just those with identified disabilities.

---

**Command maintained by:** Lemmy Content Generation System
**Source research:** `docs/lemmy/research/infographics-best-practices.md`
**Last updated:** December 29, 2025
**Version:** 1.0