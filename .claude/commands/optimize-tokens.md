# Token Optimization Command

Optimize markdown for minimal token consumption while preserving all substantive points.

## Core Approach

Expert technical writer: Analyze source, understand ALL points, drastically reduce content while preserving ALL points.

## Optimization Strategies

**Rephrase for conciseness:**
- Verbose → terse, information-dense
- Example: "several factors need consideration when implementing in production" → "Key production factors:"

**Reorganize:**
- Group related concepts
- Eliminate redundancy
- Hierarchical bullets

**Reduce to fragments:**
- Bullet points/fragments over narratives
- Strip unnecessary articles (a, an, the)
- Remove transitions ("In conclusion," "As mentioned," "It's important that")

**Extract URLs:**
- Unique URLs at document end
- Strip querystring parameters
- Deduplicate

**Remove fluff:**
- Filler words (very, really, quite, actually, basically)
- Redundant examples (keep most illustrative only)
- Excessive context
- Marketing language, superlatives

---

## Preserve vs. Remove

✅ **Preserve:**
- Unique facts, claims, arguments
- Technical details, numbers, dates, statistics
- Logical relationships
- Critical warnings/caveats
- Source attributions, citations
- Decision criteria, frameworks
- Recommendations, action items
- Comparison tables, data matrices

❌ **Remove:**
- Introductory/transitional phrases
- Redundant examples (keep 1-2 best)
- Verbose explanations (bullet suffices)
- Marketing language ("revolutionary," "cutting-edge")
- Excessive context
- Restated points
- Overly detailed background

---

## Process

### Step 1: Analysis
1. Identify all distinct points/claims/facts
2. Map document structure
3. Extract all URLs
4. Identify redundancies

### Step 2: Optimization
1. Preserve all unique information
2. Reduce prose to bullet fragments
3. Eliminate redundancy, filler, verbose phrasing
4. Consolidate related concepts
5. Extract URL list (Sources section at end)

### Step 3: Verification
- [ ] All substantive points preserved
- [ ] No factual information lost
- [ ] URLs extracted/cleaned
- [ ] Redundancy eliminated
- [ ] Maximum conciseness
- [ ] Logical flow maintained
- [ ] Markdown properly formatted

---

## Output Format

```markdown
# [Title] - Optimized

## Summary
[1-2 sentence overview]

## Key Points
- [Point 1]
- [Point 2]

## [Section Name]
- [Optimized content]
  - [Sub-point]

## Decision Criteria / Recommendations
- [Actionable items]

## Sources
- [URL 1]
- [URL 2]

---
**Optimization Stats**
- Original: ~X tokens
- Optimized: ~Y tokens
- Reduction: Z%
```

---

## Token Estimation

- 1 token ≈ 4 characters
- Target reduction: 40-70%
- Acceptable: 30-75% (depends on verbosity)
- **Maximum:** No information loss

**Example:**

**Before (~150 tokens):**
```
When evaluating potential Third Space locations in Seattle, business owners need to
carefully consider demographic composition. First, ensure sufficient population of
young professionals 25-44 with disposable income for premium desserts. Additionally,
analyze competitive landscape for too many similar late-night dessert venues.
```

**After (~45 tokens):**
```
## Location Criteria
- **Demographics:** 25-44 age range, disposable income for premium desserts
- **Competition:** Analyze late-night dessert venues in area
```
**Reduction: 70%**

---

## Special Cases

### Tables
Preserve but optimize content:

**Before:**
| Factor | Description | Why It Matters |
|--------|-------------|----------------|
| Demographics | Age, income, lifestyle of residents | Determine if market exists |

**After:**
| Factor | Key Metrics | Importance |
|--------|-------------|------------|
| Demographics | Age 25-44, income $50K+, renter % | Market validation |

### Decision Frameworks
Preserve logic, compress examples:

**Before:**
```
If location has 6+ green flags, pursue detailed research. For example, good demographics,
low competition, strong foot traffic, excellent parking, transit access, moderate rent
= 6 flags, investigate further.
```

**After:**
```
**Decision Rule:**
- 6+ green flags → Pursue detailed research
- 4-5 green flags, 0-1 red → Consider if strategic advantage
- 3+ red flags → Pass
```

### Citations
Preserve, consolidate:

**Before:**
```
Point2Homes (https://...) shows median income $132,560. AreaVibes (https://...)
reports similar. Zumper (https://...) shows rental data.
```

**After:**
```
Median household income: $132,560

**Sources:** Point2Homes, AreaVibes, Zumper (see URLs)
```

---

## Target Reduction by Type

**Research Reports (R-0XX):** 40-60%  
- 500-900 lines → 200-450 lines  
- Preserve data points, compress narratives

**Framework Documents:** 30-40%  
- 460 lines → 275-320 lines  
- Preserve methodology, reduce examples

**Meeting Notes/Docs:** 50-70%  
- Bullets only, fragment sentences

**Email/Communication:** 60-80%  
- Ultra-concise  
- Action items, dates, decisions only

---

## Anti-Patterns

❌ **Don't sacrifice clarity:**
```
Bad: "CH: 6+ GF, pursue"
Good: "Capitol Hill: 6+ green flags → Pursue research"
```

❌ **Don't remove decision context:**
```
Bad: "Rent too high"
Good: "Rent: $42/SF exceeds $38/SF threshold → Pass"
```

❌ **Don't over-abbreviate domain terms:**
```
Bad: "TS loc eval"
Good: "Third Space location evaluation"
```

❌ **Don't eliminate comparisons:**
```
Bad: "Good demographics"
Good: "Demographics: 48% age 25-44 vs 35% Seattle avg → Exceeds target"
```

---

## Success Metrics

**Success:**
- ✅ Information preservation: 100%
- ✅ Token reduction: 40-70%
- ✅ Readability: High (scannable, clear)
- ✅ Actionability: Clear decisions/recommendations
- ✅ Verifiability: Sources cited, numbers traceable

**Failure:**
- ❌ Key facts missing
- ❌ Decision criteria unclear
- ❌ Relationships lost
- ❌ Recommendations not actionable
- ❌ Sources not traceable

---

## Philosophy

**Ruthlessly eliminate words, religiously preserve meaning.**

**Quality over compression:** 70% with information loss vs 50% with perfect preservation → choose preservation.

**Iterative:** First pass 40%, second pass +10-20%.

**Context matters:** Business-critical docs (investor pitches, grants) 30-40%; internal notes 60-70%.

---

**Version:** 1.0  
**Updated:** Dec 25, 2025
