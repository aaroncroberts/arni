# /gemini-generate: Gemini Infographic Generator

**Purpose:** Automate gemini.google.com infographic generation using Playwright (Nano Banana workflow)

**Free Alternative:** When Gemini API hits quota limits, use this browser automation approach with separate quota

---

## Quick Start

```bash
# Basic usage (generates 3 landscape infographics)
python3 .claude/scripts/gemini-generate.py \
  --content docs/writing/topic/content.md \
  --prompt docs/writing/topic/prompt.md \
  --output-dir docs/writing/topic/visuals \
  --name topic-name

# Generate 5 variants in portrait mode
python3 .claude/scripts/gemini-generate.py \
  --content data.md \
  --prompt style.md \
  --variants 5 \
  --aspect-ratio portrait \
  --output-dir ./output

# Batch 2 for iterative generation (generates variants 4-6)
python3 .claude/scripts/gemini-generate.py \
  --content data.md \
  --prompt style.md \
  --batch 2 \
  --name barriers
```

---

## How It Works

### 1. Browser Automation with Playwright
- Launches Chromium browser (visible, not headless)
- Navigates to gemini.google.com
- Waits for user sign-in (if needed)
- Automatically submits prompts and downloads images

### 2. Intelligent Prompt Building
- Combines content + direction/style prompt
- Adds explicit aspect ratio instructions (critical!)
- Includes variant number for differentiation
- Example combined prompt:
  ```
  CRITICAL: LANDSCAPE ASPECT RATIO (16:9)
  [Style instructions from prompt file]
  ---
  CONTENT:
  [Data from content file]
  ---
  VARIANT #1: Generate unique visual approach
  ```

### 3. Automatic Download & Naming
- Detects generated images via download button
- Saves with sequential naming: `name-1.png`, `name-2.png`, etc.
- Optional WebP conversion using todd-image-convert

---

## Command-Line Arguments

### Required
- `--content FILE_OR_TEXT` - Content/data (file path or direct text)
- `--prompt FILE_OR_TEXT` - Style/direction prompt (file path or direct text)

### Optional
- `--output-dir DIR` - Output directory (default: current directory)
- `--name BASE_NAME` - Base name for files (default: `infographic`)
- `--variants N` - Number of variants (1-5, default: 3)
- `--aspect-ratio {landscape|portrait|square}` - Aspect ratio (default: `landscape`)
- `--batch N` - Batch number for iteration (default: 1)
- `--skip-webp` - Skip WebP conversion, only generate PNGs
- `--resolution RES` - Resolution for WebP (default: `1080p`)

---

## Aspect Ratio Options

### Landscape (Default) ✅
```bash
--aspect-ratio landscape
```
- Dimensions: ~2752 x 1536 (16:9)
- Best for: Curriculum, presentations, web content
- Matches NotebookLM default output

### Portrait
```bash
--aspect-ratio portrait
```
- Dimensions: ~1792 x 2400 (9:16)
- Best for: Mobile displays, vertical social media
- Less common for infographics

### Square
```bash
--aspect-ratio square
```
- Dimensions: ~2048 x 2048 (1:1)
- Best for: Social media posts (Instagram, etc.)
- Compact, versatile format

---

## Iterative Generation Workflow

### Batch 1: Initial Generation
```bash
python3 .claude/scripts/gemini-generate.py \
  --content capabilities.content.md \
  --prompt capabilities.prompt.md \
  --output-dir docs/writing/1-transformation/visuals/capabilities \
  --name capabilities
```

**Output:** `capabilities-1.png`, `capabilities-2.png`, `capabilities-3.png` (+ webp)

### Evaluate Results
```bash
/ig-evaluate docs/writing/1-transformation/visuals/capabilities/*.webp
```

**Check:** If best score < 90%, continue to batch 2

### Batch 2: Iterative Improvement
```bash
python3 .claude/scripts/gemini-generate.py \
  --content capabilities.content.md \
  --prompt capabilities.prompt.md \  # Can update with learnings from eval
  --output-dir docs/writing/1-transformation/visuals/capabilities \
  --name capabilities \
  --batch 2
```

**Output:** `capabilities-4.png`, `capabilities-5.png`, `capabilities-6.png` (+ webp)

### Repeat Until 90%+
Continue with `--batch 3`, `--batch 4`, etc. until evaluation shows 90%+ score

---

## User Interaction Flow

### First Run (Sign-In Required)
```
🌐 Launching browser...
🔗 Navigating to gemini.google.com...
⏸️  Checking sign-in status...
⚠️  Not signed in. Please sign in to Gemini...
   Press ENTER when you're signed in and ready to continue...
```
**Action:** User signs in to Google account, then presses ENTER

### Subsequent Runs (Cached Session)
```
🌐 Launching browser...
🔗 Navigating to gemini.google.com...
⏸️  Checking sign-in status...
✅ Already signed in!
```
**Action:** Automatic, no user interaction needed

### Generation Progress
```
🎨 Generating variant 1 (1/3)...
   📝 Prompt length: 1842 chars
   ⏳ Submitting prompt...
   🖼️  Waiting for image generation...
   ✅ Image generated!
   ⬇️  Downloading image...
   ✅ Saved: capabilities-1.png
   ⏸️  Pausing 3 seconds before next variant...
```

---

## Integration with EventAI Workflow

### Standard Infographic Generation Pipeline

**Step 1: Prepare Content**
```bash
# Create content.md with data/statistics
# Create prompt.md with style instructions
```

**Step 2: Generate Initial Variants**
```bash
python3 .claude/scripts/gemini-generate.py \
  --content topic.content.md \
  --prompt topic.prompt.md \
  --output-dir docs/writing/topic/visuals \
  --name topic \
  --aspect-ratio landscape
```

**Step 3: Evaluate**
```bash
/ig-evaluate docs/writing/topic/visuals/*.webp
```

**Step 4: Iterate if Needed**
```bash
# If score < 90%, update prompt.md with learnings
# Then generate batch 2
python3 .claude/scripts/gemini-generate.py \
  --content topic.content.md \
  --prompt topic.prompt.md \
  --output-dir docs/writing/topic/visuals \
  --name topic \
  --batch 2
```

**Step 5: Select Winner**
```bash
# Review topic.eval.md
# Use highest-scoring variant (90%+) in curriculum
```

---

## Comparison: Gemini API vs Browser Automation

| Feature | Gemini API | Browser Automation |
|---------|------------|-------------------|
| **Setup** | API key required | Google account sign-in |
| **Quota** | 0/day (free tier eliminated Dec 2025) | Separate quota (generous) |
| **Cost** | $0.039/image (paid tier) | Free |
| **Speed** | Fast (API call) | Slower (browser rendering) |
| **Automation** | Fully programmatic | Semi-automated (initial sign-in) |
| **Reliability** | High (when quota available) | Dependent on UI stability |
| **Best For** | Production at scale | Free iteration, development |

**Recommendation:** Use browser automation (this tool) for free iterative generation until finding 90%+ quality, then consider API for production at scale.

---

## Troubleshooting

### "Playwright not installed"
```bash
pip install playwright
playwright install  # Downloads browser binaries
```

### "Timeout waiting for image"
- Gemini may be slow or overloaded
- Check browser window - is image actually generating?
- Increase timeout in script if needed (line 131: `timeout=120000`)

### "No such file or directory" (content/prompt)
- Check file paths are correct
- Use absolute paths if relative paths fail
- Or provide text directly: `--content "Your data here"`

### Sign-in not detected
```bash
# Press ENTER after you're signed in
# Look for the text input area in browser
```

### Browser closes immediately
- Check for errors in console output
- Ensure you have internet connection
- Verify gemini.google.com is accessible

---

## Advanced Usage

### Direct Text Input (No Files)
```bash
python3 .claude/scripts/gemini-generate.py \
  --content "AI adoption: 47% (2025), 95% (2035)" \
  --prompt "Create minimalist infographic with EventAI colors" \
  --name quick-test
```

### High-Volume Generation
```bash
# Generate 5 variants
python3 .claude/scripts/gemini-generate.py \
  --content data.md \
  --prompt style.md \
  --variants 5 \
  --name topic
```

### PNG Only (Skip WebP)
```bash
python3 .claude/scripts/gemini-generate.py \
  --content data.md \
  --prompt style.md \
  --skip-webp
```

### Custom Resolution WebP
```bash
python3 .claude/scripts/gemini-generate.py \
  --content data.md \
  --prompt style.md \
  --resolution 4k  # or 720p, 1080p, original
```

---

## File Naming Convention

**Pattern:** `{base_name}-{variant_number}.{png|webp}`

**Example:**
```
barriers/
  barriers-1.png, barriers-1.webp  # Batch 1
  barriers-2.png, barriers-2.webp
  barriers-3.png, barriers-3.webp
  barriers-4.png, barriers-4.webp  # Batch 2 (if needed)
  barriers-5.png, barriers-5.webp
  barriers-6.png, barriers-6.webp
```

---

## Success Story: Capabilities Infographics

**Context:** Needed 90%+ quality infographic, NotebookLM best was 89% (V3)

**Approach:**
1. Used gemini-generate.py to create 3 landscape variants
2. Specified exact EventAI hex codes in prompt
3. Generated strategic variations: Standard, Card-based, Minimalist
4. Auto-converted to 1080p WebP

**Result:**
- V9: 85% (Good)
- V10: 85% (Good)
- **V11: 91% (Excellent)** ✅ Goal achieved!

**Key Success Factors:**
- Explicit landscape aspect ratio specification
- Exact color hex codes in prompt
- Minimalist variant with 40%+ white space
- Completely flat design (no shadows/gradients)

**Time:** ~15 minutes for full workflow (vs hours of manual iteration)

---

## See Also

- `/ig-evaluate` - Evaluate generated infographics
- `todd-image-convert` - PNG to WebP conversion
- EventAI Visual Identity Guide - Brand colors and typography
- Gemini Chat: https://gemini.google.com/app

---

*Skill created: December 29, 2025*
*Powered by: Playwright + Gemini (gemini.google.com)*
*Integration: EventAI + Lemmy Content Generation System*