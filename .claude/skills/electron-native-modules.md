# Electron Native Modules Skill

Use this skill when diagnosing or fixing native module issues in Electron applications, Node.js ABI mismatches, or multi-runtime environments.

## When to Use

- `NODE_MODULE_VERSION` mismatch errors
- Native module crashes in Electron but works in tests
- `better-sqlite3`, `sharp`, or other native module issues
- Setting up dual Node/Electron environments

## Quick Diagnosis

### Error Pattern

```
NODE_MODULE_VERSION X. This version requires NODE_MODULE_VERSION Y.
```

### ABI Reference (Memorize These)

| ABI | Runtime | Use Case |
|-----|---------|----------|
| **131** | **Node.js 24.x** | CLI tools, scripts, Vitest |
| **144** | **Electron 40** | GUI apps |
| 127 | Node.js 22.x | Legacy CLI |
| 140 | Electron 39 | Legacy GUI |

**Key insight**: Node and Electron **never share ABIs**. Electron 40 bundles Node 24 internally but has ABI 144, not 131.

### Check Your Runtime

```bash
node -e "console.log('ABI:', process.versions.modules, 'Node:', process.version)"
```

## Proven Fixes (Tested in todd-lab)

### Fix for CLI Tools (Node.js)

```bash
npm rebuild better-sqlite3
```

Verify:
```bash
node -e "require('better-sqlite3'); console.log('OK')"
```

### Fix for Electron Apps

From todd-lab/client-nextjs:
```bash
npm run sqlite:rebuild-electron
# or directly:
npx @electron/rebuild -f -w better-sqlite3
```

Verify:
```bash
npm run test:electron-native
```

### Fix for Dual-ABI Environment

From todd-lab/client-nextjs:
```bash
npm run sqlite:setup-dual
npm run sqlite:verify
```

## Tested Scripts Reference (todd-lab)

| Script | Purpose |
|--------|---------|
| `npm run sqlite:rebuild-node` | Rebuild for Node.js (CLI) |
| `npm run sqlite:rebuild-electron` | Rebuild for Electron (GUI) |
| `npm run sqlite:setup-dual` | Install both ABIs side-by-side |
| `npm run sqlite:verify` | Verify dual setup |
| `npm run test:electron-native` | Test native modules IN Electron |

## Common Scenarios

### Scenario 1: CLI Tool Fails After Electron Work

**Error**: todd-bishop fails with ABI 140 vs 127

**Cause**: You ran `npm run sqlite:rebuild-electron` in todd-lab, npm cache propagated it.

**Fix**:
```bash
cd /path/to/todd-bishop
npm rebuild better-sqlite3
node -e "require('better-sqlite3'); console.log('OK')"
```

### Scenario 2: Vitest Passes But Electron Crashes

**Error**: All tests green, app crashes on startup

**Cause**: Vitest runs in Node.js (ABI 127), Electron needs ABI 140.

**Fix**:
```bash
cd .todd/lab/client-nextjs
npm run sqlite:rebuild-electron
npm run test:electron-native  # MUST pass before claiming fix works
```

### Scenario 3: Inconsistent Behavior

**Symptom**: Works sometimes, fails other times

**Cause**: Different Node versions in PATH (nvm switching)

**Diagnosis**:
```bash
which node && node --version && nvm current
```

**Fix**: Ensure consistent Node, then rebuild:
```bash
nvm use 22
npm rebuild better-sqlite3
```

## Electron-Specific Issues

### ELECTRON_RUN_AS_NODE Bug

**Symptom**: `require('electron').app` is `undefined`

**Cause**: `ELECTRON_RUN_AS_NODE=1` in environment makes Electron run as plain Node.

**Diagnosis**:
```bash
echo $ELECTRON_RUN_AS_NODE
```

**Fix**:
```bash
unset ELECTRON_RUN_AS_NODE
```

### macOS Quarantine

**Symptom**: Electron partially initializes, `process.type` undefined

**Fix**:
```bash
xattr -cr node_modules/electron/dist/Electron.app
```

### GUI App PATH Issues

macOS GUI apps don't inherit shell PATH. Use `fix-path`:

```javascript
import fixPath from 'fix-path';
fixPath(); // Call early in main process
```

## Prevention: Startup Check

Add to CLI tools (like todd-bishop):

```javascript
function checkNativeModules() {
  try {
    require('better-sqlite3');
  } catch (e) {
    if (e.message.includes('NODE_MODULE_VERSION')) {
      console.error('Native module ABI mismatch.');
      console.error('Fix: npm rebuild better-sqlite3');
      process.exit(1);
    }
    throw e;
  }
}
checkNativeModules();
```

## Prevention: Electron Test Script

Create `tests/electron/verify-native-modules.js` (tested pattern from todd-lab):

```javascript
// electron-test-main.js - runs IN Electron
const { app } = require('electron');
app.disableHardwareAcceleration();

app.whenReady().then(() => {
  try {
    const Database = require('better-sqlite3');
    const db = new Database(':memory:');
    db.prepare('SELECT 1').get();
    db.close();
    console.log('Native modules OK');
    app.exit(0);
  } catch (e) {
    console.error('FAIL:', e.message);
    if (e.message.includes('NODE_MODULE_VERSION')) {
      console.error('Fix: npm run sqlite:rebuild-electron');
    }
    app.exit(1);
  }
});
```

```javascript
// verify-native-modules.js - spawns Electron
const { spawn } = require('child_process');
const electron = require('electron');

spawn(electron, ['tests/electron/electron-test-main.js'], {
  stdio: 'inherit'
}).on('close', process.exit);
```

## TDD Workflow for Native Module Fixes

```bash
# 1. RED - Verify the failure
npm run test:electron-native  # Should FAIL

# 2. FIX - Rebuild for correct ABI
npm run sqlite:rebuild-electron

# 3. GREEN - Verify the fix
npm run test:electron-native  # Should PASS

# 4. NEVER skip step 3 - "npm test" passing is NOT enough
```

## See Also

- `docs/research/electron.md` - Full Electron dev guide
- `docs/research/node-abi.md` - ABI deep dive
- `docs/troubleshooting/bishop-native-module-fix.md` - Quick fix for Bishop
