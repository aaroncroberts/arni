#!/usr/bin/env bash
# Create a Beads task that appends a confirmation note to COPILOT-INTEGRATION-LOG.md
# Usage: create-copilot-log-task.sh [parent-bead-id] [--dry-run]

set -euo pipefail

# Ensure PATH includes common global locations
export PATH="$PATH:/usr/local/bin:/opt/homebrew/bin:$HOME/.local/bin:/usr/local/sbin:/usr/sbin:/bin"

PARENT=${1:-}
DRY_RUN=false
for arg in "$@"; do
  if [ "$arg" = "--dry-run" ] || [ "$arg" = "-n" ]; then
    DRY_RUN=true
  fi
done

BD_BIN=${BD_BIN:-$(command -v bd 2>/dev/null || true)}
if [ -z "$BD_BIN" ]; then
  echo "bd CLI not found on PATH. Please install bd or set BD_BIN to its path." >&2
  exit 2
fi

TIMESTAMP_CMD='date -u +%Y-%m-%dT%H:%M:%SZ'
NOTE_COMMAND='echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) - Copilot integration: created task to append to COPILOT-INTEGRATION-LOG.md" >> COPILOT-INTEGRATION-LOG.md'

DESC="Append timestamped confirmation to COPILOT-INTEGRATION-LOG.md\n\nWhen executing, run the following command in the repository root:\n\n$NOTE_COMMAND\n\nThis task was created by create-copilot-log-task.sh"

TITLE="Task: Append Copilot integration confirmation"

if [ "$DRY_RUN" = true ]; then
  echo "DRY RUN: Would create task with title: $TITLE"
  echo "Parent: ${PARENT:-<none>}"
  echo "Description:\n$DESC"
  exit 0
fi

CMD=("$BD_BIN" create --title="$TITLE" --type=task)
if [ -n "$PARENT" ]; then
  CMD+=(--parent="$PARENT")
fi
CMD+=(--description "$DESC" --silent)

echo "Creating Beads task..."
OUT=$("${CMD[@]}" 2>&1) || {
  echo "bd create failed:" >&2
  echo "$OUT" >&2
  exit 3
}

ID=$(printf '%s' "$OUT" | head -n1 | tr -d '\r')
echo "Created task: $ID"
echo "Task description instructs to append to COPILOT-INTEGRATION-LOG.md when executed."

exit 0
