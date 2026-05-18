#!/usr/bin/env bash
# Install the connection-leak skills into Claude Code's skills directory.
# Re-runnable: existing installs are replaced. Use --link for edit-in-place dev.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILLS_DIR="${CLAUDE_SKILLS_DIR:-$HOME/.claude/skills}"
SKILLS=(connection-leak-hunt connection-leak-jdbc connection-leak-flink connection-leak-http-grpc)

MODE="copy"
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $0 [options]

Install the connection-leak skills into Claude Code's skills directory.

Options:
  --link        Symlink instead of copy (edits in this repo are picked up live).
  --uninstall   Remove the four skills from the target directory.
  --dry-run     Print actions without executing them.
  -h, --help    Show this help.

Target directory: \$CLAUDE_SKILLS_DIR (currently: $SKILLS_DIR)
Skills:           ${SKILLS[*]}
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --link)      MODE="link" ;;
    --uninstall) MODE="uninstall" ;;
    --dry-run)   DRY_RUN=1 ;;
    -h|--help)   usage; exit 0 ;;
    *)           echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [ -z "${SKILLS_DIR}" ]; then
  echo "Error: target skills directory is empty (CLAUDE_SKILLS_DIR=\"$SKILLS_DIR\")." >&2
  exit 1
fi

if [ "$MODE" != "uninstall" ]; then
  for skill in "${SKILLS[@]}"; do
    if [ ! -f "$SCRIPT_DIR/$skill/SKILL.md" ]; then
      echo "Error: $SCRIPT_DIR/$skill/SKILL.md not found." >&2
      echo "Run install.sh from the repo root (where the four connection-leak-* folders live)." >&2
      exit 1
    fi
  done
fi

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '+ '; printf '%q ' "$@"; printf '\n'
  else
    "$@"
  fi
}

if [ ! -d "$SKILLS_DIR" ]; then
  run mkdir -p "$SKILLS_DIR"
fi

case "$MODE" in
  copy)
    for skill in "${SKILLS[@]}"; do
      target="$SKILLS_DIR/$skill"
      if [ -e "$target" ] || [ -L "$target" ]; then
        run rm -rf "$target"
      fi
      run cp -R "$SCRIPT_DIR/$skill" "$target"
      echo "installed: $target"
    done
    ;;
  link)
    for skill in "${SKILLS[@]}"; do
      target="$SKILLS_DIR/$skill"
      if [ -e "$target" ] || [ -L "$target" ]; then
        run rm -rf "$target"
      fi
      run ln -s "$SCRIPT_DIR/$skill" "$target"
      echo "linked: $target -> $SCRIPT_DIR/$skill"
    done
    ;;
  uninstall)
    for skill in "${SKILLS[@]}"; do
      target="$SKILLS_DIR/$skill"
      if [ -e "$target" ] || [ -L "$target" ]; then
        run rm -rf "$target"
        echo "removed: $target"
      else
        echo "not present: $target"
      fi
    done
    ;;
esac

if [ "$DRY_RUN" -eq 0 ] && [ "$MODE" != "uninstall" ]; then
  echo
  echo "Done. Restart Claude Code to pick up the new skills."
  echo "Verify: ls -la \"$SKILLS_DIR\" | grep connection-leak"
fi
