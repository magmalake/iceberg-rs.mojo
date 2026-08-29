#!/usr/bin/env bash
#
# Stage all changes, commit with the standard trailers (no GPG prompt), and push.
# One approvable command — do NOT chain `&& git push` onto it.
#
#   tools/commit.sh "<commit message>"
set -euo pipefail

MSG="${1:?usage: tools/commit.sh \"message\"}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

git -C "$ROOT" add -A
git -C "$ROOT" -c commit.gpgsign=false commit \
  -m "$MSG" \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" \
  -m "Claude-Session: https://claude.ai/code/session_015or6oTZmb7EQTXTbALbBXY"
git -C "$ROOT" push -u origin HEAD
