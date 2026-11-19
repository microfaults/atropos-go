@AGENTS.md

## Claude Code only

<!-- Keep this file to the import plus Claude-specific notes; repo knowledge belongs in AGENTS.md. Last verified 2026-11-16. -->
- This tree is a `.claude/worktrees/` worktree. Subagents started from here load the main checkout's startup instructions (`faults-lab/atropos-go`, an older `develop`), not this file. When you dispatch one, give it this worktree's absolute path and tell it to read `AGENTS.md` here first.
- Auto memory under `~/.claude/projects/` is machine-local and keyed by the launch directory. It is not a substitute for `AGENTS.md`: put durable repo facts there.
