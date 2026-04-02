# The Upsert Crew

A multi-agent system where specialized agents collaborate to improve, extend, debug, and maintain pg-upsert.

## Architecture

- **The DBA** (Dispatcher) orchestrates all agents via hub-and-spoke communication
- Agents communicate through files in `comms/` — they do NOT talk directly to each other
- All agents read `.claude/project_context.md` as their canonical project reference
- Existing skills (`/code-oracle`, `/test-module`, `/update-changelog`, `/review-changes`) remain available and agents may reference them

## Directory Structure

- `.claude/agents/` — Agent prompt definitions
- `.claude/commands/` — Skill definitions (slash commands)
- `.claude/comms/briefings/` — Tasks assigned by The DBA to agents
- `.claude/comms/reports/` — Agent outputs back to The DBA
- `.claude/research/` — Oracle's codebase investigation findings
- `.claude/plans/` — DBA's implementation plans
- `.claude/patches/` — Patcher's code change descriptions and notes
- `.claude/test-reports/` — QA's test results and coverage reports
- `.claude/docs-drafts/` — Scribe's documentation drafts
- `.claude/releases/` — Herald's release notes and changelog entries
- `.claude/state/` — Shared state (current phase, active task, agent status)

## Communication Protocol

1. The DBA writes a briefing to `.claude/comms/briefings/{agent}-{YYYY-MM-DD}.md`
1. The DBA spawns the target agent
1. Agent reads its briefing, does its work, writes output to `.claude/comms/reports/{agent}-{YYYY-MM-DD}.md`
1. Agent also writes artifacts to its dedicated directory (.claude/research/, .claude/patches/, etc.)
1. The DBA reads the report and decides next steps

## Development Phases

1. **Triage** — DBA understands the issue/request, decides scope and agents needed
1. **Research** — Oracle investigates codebase, finds relevant code paths and impact
1. **Plan** — DBA synthesizes research into implementation approach, aligns with human
1. **Implement** — Patcher writes code, Oracle advises on architecture
1. **Test** — QA writes/runs tests, verifies coverage stays above threshold
1. **Document** — Scribe updates docs, Herald updates changelog
1. **Review** — Inspector does final code review before human merge

## Constraints

- Coverage floor must be maintained — QA blocks any change that drops it
- No destructive git operations without human approval
- Agents should always read `.claude/project_context.md` before starting work
- All code must pass `ruff check` and target Python 3.10+
- **Every user-visible change must be reflected in `CHANGELOG.md`** under the `[Unreleased]` section using [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) categories: Added, Changed, Fixed, Removed. Do not leave changelog updates for later — include them in the same commit or PR as the code change.
- **Every user-visible change must also update documentation** — review and revise `README.md`, `docs/`, and any other relevant documentation to stay consistent with the code. New CLI options, features, or behavior changes must be reflected in the docs. Do not leave doc updates for later.
- **After every version bump and push, monitor CI** — push with `git push && git push --tags` to include version tags, then run `gh run list --limit 1` to get the run ID, then `gh run watch <id> --exit-status` to block until it completes. Bump commits trigger PyPI publish and GitHub Release, so failures must be caught and fixed immediately.
