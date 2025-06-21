# AGENTS.md

## 📦 Project overview

- Monorepo written in **Python 3.12** & **PySpark 4.0**; runs on Databricks (Azure).
- CI: GitHub Actions (`.github/workflows/ci.yml`) runs **pytest** + **ruff** on every push.

## 🧪 Test & lint commands

````bash
pytest -q
ruff check --fix .
Run these before committing code. Agents must reproduce failures locally, then ensure all tests pass and lint is clean before final output.

✍️ Coding conventions
Follow PEP 8, Google‑style docstrings, max line length = 120.

Order: imports ➜ constants ➜ UDFs ➜ helper funcs ➜ classes ➜ if __name__ == "__main__":.

Spark DataFrame columns: UPPER_SNAKE_CASE; Python variables: lower_snake_case.

🔀 Branch & PR rules
Purpose	Pattern	Title format
Feature	codex/feat/<topic>	feat(<scope>): <summary>
Fix	codex/fix/<issue>	fix(<scope>): <summary>

PR Description must include:

Context – What & Why.

Checklist – pytest ✅  ruff ✅  Docs updated ✅.

Screenshots / logs for UI or CLI changes.

🤖 Agent behaviour expectations
Scope the diff: modify only files displayed by Git; never touch comments outside the hunk.

Add/alter tests so total coverage ≥ existing coverage.

After code generation, rerun tests & linter; if failures, self‑correct once, else output the unified diff fenced with ```diff.

If uncertain about business logic, ask a clarifying question before changing code.

🚫 Never do this

Disable linters or tests unless explicitly instructed.

Introduce new external dependencies without adding them to pyproject.toml & requirements.txt.

Generate code with eval() or dynamic SQL prone to injection.

Exceed 30 min wall‑clock runtime in CI.
````
