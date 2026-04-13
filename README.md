# Markov

Markov is an agent intent audit layer for S3. It captures task context, records object-level S3 mutations, detects divergence, and exposes results via a small API and demo UI.

## Project layout

- `sdk/` — Python instrumentation wrapper (`AuditedS3Client`) + storage + divergence detector
- `api/` — FastAPI read API for executions and object actions
- `ui/` — React single-page UI for execution feed and detail view
- `demo/` — local end-to-end seed/demo script using moto

## Quick start

1. Create env and install Python dependencies:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Run demo data generation:
   - `python demo/seed.py`
3. Start API (same shell/env):
   - `export MARKOV_DB_PATH="$(python3 -c "import os,tempfile; print(os.path.join(tempfile.gettempdir(), 'markov-demo.db'))")"`
   - `uvicorn api.main:app --reload --host 127.0.0.1 --port 8000`
4. Start UI:
   - `cd ui`
   - `npm install`
   - `npm run dev`
5. Open the Vite URL (usually `http://localhost:5173`).

## Notes

- This repository is MVP scope only (no auth, no multi-tenancy, no restore/rollback).
- Detection is regex-based (no LLM calls).
- Runtime data is stored in SQLite by default.

## License

MIT — see `LICENSE`.
