Exit code: 0
Wall time: 0.5 seconds
Output:
# Project Case Studies

## 1. Operational review and pricing automation

**Problem:** Analysts repeatedly opened, filtered, classified, and split large operational files. Manual handling was slow and created inconsistent outputs.

**Solution:** Built reusable Python processors with configurable business rules, price-cache support, anomaly checks, timestamped output folders, input archiving, real-time logs, and desktop interfaces for non-technical users.

**Engineering strengths:** pandas, SQLite/parquet/pickle cache options, Tkinter, CLI design, validation, failure isolation, auditability.

## 2. Scheduled KPI reporting pipeline

**Problem:** Team KPI reporting depended on manual checking, cleaning, counting, and dashboard updates across multiple Google Sheets.

**Solution:** Built a deterministic ETL pipeline that discovers source Sheets in Drive, normalizes fields and operational dates, aggregates valid records, exports dashboard-ready summaries, and records structured run logs. GitHub Actions provides scheduling and manual recovery runs.

**Engineering strengths:** modular readers/processors/exporters, service-account auth, late-night reporting-date rules, structured JSON logs, safe skip/reject behavior.

## 3. Paper crypto trading and research platform

**Problem:** Strategy ideas needed a safe environment for evidence gathering, risk control, and forward testing.

**Solution:** Built a paper-only platform with Bybit public market data, multiple strategies, persistent accounts and trades, realistic fees/slippage, next-candle execution to avoid lookahead bias, drawdown controls, and a React dashboard. Added research-only leverage modelling and explicit gates that keep mainnet execution disabled.

**Engineering strengths:** Python service layer, React/Vite, SQLite, backtesting, risk design, notifications, Docker and hosted deployment configuration, unit tests.

## 4. AI-assisted content operations platform

**Problem:** Content ideation and review required consistent drafting, originality checks, risk controls, and traceable human approval.

**Solution:** Built a LangGraph workflow that reads queued sources, extracts and clusters ideas, generates channel-specific drafts, evaluates quality, retries weak results, performs QA/risk checks, and persists outputs for review. Added Postgres-backed source queues, authenticated web access, audit logs, optional Google Sheets mirroring, and Langfuse traces.

**Engineering strengths:** orchestration graphs, structured prompts, evaluation thresholds, deduplication hashes, regeneration, human-in-the-loop safety, FastAPI/Postgres, Vercel/Render routing.

## 5. Controlled job-search automation

**Problem:** Daily job discovery, relevance screening, document tailoring, and application tracking consume significant time.

**Solution:** Built a Python package that collects Malaysian job listings through configured sources, ranks fit, creates tailored resume and cover-letter drafts, emails a digest, tracks approval in SQLite, and opens approved applications for controlled browser autofill. It deliberately stops before final submission and does not bypass login or CAPTCHA.

**Engineering strengths:** package/CLI design, SQLite state, document generation, SMTP, external API integration, Playwright safety boundaries.

## 6. Learning foundations

Created practical exercises and notebooks covering pandas, SQL, regression, Google Sheets API integration, and bilingual technical explanation. These foundations evolved into the larger automation and product systems above.

