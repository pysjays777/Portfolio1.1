# Business Process Automation Portfolio

**Role:** Project Operations | Process Automation Specialist

---

## About Me

I am a person who deep diving into Automation with hands-on experience in designing and building automation solutions that streamline operational workflows. My work focuses on identifying repetitive, manual processes and transforming them into reliable, scalable automation tools — reducing processing time, minimizing human error, and freeing up teams to focus on higher-value analysis.

I bridge the gap between business requirements and technical implementation. While my primary role involves stakeholder communication, requirements gathering, and process mapping, I also develop working prototypes and production-ready automation scripts using Python and Google Workspace APIs.

---

## Core Competencies

- **Process Analysis & Optimization** — Mapping end-to-end business workflows, identifying bottlenecks, and designing automation strategies
- **Data Wrangling & Transformation** — Processing large-scale CSV/Excel datasets with pandas, handling data quality issues, and building validation pipelines
- **Desktop Tool Development** — Building user-friendly GUI applications (Tkinter) so non-technical team members can run automation independently
- **Google Workspace Integration** — Automating Google Sheets and Google Drive operations via service accounts and APIs for real-time reporting
- **Rule-Based Decision Engines** — Designing configurable rule engines that encode business logic for automated record classification and routing

---

## Projects

### 1. CSV Split Tool
**`projects/csv_split_tool.py`**

A desktop GUI application for splitting large CSV/Excel files into manageable parts with configurable row limits.

**Business Problem:** Operations teams regularly receive data exports containing millions of rows. Downstream systems have upload limits (e.g., 950,000 rows per file), requiring manual splitting, a tedious and error-prone process.

**Solution:**
- GUI-based tool with file browsing, column selection, and pattern-based file discovery
- Automatic splitting into parts at configurable row thresholds
- Flexible header renaming: keep original, replace spaces with underscores, or apply custom JSON mappings
- Supports CSV, TXT, and Excel formats
- Generates per-file and global summary JSONs for audit trail

**Tech Stack:** Python, pandas, Tkinter

---

### 2. Review Workflow Automation
**`projects/review_workflow_automation.py`**

An automated workflow engine that processes product listing review files through a configurable rule engine.

**Business Problem:** Review analysts manually sort thousands of product listings per session — categorizing by source channel, checking reviewer assignments, flagging anomalies, and generating approval/return files. This took significant time and was prone to classification errors.

**Solution:**
- Rule engine architecture: business rules are defined as simple mask + output builder pairs, making them easy to add/modify without touching core logic
- Automatic categorization by entry source (Vendor vs Direct channels)
- Anomaly detection for records with missing or unexpected account assignments
- Auto-generates structured output files: approval lists, return-for-review lists, external case reports, and anomaly logs
- Session-based output organization with processed file archiving

**Tech Stack:** Python, pandas, dataclasses

---

### 3. Data Processor (GUI + Core Engine)
**`projects/data_processor_gui.py`** | **`projects/processor_core.py`**

A two-component system: a core processing engine and a desktop GUI wrapper for batch CSV processing.

**Business Problem:** Analysts needed to classify thousands of records daily based on pricing thresholds, separating items that require manual review from those eligible for auto-approval. The manual process involved opening each file, applying filters, copying data, and creating separate output files.

**Solution:**
- Core engine reads CSV files, applies pricing threshold logic, and splits records into review-required vs auto-approved categories
- Desktop GUI provides a simple interface for non-technical users: browse folders, click Run, view real-time logs
- Threaded execution keeps the UI responsive during processing
- Batch output organization with timestamped folders
- Automatic archiving of processed inputs to prevent duplicate processing
- Optional Google Sheets integration for centralized audit logging
- ASCII grid summary table printed after each batch for quick verification

**Tech Stack:** Python, pandas, Tkinter, Google Sheets API (gspread)

---

### 4. Google Sheets Dashboard Aggregator
**`projects/gsheets_dashboard_aggregator.py`**

An automated reporting tool that aggregates data from multiple Google Sheets across a shared Drive folder into a centralized dashboard.

**Business Problem:** Team leads maintained individual tracking spreadsheets in a shared Google Drive folder. Generating monthly summaries required manually opening each spreadsheet, copying data, and consolidating, a process that took hours and was frequently out of date.

**Solution:**
- Automatically discovers and reads all Google Sheets in a specified Drive folder
- Flexible header matching with alias support (handles spacing/casing variations across different team members' sheets)
- Smart date parsing for date fields without year information, with worksheet-title fallback
- Generates four dashboard tabs:
  - **Monthly Summary** — Request volume and approval rates by month
  - **Reason Summary** — Breakdown of request reasons with approval metrics
  - **Reason by Month** — Cross-tabulation for trend analysis
  - **Data Quality Issues** — Automatic logging of parsing errors for data stewardship
- Retry logic with exponential backoff for API resilience
- Writes results back to a designated Dashboard spreadsheet automatically

**Tech Stack:** Python, pandas, gspread, Google Drive API, Google Sheets API

---

## Technical Approach

My automation philosophy centers on a few principles:

1. **Configuration over code changes** — Business rules, column mappings, and thresholds are defined in dataclasses and config objects, not hardcoded. This means analysts can adjust parameters without modifying logic.

2. **GUI for accessibility** — Every automation that is used by non-technical team members gets a Tkinter GUI. Complexity is hidden behind simple Browse/Run interfaces.

3. **Audit trail by default** — Every processing run generates summary files (JSON or CSV), timestamps outputs, and archives inputs. This supports compliance and makes troubleshooting straightforward.

4. **Fail gracefully** — Files that fail processing are moved to separate error directories rather than blocking the entire batch. Data quality issues are logged, not silently dropped.

---

## Tech Stack Summary

| Category | Tools |
|---|---|
| Language | Python 3.10+ |
| Data Processing | pandas |
| GUI Framework | Tkinter |
| Cloud Integration | Google Sheets API, Google Drive API, gspread |
| Authentication | Google Service Accounts (OAuth2) |
| Architecture | Dataclass-based config, Rule engine pattern |

---

## Contact

Feel free to explore the code in the `projects/` directory. Each file is self-contained with docstrings explaining the business context and technical approach.
