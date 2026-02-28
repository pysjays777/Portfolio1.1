"""Processor Core

Core engine for batch CSV processing with rule-based classification.
Processes data files by:
- Filtering records based on configurable price thresholds
- Splitting data into review-required vs auto-approved categories
- Generating structured output files for downstream workflows
- Logging results to Google Sheets for audit trail
- Archiving processed inputs to prevent re-processing

Designed to work standalone or as a backend for the Data Processor GUI.
"""

import sys
import shutil
from pathlib import Path
import pandas as pd
from datetime import datetime
import os
import json


# ---------- Helpers ----------
def discount_to_num(series: pd.Series) -> pd.Series:
    """Coerce Discount Price to numeric (remove BOM, symbols, commas)."""
    s = series.astype(str).str.replace('\ufeff', '', regex=False)
    s = s.str.replace(r'[^0-9.\-]', '', regex=True)
    return pd.to_numeric(s, errors='coerce').fillna(0)


def _ellipsis_mid(text: str, width: int) -> str:
    """Truncate text with ellipsis in the middle to fit width."""
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return "." * width
    keep = width - 3
    head = keep // 2
    tail = keep - head
    return text[:head] + "..." + text[-tail:]


def print_grid(rows, headers, right_align=None, max_col=None):
    """
    Print rows (list[dict]) as an ASCII grid table.
    - right_align: set of column names to right-align
    - max_col: per-column maximum width overrides
    """
    right_align = right_align or set()

    term_cols = shutil.get_terminal_size(fallback=(120, 24)).columns

    default_caps = {
        "STATUS": 8,
        "FILE": 36,
        "with discount": 14,
        "OUT_REVIEW": 36,
        "without discount": 14,
        "APPROVE_FILE": 36,
        "REASON": 60,
    }
    if max_col:
        default_caps.update(max_col)

    str_rows = []
    raw_max = {h: len(h) for h in headers}
    for r in rows:
        srow = {}
        for h in headers:
            v = r.get(h, "")
            sval = "" if v is None else str(v)
            srow[h] = sval
            raw_max[h] = max(raw_max[h], len(sval))
        str_rows.append(srow)

    target_w = {h: min(raw_max[h], default_caps.get(h, 30)) for h in headers}

    def grid_width(col_w):
        N = len(col_w)
        return 2 + sum(col_w.values()) + 3 * (N - 1) + 2

    priority = ["REASON", "FILE", "OUT_REVIEW", "APPROVE_FILE"]
    min_w = 6
    while grid_width(target_w) > term_cols:
        shrunk = False
        for col in priority:
            if col in target_w and target_w[col] > min_w:
                target_w[col] -= 1
                shrunk = True
                if grid_width(target_w) <= term_cols:
                    break
        if not shrunk:
            break

    def hline(char="-", cross="+"):
        return cross + cross.join(char * (target_w[h] + 2) for h in headers) + cross

    def fmt_row(row_dict):
        cells = []
        for h in headers:
            txt = _ellipsis_mid(row_dict[h], target_w[h])
            if h in right_align:
                cells.append(txt.rjust(target_w[h]))
            else:
                cells.append(txt.ljust(target_w[h]))
        return "| " + " | ".join(cells) + " |"

    print(hline("-", "+"))
    print(fmt_row({h: h for h in headers}))
    print(hline("=", "+"))
    for r in str_rows:
        print(fmt_row(r))
        print(hline("-", "+"))
# ---------- end helpers ----------

DESIRED_ORDER = [
    'Shop ID', 'Item ID', 'Model ID', 'Discount Price', 'Promo Price',
    '', 'Purchase Limit', 'Promo Stock',
    'Current Price', 'Reference Price', 'Price Check (Soft)', 'Max Entry Price',
    'Creator', 'Item Details'
]


def run_processor(input_dir, output_dir, log_callback=None):
    """
    Core processing logic:
      - Output written to output_dir/processed_YYYYMMDD_HHMMSS/ batch folder
      - Successfully processed input CSVs archived to input_dir/_processed/
      - Skipped/failed files remain in place
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Batch & archive directories
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = output_dir / f"ready_for_next_process_{ts}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = input_dir / f"_processed_files"
    archive_dir.mkdir(parents=True, exist_ok=True)

    class _Logger:
        def __init__(self, cb): self.cb = cb
        def write(self, s):
            if not self.cb: return
            s = s.replace("\r\n", "\n")
            for line in s.split("\n"):
                if line.strip() != "":
                    self.cb(line)
        def flush(self): pass

    old_stdout = sys.stdout
    if log_callback:
        sys.stdout = _Logger(log_callback)

    try:
        processed, skipped = 0, 0
        rows = []
        gsheet_records = []

        # Materialize file list before processing to avoid iterator issues
        csv_files = sorted(list(input_dir.glob('*.csv')))

        for csv_path in csv_files:
            name = csv_path.name.lower()
            if 'pendingreview' not in name:
                skipped += 1
                rows.append({
                    "STATUS": "SKIP",
                    "FILE": csv_path.name,
                    "with discount": "-",
                    "OUT_REVIEW": "-",
                    "without discount": "-",
                    "APPROVE_FILE": "-",
                    "REASON": "not a 'PendingReview' file",
                })
                continue

            try:
                df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
                df.columns = [c.replace('\ufeff', '').strip() for c in df.columns]

                if 'Discount Price' not in df.columns or 'Model ID' not in df.columns:
                    skipped += 1
                    rows.append({
                        "STATUS": "SKIP",
                        "FILE": csv_path.name,
                        "with discount": "-",
                        "OUT_REVIEW": "-",
                        "without discount": "-",
                        "APPROVE_FILE": "-",
                        "REASON": "missing required columns",
                    })
                    continue

                # Core processing logic
                discount = discount_to_num(df['Discount Price'])
                mask_gt = discount > 0
                mask_eq0 = discount == 0

                present = [c for c in DESIRED_ORDER if c in df.columns]
                out = df[present].copy()
                for col in [c for c in DESIRED_ORDER if c not in df.columns]:
                    out[col] = pd.NA
                out = out[DESIRED_ORDER]

                # Output to batch directory
                out_review = out[mask_gt]
                out_review_path = batch_dir / f"REVIEW_CHECK_{csv_path.stem}.csv"
                out_review.to_csv(out_review_path, index=False, encoding='utf-8-sig')

                approve_cols = [
                    'Model ID',
                    'Action(input: reject/approve/proceed/edit)',
                    'Reason',
                    'Promo Price',
                    'Discount Price',
                    'Promo Stock',
                ]
                approve_df = pd.DataFrame({
                    'Model ID': df.loc[mask_eq0, 'Model ID'].astype(object).values,
                    'Action(input: reject/approve/proceed/edit)': 'approve',
                    'Reason': '',
                    'Promo Price': '',
                    'Discount Price': '',
                    'Promo Stock': '',
                }, columns=approve_cols)
                approve_path = batch_dir / f"APPROVED_{csv_path.stem}.csv"
                approve_df.to_csv(approve_path, index=False, encoding='utf-8-sig')

                # Archive processed input file
                move_err = None
                try:
                    dest_path = archive_dir / csv_path.name
                    shutil.move(str(csv_path), str(dest_path))
                except Exception as e:
                    move_err = str(e)

                processed += 1
                rows.append({
                    "STATUS": "OK",
                    "FILE": csv_path.name,
                    "with discount": str(mask_gt.sum()),
                    "OUT_REVIEW": out_review_path.name,
                    "without discount": str(mask_eq0.sum()),
                    "APPROVE_FILE": approve_path.name,
                    "REASON": "" if move_err is None else f"move input failed: {move_err}",
                })
                gsheet_records.append({
                    "timestamp": datetime.now().isoformat(timespec='seconds'),
                    "file": csv_path.name,
                    "with_discount": int(mask_gt.sum()),
                    "without_discount": int(mask_eq0.sum()),
                    "batch_folder": str(batch_dir),
                })

            except Exception as e:
                skipped += 1
                rows.append({
                    "STATUS": "SKIP",
                    "FILE": csv_path.name,
                    "with discount": "-",
                    "OUT_REVIEW": "-",
                    "without discount": "-",
                    "APPROVE_FILE": "-",
                    "REASON": f"{e}",
                })

        headers = ["STATUS", "FILE", "with discount", "OUT_REVIEW", "without discount", "APPROVE_FILE", "REASON"]
        right_align = {"with discount", "without discount"}
        print_grid(
            rows, headers, right_align=right_align,
            max_col={"FILE": 30, "OUT_REVIEW": 30, "APPROVE_FILE": 30, "REASON": 40}
        )

        print(f"Batch output folder: {batch_dir}")
        print(f"Archived processed inputs to: {archive_dir}")
        print(f"Done. processed={processed}, skipped={skipped}")

        maybe_update_gsheet(gsheet_records, input_dir, log_callback=log_callback)
        return processed, skipped

    finally:
        sys.stdout = old_stdout


def _load_gsheet_config(input_dir, log_callback=None):
    config = {}
    credentials_path = os.getenv("GSHEETS_CREDENTIALS")
    spreadsheet_id = os.getenv("GSHEETS_SPREADSHEET_ID")
    worksheet_name = os.getenv("GSHEETS_WORKSHEET", "Sheet1")
    config_path = os.getenv("GSHEETS_CONFIG")

    if config_path and Path(config_path).exists():
        with open(config_path, "r", encoding="utf-8") as f:
            config.update(json.load(f))
    else:
        default_config_path = Path(input_dir) / "gsheets_config.json"
        if default_config_path.exists():
            with open(default_config_path, "r", encoding="utf-8") as f:
                config.update(json.load(f))

    config.setdefault("credentials_path", credentials_path)
    config.setdefault("spreadsheet_id", spreadsheet_id)
    config.setdefault("worksheet", worksheet_name)

    if log_callback and (config_path or (Path(input_dir) / "gsheets_config.json").exists()):
        log_callback(f"Google Sheets config loaded (credentials_path={config.get('credentials_path')}).")
    return config


def maybe_update_gsheet(records, input_dir, log_callback=None):
    """
    Append processing records to Google Sheet if configuration is present.
    Supports configuration via environment variables or a JSON config file.
    """
    if not records:
        return

    config = _load_gsheet_config(input_dir, log_callback=log_callback)
    credentials_path = config.get("credentials_path")
    spreadsheet_id = config.get("spreadsheet_id")
    worksheet_name = config.get("worksheet", "Sheet1")

    if not credentials_path or not spreadsheet_id:
        if log_callback:
            log_callback(
                "Google Sheets update skipped (missing credentials_path or spreadsheet_id)."
            )
        return

    import gspread

    client = gspread.service_account(filename=credentials_path)
    worksheet = client.open_by_key(spreadsheet_id).worksheet(worksheet_name)

    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(
            ["timestamp", "file", "with_discount", "without_discount", "batch_folder"]
        )

    rows = [
        [
            r["timestamp"],
            r["file"],
            r["with_discount"],
            r["without_discount"],
            r["batch_folder"],
        ]
        for r in records
    ]
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")

    if log_callback:
        log_callback(f"Google Sheets updated: {spreadsheet_id} ({worksheet_name})")
