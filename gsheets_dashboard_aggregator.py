"""Google Sheets Dashboard Aggregator

Aggregates data from multiple Google Sheets in a Google Drive folder
into a centralized dashboard spreadsheet with automated summaries.

What this does:
- Lists ALL Google Sheets files inside a specified Google Drive folder
- Reads every worksheet (tab) in each file
- Extracts key columns: Request Date, Reason, Completion status
- Parses dates with a configurable report year (for date fields without year)
- Builds dashboard tables and writes them back to a Dashboard spreadsheet

Dashboard output tabs:
- Monthly_Summary: Monthly request volume and approval rates
- Reason_Summary: Breakdown of request reasons with approval rates
- Reason_By_Month: Cross-tabulation of reasons by month
- Data_Quality_Issues: Logs of parsing errors and missing data

Prerequisites:
1) Google Cloud project with Drive API and Sheets API enabled
2) Service Account with JSON key
3) Source folder and Dashboard spreadsheet shared with the service account
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------
# Config
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

DEFAULT_YEAR = 2025
DEFAULT_HEADER_ROW = 5
DEFAULT_TIMEZONE = "Asia/Singapore"
APPROVED_VALUE = "Approved"

DASH_TABS = {
    "monthly": "Monthly_Summary",
    "reason": "Reason_Summary",
    "reason_by_month": "Reason_By_Month",
    "issues": "Data_Quality_Issues",
}


# -----------------------------
# Helpers: header normalization
# -----------------------------

def _norm_key(s: str) -> str:
    """Normalize header keys to match even if users change spacing/case."""
    s = (s or "").strip().lower()
    return re.sub(r"[^a-z0-9]", "", s)


# Accept a few reasonable variations for flexible header matching
HEADER_ALIASES = {
    "request_date": {"requestdate", "requestdt", "reqdate", "requestedate", "Request date"},
    "reason": {"reason", "reasons", "Reason"},
    "completion": {"completion", "status", "complete", "completed", "Completion"},
}

NORMALIZED_HEADER_ALIASES = {
    canonical: {_norm_key(alias) for alias in alias_set}
    for canonical, alias_set in HEADER_ALIASES.items()
}


def map_headers(headers: List[str]) -> Dict[str, str]:
    """Return mapping from canonical -> actual header in sheet."""
    norm_to_actual = {_norm_key(h): h for h in headers}
    out: Dict[str, str] = {}

    for canonical, alias_set in NORMALIZED_HEADER_ALIASES.items():
        for alias in alias_set:
            if alias in norm_to_actual:
                out[canonical] = norm_to_actual[alias]
                break

    return out


# -----------------------------
# Helpers: date parsing
# -----------------------------

MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_sheet_title_mmdd(title: str, year: int) -> Optional[dt.date]:
    """Parse worksheet title like 'MM/DD' or 'M/D' into a date in the given year."""
    if not title:
        return None
    m = re.match(r"^\s*(\d{1,2})\s*[\/\-]\s*(\d{1,2})\s*$", title)
    if not m:
        return None

    mm = int(m.group(1))
    dd = int(m.group(2))
    try:
        return dt.date(year, mm, dd)
    except ValueError:
        return None


def parse_request_date_day_month(s: str, year: int) -> Optional[dt.date]:
    """Parse Request Date like '1 Jan' / '2 January' / '01 Feb' into date(year, month, day)."""
    if not s:
        return None

    raw = str(s).strip()
    if not raw:
        return None

    m = re.match(r"^\s*(\d{1,2})\s*[-/ ]\s*([A-Za-z]{3,9})\s*$", raw)
    if not m:
        m2 = re.match(r"^\s*(\d{1,2})([A-Za-z]{3,9})\s*$", raw)
        if not m2:
            return None
        day = int(m2.group(1))
        mon = m2.group(2)
    else:
        day = int(m.group(1))
        mon = m.group(2)

    mon_key = mon.strip().lower()
    mon_key = mon_key[:3] if mon_key not in MONTH_MAP else mon_key

    if mon_key not in MONTH_MAP:
        mon_key_full = mon.strip().lower()
        if mon_key_full in MONTH_MAP:
            month = MONTH_MAP[mon_key_full]
        else:
            return None
    else:
        month = MONTH_MAP[mon_key]

    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def normalize_reason(value: str) -> str:
    if value is None:
        return "(blank)"
    normalized = re.sub(r"\s+", " ", str(value)).strip().lower()
    normalized = normalized.rstrip("/")
    return normalized if normalized else "(blank)"


# -----------------------------
# Data Quality Issues
# -----------------------------

@dataclass
class Issue:
    level: str  # "worksheet" or "row"
    source_file_name: str
    source_file_id: str
    source_sheet: str
    row_index: Optional[int]
    field: str
    raw_value: str
    message: str


def issues_to_df(issues: List[Issue]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "level": i.level,
                "source_file_name": i.source_file_name,
                "source_file_id": i.source_file_id,
                "source_sheet": i.source_sheet,
                "row_index": i.row_index,
                "field": i.field,
                "raw_value": i.raw_value,
                "message": i.message,
            }
            for i in issues
        ]
    )


# -----------------------------
# Google API: Drive list
# -----------------------------

def build_services(service_account_path: str):
    creds = Credentials.from_service_account_file(service_account_path, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds)
    gc = gspread.authorize(creds)
    return drive, gc


def list_spreadsheets_in_folder(drive, folder_id: str) -> List[Dict[str, str]]:
    """Return [{id, name}] of Google Sheets files in the folder."""
    q = (
        f"'{folder_id}' in parents and "
        "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    )

    files: List[Dict[str, str]] = []
    page_token = None

    while True:
        resp = (
            drive.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, createdTime)",
                pageToken=page_token,
                pageSize=1000,
            )
            .execute()
        )

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


# -----------------------------
# Read sheets -> DataFrame
# -----------------------------

def _retry(operation, *, attempts: int = 3, backoff_s: float = 1.0, description: str = "operation"):
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= attempts:
                raise
            sleep_for = backoff_s * (2 ** (attempt - 1))
            logger.warning("Retrying %s after error: %s (sleep %.1fs)", description, exc, sleep_for)
            time.sleep(sleep_for)


def values_to_df(
    values: List[List[str]],
    *,
    header_row_index: int,
    include_row_index: bool = True,
) -> Tuple[pd.DataFrame, List[str]]:
    if not values or len(values) <= header_row_index:
        return pd.DataFrame(), []

    headers = [str(h).strip() for h in values[header_row_index]]
    rows = values[header_row_index + 1:]

    # Normalize row lengths
    max_len = max(len(headers), max((len(r) for r in rows), default=0))
    if len(headers) < max_len:
        headers += [f"__extra_{i}" for i in range(len(headers), max_len)]

    norm_rows = []
    for r in rows:
        rr = [str(x) if x is not None else "" for x in r]
        if len(rr) < max_len:
            rr += [""] * (max_len - len(rr))
        norm_rows.append(rr[:max_len])

    df = pd.DataFrame(norm_rows, columns=headers)
    if include_row_index:
        row_offset = header_row_index + 2
        df.insert(0, "__source_row_index", [row_offset + i for i in range(len(df))])

    # Drop rows that are completely empty
    if not df.empty:
        df = df.loc[~(df.apply(lambda r: all((str(x).strip() == "") for x in r), axis=1))].copy()

    return df, headers


def read_spreadsheet_all_worksheets(
    gc: gspread.Client,
    spreadsheet_id: str,
    spreadsheet_name: str,
    year: int,
    dashboard_id_to_exclude: Optional[str],
    issues: List[Issue],
    header_row_index: int,
    sheet_name_contains: str = "",
) -> pd.DataFrame:
    """Read all worksheets in one spreadsheet and return concatenated rows."""

    if dashboard_id_to_exclude and spreadsheet_id == dashboard_id_to_exclude:
        logger.info("Skipping dashboard spreadsheet found inside folder: %s", spreadsheet_id)
        return pd.DataFrame()

    try:
        sh = _retry(lambda: gc.open_by_key(spreadsheet_id), description=f"open_spreadsheet({spreadsheet_id})")
    except Exception as e:
        issues.append(
            Issue(
                level="worksheet",
                source_file_name=spreadsheet_name,
                source_file_id=spreadsheet_id,
                source_sheet="(spreadsheet)",
                row_index=None,
                field="open",
                raw_value="",
                message=f"Failed to open spreadsheet: {e}",
            )
        )
        return pd.DataFrame()

    out_parts: List[pd.DataFrame] = []
    needle = sheet_name_contains.casefold().strip()

    for ws in sh.worksheets():
        if needle and needle not in ws.title.casefold():
            continue

        title = ws.title
        sheet_date = parse_sheet_title_mmdd(title, year)
        if sheet_date is None:
            issues.append(
                Issue(
                    level="worksheet",
                    source_file_name=spreadsheet_name,
                    source_file_id=spreadsheet_id,
                    source_sheet=title,
                    row_index=None,
                    field="sheet_title",
                    raw_value=title,
                    message="Worksheet title is not MM/DD; sheet_date fallback will be unavailable.",
                )
            )

        try:
            values = _retry(lambda: ws.get_all_values(), description=f"get_all_values({spreadsheet_id}:{title})")
        except Exception as e:
            issues.append(
                Issue(
                    level="worksheet",
                    source_file_name=spreadsheet_name,
                    source_file_id=spreadsheet_id,
                    source_sheet=title,
                    row_index=None,
                    field="read",
                    raw_value="",
                    message=f"Failed to read worksheet: {e}",
                )
            )
            continue

        df_raw, headers = values_to_df(values, header_row_index=header_row_index)
        if df_raw.empty:
            continue

        header_map = map_headers(headers)
        missing = [k for k in ("reason", "completion") if k not in header_map]

        if missing:
            issues.append(
                Issue(
                    level="worksheet",
                    source_file_name=spreadsheet_name,
                    source_file_id=spreadsheet_id,
                    source_sheet=title,
                    row_index=None,
                    field="header",
                    raw_value=" | ".join(headers[:20]),
                    message=f"Missing required columns: {missing}. This worksheet will be skipped.",
                )
            )
            continue

        df = df_raw[[header_map["reason"], header_map["completion"]]].copy()
        df.columns = ["reason_raw", "completion_raw"]
        df["source_row_index"] = df_raw.get("__source_row_index")
        df["source_file_name"] = spreadsheet_name
        df["source_file_id"] = spreadsheet_id
        df["source_sheet"] = title
        df["sheet_date"] = pd.to_datetime(sheet_date) if sheet_date else pd.NaT
        df["approved_flag"] = (
                df["completion_raw"].astype(str).str.strip().str.casefold()
                == APPROVED_VALUE.casefold()
        )
        df["reason"] = df["reason_raw"].map(normalize_reason)

        out_parts.append(df)

    if not out_parts:
        return pd.DataFrame()

    return pd.concat(out_parts, ignore_index=True)


# -----------------------------
# Cleaning and feature building
# -----------------------------

def clean_and_enrich(df: pd.DataFrame, year: int, issues: List[Issue]) -> pd.DataFrame:
    if df.empty:
        return df

    for col in ["reason_raw", "completion_raw"]:
        df[col] = df[col].astype(str).map(lambda x: x.strip())
        df.loc[df[col].isin(["None", "nan", "NaN"]), col] = ""

    # event_date priority: sheet_date, then file_created_time
    df["file_created_time"] = pd.to_datetime(df.get("file_created_time", pd.NaT), errors="coerce")
    df["sheet_date"] = pd.to_datetime(df.get("sheet_date", pd.NaT), errors="coerce")

    # Build event_date with consistent precision
    event = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    event = event.combine_first(df["sheet_date"])
    event = event.combine_first(df["file_created_time"])

    df["event_date"] = event

    no_date_mask = df["event_date"].isna()
    if no_date_mask.any():
        sub = df.loc[no_date_mask, ["source_file_name", "source_file_id", "source_sheet"]].head(200)
        for i, row in sub.iterrows():
            row_index = df.at[i, "source_row_index"] if "source_row_index" in df.columns else int(i) + 6
            issues.append(
                Issue(
                    level="row",
                    source_file_name=str(row["source_file_name"]),
                    source_file_id=str(row["source_file_id"]),
                    source_sheet=str(row["source_sheet"]),
                    row_index=row_index,
                    field="file_created_time",
                    raw_value="",
                    message="No file createdTime available; row excluded from month stats.",
                )
            )

    df_valid = df.loc[~no_date_mask].copy()
    df_valid["month"] = df_valid["event_date"].dt.to_period("M").astype(str)

    return df_valid


# -----------------------------
# Aggregations
# -----------------------------

def build_monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["month", "total_requests", "approved_count", "approved_rate", "distinct_reason"])

    g = df.groupby("month", dropna=False)
    out = pd.DataFrame(
        {
            "total_requests": g.size(),
            "approved_count": g["approved_flag"].sum().astype(int),
            "distinct_reason": g["reason"].nunique(),
        }
    ).reset_index()

    out["approved_rate"] = (out["approved_count"] / out["total_requests"]).round(4)
    out = out.sort_values("month", ascending=True).reset_index(drop=True)
    return out[["month", "total_requests", "approved_count", "approved_rate", "distinct_reason"]]


def build_reason_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["reason", "count", "approved_count", "approved_rate"])

    g = df.groupby("reason", dropna=False)
    out = pd.DataFrame(
        {
            "count": g.size(),
            "approved_count": g["approved_flag"].sum().astype(int),
        }
    ).reset_index()

    out["approved_rate"] = (out["approved_count"] / out["count"]).round(4)
    out = out.sort_values(["count", "reason"], ascending=[False, True]).reset_index(drop=True)
    return out[["reason", "count", "approved_count", "approved_rate"]]


def build_reason_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["month", "reason", "count", "approved_count", "approved_rate"])

    g = df.groupby(["month", "reason"], dropna=False)
    out = pd.DataFrame(
        {
            "count": g.size(),
            "approved_count": g["approved_flag"].sum().astype(int),
        }
    ).reset_index()

    out["approved_rate"] = (out["approved_count"] / out["count"]).round(4)
    out = out.sort_values(["month", "count", "reason"], ascending=[True, False, True]).reset_index(drop=True)
    return out[["month", "reason", "count", "approved_count", "approved_rate"]]


# -----------------------------
# Write back to dashboard
# -----------------------------

def ensure_worksheet(sh: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 20) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
        return ws
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        return ws


def write_df(ws: gspread.Worksheet, df: pd.DataFrame, *, clear_first: bool = True, max_chunk_rows: int = 2000) -> None:
    if clear_first:
        ws.clear()

    if df is None or df.empty:
        ws.update([["(no data)"]])
        return

    values = [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()

    try:
        ws.resize(rows=max(len(values), 2), cols=max(len(df.columns), 2))
    except Exception:
        pass

    # Chunk updates to reduce request size
    start_row = 1
    while values:
        chunk = values[:max_chunk_rows]
        values = values[max_chunk_rows:]

        cell_range = f"A{start_row}"
        ws.update(cell_range, chunk, value_input_option="RAW")
        start_row += len(chunk)


# -----------------------------
# Main
# -----------------------------

def run(
    folder_id: str,
    dashboard_id: str,
    service_account_path: str,
    year: int,
    header_row: int,
    timezone: str,
    sheet_name_contains: str = "",
) -> None:
    drive, gc = build_services(service_account_path)

    files = _retry(
        lambda: list_spreadsheets_in_folder(drive, folder_id),
        description="list_spreadsheets_in_folder",
    )
    logger.info("Found %d spreadsheets in folder.", len(files))

    issues: List[Issue] = []
    all_parts: List[pd.DataFrame] = []

    for f in files:
        sid = f["id"]
        name = f.get("name", sid)
        logger.info("Reading spreadsheet: %s (%s)", name, sid)

        df_one = _retry(
            lambda: read_spreadsheet_all_worksheets(
                gc=gc,
                spreadsheet_id=sid,
                spreadsheet_name=name,
                year=year,
                dashboard_id_to_exclude=dashboard_id,
                issues=issues,
                header_row_index=header_row - 1,
                sheet_name_contains=sheet_name_contains,
            ),
            description=f"read_spreadsheet_all_worksheets({sid})",
        )

        if not df_one.empty:
            created = pd.to_datetime(f.get("createdTime", None), utc=True, errors="coerce")

            if pd.notna(created):
                try:
                    created_local = created.tz_convert(timezone).tz_localize(None)
                except Exception:
                    logger.warning("Invalid timezone %s; falling back to UTC.", timezone)
                    created_local = created.tz_convert("UTC").tz_localize(None)
            else:
                created_local = pd.NaT

            df_one["file_created_time"] = created_local

            all_parts.append(df_one)

    if not all_parts:
        logger.warning("No data read from folder.")
        df_all_raw = pd.DataFrame(columns=[
            "reason_raw", "completion_raw",
            "source_file_name", "source_file_id", "source_sheet", "sheet_date"
        ])
    else:
        df_all_raw = pd.concat(all_parts, ignore_index=True)

    logger.info("Total raw rows loaded: %d", len(df_all_raw))

    df_clean = clean_and_enrich(df_all_raw, year=year, issues=issues)
    logger.info("Rows with usable event_date in %d: %d", year, len(df_clean))

    monthly = build_monthly_summary(df_clean)
    reason = build_reason_summary(df_clean)
    reason_by_month = build_reason_by_month(df_clean)
    issues_df = issues_to_df(issues)

    # Write to dashboard
    dash = _retry(lambda: gc.open_by_key(dashboard_id), description="open_dashboard")
    ws_monthly = ensure_worksheet(dash, DASH_TABS["monthly"], rows=max(len(monthly) + 50, 200), cols=max(len(monthly.columns) + 5, 10))
    ws_reason = ensure_worksheet(dash, DASH_TABS["reason"], rows=max(len(reason) + 200, 500), cols=max(len(reason.columns) + 5, 10))
    ws_reason_by_month = ensure_worksheet(dash, DASH_TABS["reason_by_month"], rows=max(len(reason_by_month) + 200, 500), cols=max(len(reason_by_month.columns) + 5, 10))
    ws_issues = ensure_worksheet(dash, DASH_TABS["issues"], rows=max(len(issues_df) + 200, 500), cols=max(len(issues_df.columns) + 5, 12))

    logger.info("Writing dashboard tabs...")
    write_df(ws_monthly, monthly)
    write_df(ws_reason, reason)
    write_df(ws_reason_by_month, reason_by_month)
    write_df(ws_issues, issues_df)

    logger.info("Done. Dashboard updated: %s", dashboard_id)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate Google Sheets in a Drive folder into a dashboard.")
    p.add_argument("--service-account", required=False, default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""), help="Path to service account JSON")
    p.add_argument("--folder-id", required=False, default=os.getenv("GS_FOLDER_ID", ""), help="Google Drive folder ID containing source spreadsheets")
    p.add_argument("--dashboard-id", required=False, default=os.getenv("GS_DASHBOARD_ID", ""), help="Dashboard spreadsheet ID to write results")
    p.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Report year, default 2025")
    p.add_argument("--header-row", type=int, default=DEFAULT_HEADER_ROW, help="1-based header row index (default 5)")
    p.add_argument("--timezone", default=os.getenv("GS_TIMEZONE", DEFAULT_TIMEZONE), help="Timezone for file createdTime")
    p.add_argument("--sheet-name-contains", default="", help="Only read worksheets whose title contains this value (case-insensitive)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not args.service_account or not args.folder_id or not args.dashboard_id:
        raise SystemExit(
            "Missing required inputs. Provide --service-account, --folder-id, --dashboard-id "
            "or set env vars GOOGLE_SERVICE_ACCOUNT_JSON / GS_FOLDER_ID / GS_DASHBOARD_ID."
        )
    if not os.path.isfile(args.service_account):
        raise SystemExit(f"Service account file not found: {args.service_account}")
    if args.header_row < 1:
        raise SystemExit("--header-row must be a 1-based row index (>= 1).")

    run(
        folder_id=args.folder_id,
        dashboard_id=args.dashboard_id,
        service_account_path=args.service_account,
        year=args.year,
        header_row=args.header_row,
        timezone=args.timezone,
        sheet_name_contains=args.sheet_name_contains,
    )
