"""Review Workflow Automation

Automates the review and approval workflow for product listing data.
Processes CSV files through a configurable rule engine to:
- Filter records by reviewer assignment
- Categorize entries by source channel (Vendor vs Direct)
- Auto-generate approval files for qualifying records
- Flag anomalies for manual review
- Route records back to quality check when needed

The rule engine is extensible — new business rules can be added
as simple mask + output builder pairs without modifying core logic.
"""

import os
import glob
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Tuple

import pandas as pd
from datetime import datetime
import shutil


# =========================
# Config (Dataclasses)
# =========================
@dataclass(frozen=True)
class Columns:
    model_id: str = "Model ID"
    shop_id: str = "Shop ID"
    item_id: str = "Item ID"

    discount_price: str = "Discount Price"
    promo_price: str = "Promo Price"
    purchase_limit: str = "Purchase Limit"
    promo_stock: str = "Promo Stock"
    current_price: str = "Current Price"
    reference_price: str = "Reference Price"
    price_check_soft: str = "Price Check (Soft)"
    max_entry_price: str = "Max Entry Price"

    entry_source: str = "Entry Source"
    creator: str = "Creator"
    item_details: str = "Item Details"

    secondary_reviewer: str = "Secondary Reviewer"
    account_manager: str = "Account Manager"


@dataclass(frozen=True)
class Config:
    input_dir: str
    file_glob: str
    output_root_dir: str

    cols: Columns = Columns()

    reviewer_keyword: str = "analyst.user"
    external_keyword: str = "external"
    dash_token: str = "-"

    source_vendor_token: str = "SOURCE_VENDOR"
    source_direct_token: str = "SOURCE_DIRECT"

    exclude_external_cases_from_main_flow: bool = True

    # Session extraction pattern: _<sid>_<YYYY_MM_DD_HH_MM_SS>
    session_re: re.Pattern = re.compile(r"_(\d{10,})_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})")

    warn_print_limit: int = 20

    read_csv_kwargs: dict = field(default_factory=lambda: dict(dtype=str, keep_default_na=False, encoding="utf-8-sig"))

    @property
    def approve_file_cols(self) -> List[str]:
        return [
            self.cols.model_id,
            "Action",
            "Reason",
            self.cols.promo_price,
            self.cols.discount_price,
            self.cols.promo_stock,
        ]

    @property
    def return_for_review_cols(self) -> List[str]:
        return [self.cols.model_id, "Action", "Reason"]

    @property
    def external_case_cols(self) -> List[str]:
        return [
            self.cols.shop_id,
            self.cols.item_id,
            self.cols.model_id,
            self.cols.discount_price,
            self.cols.promo_price,
            self.cols.purchase_limit,
            self.cols.promo_stock,
            self.cols.current_price,
            self.cols.reference_price,
        ]

    @property
    def required_cols(self) -> List[str]:
        return sorted(
            set(
                [
                    self.cols.model_id,
                    self.cols.shop_id,
                    self.cols.item_id,
                    self.cols.entry_source,
                    self.cols.secondary_reviewer,
                    self.cols.account_manager,
                    self.cols.discount_price,
                    self.cols.promo_price,
                    self.cols.purchase_limit,
                    self.cols.promo_stock,
                    self.cols.current_price,
                    self.cols.reference_price,
                    self.cols.price_check_soft,
                    self.cols.max_entry_price,
                    self.cols.creator,
                    self.cols.item_details,
                ]
            )
        )


# =========================
# Helpers
# =========================
def ensure_cols(df: pd.DataFrame, cols: List[str], file_path: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[Missing columns] {missing} in file: {file_path}")


def norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def out_path(session_dir: str, stem: str, suffix: str) -> str:
    return os.path.join(session_dir, f"{stem}__{suffix}.csv")


def reviewer_ok_series(df: pd.DataFrame, cfg: Config) -> pd.Series:
    r = norm(df[cfg.cols.secondary_reviewer])
    return r.eq("") | r.str.contains(cfg.reviewer_keyword, case=False, na=False)


def has_external_series(df: pd.DataFrame, cfg: Config) -> pd.Series:
    rm = norm(df[cfg.cols.account_manager])
    return rm.str.contains(cfg.external_keyword, case=False, na=False)


def is_dash_series(df: pd.DataFrame, cfg: Config) -> pd.Series:
    rm = norm(df[cfg.cols.account_manager])
    return rm.eq(cfg.dash_token)


def source_upper(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return norm(df[cfg.cols.entry_source]).str.upper()


def extract_session_id_from_filename(file_path: str, cfg: Config) -> str:
    stem = os.path.splitext(os.path.basename(file_path))[0]
    m = cfg.session_re.search(stem)
    return m.group(1) if m else "unknown_session"


def make_session_output_dir(cfg: Config, run_ts: str, sid: str) -> str:
    session_dir = os.path.join(cfg.output_root_dir, f"{run_ts}__{sid}")
    os.makedirs(session_dir, exist_ok=True)
    return session_dir


def _unique_path(dst: str) -> str:
    """If destination exists, generate a uniquely numbered path."""
    if not os.path.exists(dst):
        return dst

    base, ext = os.path.splitext(dst)
    for i in range(1, 10_000):
        cand = f"{base} ({i}){ext}"
        if not os.path.exists(cand):
            return cand
    raise FileExistsError(f"Too many name collisions: {dst}")


def safe_move(src: str, dst_dir: str, mode: str = "rename") -> str | None:
    """
    Move source file into destination directory.

    mode:
      - "skip": if destination exists, do nothing, return None
      - "rename": if destination exists, auto-rename
      - "overwrite": if destination exists, remove it then move
    """
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, os.path.basename(src))

    if os.path.exists(dst):
        if mode == "skip":
            return None
        if mode == "rename":
            dst = _unique_path(dst)
        elif mode == "overwrite":
            os.remove(dst)
        else:
            raise ValueError("mode must be 'skip' | 'rename' | 'overwrite'")

    return shutil.move(src, dst)


# =========================
# Writer functions
# =========================
def write_csv_if_any(df: pd.DataFrame, path: str) -> None:
    if df.empty:
        return
    df.to_csv(path, index=False, encoding="utf-8-sig")


def build_approve_df(df_rows: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    model_id = norm(df_rows[cfg.cols.model_id])
    out = pd.DataFrame(
        {
            cfg.cols.model_id: model_id,
            "Action": "approve",
            "Reason": "",
            cfg.cols.promo_price: "",
            cfg.cols.discount_price: "",
            cfg.cols.promo_stock: "",
        }
    )
    return out[cfg.approve_file_cols]


def build_return_for_review_df(df_rows: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    model_id = norm(df_rows[cfg.cols.model_id])
    out = pd.DataFrame(
        {
            cfg.cols.model_id: model_id,
            "Action": "return for review",
            "Reason": "",
        }
    )
    return out[cfg.return_for_review_cols]


def build_external_case_df(df_rows: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    return df_rows[cfg.external_case_cols].copy()


def build_direct_anomaly_df(df_rows: pd.DataFrame, filename: str, cfg: Config) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            cfg.cols.shop_id: norm(df_rows[cfg.cols.shop_id]),
            cfg.cols.item_id: norm(df_rows[cfg.cols.item_id]),
            cfg.cols.model_id: norm(df_rows[cfg.cols.model_id]),
            cfg.cols.discount_price: norm(df_rows[cfg.cols.discount_price]),
            cfg.cols.promo_price: norm(df_rows[cfg.cols.promo_price]),
            "Action (placeholder)": "",
            cfg.cols.purchase_limit: norm(df_rows[cfg.cols.purchase_limit]),
            cfg.cols.promo_stock: norm(df_rows[cfg.cols.promo_stock]),
            cfg.cols.current_price: norm(df_rows[cfg.cols.current_price]),
            cfg.cols.reference_price: norm(df_rows[cfg.cols.reference_price]),
            cfg.cols.price_check_soft: norm(df_rows[cfg.cols.price_check_soft]),
            cfg.cols.max_entry_price: norm(df_rows[cfg.cols.max_entry_price]),
            cfg.cols.creator: norm(df_rows[cfg.cols.creator]),
            cfg.cols.item_details: norm(df_rows[cfg.cols.item_details]),
            cfg.cols.secondary_reviewer: norm(df_rows[cfg.cols.secondary_reviewer]),
            cfg.cols.account_manager: norm(df_rows[cfg.cols.account_manager]),
            "Source File": filename,
            "Row Index (0-based)": df_rows.index,
        }
    )

    is_dash = is_dash_series(df_rows, cfg)
    has_ext = has_external_series(df_rows, cfg)
    out["Why"] = [
        ("dash" if d else "") + ("|external" if e else "")
        for d, e in zip(is_dash.tolist(), has_ext.tolist())
    ]
    return out


# =========================
# Rule Engine
# =========================
@dataclass
class Rule:
    name: str
    mask_fn: Callable[[pd.DataFrame, Config], pd.Series]
    output_suffix: str
    build_output_fn: Callable[[pd.DataFrame, Config], pd.DataFrame]
    exclude_from_next: bool


def apply_rules(
    df: pd.DataFrame, rules: List[Rule], cfg: Config
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, Dict[str, int]]:
    remaining = df.copy()
    outputs: Dict[str, pd.DataFrame] = {}
    counts: Dict[str, int] = {}

    for rule in rules:
        if remaining.empty:
            counts[rule.name] = 0
            continue

        mask = rule.mask_fn(remaining, cfg)
        picked = remaining.loc[mask].copy()
        counts[rule.name] = int(mask.sum())

        if not picked.empty:
            outputs[rule.output_suffix] = rule.build_output_fn(picked, cfg)

        if rule.exclude_from_next and mask.any():
            remaining = remaining.loc[~mask].copy()

    return outputs, remaining, counts


# =========================
# Main per-file processing
# =========================
def process_one_file(fp: str, session_dir: str, cfg: Config) -> None:
    filename = os.path.basename(fp)
    stem, _ = os.path.splitext(filename)
    print(f"\n=== Processing: {filename} ===")

    df = pd.read_csv(fp, **cfg.read_csv_kwargs)

    # Validate columns used anywhere downstream
    ensure_cols(df, cfg.required_cols, fp)

    # ----- Rule 0: External cases (highest priority) -----
    def mask_external_cases(d: pd.DataFrame, c: Config) -> pd.Series:
        return reviewer_ok_series(d, c) & has_external_series(d, c)

    external_rule = Rule(
        name="external_cases",
        mask_fn=mask_external_cases,
        output_suffix="external_cases",
        build_output_fn=build_external_case_df,
        exclude_from_next=cfg.exclude_external_cases_from_main_flow,
    )

    outputs_0, df_after_external, counts_0 = apply_rules(df, [external_rule], cfg)

    # ----- Gate: Reviewer filter for main flow -----
    df_main = df_after_external.loc[reviewer_ok_series(df_after_external, cfg)].copy()
    main_reviewer_count = len(df_main)

    if df_main.empty:
        for suffix, odf in outputs_0.items():
            p = out_path(session_dir, stem, suffix)
            write_csv_if_any(odf, p)
            if not odf.empty:
                print(f"Saved: {p} ({len(odf)} rows)")
        print(f"[STATS] reviewer_ok(main)=0, external_cases={counts_0['external_cases']}")
        return

    # ----- Split VENDOR and DIRECT within main flow -----
    ns = source_upper(df_main, cfg)
    vendor_df = df_main.loc[ns.str.contains(cfg.source_vendor_token, na=False)].copy()
    direct_df = df_main.loc[ns.str.contains(cfg.source_direct_token, na=False)].copy()

    # VENDOR output (approve all)
    approve_out = build_approve_df(vendor_df, cfg) if not vendor_df.empty else pd.DataFrame(columns=cfg.approve_file_cols)

    # DIRECT: normal vs anomaly (based on Account Manager field)
    if direct_df.empty:
        return_for_review_out = pd.DataFrame(columns=cfg.return_for_review_cols)
        anomalies_out = pd.DataFrame()
        direct_normal_df = direct_df
        direct_anomaly_df = direct_df
    else:
        direct_is_dash = is_dash_series(direct_df, cfg)
        direct_has_ext = has_external_series(direct_df, cfg)

        direct_anomaly_mask = direct_is_dash | direct_has_ext
        direct_anomaly_df = direct_df.loc[direct_anomaly_mask].copy()
        direct_normal_df = direct_df.loc[~direct_anomaly_mask].copy()

        return_for_review_out = build_return_for_review_df(direct_normal_df, cfg) if not direct_normal_df.empty else pd.DataFrame(columns=cfg.return_for_review_cols)
        anomalies_out = build_direct_anomaly_df(direct_anomaly_df, filename, cfg) if not direct_anomaly_df.empty else pd.DataFrame()

    # ----- Write outputs (per file) -----
    for suffix, odf in outputs_0.items():
        p = out_path(session_dir, stem, suffix)
        write_csv_if_any(odf, p)
        if not odf.empty:
            print(f"Saved: {p} ({len(odf)} rows)")

    if not approve_out.empty:
        p = out_path(session_dir, stem, "approve_from_vendor")
        write_csv_if_any(approve_out, p)
        print(f"Saved: {p} ({len(approve_out)} rows)")

    if not return_for_review_out.empty:
        p = out_path(session_dir, stem, "return_for_review")
        write_csv_if_any(return_for_review_out, p)
        print(f"Saved: {p} ({len(return_for_review_out)} rows)")

    if anomalies_out is not None and not anomalies_out.empty:
        p = out_path(session_dir, stem, "direct_anomalies")
        write_csv_if_any(anomalies_out, p)
        print(f"[WARNING] Saved: {p} ({len(anomalies_out)} rows)")
        print("\n--- Warning Preview ---")
        print(anomalies_out.head(cfg.warn_print_limit).to_string(index=False))

    # ----- Stats -----
    print("\n--- Stats ---")
    print("total rows:", len(df))
    print("external_cases:", counts_0["external_cases"])
    print("reviewer_ok(main):", main_reviewer_count)
    print("vendor(main):", len(vendor_df))
    print("direct(main):", len(direct_df))
    print("direct_normal -> return_for_review:", len(direct_normal_df))
    print("direct_anomaly:", len(direct_anomaly_df))


def main():
    input_dir = r"./sample_data"
    cfg = Config(
        input_dir=input_dir,
        file_glob=os.path.join(input_dir, "*PendingReview*.csv"),
        output_root_dir=os.path.join(input_dir, "output"),
        exclude_external_cases_from_main_flow=True,
        reviewer_keyword="analyst.user",
        external_keyword="external",
    )

    os.makedirs(cfg.output_root_dir, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    files = sorted(glob.glob(cfg.file_glob))
    if not files:
        print(f"No files matched: {cfg.file_glob}")
        return

    sid_to_dir: Dict[str, str] = {}

    for fp in files:
        sid = extract_session_id_from_filename(fp, cfg)
        if sid not in sid_to_dir:
            sid_to_dir[sid] = make_session_output_dir(cfg, run_ts, sid)
            print("Session output directory:", sid_to_dir[sid])

        session_dir = sid_to_dir[sid]

        processed_dir = os.path.join(session_dir, "_processed_inputs")
        failed_dir = os.path.join(session_dir, "_failed_inputs")

        try:
            process_one_file(fp, session_dir, cfg)

            moved_to = safe_move(fp, processed_dir, mode="rename")
            if moved_to:
                print(f"[MOVED] {fp} -> {moved_to}")

        except Exception as e:
            try:
                moved_to = safe_move(fp, failed_dir, mode="rename")
                if moved_to:
                    print(f"[FAILED MOVED] {fp} -> {moved_to}")
            except Exception as move_err:
                print(f"[MOVE FAILED] {fp} ({move_err})")

            print(f"[ERROR] processing {fp}: {e}")


if __name__ == "__main__":
    main()
