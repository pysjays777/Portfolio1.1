"""CSV Split Tool (GUI)

A desktop GUI application for splitting large CSV/Excel files into
manageable parts with configurable row limits, column selection,
and header renaming options.

Use cases supported:
1) Keep original output headers (default)
2) Rename output headers at export time, via GUI:
   - Replace spaces with underscore
   - Custom JSON mapping (source -> output)

Features:
- Split output into parts of configurable row count (default 950,000)
- Per-input-file outputs are NOT combined (each input file gets its own part files)
- Output filenames use extracted Session ID + a shortened input filename stem
- Supports CSV, TXT, and Excel formats
"""

from __future__ import annotations

import os
import glob
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Iterable, Tuple

import pandas as pd


# =========================
# Config + Context
# =========================
@dataclass
class ToolConfig:
    input_dir: str = ""
    pattern: str = "*.*"
    output_dir: str = ""  # if blank -> <input_dir>/output

    # Split definition
    part_rows: int = 950_000

    # Reading
    csv_encoding: str = "utf-8-sig"
    csv_dtype: str = "str"
    csv_keep_default_na: bool = False

    # Chunk read size (tune for memory vs speed)
    read_chunksize: int = 200_000

    # Selected columns (GUI sets these)
    selected_columns: List[str] = field(default_factory=list)

    # Output header behavior
    rename_mode: str = "keep"  # keep | underscore | custom
    rename_map: Dict[str, str] = field(default_factory=dict)


@dataclass
class RunContext:
    run_ts: str
    session_dir: str
    matched_files: List[str] = field(default_factory=list)

    # Summary
    total_input_rows: int = 0
    total_output_rows: int = 0
    output_parts: int = 0
    files_processed: int = 0


# =========================
# Filename helpers
# =========================
SESSION_RE = re.compile(r"_(\d{10,})_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})")


def extract_session_id(fp: str) -> str:
    """Best-effort extract session ID from filename. Falls back to first 10+ digit run."""
    stem = os.path.splitext(os.path.basename(fp))[0]
    m = SESSION_RE.search(stem)
    if m:
        return m.group(1)

    nums = re.findall(r"\d{10,}", stem)
    if nums:
        return nums[0]

    return "unknown_session"


def safe_name(s: str, max_len: int = 50) -> str:
    # Remove OS-forbidden filename characters
    s = re.sub(r'[<>:"/\\|?*]', "_", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len]


# =========================
# Stage 1: Discover files
# =========================
def discover_files(cfg: ToolConfig) -> List[str]:
    if not cfg.input_dir:
        return []
    pat = os.path.join(cfg.input_dir, cfg.pattern)
    files = sorted(glob.glob(pat))
    return [f for f in files if os.path.isfile(f)]


# =========================
# Stage 2: Inspect headers (union across files)
# =========================
def inspect_headers(files: List[str], encoding: str) -> Tuple[List[str], Dict[str, List[str]]]:
    union = set()
    per_file: Dict[str, List[str]] = {}

    for fp in files:
        ext = os.path.splitext(fp)[1].lower()
        try:
            if ext in (".csv", ".txt"):
                hdr = list(pd.read_csv(fp, nrows=0, encoding=encoding).columns)
            elif ext in (".xlsx", ".xls"):
                hdr = list(pd.read_excel(fp, nrows=0).columns)
            else:
                continue
        except Exception:
            continue

        per_file[fp] = hdr
        union.update(hdr)

    return sorted(union), per_file


# =========================
# Output header logic
# =========================
def compute_output_header(src: str, cfg: ToolConfig) -> str:
    if cfg.rename_mode == "keep":
        return src

    if cfg.rename_mode == "underscore":
        return re.sub(r"\s+", "_", str(src).strip())

    if cfg.rename_mode == "custom":
        return cfg.rename_map.get(src, src)

    return src


def validate_output_headers(selected_cols: List[str], cfg: ToolConfig) -> List[str]:
    out_cols = [compute_output_header(c, cfg) for c in selected_cols]
    seen = set()
    dups = []
    for c in out_cols:
        if c in seen:
            dups.append(c)
        else:
            seen.add(c)
    if dups:
        raise ValueError(f"Duplicate output headers detected: {sorted(set(dups))}")
    return out_cols


def select_and_rename(df: pd.DataFrame, selected_cols: List[str], cfg: ToolConfig) -> pd.DataFrame:
    """Build output DataFrame in the exact selected order with mapped output headers.

    Missing input columns are created as blank columns so the output schema stays stable.
    """
    out = pd.DataFrame(index=df.index)
    for src in selected_cols:
        out_name = compute_output_header(src, cfg)
        if src in df.columns:
            out[out_name] = df[src]
        else:
            out[out_name] = ""
    return out


# =========================
# Export: per-file writer with part splitting
# =========================
class PartWriter:
    def __init__(self, out_dir: str, prefix: str, part_rows: int, encoding: str):
        self.out_dir = out_dir
        self.prefix = prefix
        self.part_rows = int(part_rows)
        self.encoding = encoding

        self.part_idx = 1
        self.part_written_rows = 0
        self.total_written_rows = 0
        self.output_paths: List[str] = []

    def _current_path(self) -> str:
        return os.path.join(self.out_dir, f"{self.prefix}__part{self.part_idx:03d}.csv")

    def _rotate(self) -> None:
        self.part_idx += 1
        self.part_written_rows = 0

    def write_df(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        start = 0
        n = len(df)

        while start < n:
            remaining = self.part_rows - self.part_written_rows
            if remaining <= 0:
                self._rotate()
                remaining = self.part_rows

            take = min(remaining, n - start)
            chunk = df.iloc[start : start + take]

            path = self._current_path()
            first_time_for_this_part = self.part_written_rows == 0 and not os.path.exists(path)

            chunk.to_csv(
                path,
                mode="a",
                header=first_time_for_this_part,
                index=False,
                encoding=self.encoding,
            )

            if path not in self.output_paths:
                self.output_paths.append(path)

            self.part_written_rows += take
            self.total_written_rows += take
            start += take


def iter_csv_chunks(fp: str, cfg: ToolConfig) -> Iterable[pd.DataFrame]:
    yield from pd.read_csv(
        fp,
        dtype=cfg.csv_dtype,
        keep_default_na=cfg.csv_keep_default_na,
        encoding=cfg.csv_encoding,
        chunksize=cfg.read_chunksize,
    )


def export_selected_columns_in_chunks(files: List[str], ctx: RunContext, cfg: ToolConfig) -> None:
    os.makedirs(ctx.session_dir, exist_ok=True)

    # Validate once before export (prevents partial writes)
    validate_output_headers(cfg.selected_columns, cfg)

    all_output_files: List[str] = []

    for fp in files:
        ext = os.path.splitext(fp)[1].lower()
        sid = extract_session_id(fp)

        in_stem = os.path.splitext(os.path.basename(fp))[0]
        short_stem = safe_name(in_stem, 40)
        prefix = safe_name(f"{sid}__{short_stem}", 80)

        writer = PartWriter(ctx.session_dir, prefix, cfg.part_rows, cfg.csv_encoding)

        if ext in (".csv", ".txt"):
            for chunk in iter_csv_chunks(fp, cfg):
                ctx.total_input_rows += len(chunk)
                out_chunk = select_and_rename(chunk, cfg.selected_columns, cfg)
                writer.write_df(out_chunk)

        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(fp, dtype=str)
            ctx.total_input_rows += len(df)
            out_df = select_and_rename(df, cfg.selected_columns, cfg)
            writer.write_df(out_df)

        else:
            continue

        ctx.files_processed += 1
        ctx.total_output_rows += writer.total_written_rows
        all_output_files.extend(writer.output_paths)

        # Per-file summary (useful for audit trail)
        per_file_summary = {
            "input_file": fp,
            "sid": sid,
            "output_prefix": prefix,
            "selected_columns": cfg.selected_columns,
            "rename_mode": cfg.rename_mode,
            "rename_map": cfg.rename_map if cfg.rename_mode == "custom" else {},
            "part_rows": cfg.part_rows,
            "total_output_rows_for_this_file": writer.total_written_rows,
            "output_files": writer.output_paths,
        }
        with open(os.path.join(ctx.session_dir, f"{prefix}__summary.json"), "w", encoding="utf-8") as f:
            json.dump(per_file_summary, f, ensure_ascii=False, indent=2)

    ctx.output_parts = len(all_output_files)

    # Global summary
    summary = {
        "run_ts": ctx.run_ts,
        "session_dir": ctx.session_dir,
        "pattern": cfg.pattern,
        "files": files,
        "selected_columns": cfg.selected_columns,
        "rename_mode": cfg.rename_mode,
        "part_rows": cfg.part_rows,
        "files_processed": ctx.files_processed,
        "total_input_rows": ctx.total_input_rows,
        "total_output_rows": ctx.total_output_rows,
        "output_parts": ctx.output_parts,
        "output_files": all_output_files,
    }

    with open(os.path.join(ctx.session_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


# =========================
# Pipeline Orchestrator
# =========================
def run_pipeline(cfg: ToolConfig, log_fn=print) -> RunContext:
    run_ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_root = cfg.output_dir.strip() if cfg.output_dir else os.path.join(cfg.input_dir, "output")
    session_dir = os.path.join(out_root, f"{run_ts}__session")

    ctx = RunContext(run_ts=run_ts, session_dir=session_dir)

    files = discover_files(cfg)
    ctx.matched_files = files
    if not files:
        log_fn("No files matched.")
        return ctx

    if not cfg.selected_columns:
        log_fn("No columns selected.")
        return ctx

    os.makedirs(session_dir, exist_ok=True)

    export_selected_columns_in_chunks(files, ctx, cfg)

    log_fn(f"Done. Output parts: {ctx.output_parts}. Rows: {ctx.total_output_rows}.")
    log_fn(f"Session dir: {ctx.session_dir}")

    return ctx


# =========================
# Tkinter GUI
# =========================
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CSV Split Tool")
        self.geometry("1050x720")

        self.cfg = ToolConfig()
        self.files: List[str] = []

        self._build_ui()

    def _build_ui(self):
        # Top: paths
        frm = ttk.Frame(self)
        frm.pack(fill="x", padx=10, pady=10)

        ttk.Label(frm, text="Input folder").grid(row=0, column=0, sticky="w")
        self.input_var = tk.StringVar(value="")
        ttk.Entry(frm, textvariable=self.input_var, width=70).grid(row=0, column=1, sticky="we", padx=5)
        ttk.Button(frm, text="Browse", command=self.browse_input).grid(row=0, column=2)

        ttk.Label(frm, text="Glob pattern").grid(row=1, column=0, sticky="w")
        self.pattern_var = tk.StringVar(value="*PendingReview*.csv")
        ttk.Entry(frm, textvariable=self.pattern_var, width=70).grid(row=1, column=1, sticky="we", padx=5)
        ttk.Button(frm, text="Scan", command=self.scan_files).grid(row=1, column=2)

        ttk.Label(frm, text="Output folder").grid(row=2, column=0, sticky="w")
        self.output_var = tk.StringVar(value="")
        ttk.Entry(frm, textvariable=self.output_var, width=70).grid(row=2, column=1, sticky="we", padx=5)
        ttk.Button(frm, text="Browse", command=self.browse_output).grid(row=2, column=2)

        ttk.Label(frm, text="Split rows per part").grid(row=3, column=0, sticky="w")
        self.part_rows_var = tk.StringVar(value="950000")
        ttk.Entry(frm, textvariable=self.part_rows_var, width=12).grid(row=3, column=1, sticky="w", padx=5)

        frm.columnconfigure(1, weight=1)

        # Middle: file list + columns selection
        mid = ttk.Frame(self)
        mid.pack(fill="both", expand=True, padx=10, pady=10)

        left = ttk.LabelFrame(mid, text="Matched files")
        left.pack(side="left", fill="both", expand=True, padx=(0, 8))

        self.file_list = tk.Listbox(left, height=12)
        self.file_list.pack(fill="both", expand=True, padx=6, pady=6)

        ttk.Button(left, text="Load Columns", command=self.load_columns).pack(pady=(0, 6))

        right = ttk.LabelFrame(mid, text="Columns")
        right.pack(side="left", fill="both", expand=True)

        cols = ttk.Frame(right)
        cols.pack(fill="both", expand=True, padx=6, pady=6)

        self.avail_list = tk.Listbox(cols)
        self.sel_list = tk.Listbox(cols)

        self.avail_list.grid(row=0, column=0, sticky="nsew")
        self.sel_list.grid(row=0, column=2, sticky="nsew")

        btns = ttk.Frame(cols)
        btns.grid(row=0, column=1, padx=8)

        ttk.Button(btns, text=">", command=self.add_col).pack(fill="x", pady=3)
        ttk.Button(btns, text="<", command=self.remove_col).pack(fill="x", pady=3)
        ttk.Separator(btns, orient="horizontal").pack(fill="x", pady=8)
        ttk.Button(btns, text="Up", command=lambda: self.move_sel(-1)).pack(fill="x", pady=3)
        ttk.Button(btns, text="Down", command=lambda: self.move_sel(1)).pack(fill="x", pady=3)

        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(2, weight=1)
        cols.rowconfigure(0, weight=1)

        # Output header options
        opt = ttk.LabelFrame(self, text="Output header")
        opt.pack(fill="x", padx=10, pady=(0, 10))

        self.rename_mode_var = tk.StringVar(value="keep")
        ttk.Radiobutton(opt, text="Keep original headers", variable=self.rename_mode_var, value="keep").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Radiobutton(opt, text="Replace spaces with '_'", variable=self.rename_mode_var, value="underscore").grid(row=0, column=1, sticky="w", padx=6, pady=4)
        ttk.Radiobutton(opt, text="Custom mapping (JSON)", variable=self.rename_mode_var, value="custom").grid(row=0, column=2, sticky="w", padx=6, pady=4)

        ttk.Label(opt, text='Example: {"Model ID": "Model_ID"}').grid(row=1, column=0, columnspan=3, sticky="w", padx=6)

        self.rename_json = tk.Text(opt, height=4)
        self.rename_json.grid(row=2, column=0, columnspan=3, sticky="we", padx=6, pady=6)
        self.rename_json.insert("end", "{}")

        opt.columnconfigure(2, weight=1)

        # Bottom: run + log
        bottom = ttk.Frame(self)
        bottom.pack(fill="both", expand=False, padx=10, pady=(0, 10))

        ttk.Button(bottom, text="Run", command=self.run).pack(side="left")

        self.log = tk.Text(bottom, height=8)
        self.log.pack(side="left", fill="both", expand=True, padx=8)

    def logln(self, msg: str):
        self.log.insert("end", msg + "\n")
        self.log.see("end")

    def browse_input(self):
        d = filedialog.askdirectory()
        if d:
            self.input_var.set(d)

    def browse_output(self):
        d = filedialog.askdirectory()
        if d:
            self.output_var.set(d)

    def scan_files(self):
        self.files = []
        self.file_list.delete(0, "end")

        input_dir = self.input_var.get().strip()
        pattern = self.pattern_var.get().strip() or "*.*"

        self.cfg.input_dir = input_dir
        self.cfg.pattern = pattern

        files = discover_files(self.cfg)
        self.files = files

        for fp in files:
            self.file_list.insert("end", os.path.basename(fp))

        self.logln(f"Matched files: {len(files)}")

    def load_columns(self):
        if not self.files:
            messagebox.showinfo("Info", "Scan files first")
            return

        union, _ = inspect_headers(self.files, encoding=self.cfg.csv_encoding)

        self.avail_list.delete(0, "end")
        self.sel_list.delete(0, "end")

        for c in union:
            self.avail_list.insert("end", c)

        self.logln(f"Loaded columns (union): {len(union)}")

    def add_col(self):
        sel = list(self.avail_list.curselection())
        for i in sel:
            val = self.avail_list.get(i)
            if val not in self.sel_list.get(0, "end"):
                self.sel_list.insert("end", val)

    def remove_col(self):
        sel = list(self.sel_list.curselection())
        for i in reversed(sel):
            self.sel_list.delete(i)

    def move_sel(self, delta: int):
        sel = list(self.sel_list.curselection())
        if len(sel) != 1:
            return
        i = sel[0]
        j = i + delta
        if j < 0 or j >= self.sel_list.size():
            return
        val = self.sel_list.get(i)
        self.sel_list.delete(i)
        self.sel_list.insert(j, val)
        self.sel_list.selection_set(j)

    def run(self):
        if not self.files:
            messagebox.showinfo("Info", "Scan files first")
            return

        selected = list(self.sel_list.get(0, "end"))
        if not selected:
            messagebox.showinfo("Info", "Select at least 1 column")
            return

        self.cfg.input_dir = self.input_var.get().strip()
        self.cfg.pattern = self.pattern_var.get().strip() or "*.*"
        self.cfg.output_dir = self.output_var.get().strip()

        try:
            self.cfg.part_rows = int(self.part_rows_var.get().strip())
        except ValueError:
            messagebox.showerror("Error", "Split rows must be an integer")
            return

        self.cfg.selected_columns = selected

        self.cfg.rename_mode = self.rename_mode_var.get().strip() or "keep"
        self.cfg.rename_map = {}

        if self.cfg.rename_mode == "custom":
            raw = self.rename_json.get("1.0", "end").strip()
            if raw:
                try:
                    m = json.loads(raw)
                    if not isinstance(m, dict):
                        messagebox.showerror("Error", "Custom mapping must be a JSON object (dict).")
                        return
                    self.cfg.rename_map = {str(k): str(v) for k, v in m.items()}
                except Exception as e:
                    messagebox.showerror("Error", f"Invalid JSON mapping: {e}")
                    return

        try:
            validate_output_headers(self.cfg.selected_columns, self.cfg)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        self.logln("Running pipeline...")
        ctx = run_pipeline(self.cfg, log_fn=self.logln)
        if ctx.output_parts == 0:
            self.logln("No output generated (check selection/files).")


if __name__ == "__main__":
    App().mainloop()
