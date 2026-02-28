"""Data Processor GUI

A desktop GUI application that wraps the core CSV processing engine,
providing a user-friendly interface for batch processing data files.

Features:
- Browse and select input/output folders
- Real-time processing log with status updates
- Threaded execution to keep the UI responsive
- One-click access to open the output folder
"""

import os
import threading
import traceback
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox
from processor_core import run_processor

APP_TITLE = "Data Processor"
DEFAULT_WIDTH = 820
DEFAULT_HEIGHT = 520


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry(f"{DEFAULT_WIDTH}x{DEFAULT_HEIGHT}")
        self.resizable(True, True)

        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.running = False

        self._build_ui()

    def _build_ui(self):
        pad = {'padx': 10, 'pady': 8}

        # Input folder selection
        frm_in = tk.Frame(self)
        frm_in.pack(fill='x', **pad)
        tk.Label(frm_in, text="Input folder:").pack(side='left')
        tk.Entry(frm_in, textvariable=self.input_var, width=80).pack(side='left', padx=8, fill='x', expand=True)
        tk.Button(frm_in, text="Browse", command=self.browse_input).pack(side='left')

        # Output folder selection
        frm_out = tk.Frame(self)
        frm_out.pack(fill='x', **pad)
        tk.Label(frm_out, text="Output folder:").pack(side='left')
        tk.Entry(frm_out, textvariable=self.output_var, width=80).pack(side='left', padx=8, fill='x', expand=True)
        tk.Button(frm_out, text="Browse", command=self.browse_output).pack(side='left')

        # Action buttons
        frm_btn = tk.Frame(self)
        frm_btn.pack(fill='x', **pad)
        self.run_btn = tk.Button(frm_btn, text="Run", command=self.on_run, width=12)
        self.run_btn.pack(side='left')
        tk.Button(frm_btn, text="Open Output Folder", command=self.open_output, width=18).pack(side='left', padx=8)

        # Log area
        frm_log = tk.Frame(self)
        frm_log.pack(fill='both', expand=True, **pad)
        tk.Label(frm_log, text="Log:").pack(anchor='w')
        self.txt = tk.Text(frm_log, wrap='none', height=20)
        self.txt.pack(fill='both', expand=True)
        self.txt.configure(state='disabled')

        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status = tk.Label(self, textvariable=self.status_var, anchor='w', relief='sunken')
        status.pack(fill='x')

    def browse_input(self):
        path = filedialog.askdirectory(title="Select input folder")
        if path:
            self.input_var.set(path)
            # Default output to same path if not yet set
            if not self.output_var.get():
                self.output_var.set(path)

    def browse_output(self):
        path = filedialog.askdirectory(title="Select output folder")
        if path:
            self.output_var.set(path)

    def append_log(self, line: str):
        self.txt.configure(state='normal')
        self.txt.insert('end', line + "\n")
        self.txt.see('end')
        self.txt.configure(state='disabled')

    def set_running(self, flag: bool):
        self.running = flag
        self.run_btn.configure(state='disabled' if flag else 'normal')
        self.status_var.set("Running..." if flag else "Ready")

    def on_run(self):
        if self.running:
            return

        in_dir = self.input_var.get().strip()
        out_dir = self.output_var.get().strip()

        if not in_dir:
            messagebox.showwarning("Missing", "Please choose an input folder.")
            return
        if not Path(in_dir).exists():
            messagebox.showerror("Invalid", "Input folder does not exist.")
            return
        if not out_dir:
            messagebox.showwarning("Missing", "Please choose an output folder.")
            return
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            messagebox.showerror("Invalid", f"Cannot create output folder:\n{e}")
            return

        # Clear previous log
        self.txt.configure(state='normal')
        self.txt.delete('1.0', 'end')
        self.txt.configure(state='disabled')

        self.set_running(True)

        def worker():
            try:
                self.append_log(f"Input:  {in_dir}")
                self.append_log(f"Output: {out_dir}")
                self.append_log("----- start -----")
                processed, skipped = run_processor(in_dir, out_dir, log_callback=self.append_log)
                self.append_log("----- done -----")
                self.append_log(f"Summary: processed={processed}, skipped={skipped}")
                self.status_var.set(f"Done. processed={processed}, skipped={skipped}")
            except Exception:
                err = traceback.format_exc()
                self.append_log(err)
                self.status_var.set("Error")
                messagebox.showerror("Error", err)
            finally:
                self.set_running(False)

        threading.Thread(target=worker, daemon=True).start()

    def open_output(self):
        path = self.output_var.get().strip()
        if not path:
            messagebox.showinfo("Info", "No output folder selected.")
            return
        p = Path(path)
        if not p.exists():
            messagebox.showerror("Not found", "Output folder does not exist.")
            return
        try:
            os.startfile(str(p))
        except Exception:
            messagebox.showinfo("Info", f"Output folder: {p}")


if __name__ == "__main__":
    app = App()
    app.mainloop()
