#!/usr/bin/env python3
"""HG Device Console — GUI front-end for the Hologic demo agent.

A field-service-friendly window on the device (no command line). Same agent
core as agent.py; this only adds a UI. Demonstrates the Q2 "no IT-specialist
knowledge" and Q7 "end user must approve and install" requirements: software
updates arrive as an approved package and the operator clicks Install.

Run:  python agent_gui.py      (needs certs/ and config.json, like agent.py)
tkinter ships with Python — no extra install.
"""
import queue
import threading
import tkinter as tk
from tkinter import ttk

from agent import Agent, AGENT_VERSION, LOG_SINKS

PURPLE = "#4B2680"
ACCENT = "#8626C4"
OK = "#10B981"
GREY = "#9AA0A6"
AMBER = "#F59E0B"


class Console:
    def __init__(self, root):
        self.root = root
        self.log_q = queue.Queue()
        LOG_SINKS.append(self.log_q.put)

        self.agent = Agent()
        self.agent.require_manual_ota = True  # operator approves installs (Q7)

        root.title("HG Device Console — DIM-4521")
        root.configure(bg="white")
        root.geometry("640x560")
        root.protocol("WM_DELETE_WINDOW", self.on_close)

        # header
        hdr = tk.Frame(root, bg=PURPLE)
        hdr.pack(fill="x")
        tk.Label(hdr, text="HOLOGIC", bg=PURPLE, fg="white",
                 font=("Helvetica", 18, "bold")).pack(anchor="w", padx=16, pady=(12, 0))
        tk.Label(hdr, text="Dimensions Workstation  ·  Connectivity Agent",
                 bg=PURPLE, fg="#D7CDEC", font=("Helvetica", 11)).pack(anchor="w", padx=16, pady=(0, 12))

        # status grid
        grid = tk.Frame(root, bg="white")
        grid.pack(fill="x", padx=16, pady=12)
        self.vals = {}
        fields = ["Connection", "Installed SW", "Agent", "Total Exams", "Queue Depth", "Last Error"]
        for i, f in enumerate(fields):
            r, c = divmod(i, 3)
            cell = tk.Frame(grid, bg="white")
            cell.grid(row=r, column=c, sticky="w", padx=(0, 32), pady=6)
            tk.Label(cell, text=f.upper(), bg="white", fg=GREY,
                     font=("Helvetica", 9, "bold")).pack(anchor="w")
            v = tk.Label(cell, text="—", bg="white", fg="#141414", font=("Helvetica", 15, "bold"))
            v.pack(anchor="w")
            self.vals[f] = v

        # OTA banner (hidden until an approved update is pending)
        self.ota = tk.Frame(root, bg="#FBF3D9")
        self.ota_label = tk.Label(self.ota, bg="#FBF3D9", fg="#8a6d0b", font=("Helvetica", 12))
        self.ota_label.pack(side="left", padx=12, pady=10)
        self.install_btn = tk.Button(self.ota, text="Install Update", bg=ACCENT, fg="white",
                                     relief="flat", font=("Helvetica", 11, "bold"),
                                     command=self.on_install)
        self.install_btn.pack(side="right", padx=12, pady=8)

        # log pane
        self.activity_label = tk.Label(root, text="Activity", bg="white", fg=GREY,
                                       font=("Helvetica", 9, "bold"))
        self.activity_label.pack(anchor="w", padx=16)
        self.text = tk.Text(root, height=12, bg="#141414", fg="#E7E5E4",
                            font=("Menlo", 11), relief="flat", wrap="word")
        self.text.pack(fill="both", expand=True, padx=16, pady=(4, 8))
        self.text.configure(state="disabled")

        # buttons
        btns = tk.Frame(root, bg="white")
        btns.pack(fill="x", padx=16, pady=(0, 14))
        tk.Button(btns, text="Perform Exam", relief="flat", bg="#EFEDEB",
                  font=("Helvetica", 11), command=lambda: self.bg(self.agent.perform_exam)
                  ).pack(side="left")
        tk.Button(btns, text="Raise Error + Capture Logs", relief="flat", bg="#EFEDEB",
                  font=("Helvetica", 11), command=lambda: self.bg(self.agent.handle_error_event)
                  ).pack(side="left", padx=8)

        threading.Thread(target=self.agent.run, daemon=True).start()
        self.tick()

    def bg(self, fn):
        threading.Thread(target=fn, daemon=True).start()

    def on_install(self):
        self.agent.approve_pending_ota()

    def on_close(self):
        self.agent.stop = True
        self.root.after(300, self.root.destroy)

    def tick(self):
        # drain logs
        while not self.log_q.empty():
            line = self.log_q.get_nowait()
            self.text.configure(state="normal")
            self.text.insert("end", line + "\n")
            self.text.see("end")
            self.text.configure(state="disabled")
        # status
        a = self.agent
        self.vals["Connection"].config(text="Online" if a.connected else "Offline",
                                       fg=OK if a.connected else GREY)
        self.vals["Installed SW"].config(text=a.sw_version)
        self.vals["Agent"].config(text=AGENT_VERSION)
        self.vals["Total Exams"].config(text=str(a.exam_count))
        self.vals["Queue Depth"].config(text=str(a.store.depth()))
        self.vals["Last Error"].config(text=a.last_error or "—",
                                       fg="#EF4444" if a.last_error else "#141414")
        # OTA banner: show above the Activity label when an approved update waits
        if a.pending_ota and not a.updating:
            self.ota_label.config(text=f"Approved update {a.pending_ota[0]} is available for this device.")
            if not self.ota.winfo_ismapped():
                self.ota.pack(fill="x", padx=16, pady=(0, 8), before=self.activity_label)
            self.install_btn.config(state="normal")
        elif self.ota.winfo_ismapped():
            self.ota.pack_forget()
        self.root.after(1000, self.tick)


if __name__ == "__main__":
    root = tk.Tk()
    Console(root)
    root.mainloop()
