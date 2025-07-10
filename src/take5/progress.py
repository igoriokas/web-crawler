import tkinter as tk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import sqlite3
import os
import time
import glob
import counter
import threading
import matplotlib.pyplot as plt
import main


RUN_FOLDER = '.'
DBNAME = f"{RUN_FOLDER}/state.db"
LOGFILE = f"{RUN_FOLDER}/log.log"


def draw_progress_bar(ax):    
    with sqlite3.connect(DBNAME) as conn:
        df = pd.read_sql("SELECT * FROM pages", conn)

    # counts
    counts = df['status'].value_counts()
    visited = counts.get('visited', 0)
    failed = counts.get('failed', 0)
    queued = counts.get('queued', 0)
    done = visited + failed
    total = done + queued
    
    ax.clear()
    ax.barh(0, visited, color='green', label=f'Visited: {visited} ({visited/total:.0%})')
    ax.barh(0, failed, left=visited, color='red', label=f'Failed : {failed} ({failed/total:.0%})')
    ax.barh(0, queued, left=done, color='orange', label=f'Queued : {queued} ({queued/total:.0%})')

    ax.set_title(f"Crawling Progress: {done} of {total} pages", fontsize=12, pad=5)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.5), ncol=3, fontsize=10, frameon=False)
    ax.set_xlim(0, total)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.set_frame_on(False)
 
    plt.subplots_adjust(bottom=0.3)
    plt.tight_layout()
    # plt.show()
    return queued

# Create the Tkinter window
root = tk.Tk()
root.title("My Crawler")
root.geometry("1200x900")

# Create a Canvas widget and add it to Tkinter
fig, ax = plt.subplots(figsize=(8, 1.5), dpi=50)
canvas = FigureCanvasTkAgg(fig, master=root)
draw_progress_bar(ax)
canvas.draw()
canvas.get_tk_widget().pack(fill=tk.X, expand=None, padx=10, pady=10)


# Control buttons
paused = False

def toggle_pause():
    global paused
    paused = not paused
    btn_pause.config(text="Resume" if paused else "Pause")

def stop_follow():
    main.stop_flag = True
    btn_pause.config(state=tk.DISABLED)
    btn_stop.config(state=tk.DISABLED)

def clear_log():
    text_box.config(state='normal')
    text_box.delete("1.0", tk.END)
    text_box.config(state='disabled')

button_frame = tk.Frame(root)
button_frame.pack(fill=tk.X)
btn_pause = tk.Button(button_frame, text="Pause", width=10, command=toggle_pause)
btn_pause.pack(side=tk.LEFT, padx=5, pady=5)
btn_stop = tk.Button(button_frame, text="Stop", width=10, command=stop_follow)
btn_stop.pack(side=tk.LEFT, padx=5, pady=5)
btn_clear = tk.Button(button_frame, text="Clear log", width=10, command=clear_log)
btn_clear.pack(side=tk.RIGHT, padx=5, pady=5)

# Frame to hold text and scrollbars
frame = tk.Frame(root)
frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

# Text widget
text_box = tk.Text(frame, bg="ivory", fg="black", wrap=tk.NONE, state="disabled")
text_box.grid(row=0, column=0, sticky="nsew")

# Vertical scrollbar
yscroll = tk.Scrollbar(frame, orient=tk.VERTICAL, command=text_box.yview)
yscroll.grid(row=0, column=1, sticky="ns")
text_box.config(yscrollcommand=yscroll.set)

# Horizontal scrollbar
xscroll = tk.Scrollbar(frame, orient=tk.HORIZONTAL, command=text_box.xview)
xscroll.grid(row=1, column=0, sticky="ew")
text_box.config(xscrollcommand=xscroll.set)

# Make the text box expand with window
frame.grid_rowconfigure(0, weight=1)
frame.grid_columnconfigure(0, weight=1)


# Track file position
file = open(LOGFILE, "r")
# file.seek(0, os.SEEK_END)  # Go to the end of the file

def update_plot():
    draw_progress_bar(ax)
    canvas.draw()

    lines = file.read()
    if lines:
        text_box.config(state='normal')
        text_box.insert(tk.END, lines)
        text_box.see(tk.END)
        text_box.config(state='disabled')

    root.after(1000, update_plot)

update_plot()

# Start crawler thread
thread = threading.Thread(target=main.main, args=(), daemon=True)
thread.start()

def on_close():
    main.stop_flag = True
    while thread.is_alive():
        print("waiting for crawler to stop ...")
        time.sleep(1)    
    root.destroy()
    
root.protocol("WM_DELETE_WINDOW", on_close)
root.createcommand('tk::mac::Quit', on_close) # Intercept macOS Quit from menu (Cmd+Q)

# Start the Tkinter event loop
root.focus_force()            # Grab keyboard focus
root.mainloop()