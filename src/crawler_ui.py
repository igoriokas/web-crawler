import tkinter as tk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import sqlite3
import time
import glob
import word_counter
import threading
import matplotlib.pyplot as plt
import word_counter
import logging

# my imports
import config as cfg
import crawler_cli
import lockfile
import state

if __name__ == "__main__": # starting directly from crawler_ui.py
    cfg.argparse_and_init('crawler-ui')

logger = logging.getLogger('crawler.ui')
logger.info('Start in UI mode')

# If another crawler is already running using the same WORKDIR don't even start the UI
if lockfile.LockFile().is_locked():
    print(f"Another crawler process is already running in {cfg.WORKDIR}, EXIT") # intentionally not logging
    exit()

# create DB tables if starting from scratch, for UI display
with state.CrawlerState():
    pass

def get_all_pages():
    with sqlite3.connect(cfg.DB_PATH) as conn:
        pages = pd.read_sql("SELECT * FROM pages", conn)
        words = pd.read_sql("SELECT * FROM words ORDER BY count DESC, word ASC", conn)
        return pages, words

def draw_progress_bar(df, ax):    
    # counts
    counts = df['status'].value_counts()
    visited = counts.get('visited', 0)
    failed = counts.get('failed', 0)
    queued = counts.get('queued', 0)
    done = visited + failed
    total = done + queued or 1 # to avoid division by zero at start
    
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
root.title("My Web Crawler")
root.geometry("1200x900")

# Create a Canvas widget and add it to Tkinter
fig, ax = plt.subplots(figsize=(8, 1.5), dpi=50)
canvas = FigureCanvasTkAgg(fig, master=root)
draw_progress_bar(get_all_pages()[0], ax)
canvas.draw()
canvas.get_tk_widget().pack(fill=tk.X, expand=None, padx=10, pady=10)


# Control buttons

def toggle_pause():
    crawler_cli.pause = not crawler_cli.pause
    logger.info(f'main.pause - {crawler_cli.pause}')
    btn_pause.config(text="Resume" if crawler_cli.pause else "Pause")

def stop_follow():
    crawler_cli.stop = True
    logger.info(f'main.stop - {crawler_cli.stop}')
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
btn_clear = tk.Button(button_frame, text="Clear log", width=10, command=lambda: textbox_set_text(text_box, None))
btn_clear.pack(side=tk.RIGHT, padx=5, pady=5)

# ROW 3
row3 = tk.Frame(root)
row3.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
t1 = tk.Text(row3, bg="ivory", fg="black", wrap=tk.NONE, height=10, width=60)
t1.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
t3 = tk.Text(row3, bg="ivory", fg="black", wrap=tk.WORD, height=10, width=20)
t3.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
t2 = tk.Text(row3, bg="ivory", fg="black", wrap=tk.NONE, height=10, width=20)
t2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)


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


def textbox_set_text(tb:tk.Text, text:str):
    text = text or ""
    tb.config(state='normal')
    tb.delete("1.0", tk.END)
    tb.insert("1.0", text)
    tb.config(state='disabled')


def textbox_append_text(tb:tk.Text, text:str):
    text = text or ""
    tb.config(state='normal')
    tb.insert(tk.END, text)
    tb.see(tk.END)
    tb.config(state='disabled')


logfile = open(cfg.LOGFILE, "r")

def update_plot():
    pages, words = get_all_pages()
    draw_progress_bar(pages, ax)
    canvas.draw()

    er = pages.groupby('error')['sid'].count().to_frame().sort_values('sid', ascending=False).reset_index()
    er = er.rename(columns={'sid':'count'})
    textbox_set_text(t1, f"ERROR COUNTS:\n\n{er.to_string(index=False, header=False)}")
    textbox_append_text(t1, f"\n\nIs word count from disk identical to DB word count - {words.equals(word_counter.sum_counters_folder_df(f'{cfg.WORKDIR}/words'))}")

    textbox_set_text(t2, f"TOP WORD COUNTS:\n\n{words[:100].to_string(index=False, header=False)}")

    files_cnt_text = 'FILES PRODUCED:\n\n'
    files_cnt_text += f'  pages: {len(glob.glob(f'{cfg.WORKDIR}/pages/**/*.*', recursive=True))}\n'
    files_cnt_text += f'   text: {len(glob.glob(f'{cfg.WORKDIR}/text/**/*.*',  recursive=True))}\n'
    files_cnt_text += f'   word: {len(glob.glob(f'{cfg.WORKDIR}/words/**/*.*', recursive=True))}\n'
    textbox_set_text(t3, files_cnt_text) 

    textbox_append_text(text_box, logfile.read())
    root.after(1000, update_plot)

# Start crawler thread
thread = threading.Thread(target=crawler_cli.main, args=(), daemon=True)
thread.start()

def on_close():
    crawler_cli.stop = True
    while thread.is_alive():
        print("waiting for crawler to stop ...")
        time.sleep(1)    
    logfile.close()
    root.destroy()
    
root.protocol("WM_DELETE_WINDOW", on_close)
root.createcommand('tk::mac::Quit', on_close) # Intercept macOS Quit from menu (Cmd+Q)

# Start the Tkinter event loop
root.focus_force()            # Grab keyboard focus
update_plot()
root.mainloop()