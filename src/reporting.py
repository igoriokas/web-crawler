import pandas as pd
import sqlite3
import glob
import logging
import time

# my imports
import config as cfg
import state

logger = logging.getLogger('crawler.reporter')

class Reporter():
    def __init__(self):
        self.updated_at = 0
        self.visited = 0
        self.failed = 0
        self.queued = 0
        self.done = 0
        self.total = 0
        self.errors_text = None
        self.files_cnt_text = None
        self.top_words_text = None
        self.pages = None
        self.words = None
        self.durations = None

    def refresh(self, freshness:float):
        if (time.time() - self.updated_at) > freshness:
            logger.debug('refresh report')
            self.prepare_report()
        return self

    def read_db(self):
        with sqlite3.connect(cfg.DB_PATH) as conn:
            self.pages     = pd.read_sql("SELECT * FROM pages", conn)
            self.words     = pd.read_sql("SELECT * FROM words ORDER BY count DESC, word ASC", conn)
            self.attempts  = pd.read_sql("SELECT sid, attempt FROM attempts", conn)
            self.durations = pd.read_sql("SELECT attempt, fetch_duration, total_duration FROM attempts WHERE status = 200", conn)

        counts = self.pages['status'].value_counts()
        self.visited = counts.get('visited', 0)
        self.failed = counts.get('failed', 0)
        self.queued = counts.get('queued', 0)
        self.done = self.visited + self.failed
        self.total = (self.done + self.queued) or 1 # to avoid division by zero at start
        return self

    def prepare_report(self):
        self.read_db()
        mean_attempts = float(self.attempts.groupby('sid').count().mean()['attempt'])
        duration_means = self.durations.mean() if len(self.durations) > 0 else None

        er = self.pages.groupby('error')['sid'].count().to_frame().sort_values('sid', ascending=False).reset_index()
        if len(er) > 0:
            er = er.rename(columns={'sid':'count'})
            er = er.to_string(index=False, header=False)
        else:
            er = 'no errors'

        self.errors_text  = f'ERROR COUNTS:\n\n{er}\n'
        # self.errors_text +=  '\n\n-----------------------------------------------------------'
        # self.errors_text += f"\n\nIs word count from disk identical to DB word count - {words.equals(word_counter.sum_counters_folder_df(f'{cfg.WORKDIR}/words'))}"

        self.files_cnt_text  =  'FILES PRODUCED:\n\n'
        self.files_cnt_text += f'  pages/: {len(glob.glob(f'{cfg.WORKDIR}/pages/**/*.*', recursive=True))}\n'
        self.files_cnt_text += f'   text/: {len(glob.glob(f'{cfg.WORKDIR}/text/**/*.*',  recursive=True))}\n'
        self.files_cnt_text += f'  words/: {len(glob.glob(f'{cfg.WORKDIR}/words/**/*.*', recursive=True))}\n'
        self.files_cnt_text += '\n\n'
        if duration_means is not None:
            self.files_cnt_text += 'STATISTICS (per page):\n\n'
            self.files_cnt_text += f'  mean attempts:       {mean_attempts:.2f}\n\n'
            self.files_cnt_text += f'  mean fetch duration: {float(duration_means['fetch_duration']):.3f} secs\n'
            self.files_cnt_text += f'  mean total duration: {float(duration_means['total_duration']):.3f} secs\n'

        self.top_words_text = f'TOP(50) WORD COUNTS:\n\n{self.words[:50].to_string(index=False, header=False)}'            
        self.updated_at = time.time()
        return self


    def write_report_file(self):
        self.prepare_report()
        with open(f'{cfg.REPORT_FILE}', 'w') as f:
            f.write(    '-----------------------------------------------------------\n\n')
            f.write(f'CRAWL {cfg.START_URL} -> {cfg.WORKDIR} (max_depth: {cfg.MAX_DEPTH}, max_attempts: {cfg.MAX_ATTEMPTS})\n')
            if self.done > 0 and self.queued == 0:
                f.write('\nCRAWL COMPLETED\n\n')
                f.write(f'Original web pages stored in:  {cfg.WORKDIR}/pages/ \n')
                f.write(f'Pages in plain text stored in: {cfg.WORKDIR}/text/  \n')
                f.write(f'Final word counts stored in:   {cfg.COUNTS_FILE}    \n')
            f.write('\n\n-----------------------------------------------------------\n')
            f.write('PROGRESS STATS:\n\n')
            f.write(f'{self.visited:8d} pages downloaded\n')
            f.write(f'{self.failed:8d} pages failed\n')
            f.write(f'{self.queued:8d} pages still queued\n')
            f.write('\n\n-----------------------------------------------------------\n')
            f.write(self.files_cnt_text)
            f.write('\n\n-----------------------------------------------------------\n')
            f.write(self.errors_text)
            f.write('\n\n-----------------------------------------------------------\n')
            f.write(self.top_words_text)
            f.write('\n\n-----------------------------------------------------------\n')


