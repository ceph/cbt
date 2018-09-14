import sys
import sqlite3
import os
import yaml
import csv

import settings
import logging
import benchmarkfactory

VERSION = 0.1

logger = logging.getLogger('cbt')

class DB:
    def __init__(self, rebuild):
        self.archive_dir = settings.general.get('archive_dir')
        self.db_file = os.path.join(self.archive_dir, 'results.sqlite3')
        create_db = False

        if os.path.exists(self.db_file):
            if rebuild:
                logger.info('Results index found.  Rebuilding.')
                os.remove(self.db_file)
                create_db = True
        else:
            logger.info('Results index not found.  Creating.')
            create_db = True

        self.conn = sqlite3.connect(self.db_file)
        self.c = self.conn.cursor()

        if create_db:
            self.create()

    def execute(self, query):
        self.c.execute(query)

    def commit(self):
        self.conn.commit()

    def create(self):
        self.execute('''CREATE TABLE if not exists settings (key TEXT, value TEXT)''')
        self.execute("INSERT INTO settings VALUES ('version', %s)" % VERSION)
        results_dir = os.path.join(self.archive_dir, 'results')

        for test_dir in os.listdir(results_dir):
            test_dir_full = os.path.join(results_dir, test_dir)
            if os.path.isdir(test_dir_full):
                self.index_results(test_dir_full)

        self.commit()

    def index_results(self, test_dir):
        yaml_file = os.path.join(test_dir, 'benchmark_config.yaml')
        with open(yaml_file, 'r') as f:
           bconfig = yaml.load(f)

#        print bconfig
        bench_name =  bconfig.get('benchmark', {}).get('benchmark', "")
        benchmark = benchmarkfactory.get_object(bench_name)
        benchmark.index_results(test_dir, self)

    def query(self, q):
        logger.info('Executing Query: %s' %q)
        self.execute(q)

        if settings.general.get('format') == 'json': 
            r = [dict((self.c.description[i][0], value) \
               for i, value in enumerate(row)) for row in self.c.fetchall()]
            logger.info((r[0] if r else None))
        elif settings.general.get('format') == 'csv':
            r = self.c.fetchall()
            w = csv.writer(sys.stdout)
            w.writerows(r)
        else:
            logger.info((self.c.fetchone()))

    def close(self):
        self.c.close()
        self.conn.close()

