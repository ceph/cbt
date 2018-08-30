import sys
import sqlite3
import os
import yaml
import csv

import settings

VERSION = 0.1

class DB:
    def __init__(self, rebuild):
        self.archive_dir = settings.general.get('archive_dir')
        self.db_file = os.path.join(self.archive_dir, 'results.sqlite3')
        create_db = False

        if os.path.exists(self.db_file):
            if rebuild:
                print 'Results index found.  Rebuilding.'
                os.remove(self.db_file)
                create_db = True
        else:
            print 'Results index not found.  Creating.'
            create_db = True

        self.conn = sqlite3.connect(self.db_file)
        self.c = self.conn.cursor()

        if create_db:
            self.create()

    def create(self):
        self.c.execute('''CREATE TABLE if not exists settings (key TEXT, value TEXT)''')
        self.c.execute("INSERT INTO settings VALUES ('version', %s)" % VERSION)
        results_dir = os.path.join(self.archive_dir, 'results')

        for test_dir in os.listdir(results_dir):
            test_dir_full = os.path.join(results_dir, test_dir)
            if os.path.isdir(test_dir_full):
                self.analyze_dir(test_dir_full)

        self.conn.commit()

    def analyze_dir(self, test_dir):
        yaml_file = os.path.join(test_dir, 'benchmark_config.yaml')
        with open(yaml_file, 'r') as f:
           bconfig = yaml.load(f)

        print bconfig

    def query(self, q):
        print 'Executing Query: %s' %q
        self.c.execute(q)

        if settings.general.get('format') == 'json': 
            r = [dict((self.c.description[i][0], value) \
               for i, value in enumerate(row)) for row in self.c.fetchall()]
            print (r[0] if r else None) 
        elif settings.general.get('format') == 'csv':
            r = self.c.fetchall()
            w = csv.writer(sys.stdout)
            w.writerows(r)
        else:
            print(self.c.fetchone())

    def close(self):
        self.c.close()
        self.conn.close()

