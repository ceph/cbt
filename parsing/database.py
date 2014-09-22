import sqlite3

conn = sqlite3.connect(':memory:')

FORMAT=['hash', 'testname', 'iteration', 'benchmark', 'osdra', 'opsize', 'cprocs', 'iodepth', 'testtype', 'writebw', 'readbw']
TYPES={'hash':'text primary key', 'testname':'text', 'iteration':'integer', 'benchmark':'text', 'osdra':'integer', 'opsize':'integer', 'cprocs':'integer', 'iodepth':'integer', 'testtype':'text', 'writebw':'real', 'readbw':'real'}

def create_db():
    c = conn.cursor()
    q = 'CREATE TABLE if not exists results ('
    values = []
    for key in FORMAT:
        values.append("%s %s" % (key,TYPES[key]))
    q += ', '.join(values)+')'
    print q
    c.execute(q)
    conn.commit()

def insert(values):
    c = conn.cursor()
    c.execute('INSERT INTO results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?)', values)
    conn.commit()

def update_readbw(hashval, bw):
    c = conn.cursor()
    c.execute('UPDATE results SET readbw = readbw + ? WHERE hash = ?', (bw, hashval))
    conn.commit()

def update_writebw(hashval, bw):
    c = conn.cursor()
    c.execute('UPDATE results SET writebw = writebw + ? WHERE hash = ?', (bw, hashval))
    conn.commit()

def get_values(column):
    c = conn.cursor()
    # Careful here, this could lead to an SQL injection but appears necessary
    # since bindings can't be used for column names.
    c.execute('SELECT distinct %s FROM results ORDER BY %s' % (column, column))
    return [item[0] for item in c.fetchall()]

def fetch_table(params):
    c = conn.cursor()
    distincts = {}

    for param in params:
        distincts[param] = get_values(param)

    c.execute('SELECT testname,%s,readbw,writebw FROM results ORDER BY %s,testname' % (','.join(params), ','.join(params)))
    testnames = get_values('testname')

    table = []
    writerow = []
    readrow = []
    for row in c.fetchall():
        # Check to make sure we aren't missing a test
        while row[0] != testnames[len(writerow)]:
             blank = ['%s' % testnames[len(writerow)], '']
             writerow.append(blank)
             readrow.append(blank)
        writerow.append([row[0],row[-1]])
        readrow.append([row[0],row[-2]])
        if len(writerow) == len(testnames):
             pre = []
             for i in xrange(0, len(params)):
                  pre.append([params[i],row[i+1]])
             table.append(pre + [['optype', 'write']] + writerow)
             table.append(pre + [['optype', 'read']] + readrow)
             writerow = []
             readrow = []
    return table
