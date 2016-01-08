#!/usr/bin/env python

# TODO:  handle column default values
#
# TODO:  timestamp columns with specifiers
#
# TODO:  check if the following is the same for the common tables:
# tables.engine
# tables.table_collation
#
# TODO:  add a hash of the full path to the database names.
# right now just don't deal with schema files
# with the same base name.
#
# db1 = "%s_%s" % (file1, hash1.hexdigest())
# db2 = "%s_%s" % (file2, hash2.hexdigest())
    


import MySQLdb
# docs at http://mysql-python.sourceforge.net/MySQLdb.html

import re
import sys
import pprint
import argparse
import time
import os
import hashlib
import string
import subprocess
import dsn

from warnings import filterwarnings

# function normalize {
#     egrep -v "^[[:space:]]*--|^[[:space:]]*SET"  | \
#     egrep -v '^[[:space:]]*\$'  | \
#     sed -r "s/ AUTO_INCREMENT=([0-9]+)//"  | \
#     tr '[A-Z]' '[a-z]'  | \
#     sed -r -e "s/[[:space:]]?default[[:space:]]+collate[=[:space:]]+[a-z0-9_]+//" \
#            -e "s/[[:space:]]?collate[=[:space:]]+[a-z0-9_]+//" \
#            -e "s/[[:space:]]?[%][a-z0-9_]+[%]//" | \
#     grep -v 'set character_set_client'  | \
#     grep -v 'saved_cs_client'  | \
#     sed '/^\s*$/d' | \
#     sed 's/^\s*//' | \
#     sed 's/\s*$//' | \
#     sed "s/[[:space:]]\+/ /g" | \
#     sed 's/[[:space:]]*\([=,()]\)[[:space:]]*/\1/g' | \
#     sed 's/,//g' | \
#     sed "s/ comment='.*'//g" | \
#     sed "s/ comment '.*'//g"
# }

def normalize(s):
    lines = s.split('\n')
    lines = [l + '\n' for l in lines]
    lines.insert(0, 'echo')

    p1 = subprocess.Popen(lines, stdout=subprocess.PIPE)
    filtercmd = """
     egrep -v "^[[:space:]]*--|^[[:space:]]*SET"  | \
     egrep -v '^[[:space:]]*\$'  | \
     sed -E "s/ AUTO_INCREMENT=([0-9]+)//"  | \
     tr '[A-Z]' '[a-z]'  | \
     sed -E -e "s/[[:space:]]?default[[:space:]]+collate[=[:space:]]+[a-z0-9_]+//" \
            -e "s/[[:space:]]?collate[=[:space:]]+[a-z0-9_]+//" \
            -e "s/[[:space:]]?[%][a-z0-9_]+[%]//" | \
     grep -v 'set character_set_client'  | \
     grep -v 'saved_cs_client'  | \
     sed '/^\s*$/d' | \
     sed 's/^\s*//' | \
     sed 's/\s*$//' | \
     sed "s/[[:space:]]\+/ /g" | \
     sed 's/[[:space:]]*\([=,()]\)[[:space:]]*/\1/g' | \
     sed 's/,//g' | \
     sed "s/ comment='.*'//g" | \
     sed "s/ comment '.*'//g" | \
     sort
"""
    p2 = subprocess.check_output(filtercmd, stdin=p1.stdout, shell=True)
    p1.stdout.close()
    p2 = re.sub(r'[^\w]', '', p2)
    return p2

def shacmd(s):
    hash = hashlib.sha1()
    hash.update(s)
    return hash.hexdigest()

def dbdump(dbname):
    dumpcmd = "mysqldump -u %(user)s -p%(passwd)s --no-data %(dbname)s" % {
        "user" : dsn.user,
        "passwd" : dsn.passwd,
        "dbname" : dbname
        }

    # suppress warnings from mysqldump.  i know, i know.
    with open(os.devnull, 'w') as devnull:
        return subprocess.check_output(dumpcmd, stderr=devnull, shell=True)

def dbchecksum(dbname):
    return shacmd(normalize(dbdump(dbname)))

def diff_column(cursor, table, column, db1, db2):
    metadata_columns = ['column_type', 'is_nullable', 'column_default']
    query = """
 select %(metadata_columns)s
 from information_schema.columns
 where table_schema = '%(db)s' and table_name = '%(table)s' and column_name = '%(column)s'
"""
    q1 = query % { "metadata_columns" : ', '.join(metadata_columns),
                   "db" : db1,
                   "table" : table,
                   "column" : column }

    q2 = query % { "metadata_columns" : ', '.join(metadata_columns),
                   "db" : db2,
                   "table" : table,
                   "column" : column }

    cursor.execute(q1)
    for row in cursor.fetchall():
        d1 = dict(zip(metadata_columns,
                      row))

    cursor.execute(q2)
    for row in cursor.fetchall():
        d2 = dict(zip(metadata_columns,
                      row))

    # if anything changed, the modify clause has
    # to have everything in it.  otherwise it must be empty.

    for c in metadata_columns:
        if d1[c] != d2[c]:
            return d2

def construct_altertable(cursor, fromdb, todb, table, **kwargs):
    """constructs the alter table statement to reflect column drops, adds, and mods
    to a single table.
    """

    drop = None
    add = None
    diffs = None

    if 'drop' in kwargs:
        drop = kwargs['drop']

    if 'add' in kwargs:
        add = kwargs['add']

    if 'diffs' in kwargs:
        diffs = kwargs['diffs']

    if drop is None and add is None and diffs is None:
        return None

    clauses = []
    if drop is not None:
        for d in drop:
            clauses.append("DROP COLUMN %(column)s" % {
                    "column" : d
                    })

    # for columns that we add, go get the metadata
    if add is not None:
        metadata_columns = ['column_type', 'is_nullable', 'column_default', 'column_name']
        addcolumns = ',\''.join(add)
        sql = """
 select %(metadata_columns)s
 from information_schema.columns
 where table_schema = '%(db)s' and
 table_name = '%(table)s' and
 column_name in ('%(columns)s')""" % {
            "metadata_columns" : ', '.join(metadata_columns),
            "db" : todb,
            "table" : table,
            "columns" : addcolumns }
        cursor.execute(sql)
        for row in cursor.fetchall():
            d1 = dict(zip(metadata_columns,
                      row))
            clause = ""
            if d1['is_nullable'] == 'NO':
                clause = ("ADD COLUMN %(colname)s %(data_type)s NOT NULL" % {
                        "colname" : d1['column_name'],
                        "data_type" : d1['column_type']
                        })
            else:
                clause = ("ADD COLUMN %(colname)s %(data_type)s" % {
                        "colname" : d1['column_name'],
                        "data_type" : d1['column_type']
                        })
            if d1['column_default']:
                clause += " DEFAULT %s" % (d1['column_default'])
            clauses.append(clause)

    if diffs is not None:
        for k in diffs.keys():
            clause = "MODIFY COLUMN %(column)s %(datatype)s" % {
                "column" : k,
                "datatype" : diffs[k]['column_type']
                }
            if 'is_nullable' in diffs[k]:
                if diffs[k]['is_nullable'] == 'NO':
                    clause += " NOT"
                clause += " NULL"
            if 'column_default' in diffs[k]:
                if diffs[k]['column_default']:
                    clause += " DEFAULT %s" % diffs[k]['column_default']
            clauses.append(clause)
    
    if len(clauses) > 0:
        dml = "ALTER TABLE %(db)s.%(table)s " % {
            "db" : fromdb,
            "table" : table
            }

        return dml + ', '.join(clauses)

def diff_table(cursor, table, db1, db2):
    query = "select column_name from information_schema.columns where table_schema = '%s' and table_name = '%s'"
    q1 = query % (db1, table)
    q2 = query % (db2, table)

    columns1 = set()
    columns2 = set()

    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['column_name'],
                     row))
        columns1.add(d['column_name'])

    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['column_name'],
                     row))
        columns2.add(d['column_name'])

    common = columns1 & columns2
    columns_to_drop = columns1 - columns2
    columns_to_add = columns2 - columns1
    
    any_changes = False
    if len(columns_to_drop) > 0:
        any_changes = True

    if len(columns_to_add) > 0:
        any_changes = True

    column_diffs = {}
    for c in common:
        diffs = diff_column(cursor, table, c, db1, db2)
        if diffs is not None:
            column_diffs[c] = diffs

    if len(column_diffs) > 0:
        any_changes = True

    if len(columns_to_drop) == 0:
        columns_to_drop = None
    else:
        columns_to_drop = tuple(columns_to_drop)

    if len(columns_to_add) == 0:
        columns_to_add = None
    else:
        columns_to_add = tuple(columns_to_add)

    if len(column_diffs) == 0:
        column_diffs = None

    return (columns_to_drop, columns_to_add, column_diffs)

def diff_databases(cursor, db1, db2):
    tquery = "select table_name from information_schema.tables where table_schema = '%s'"
    q1 = tquery % db1
    q2 = tquery % db2

    db1tables = set()
    db2tables = set()

    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db1tables.add(d['table_name'])

    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db2tables.add(d['table_name'])

    common = db1tables & db2tables
    db1only = db1tables - db2tables
    db2only = db2tables - db1tables

    if len(db1only) > 0:
        print "============== %s only ==============" % db1
        print db1only

    if len(db2only) > 0:
        print "============== %s only ==============" % db2
        print db2only

    # just look at one table for now
    common_list = list(common)
    for c in common_list:
        diff_table(cursor, c, db1, db2)

def create_db_from_file(cursor, file):
    fh = open(file, 'r')
    schema = fh.read()
    schema = string.replace(schema, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')
    ddls = schema.split(';')
    for ddl in ddls:
        ddl = ddl.strip()
        if len(ddl) == 0:
            continue
        cursor.execute(ddl)

def main():
    filterwarnings('ignore', category = MySQLdb.Warning)
    parser = argparse.ArgumentParser()
    parser.add_argument("file1", help="input file")
    parser.add_argument("file2", help="input file")
    args = parser.parse_args()
    
    file1 = args.file1
    file2 = args.file2
    
    if not os.path.exists(file1):
        print '%s does not exist' % file1
        sys.exit(1)
    
    if not os.path.exists(file2):
        print '%s does not exist' % file2
        sys.exit(1)
    
    if not os.path.isfile(file1):
        print '%s is not a regular file' % file1
        sys.exit(1)
    
    if not os.path.isfile(file2):
        print '%s is not a regular file' % file2
        sys.exit(1)
    
    if os.path.samefile(file1, file2):
        print '%s and %s are the same file' % (file1, file2)
        sys.exit(1)
    
    path1 = os.path.abspath(file1)
    path2 = os.path.abspath(file2)
    
    # need 2 hash objects in order to use them independently
    # for each path
    
    hash1 = hashlib.md5()
    hash2 = hash1.copy()
    
    hash1.update(path1)
    hash2.update(path2)
    
    db1 = "%s" % (os.path.splitext(file1)[0])
    db2 = "%s" % (os.path.splitext(file2)[0])
    
    if db1 == db2:
        print 'not dealing with same-name schema files right now'
        sys.exit(1)
    
    db = dsn.getConnection()
    c = db.cursor()
    
    c.execute("drop database if exists %(db)s" % { "db" : db1 })
    c.execute("create database %(db)s" % { "db" : db1 })
    
    c.execute("drop database if exists %(db)s" % { "db" : db2 })
    c.execute("create database %(db)s" % { "db" : db2 })
    
    c.close()
    db.close()
    
    conn1 = dsn.getConnection(db1)
    cursor1 = conn1.cursor()
    
    conn2 = dsn.getConnection(db2)
    cursor2 = conn2.cursor()
    
    create_db_from_file(cursor1, file1)
    create_db_from_file(cursor2, file2)
    
    cursor1.close()
    conn1.close()
    cursor2.close()
    conn2.close()
    
    conn = dsn.getConnection('information_schema')
    cursor = conn.cursor()
    
    # finally.  let's get to work.
    diff_databases(cursor, db1, db2)

if __name__ == '__main__':
    main()
