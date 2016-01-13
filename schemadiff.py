#!/usr/bin/env python

# TODO:  handle auto_increment columns
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
#
# TODO:  if we drop an indexed column, we don't need to drop the
#        associated index.    
#
# TODO:  handle fulltext indexes
#
# TODO:  foreign keys
#        - drop
#        - add
#        - change ON UPDATE, ON DELETE

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

def diff_table_indexes(cursor, table, db1, db2):
    query = """
select
	s.index_name,
	concat(
	'ADD ',
	case
	when c.constraint_type = 'PRIMARY KEY' then c.constraint_type
	when c.constraint_type = 'UNIQUE' then concat('UNIQUE KEY ', s.index_name)
	else concat('KEY ', s.index_name)
	end,
	'(',
	group_concat(column_name order by seq_in_index),
	')'
	) index_def
from information_schema.statistics s

left join information_schema.table_constraints c
on s.index_name = c.constraint_name
and s.table_schema = c.table_schema
and s.table_name = c.table_name

where s.table_schema = '%(db)s'
and s.table_name = '%(table)s'
and (constraint_type <> 'foreign key' or constraint_type is null)
 group by s.table_schema, s.table_name, s.index_name
"""
    q1 = query % {
        "db" : db1,
        "table" : table }

    q2 = query % {
        "db" : db2,
        "table" : table }

    indexdefs1 = {}
    cursor.execute(q1)
    for row in cursor.fetchall():
        indexdefs1[row[0]] = row[1]

    indexdefs2 = {}
    cursor.execute(q2)
    for row in cursor.fetchall():
        indexdefs2[row[0]] = row[1]

    indexes_1 = set(indexdefs1.keys())
    indexes_2 = set(indexdefs2.keys())

    add = indexes_2 - indexes_1
    drop = indexes_1 - indexes_2
    change = indexes_1 & indexes_2

    clauses = []
    for d in drop:
        if d == 'PRIMARY':
            clauses.append("DROP PRIMARY KEY")
        else:
            clauses.append("DROP INDEX %s" % d)

    for a in add:
        clauses.append(indexdefs2[a])

    for c in change:
        if c == 'PRIMARY':
            clauses.append("DROP PRIMARY KEY")
        else:
            clauses.append("DROP INDEX %s" % c)
        clauses.append(indexdefs2[c])

    return clauses

def diff_fks(cursor, table, db1, db2):
    # this query will fetch the constraint name and the 
    # alter table statement needed to construct it.

    query = """
select
    fk_part.constraint_name,
    concat(
              'ADD CONSTRAINT ',
              fk_part.constraint_name,
              ' FOREIGN KEY ',
              '(', fk_part.fk_columns, ') REFERENCES ',
              ref_part.referenced_table_name,
              '(', ref_part.ref_columns, ')'
    ) expr
from
(
    select table_schema,
           table_name,
           constraint_name,
           group_concat(column_name order by ordinal_position) fk_columns
    from information_schema.key_column_usage
    where referenced_table_name is not null
    group by table_schema, table_name, constraint_name
) fk_part
inner join
(
    select table_schema,
           table_name,
           constraint_name,
           referenced_table_name,
           group_concat(referenced_column_name order by position_in_unique_constraint) ref_columns
    from information_schema.key_column_usage
    where referenced_table_name is not null
    group by table_schema, table_name, constraint_name
) ref_part
on  fk_part.table_schema = ref_part.table_schema
and fk_part.table_name = ref_part.table_name
and fk_part.constraint_name = ref_part.constraint_name
where fk_part.table_schema = '%(db)s'
and fk_part.table_name = '%(table)s'
"""
    q1 = query % { "db" : db1, "table" : table }
    q2 = query % { "db" : db2, "table" : table }

    fkeys1 = {}
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['constraint_name', 'expr'],
                     row))
        fkeys1[d['constraint_name']] = d['expr']

    fkeys2 = {}
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['constraint_name', 'expr'],
                     row))
        fkeys2[d['constraint_name']] = d['expr']

    k1 = set(fkeys1.keys())
    k2 = set(fkeys2.keys())

    common = k1 & k2
    drop = k1 - k2
    add = k2 - k1

    clauses = []
    for d in drop:
        clauses.append("DROP FOREIGN KEY %(k)s" % {
                "k" : d })

    for c in common:
        clauses.append("DROP FOREIGN KEY %(k)s" % {
                "k" : c })
        clauses.append(fkeys2[c])

    for a in add:
        clauses.append(fkeys2[a])

    return clauses

def diff_table_columns(cursor, table, db1, db2):
    query = "select column_name from information_schema.columns where table_schema = '%s' and table_name = '%s'"
    q1 = query % (db1, table)
    q2 = query % (db2, table)

    columns1 = set()
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['column_name'],
                     row))
        columns1.add(d['column_name'])

    columns2 = set()
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['column_name'],
                     row))
        columns2.add(d['column_name'])

    common = columns1 & columns2
    columns_to_drop = columns1 - columns2
    columns_to_add = columns2 - columns1

    query = """
select
column_name,
concat(
column_name,
' ',
column_type,
if(is_nullable = 'NO', ' NOT NULL', ''),
case
when column_default is null then ''
when column_default = '' then ''
else concat(' DEFAULT ', column_default)
end,
if(extra = '', '', concat(' ', extra)),
if(column_comment = '', '', concat(' COMMENT ''', column_comment, ''''))
) column_def
 from information_schema.columns
 where table_schema = '%(db)s'
 and table_name = '%(table)s'
 and column_name in (%(columns)s)
"""

    clauses = []

    for d in columns_to_drop:
        clauses.append("DROP COLUMN %s" % d)

    if len(columns_to_add) > 0:
        column_list = "'" + "','".join(columns_to_add) + "'"
        q = query % {
            "db" : db2,
            "table" : table,
            "columns" : column_list }
        cursor.execute(q)
        for row in cursor.fetchall():
            d = dict(zip(['column_name', 'column_def'],
                         row))
            clauses.append("ADD COLUMN %s" % d['column_def'])
            
    if len(common) > 0:
        column_list = "'" + "','".join(common) + "'"
        q1 = query % {
            "db" : db1,
            "table" : table,
            "columns" : column_list }
        cursor.execute(q1)
        d1 = {}
        for row in cursor.fetchall():
            d1[row[0]] = row[1]

        q2 = query % {
            "db" : db2,
            "table" : table,
            "columns" : column_list }
        cursor.execute(q2)
        d2 = {}
        for row in cursor.fetchall():
            d2[row[0]] = row[1]

        for c in common:
            if d1[c] != d2[c]:
                clauses.append("MODIFY COLUMN %s" % d2[c])

    return clauses

def diff_table(cursor, table, db1, db2):
    clauses = []

    clauses += diff_table_columns(cursor, table, db1, db2)
    clauses += diff_table_indexes(cursor, table, db1, db2)
    clauses += diff_fks(cursor, table, db1, db2)

    if len(clauses) > 0:
        dml = "ALTER TABLE %(db)s.%(table)s " % {
            "db" : db1,
            "table" : table
            }

        return dml + ', '.join(clauses)
            

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
