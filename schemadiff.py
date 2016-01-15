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
#        - change ON UPDATE, ON DELETE
#
# TODO:  handle comments - escape chars before reinserting

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

def diff_fks(cursor, table, db1, db2):
    query = """
select
	local_part.constraint_name,
concat (
'ADD CONSTRAINT ',
local_part.constraint_name,
' FOREIGN KEY (',
local_part.columns,
') REFERENCES ',
ref_part.referenced_table_name,
'(',
ref_part.referenced_columns,
')'
) expr
from
(
    select
    
    -- table_constraints
    c.TABLE_SCHEMA       ,
    c.TABLE_NAME         ,
    c.CONSTRAINT_NAME    ,
    
    -- key_column_usage
    group_concat(k.column_name order by k.ordinal_position) columns
    
    from information_schema.table_constraints c
    inner join information_schema.key_column_usage k
    on
    c.constraint_name = k.constraint_name and
    c.table_schema = k.table_schema and
    c.table_name = k.table_name
    
    where 
    c.constraint_type = 'FOREIGN KEY'
    
    group by c.table_schema, c.table_name, c.constraint_name
) local_part
inner join
(
    select
    
    -- table_constraints
    c.TABLE_SCHEMA       ,
    c.TABLE_NAME         ,
    c.CONSTRAINT_NAME    ,
    
    k.REFERENCED_TABLE_NAME         ,
    group_concat(k.referenced_column_name order by k.ordinal_position) referenced_columns
    
    from information_schema.table_constraints c
    inner join information_schema.key_column_usage k
    on
    c.constraint_name = k.constraint_name and
    c.table_schema = k.table_schema and
    c.table_name = k.table_name
    
    where 
    c.constraint_type = 'FOREIGN KEY'
    
    group by c.table_schema, c.table_name, c.constraint_name
) ref_part

on  local_part.table_schema = ref_part.table_schema
and local_part.table_name = ref_part.table_name
and local_part.constraint_name = ref_part.constraint_name

where local_part.table_schema = '%(db)s'
and local_part.table_name = '%(table)s'
;
"""
    q1 = query % { "db" : db1, "table" : table }
    q2 = query % { "db" : db2, "table" : table }

    select_columns = ['constraint_name', 'expr']
    indexdefs1 = {}
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs1[d['constraint_name']] = d

    indexdefs2 = {}
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs2[d['constraint_name']] = d

    indexes_1 = set(indexdefs1.keys())
    indexes_2 = set(indexdefs2.keys())

    add = indexes_2 - indexes_1
    drop = indexes_1 - indexes_2
    common = indexes_1 & indexes_2

    drop_clauses = []
    add_clauses = []

    for k in drop:
        drop_clauses.append("DROP FOREIGN KEY %s" % k)

    for k in add:
        add_clauses.append(indexdefs2[k]['expr'])

    for k in common:
        if indexdefs1[k]['expr'] != indexdefs2[k]['expr']:
            drop_clauses.append("DROP FOREIGN KEY %s" % k)
            add_clauses.append(indexdefs2[k]['expr'])

    return drop_clauses, add_clauses

def diff_plain_indexes(cursor, table, db1, db2):
    query = """
select
    s.index_name,
concat (
'ADD KEY ',
s.index_name,
'(',
group_concat(s.column_name, if(s.sub_part, concat('(', s.sub_part, ')'), '') order by s.seq_in_index),
')'
) expr
from
     information_schema.statistics s
left join
(
    select distinct
    s.table_catalog,
    s.table_schema,
    s.table_name,
    s.index_name,
    1
    
    from information_schema.statistics s
    
    inner join information_schema.key_column_usage k
    on s.table_catalog = k.table_catalog
    and s.table_schema = k.table_schema
    and s.table_name = k.table_name
    and s.index_name = k.constraint_name
    
    left join information_schema.table_constraints c
    on k.table_schema = c.table_schema
    and k.table_name = c.table_name
    and k.constraint_name = c.constraint_name
    
    where 
        s.table_schema = '%(db)s' and
        s.table_name = '%(table)s' and
        c.constraint_type <> 'foreign key'

) t
on
t.table_catalog = s.table_catalog and
t.table_schema = s.table_schema and
t.table_name = s.table_name and
t.index_name = s.index_name

where t.table_catalog is null and
s.table_schema = '%(db)s' and
s.table_name = '%(table)s'

group by s.table_catalog, s.table_schema, s.table_name, s.index_name
"""
    q1 = query % { "db" : db1, "table" : table }
    q2 = query % { "db" : db2, "table" : table }

#    print q1, ';'
#    print q2, ';'

    select_columns = ['index_name', 'expr']
    indexdefs1 = {}
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs1[d['index_name']] = d

    indexdefs2 = {}
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs2[d['index_name']] = d

    indexes_1 = set(indexdefs1.keys())
    indexes_2 = set(indexdefs2.keys())

    add = indexes_2 - indexes_1
    drop = indexes_1 - indexes_2
    common = indexes_1 & indexes_2

#    print drop
#    print add
#    print common

    drop_clauses = []
    add_clauses = []

    for k in drop:
        drop_clauses.append("DROP INDEX %s" % k)

    for k in add:
        add_clauses.append(indexdefs2[k]['expr'])

    for k in common:
        if indexdefs1[k]['expr'] != indexdefs2[k]['expr']:
            drop_clauses.append("DROP INDEX %s" % k)
            add_clauses.append(indexdefs2[k]['expr'])

    return drop_clauses, add_clauses

def diff_constrained_indexes(cursor, table, db1, db2):
    # UNIQUE or PRIMARY KEY indexes
    query = """

select
    index_name,
    constraint_type,
concat (
'ADD ',
case constraint_type
when 'PRIMARY KEY' then constraint_type
when 'UNIQUE'      then concat('UNIQUE KEY ', index_name)
end,
'(',
group_concat(column_name, if(sub_part, concat('(', sub_part, ')'), '') order by seq_in_index),
')'
) expr
from
(
    select distinct
           s.table_catalog,
           s.table_schema,
           s.table_name,
           s.index_name,
           s.column_name, 
           s.seq_in_index,
           s.sub_part,
           c.constraint_type
    
    from information_schema.statistics s
    
    inner join information_schema.key_column_usage k
    on s.table_catalog = k.table_catalog
    and s.table_schema = k.table_schema
    and s.table_name = k.table_name
    and s.index_name = k.constraint_name
    
    left join information_schema.table_constraints c
    on k.table_schema = c.table_schema
    and k.table_name = c.table_name
    and k.constraint_name = c.constraint_name
    
    where 
        s.table_schema = '%(db)s' and
        s.table_name = '%(table)s' and
        c.constraint_type <> 'foreign key'  -- constraint_type is never null
) t
group by
	table_catalog,
	table_schema,
	table_name,
	index_name,
	constraint_type
"""

    q1 = query % { "db" : db1, "table" : table }
    q2 = query % { "db" : db2, "table" : table }

    select_columns = ['index_name', 'constraint_type', 'expr']
    indexdefs1 = {}
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs1[d['index_name']] = d

    indexdefs2 = {}
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(select_columns,
                     row))
        indexdefs2[d['index_name']] = d

    indexes_1 = set(indexdefs1.keys())
    indexes_2 = set(indexdefs2.keys())

    add = indexes_2 - indexes_1
    drop = indexes_1 - indexes_2
    common = indexes_1 & indexes_2

    drop_clauses = []
    add_clauses = []

    for k in drop:
        if indexdefs1[k]['constraint_type'] == 'PRIMARY KEY':
            drop_clauses.append("DROP PRIMARY KEY")
        else:
            drop_clauses.append("DROP INDEX %s" % k)

    for k in add:
        add_clauses.append(indexdefs2[k]['expr'])

    for k in common:
        if indexdefs1[k]['expr'] != indexdefs2[k]['expr']:
            if indexdefs1[k]['constraint_type'] == 'PRIMARY KEY':
                drop_clauses.append("DROP PRIMARY KEY")
            else:
                drop_clauses.append("DROP INDEX %s" % k)
            add_clauses.append(indexdefs2[k]['expr'])

    return drop_clauses, add_clauses

def diff_table_indexes(cursor, table, db1, db2):
    drop_clauses = []
    add_clauses = []

    (drop, add) = diff_plain_indexes(cursor, table, db1, db2)
    # print "plain drop:", drop
    # print "plain add:", add

    drop_clauses += drop
    add_clauses += add

    (drop, add) = diff_constrained_indexes(cursor, table, db1, db2)
    # print "constrained drop:", drop
    # print "constrained add:", add
    drop_clauses += drop
    add_clauses += add

    (drop, add) = diff_fks(cursor, table, db1, db2)
    # print "fk drop:", drop
    # print "fk add:", add
    drop_clauses += drop
    add_clauses += add

    return drop_clauses, add_clauses

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
    when column_type like '%%char%%' then concat(' DEFAULT ''', column_default, '''')
    when column_type like '%%text' then concat(' DEFAULT ''', column_default, '''')
    when column_type = 'timestamp' and column_default <> 'current_timestamp' then concat(' DEFAULT ''', column_default, '''')
    when column_type = 'datetime' and column_default <> 'current_timestamp' then concat(' DEFAULT ''', column_default, '''')
    when column_type = 'date' and column_default <> 'current_timestamp' then concat(' DEFAULT ''', column_default, '''')
    when column_type = 'time' and column_default <> 'current_timestamp' then concat(' DEFAULT ''', column_default, '''')
    else concat(' DEFAULT ', column_default)
    end,
    if(extra = '', '', concat(' ', extra)),
    if(column_comment = '', '', concat(' COMMENT ''',
                                       replace(column_comment, '''', ''''''),
                                       ''''))
) column_def
 from information_schema.columns
 where table_schema = '%(db)s'
 and table_name = '%(table)s'
 and column_name in (%(columns)s)
"""

    drop_clauses = []
    add_clauses = []
    modify_clauses = []

    for d in columns_to_drop:
        drop_clauses.append("DROP COLUMN %s" % d)

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
            add_clauses.append("ADD COLUMN %s" % d['column_def'])
            
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
                modify_clauses.append("MODIFY COLUMN %s" % d2[c])

    return drop_clauses, add_clauses, modify_clauses

def diff_table(cursor, table, db1, db2):
    (column_drop, column_add, column_modify) = diff_table_columns(cursor, table, db1, db2)
    (index_drop, index_add) = diff_table_indexes(cursor, table, db1, db2)

    dmls = []
    if len(column_drop + index_drop) > 0:
        drop_clauses = ', '.join(column_drop + index_drop)
        dml = "ALTER TABLE %(db)s.%(table)s %(therest)s" % {
            "db" : db1,
            "table" : table,
            "therest" : drop_clauses
            }    
        dmls.append(dml)

    if len(column_add + index_add + column_modify) > 0:
        other_clauses = ', '.join(column_add + index_add + column_modify)
        dml = "ALTER TABLE %(db)s.%(table)s %(therest)s" % {
            "db" : db1,
            "table" : table,
            "therest" : other_clauses
            }
        dmls.append(dml)

    return dmls
            

def diff_databases(cursor, db1, db2):
    tquery = "select table_name from information_schema.tables where table_schema = '%s'"
    q1 = tquery % db1
    q2 = tquery % db2

    db1tables = set()
    db2tables = set()

    print "getting tables for %s" % db1
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db1tables.add(d['table_name'])

    print "getting tables for %s" % db2
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db2tables.add(d['table_name'])

    print "done with all that"

    common = db1tables & db2tables
    db1only = db1tables - db2tables
    db2only = db2tables - db1tables

    print "dropping obsolete tables"
    dmls = []
    if len(db1only) > 0:
        for dropme in db1only:
            dmls.append("DROP TABLE %(db)s.%(table)s" % {
                "db" : db1,
                "table" : dropme })

    print "adding new tables"
    if len(db2only) > 0:
        for addme in db2only:
            dmls.append("CREATE TABLE %(db1)s.%(table)s LIKE %(db2)s.%(table)s" % {
                "db1" : db1,
                "db2" : db2,
                "table" : addme })

    print "dealing with columns and indexes"

    common_list = list(common)
    for c in common_list:
        dmls += diff_table(cursor, c, db1, db2)

    for dml in dmls:
        print dml
        cursor.execute(dml)

    print "aftermath:"
    print "%s checksum:  %s" % (db1, dbchecksum(db1))
    print "%s checksum:  %s" % (db2, dbchecksum(db2))

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
