#!/usr/bin/env python

# TODO:  handle auto_increment columns
#
# TODO:  check if the following is the same for the common tables:
# tables.table_collation
#
# TODO:  add a hash of the full path to the database names.
# right now just don't deal with schema files
# with the same base name.
#
# db1 = "%s_%s" % (file1, hash1.hexdigest())
# db2 = "%s_%s" % (file2, hash2.hexdigest())
#
# TODO:  handle fulltext indexes
#
# TODO:  foreign keys
#        - change ON UPDATE, ON DELETE
#

import MySQLdb
# docs at http://mysql-python.sourceforge.net/MySQLdb.html

import logging
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
     sed "s/ comment '.*'//g"
"""
    p2 = subprocess.check_output(filtercmd, stdin=p1.stdout, shell=True)
    p1.stdout.close()
    lines = p2.split('\n')
    lines = [re.sub(r'[^\w]+', '', l) for l in lines]

    return '\n'.join(lines)

def shacmd(s):
    hash = hashlib.sha1()
    hash.update(s)
    return hash.hexdigest()

def dbdump(dbname):
    try:
        dumpcmd = "mysqldump -u %(user)s -p%(passwd)s --no-data --skip-add-drop-table %(dbname)s" % {
            "user" : dsn.user,
            "passwd" : dsn.passwd,
            "dbname" : dbname
            }
    
        # suppress warnings from mysqldump.  i know, i know.
        with open(os.devnull, 'w') as devnull:
            return subprocess.check_output(dumpcmd, stderr=devnull, shell=True)
    except Exception as e:
        logging.error(e)
        raise

def schemachecksum(dbschema):
    nized = normalize(dbschema)
    lines = nized.split('\n')
    sortedlines = sorted(lines)
    return shacmd('\n'.join(sortedlines))

def dbchecksum(dbname):
    return schemachecksum(dbdump(dbname))

def disgorge(dbname):
    # write out the raw db schema
    schema = dbdump(dbname)
    f = open('%s.sql' % dbname, 'w')
    f.write(schema)
    f.close()

    # write normalized schema
    nized = normalize(schema)
    f = open('%s.nml' % dbname, 'w')
    f.write(nized)
    f.close()

    # normalized, then sorted
    lines = nized.split('\n')
    sortedlines = sorted(lines)
    sortedlines = '\n'.join(sortedlines)
    f = open('%s.srt' % dbname, 'w')
    f.write(sortedlines)
    f.close()

    # checksum
    f = open('%s.cs' % dbname, 'w')
    f.write(shacmd(sortedlines))
    f.close()

def _format_dmls(table, db1, db2, clauses, prettyprint):
    dmls = []
    
    if len(clauses) > 0:
        if (prettyprint):
            clauses_str = ',\n\t'.join(clauses)
            dml = "ALTER TABLE %(table)s\n\t%(therest)s\n\t;" % {
                "db" : db1,
                "table" : table,
                "therest" : clauses_str
                }    
        else:
            clauses_str = ', '.join(clauses)
            dml = "ALTER TABLE %(table)s %(therest)s;" % {
                "db" : db1,
                "table" : table,
                "therest" : clauses_str
                }    

        dmls.append(dml)

    return dmls

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
    drop_clauses += drop
    add_clauses += add

    (drop, add) = diff_constrained_indexes(cursor, table, db1, db2)
    drop_clauses += drop
    add_clauses += add

    (drop, add) = diff_fks(cursor, table, db1, db2)
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

def diff_table_engines(cursor, table, db1, db2):
    query = """
select engine
 from information_schema.tables
 where table_schema = '%(db)s'
 and table_name = '%(table)s'
"""

    q1 = query % { "db" : db1, "table" : table }
    q2 = query % { "db" : db2, "table" : table }

    select_columns = ['engine']
    clauses = []

    cursor.execute(q1)
    engine1 = None
    for row in cursor.fetchall():
        engine1 = row[0]

    engine2 = None
    cursor.execute(q2)
    for row in cursor.fetchall():
        engine2 = row[0]

    if engine1 != engine2:
        clauses.append("ENGINE = %s" % engine2)

    return clauses

def diff_table(cursor, table, db1, db2, prettyprint=False):
#    logging.debug("diffing table %s" % table)
    (column_drop, column_add, column_modify) = diff_table_columns(cursor, table, db1, db2)
    (index_drop, index_add) = diff_table_indexes(cursor, table, db1, db2)
    (engines) = diff_table_engines(cursor, table, db1, db2)

    dmls = []
    dmls += _format_dmls(table, db1, db2, (column_drop + index_drop), prettyprint)
    dmls += _format_dmls(table, db1, db2, (column_add +
                                           index_add + column_modify + engines),
                         prettyprint)

    return dmls

def diff_databases(cursor, db1, db2):
    tquery = "select table_name from information_schema.tables where table_schema = '%s'"
    q1 = tquery % db1
    q2 = tquery % db2

    db1tables = set()
    db2tables = set()

    logging.debug("getting tables for %s" % db1)
    cursor.execute(q1)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db1tables.add(d['table_name'])

    logging.debug("getting tables for %s" % db2)
    cursor.execute(q2)
    for row in cursor.fetchall():
        d = dict(zip(['table_name'],
                     row))
        db2tables.add(d['table_name'])

    common = db1tables & db2tables
    tables_to_drop = db1tables - db2tables
    tables_to_add = db2tables - db1tables

    dmls = []
    for dropme in tables_to_drop:
        dmls.append("DROP TABLE %(table)s;" % {
                "db" : db1,
                "table" : dropme })

    for addme in tables_to_add:
        replacevalues = {
            "db1" : db1,
            "db2" : db2,
            "table" : addme }
        cursor.execute("show create table %(db2)s.%(table)s;" % replacevalues)
        for row in cursor.fetchall():
            ctable = row[1]
            dmls.append(ctable + ';')

    common_list = list(common)
    for c in common_list:
        dmls += diff_table(cursor, c, db1, db2, True)

    dmls.insert(0, "USE %s;" % db1)
    return dmls

def create_db_from_schema(cursor, dbname, schema):
    cursor.execute("drop database if exists %(db)s" % { "db" : dbname })
    cursor.execute("create database %(db)s" % { "db" : dbname })
    cursor.execute("use %(db)s" % { "db" : dbname })
    
    ddls = schema.split(';')
    for ddl in ddls:
        ddl = ddl.strip()
        if len(ddl) == 0:
            continue
        cursor.execute(ddl)

def diff_schemas(cursor, schema1, schema2, db1, db2, **kwargs):
    """
    kwargs:
    validate:  True or False
    dmlfile:  name of file to which DML statements should be written.
    """

    filterwarnings('ignore', category = MySQLdb.Warning)
    cs1 = schemachecksum(schema1)
    cs2 = schemachecksum(schema2)
    logging.debug("got schema checksums")

    if cs1 == cs2:
        print "databases are the same, nothing to do"
        return

    logging.debug("creating database %s" % db1)
    create_db_from_schema(cursor, db1, schema1)
    logging.debug("creating database %s" % db2)
    create_db_from_schema(cursor, db2, schema2)
    
    dmls = diff_databases(cursor, db1, db2)

    dmlfile = kwargs['dmlfile']
    if dmlfile:
        logging.debug("opening dml file %s" % dmlfile)
        dmlf = open(dmlfile, 'w')

    validate = kwargs['validate']

    for dml in dmls:
        if validate:
            logging.debug("EXECUTING: %s" % dml)
            cursor.execute(dml)
        if dmlfile:
            dmlf.write(dml + "\n")

    if dmlfile:
        dmlf.close()
        print "wrote file %s" % dmlfile

    if validate:
        print "aftermath:"
        print "%s checksum:  %s" % (db1, dbchecksum(db1))
        print "%s checksum:  %s" % (db2, dbchecksum(db2))
        disgorge(db1)
        disgorge(db2)

#    cursor.execute("drop database %(db)s" % { "db" : db1 })
#    cursor.execute("drop database %(db)s" % { "db" : db2 })

def log_in_to_p4(p4):
    try:
        # log in with existing ticket, if it's there.
        # need a way to find out if the current ticket is expired.
        # looks like p4python can't tell us that.

        p4.connect()
        tix = p4.run_tickets()
        if len(tix) > 0:
            logging.debug("got p4 login ticket")
            return

        logging.debug("couldn't get ticket, attempting user login")

        # we fall through to the code below and prompt for login
        # credentials.  this won't work if we have an existing
        # connection.  so, disconnect.
        p4.disconnect()

        # prompt for user
        default_p4_user = ''
        default_p4_client = ''
        default_p4_port = ''

        if 'P4USER' in os.environ:
            default_p4_user = os.environ['P4USER']
        elif 'USER' in os.environ:
            default_p4_user = os.environ['USER']

        if (len(default_p4_user) == 0):
            prompt = "p4 user: "
        else:
            prompt = "p4 user [%s]: " % default_p4_user

        input_p4_user = raw_input(prompt).strip()
        if len(input_p4_user) == 0:
            input_p4_user = default_p4_user
        p4.user = input_p4_user

        # prompt for client
        if 'P4CLIENT' in os.environ:
            default_p4_client = os.environ['P4CLIENT']

        if (len(default_p4_client) == 0):
            prompt = "p4 client: "
        else:
            prompt = "p4 client [%s]: " % default_p4_client

        input_p4_client = raw_input(prompt).strip()
        if len(input_p4_client) == 0:
            input_p4_client = default_p4_client
        p4.client = input_p4_client

        # prompt for port
        if 'P4PORT' in os.environ:
            default_p4_port = os.environ['P4PORT']

        if (len(default_p4_port) == 0):
            prompt = "p4 port: "
        else:
            prompt = "p4 port [%s]: " % default_p4_port

        input_p4_port = raw_input(prompt).strip()
        if len(input_p4_port) == 0:
            input_p4_port = default_p4_port
        p4.port = input_p4_port

        # prompt for password
        p4.password = getpass.getpass("P4 password:")
        p4.connect()
        p4.run_login()
        print "user %s connected to perforce" % input_p4_user
        logging.debug("P4 ticket: |%s|" % p4.password)

    except P4.P4Exception as e:
        p4.disconnect()
        for e in p4.warnings:
            logging.warning(e)
        for e in p4.errors:
            if not str(e).startswith("Password invalid"):
                # something else went wrong
                raise

def get_schema_from_filespec(p4, filespec):
    result = p4.run('print', '-q', filespec)
    result = ''.join(result[1:])
    result = string.replace(result, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')

    return result
