#!/usr/bin/env python

import logging
import re
import P4
import schemadiff
import dsn
import string
import p4credentials
import subprocess

def compare():
    p4 = P4.P4()
    
    p4.port = p4credentials.p4port
    p4.user = p4credentials.p4user
    p4.client = p4credentials.p4client
    
    conn = None
    cursor = None
    
    try:
        conn = dsn.getConnection()
        cursor = conn.cursor()
    
        p4.connect()
    
        result = p4.run('print', '-q', p4credentials.filespec + '#47')
    
        # result is an array.  [0] is metadata.  the rest of the
        # array is the file, broken up into chunks, for some bizarre reason.
    
        schema47 = ''.join(result[1:])
        schema47 = string.replace(schema47, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')
    
        result = p4.run('print', '-q', p4credentials.filespec + '#49')
        schema49 = ''.join(result[1:])
        schema49 = string.replace(schema49, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')
    
        schemadiff.driver(cursor, schema47, schema49, 'schema47', 'schema49',
                          execute=True, dmlfile='p4test.sql')
    
    except P4.P4Exception:
        for e in p4.errors:
            print e
    finally:
        p4.disconnect()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

##############
def bss_date(p4, branch):
    """
    get timestamp for b-serviceschema in the given branch.  this will
    get the timestamp for the el5 build.
    """
    filespec = "//depot/%(branch)s/build.config" % { "branch" : branch }
    
    result = p4.run('print', '-q', filespec)

    buildconfig = ''.join(result[1:])
    buildconfig = buildconfig.split('\n')

    date_re = r'\d\d\d\d\.\d\d\.\d\d-\d\d\d\d'
    date_pattern = re.compile(date_re)
    line_pattern = re.compile(p4credentials.bss_regex + date_re, re.IGNORECASE)

    # find the line that matches, then dig out the timestamp from it.
    # there should be exactly one matching line.

    branch_ts = None
    for line in buildconfig:
        match = re.search(line_pattern, line)
        if match:
            match = re.search(date_pattern, match.string)
            if match:
                branch_ts = match.group()
                break

    if branch_ts is None:
        raise Exception("no service schema timestamp found in %s" % filespec)

    # build timestamps look like: YYYY.mm.dd-HHMM (e.g. 2015.09.01-1945)
    # p4 likes dates to look like YYYY/mm/dd:HH:MM:SS

    year = branch_ts[0:4]
    month = branch_ts[5:7]
    day = branch_ts[8:10]
    hour = branch_ts[11:13]
    minute = branch_ts[13:15]

    p4_ts = "%(YYYY)s/%(mm)s/%(dd)s:%(HH)s:%(MM)s:%(SS)s" % {
        "YYYY" : year,
        "mm" : month,
        "dd" : day,
        "HH" : hour,
        "MM" : minute,
        "SS" : '59'
        }

    return p4_ts

def get_schema_for_branch(p4, filespec, branch):
    p4_ts = bss_date(p4, branch)
    version = "%(filespec)s@%(ts)s" % {
        "filespec" : filespec,
        "ts" : p4_ts }

    result = p4.run('print', '-q', version)
    result = ''.join(result[1:])
    result = string.replace(result, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')

    return result

def diff_branches(p4, cursor, filespec, frombranch, tobranch):
    """
    show the changes needed to turn the schema in frombranch to the one in tobranch.
    """

    fromschema = get_schema_for_branch(p4, filespec, frombranch)
    toschema = get_schema_for_branch(p4, filespec, tobranch)
    
    # we will use the branch names as database names.  mysql doesn't like
    # database names with hyphens.

    frombranch = string.replace(frombranch, '-', '')
    tobranch = string.replace(tobranch, '-', '')

    cursor.execute("drop database if exists %(db)s" % { "db" : frombranch })
    cursor.execute("drop database if exists %(db)s" % { "db" : tobranch })

    schemadiff.diff_schemas(cursor, fromschema, toschema,
                            frombranch, tobranch,
                            dmlfile='woohoo.sql',
                            execute=True)

    cursor.execute("drop database %(db)s" % { "db" : frombranch })
    cursor.execute("drop database %(db)s" % { "db" : tobranch })


if __name__ == '__main__':
    FORMAT = "%(asctime)-15s %(funcName)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    p4 = P4.P4()
    p4.port = p4credentials.p4port
    p4.user = p4credentials.p4user
    p4.client = p4credentials.p4client
    p4.password = p4credentials.p4password

    try:
        p4.connect()
        p4.run_login()
        conn = dsn.getConnection()
        cursor = conn.cursor()

        diff_branches(p4, cursor,
                      "//depot/b-serviceschema/db/schema/common/sql/golden/service.sql",
                      'b-server-036',
                      'b-server-038')

        cursor.close()
        conn.close()

    except P4.P4Exception:
        for e in p4.warnings:
            print e
        for e in p4.errors:
            print e
    except Exception as e:
        print e
    finally:
        p4.disconnect()
