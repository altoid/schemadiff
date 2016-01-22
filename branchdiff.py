#!/usr/bin/env python

import argparse
import os
import sys
import getpass
import MySQLdb
import logging
import re
import P4
import schemadiff
import dsn
import string
import p4credentials
import subprocess
from warnings import filterwarnings

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

    logging.debug("filespec:  %s" % version)
    return schemadiff.get_schema_from_filespec(p4, version)

def diff_branches(p4, cursor, filespec, frombranch, tobranch, database, dmlfile, validate):
    """
    show the changes needed to turn the schema in frombranch to the one in tobranch.
    """

    fromschema = get_schema_for_branch(p4, filespec, frombranch)
    toschema = get_schema_for_branch(p4, filespec, tobranch)
    
    # we will use the branch names as database names.  mysql doesn't like
    # database names with hyphens.

    frombranch = string.replace(frombranch, '-', '')
    tobranch = string.replace(tobranch, '-', '')

    db1 = "%(branch)s_%(db)s" % { 
        "branch" : frombranch,
        "db" : database
        }
    db2 = "%(branch)s_%(db)s" % { 
        "branch" : tobranch,
        "db" : database
        }

    schemadiff.diff_schemas(cursor, fromschema, toschema,
                            db1, db2,
                            dmlfile=dmlfile,
                            validate=validate)

if __name__ == '__main__':
    filterwarnings('ignore', category = MySQLdb.Warning)
    FORMAT = "%(asctime)-15s %(funcName)s %(levelname)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("oldbranch",
                        help="branch to which we will apply diffs")
    parser.add_argument("newbranch",
                        help="branch from which we will apply diffs")
    parser.add_argument("--database",
                        help="service or trio")
    parser.add_argument("--dmlfile", help="file to write dml statements")
    parser.add_argument("--validate", help="verify that the changes work",
                        action="store_true")

    args = parser.parse_args()

    oldbranch = args.oldbranch
    newbranch = args.newbranch
    database = args.database
    dmlfile = args.dmlfile

    validate = False
    if args.validate:
        validate = True

    if database not in ('service', 'trio'):
        print "bogus database \"%s\".  use service or trio" % (
            database)
        sys.exit(1)

    if database == 'service':
        filespec = "//depot/b-serviceschema/db/schema/common/sql/golden/service.sql"
    else:
        filespec = "//depot/b-serviceschema/db/schema/common/sql/golden/trio.sql"

    p4 = P4.P4()
    conn = None
    cursor = None

    try:
        schemadiff.log_in_to_p4(p4)

        conn = dsn.getConnection()
        cursor = conn.cursor()

        diff_branches(p4, 
                      cursor,
                      filespec,
                      oldbranch,
                      newbranch,
                      database,
                      dmlfile,
                      validate)

    except P4.P4Exception as p4e:
        logging.error(p4e)

        for e in p4.warnings:
            logging.warning(e)
        for e in p4.errors:
            logging.error(e)

        raise

    except Exception as e:
        logging.error(e)
        raise
    finally:
        p4.disconnect()
        if conn:
            conn.close()
        if cursor:
            cursor.close()

