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

    result = p4.run('print', '-q', version)
    result = ''.join(result[1:])
    result = string.replace(result, '%DB_COLLATION_CREATE_TABLE_COMMON%', '')

    return result

def diff_branches(p4, cursor, filespec, frombranch, tobranch, dmlfile):
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
                            dmlfile=dmlfile,
                            validate=True)

    cursor.execute("drop database %(db)s" % { "db" : frombranch })
    cursor.execute("drop database %(db)s" % { "db" : tobranch })

def log_in_to_p4(p4):
    try:
        # log in with existing ticket, if it's there.
        # need a way to find out if the current ticket is expired.
        # looks like p4python can't tell us that.

        p4.connect()
        tix = p4.run_tickets()
        if len(tix) > 0:
            return

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

if __name__ == '__main__':
    filterwarnings('ignore', category = MySQLdb.Warning)
    FORMAT = "%(asctime)-15s %(funcName)s %(levelname)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--oldbranch",
                        help="branch to which we will apply diffs")
    parser.add_argument("--newbranch",
                        help="branch from which we will apply diffs")
    parser.add_argument("--database",
                        help="service or trio")
    parser.add_argument("--dmlfile", help="file to write dml statements")

    args = parser.parse_args()

    oldbranch = args.oldbranch
    newbranch = args.newbranch
    database = args.database
    dmlfile = args.dmlfile

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
        log_in_to_p4(p4)
        if p4 is None:
            print "could not connect to perforce, quitting"
            sys.exit(1)

        conn = dsn.getConnection()
        cursor = conn.cursor()

        diff_branches(p4, 
                      cursor,
                      filespec,
                      oldbranch,
                      newbranch,
                      dmlfile)

    except P4.P4Exception as p4e:
        logging.error(p4e)

        for e in p4.warnings:
            logging.warning(e)
        for e in p4.errors:
            logging.error(e)

    except Exception as e:
        logging.error(e)
    finally:
        p4.disconnect()
        if conn:
            conn.close()
        if cursor:
            cursor.close()

