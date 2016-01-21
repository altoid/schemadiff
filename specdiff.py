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

if __name__ == '__main__':
    filterwarnings('ignore', category = MySQLdb.Warning)
    FORMAT = "%(asctime)-15s %(funcName)s %(levelname)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("oldspec",
                        help="p4 filespec for old version of schema")
    parser.add_argument("newspec",
                        help="p4 filespec for new version of schema")
    parser.add_argument("--dmlfile", help="file to write dml statements")
    parser.add_argument("--validate", help="verify that the changes work",
                        action="store_true")

    args = parser.parse_args()

    validate = False
    if args.validate:
        validate = True

    oldspec = args.oldspec
    newspec = args.newspec
    dmlfile = args.dmlfile

    p4 = P4.P4()
    conn = None
    cursor = None

    try:
        schemadiff.log_in_to_p4(p4)
        if p4 is None:
            print "could not connect to perforce, quitting"
            sys.exit(1)

        conn = dsn.getConnection()
        cursor = conn.cursor()

        schema1 = schemadiff.get_schema_from_filespec(p4, oldspec)
        schema2 = schemadiff.get_schema_from_filespec(p4, newspec)

        schemadiff.diff_schemas(cursor,
                                schema1,
                                schema2,
                                "specdiff_old",
                                "specdiff_new",
                                dmlfile=dmlfile,
                                validate=validate)

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

