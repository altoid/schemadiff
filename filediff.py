#!/usr/bin/env python

import argparse
import os
import sys
import logging
import schemadiff
import dsn
from warnings import filterwarnings

if __name__ == '__main__':
    FORMAT = "%(asctime)-15s %(funcName)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", help="actually make the changes",
                        action="store_true")
    parser.add_argument("--dmlfile", help="file to write dml statements")
    parser.add_argument("file1", help="input file")
    parser.add_argument("file2", help="input file")
    args = parser.parse_args()
    
    file1 = args.file1
    file2 = args.file2

    validate = False
    if args.validate:
        validate = True

    dmlfile = args.dmlfile
        
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
    
#    # need 2 hash objects in order to use them independently
#    # for each path
#    
#    hash1 = hashlib.md5()
#    hash2 = hash1.copy()
#    
#    hash1.update(path1)
#    hash2.update(path2)
    
    db1 = "%s" % (os.path.splitext(file1)[0])
    db2 = "%s" % (os.path.splitext(file2)[0])
    
    if db1 == db2:
        print 'not dealing with same-name schema files right now'
        sys.exit(1)
    
    # finally.  let's get to work.

    schema1 = schemadiff.read_schema_from_file(file1)
    schema2 = schemadiff.read_schema_from_file(file2)

    conn = dsn.getConnection()
    cursor = conn.cursor()
    
    schemadiff.diff_schemas(cursor, schema1, schema2, db1, db2, dmlfile=dmlfile, validate=validate)

    cursor.execute("drop database %(db)s" % { "db" : db1 })
    cursor.execute("drop database %(db)s" % { "db" : db2 })

    cursor.close()
    conn.close()
