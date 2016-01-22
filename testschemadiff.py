#!/usr/bin/env python

import MySQLdb
# docs at http://mysql-python.sourceforge.net/MySQLdb.html

import logging
import ast
import sys
import time
import os
import string
import unittest
import schemadiff
import hashlib
import dsn
from warnings import filterwarnings

def create_tables(cursor, create_table_statements, db):
    cursor.execute("use %s" % db)
    for s in create_table_statements:
        cursor.execute(s)

def apply_table_change(cursor, tableName, db1, db2):
    dmls = schemadiff.diff_table(
        cursor, tableName, db1, db2)

    dmls.insert(0, "USE %s;" % db1)
    for dml in dmls:
        cursor.execute(dml)

    cs1 = schemadiff.dbchecksum(db1)
    cs2 = schemadiff.dbchecksum(db2)

    return (cs1, cs2)


class SchemaDiffTest(unittest.TestCase):
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })
    
    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.execute("show databases")
        for row in self.cursor.fetchall():
            if row[0] == self.db1 or row[0] == self.db2:
                self.cursor.close()
                self.dbconn.close()
                raise AssertionError("database %s not deleted!" % row[0])

        self.cursor.close()
        self.dbconn.close()


class TestColumnDiffDML(SchemaDiffTest):
    """
    test add/drop/change columns.  tests
    correctness of DML statement but does not execute it.
    """
    db1 = 'TestColumnDiffDML_old'
    db2 = 'TestColumnDiffDML_new'

    def testNoDiffs(self):
        """test that two databases with the same table(s) have the same checksum.
        """
        tableName = 'objectProductOverride'
        s = """CREATE TABLE `%(table)s` (
  `objectId` bigint(20) NOT NULL,
  `objectType` smallint(6) unsigned NOT NULL,
  `productId` varchar(128) NOT NULL,
  `partnerId` int(11) NOT NULL,
  `updateDate` datetime NOT NULL,
  `omit` char(1) NOT NULL,
  `displayRank` int(11) DEFAULT NULL,
  `description` text,
  `title` varchar(128) DEFAULT NULL,
  `imageUrl` varchar(255) DEFAULT NULL,
  `author` varchar(128) DEFAULT NULL,
  `objectProductOverrideId` bigint(20) NOT NULL,
  `createDate` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(s)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(s)

        self.assertEqual(schemadiff.dbchecksum(self.db1), schemadiff.dbchecksum(self.db2))

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertEqual(0, len(dmls))

    def testDrop1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)
        
        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))
        control = "ALTER TABLE %(table)s DROP COLUMN objectId;" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[0])

    def testChange1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` smallint NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = "ALTER TABLE %(table)s MODIFY COLUMN objectType smallint(6) NOT NULL;" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[0])

    def testMakeColumnNullable(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11),
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = "ALTER TABLE %(table)s MODIFY COLUMN objectType int(11);" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[0])
                                                   
    def testMakeColumnNotNull(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11),
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = "ALTER TABLE %(table)s MODIFY COLUMN objectType int(11) NOT NULL;" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[0])
                                                   
    def testAdd1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = "ALTER TABLE %(table)s ADD COLUMN objectId bigint(20) NOT NULL;" % {
            "db" : self.db1,
            "table" : tableName
            }

        self.assertEqual(control, dmls[0])
        
    def testAdd1ColumnNullable(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20),
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = "ALTER TABLE %(table)s ADD COLUMN objectId bigint(20);" % {
            "db" : self.db1,
            "table" : tableName
            }

        self.assertEqual(control, dmls[0])
        
    def testTableDiff(self):
        """test diffs on a table where columns are added, deleted, and changed.
        """
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `objectType` smallint(6) unsigned NOT NULL,
  `objectNamespaceAndId` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(2, len(dmls))

        control = ("ALTER TABLE %(table)s "
                   "DROP COLUMN objectId;") % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[0])

        control = ("ALTER TABLE %(table)s "
                   "ADD COLUMN objectNamespaceAndId bigint(20) NOT NULL, "
                   "MODIFY COLUMN objectType smallint(6) unsigned NOT NULL;") % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dmls[1])

    
class TestColumnDiff(SchemaDiffTest):
    """
    these tests actually execute the alter table statement that is generated.
    columns only, no keys.
    """

    db1 = 'TestColumnDiff_old'
    db2 = 'TestColumnDiff_new'

    def testAdd1Column(self):
        tableName = 'stupidTable'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20),
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDrop1Column(self):
        tableName = 'stupidTable'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20),
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testTableDiff(self):
        """test diffs on a table where columns are added, deleted, and changed.
        """
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `objectType` smallint(6) unsigned NOT NULL,
  `objectNamespaceAndId` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testColumnDefaults(self):
        """test diffs on a table where columns are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  drop_default_value          int not null default 0,
  add_default_value_not_null  int not null,
  add_default_value_nullable  int,
  change_default_value        int not null default 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  drop_default_value          int not null,
  add_default_value_not_null  int not null default 11,
  add_default_value_nullable  int          default 22,
  change_default_value        int not null default 33,
  new_column_with_default     int not null default 44
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)


class TestIndexDiffDML(SchemaDiffTest):
    # changing has to be done as drop-then-add

    db1 = 'TestIndexDiffDML_old'
    db2 = 'TestIndexDiffDML_new'

    def testDropIndex(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_drop (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = ("ALTER TABLE %(table)s "
                   "DROP INDEX key_drop;") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dmls[0])

    def testAddIndex(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))
        control = ("ALTER TABLE %(table)s "
                   "ADD KEY key_add(column3);") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dmls[0])

    def testChangeIndex(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column2,column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(2, len(dmls))

        control1 = ("ALTER TABLE %(table)s "
                    "DROP INDEX key_add;"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control1, dmls[0])

        control2 = ("ALTER TABLE %(table)s "
                   "ADD KEY key_add(column2,column3);"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control2, dmls[1])


    def testDropPK(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))
        control = ("ALTER TABLE %(table)s "
                   "DROP PRIMARY KEY;") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dmls[0])

    def testAddPK(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))
        control = ("ALTER TABLE %(table)s "
                   "ADD PRIMARY KEY(column1,column2);") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dmls[0])

    def testChangePK(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(2, len(dmls))

        control1 = ("ALTER TABLE %(table)s "
                   "DROP PRIMARY KEY;"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control1, dmls[0])

        control2 = ("ALTER TABLE %(table)s "
                    "ADD PRIMARY KEY(column1,column2);"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control2, dmls[1])


class TestIndexDiff(SchemaDiffTest):
    # changing has to be done as drop-then-add

    db1 = 'TestIndexDiff_old'
    db2 = 'TestIndexDiff_new'

    def testIndexDiff(self):
        """
        test diffs on a table where ordinary indexes are added,
        deleted, and changed.  Only tests the correctness of the DML,
        does not execute it.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_change (column1),
  key key_drop (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_change (column1, column2),
  key key_add (column4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDropIndex(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_drop (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testAddIndex(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testChangeIndex(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  key key_add (column2,column3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testDropPK(self):
        """
        test diffs on a table where ordinary indexes are added, deleted, and changed.
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testAddPK(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testChangePK(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)


class TestDrop(SchemaDiffTest):
    """
    test for good behavior when indexed columns are dropped.
    """

    db1 = 'TestDrop_old'
    db2 = 'TestDrop_new'

    def testPKColumn(self):
        """
        drop a pk column - PK should disappear
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testIndexedColumn(self):
        """
        drop a column from an index - index should disappear
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testUniqueColumn(self):
        """
        drop a column from a unique index and
        the index too
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testFKColumnAndConstraint(self):
        """
        drop a column from a foreign key
        constraint and index should disappear
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)


class TestUniqueIndex(SchemaDiffTest):
    db1 = 'TestUniqueIndex_old'
    db2 = 'TestUniqueIndex_new'

    def testAddIndex(self):
        """
        add a unique index
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testDropIndex(self):
        """
        drop a unique index
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testChangeIndex(self):
        """
        change a unique index
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testMakeUnique(self):
        """
        make an ordinary index unique
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testMakeNonUnique(self):
        """
        make a unique index non-unique
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testMakePKUnique(self):
        """
        make a primary key a unique index
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testMakeUniquePK(self):
        """
        make a unique index a PK
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  UNIQUE KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)


class TestFKDiffDML(SchemaDiffTest):
    db1 = 'TestFKDiffDML_old'
    db2 = 'TestFKDiffDML_new'

    def testNoDiff(self):
        """
        auto-diff a table with a FK and make sure nothing happens.
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t1], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(0, len(dmls))

    def testChangeFK(self):
        """
        change the columns in a foreign key.
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1) REFERENCES reftable(column1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(2, len(dmls))

        control1 = ("ALTER TABLE %(table)s "
                    "DROP FOREIGN KEY fk;"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control1, dmls[0])

        control2 = ("ALTER TABLE %(table)s "
                    "ADD CONSTRAINT fk FOREIGN KEY (column1) REFERENCES reftable(column1);"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control2, dmls[1])

    def testDropFK(self):
        """
        drop a foreign key.
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))

        control = ("ALTER TABLE %(table)s "
                    "DROP FOREIGN KEY fk;"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dmls[0])

    def testAddFK(self):
        """
        add a foreign key
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(1, len(dmls))
        control1 = ("ALTER TABLE %(table)s "
                    "ADD CONSTRAINT fk FOREIGN KEY (column1,column2) REFERENCES reftable(column1,column2);"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control1, dmls[0])


class TestFKDiff(SchemaDiffTest):
    db1 = 'TestFKDiff_old'
    db2 = 'TestFKDiff_new'

    def testChangeFK(self):
        """
        change the columns in a foreign key.
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1) REFERENCES reftable(column1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDropFK(self):
        """
        drop a foreign key.
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `fk` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)
        self.assertEqual(cs1, cs2)

    def testAddFK(self):
        """
        add a foreign key
        """
        tableName = 'mytable'

        ref = """CREATE TABLE `reftable` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  PRIMARY KEY(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 0,
  column2 int not null default 0,
  column3 int not null default 0,
  column4 int not null default 0,
  KEY fk (column1, column2),
  CONSTRAINT `pookus` foreign key(column1, column2) REFERENCES reftable(column1, column2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [ref, t1], self.db1)
        create_tables(self.cursor, [ref, t2], self.db2)
        
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)


class TestMisc(SchemaDiffTest):
    db1 = 'TestMisc_old'
    db2 = 'TestMisc_new'

    def test1(self):
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  `adBrandId` int(11) NOT NULL DEFAULT '0',
  `title` varchar(128) NOT NULL DEFAULT '',
  `description` text NOT NULL,
  `imageUrl` varchar(255) DEFAULT NULL,
  `isActive` char(1) NOT NULL DEFAULT '1',
  `createDate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updateDate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`adBrandId`),
  KEY `idxTitle_AdBrand` (`title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `adBrandId` int(11) NOT NULL DEFAULT '0',
  `title` varchar(128) NOT NULL DEFAULT '',
  `description` text NOT NULL,
  `imageUrl` varchar(255) DEFAULT NULL,
  `isActive` char(1) NOT NULL DEFAULT '1',
  `createDate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updateDate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`adBrandId`),
  KEY `idxTitle_AdBrand` (`title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def test2(self):
        """
        self-diff a table with a blob key that has a prefix.
        """
        tableName = 'mytable'
        t1 = """CREATE TABLE `%(table)s` (
  `syncGroupPolicyId` int(11) NOT NULL,
  `serviceGroupName` text,
  `databaseName` varchar(20) DEFAULT NULL,
  `pollSeconds` int(11) DEFAULT NULL,
  `enabled` char(1) DEFAULT NULL,
  `sendData` varchar(7) DEFAULT NULL,
  `trySlowCheck` char(1) DEFAULT NULL,
  `owner` varchar(32) DEFAULT NULL,
  `createDate` datetime NOT NULL,
  `updateDate` datetime NOT NULL,
  `priority` int(11) DEFAULT '1000',
  `syncEvenIfIsInSync` char(1) DEFAULT NULL,
  `noPrivateData` varchar(1) DEFAULT '1',
  PRIMARY KEY (`syncGroupPolicyId`),
  UNIQUE KEY `uidx_databaseName_serviceGroupName` (`databaseName`,`serviceGroupName`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t1], self.db2)
        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        self.assertEqual(0, len(dmls))

    def test2(self):
        """
        change prefix on blob index key
        """

        tableName = 'mytable'
        t1 = """CREATE TABLE `%(table)s` (
  `syncGroupPolicyId` int(11) NOT NULL,
  `serviceGroupName` text,
  `databaseName` varchar(20) DEFAULT NULL,
  `pollSeconds` int(11) DEFAULT NULL,
  `enabled` char(1) DEFAULT NULL,
  `sendData` varchar(7) DEFAULT NULL,
  `trySlowCheck` char(1) DEFAULT NULL,
  `owner` varchar(32) DEFAULT NULL,
  `createDate` datetime NOT NULL,
  `updateDate` datetime NOT NULL,
  `priority` int(11) DEFAULT '1000',
  `syncEvenIfIsInSync` char(1) DEFAULT NULL,
  `noPrivateData` varchar(1) DEFAULT '1',
  PRIMARY KEY (`syncGroupPolicyId`),
  UNIQUE KEY `uidx_databaseName_serviceGroupName` (`databaseName`,`serviceGroupName`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `syncGroupPolicyId` int(11) NOT NULL,
  `serviceGroupName` text,
  `databaseName` varchar(20) DEFAULT NULL,
  `pollSeconds` int(11) DEFAULT NULL,
  `enabled` char(1) DEFAULT NULL,
  `sendData` varchar(7) DEFAULT NULL,
  `trySlowCheck` char(1) DEFAULT NULL,
  `owner` varchar(32) DEFAULT NULL,
  `createDate` datetime NOT NULL,
  `updateDate` datetime NOT NULL,
  `priority` int(11) DEFAULT '1000',
  `syncEvenIfIsInSync` char(1) DEFAULT NULL,
  `noPrivateData` varchar(1) DEFAULT '1',
  PRIMARY KEY (`syncGroupPolicyId`),
  UNIQUE KEY `uidx_databaseName_serviceGroupName` (`databaseName`,`serviceGroupName`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)
        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testComment(self):
        """
        handle comments that have '' in them
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 123 COMMENT 'i don''t like this comment'
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDefaultEmptyString(self):
        """
        adding a varchar with a default of '' wasn't working
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null,
  column2 varchar(123) not null default ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDefaults(self):
        """
        set default values of various types
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null,
  column2 varchar(11) not null,
  column3 timestamp not null,
  column4 timestamp not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null default 123,
  column2 varchar(11) not null default 'whee',
  column3 timestamp not null default CURRENT_TIMESTAMP,
  column4 timestamp not null default '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testDateTimeDefaults(self):
        """
        default values for temporal types
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 datetime,
  column2 datetime,
  column3 timestamp not null,
  column4 timestamp not null,
  c5 date,
  c6 time
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 datetime default CURRENT_TIMESTAMP,
  column2 datetime default '0000-00-00 00:00:00',
  column3 timestamp not null default CURRENT_TIMESTAMP,
  column4 timestamp not null default '0000-00-00 00:00:00',
  c5 date default '2000-01-01',
  c6 time default '12:34:56'
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)


class TestEngine(SchemaDiffTest):
    """
    these tests actually execute the alter table statement that is generated.
    columns only, no keys.
    """

    db1 = 'TestEngine_old'
    db2 = 'TestEngine_new'

    def testChangeEngine(self):
        """
        change storage engine
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  column1 int not null,
  column2 varchar(11) not null,
  column3 timestamp not null,
  column4 timestamp not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  column1 int not null,
  column2 varchar(11) not null,
  column3 timestamp not null,
  column4 timestamp not null
) ENGINE=MyISAM DEFAULT CHARSET=utf8""" % { "table" : tableName }

        create_tables(self.cursor, [t1], self.db1)
        create_tables(self.cursor, [t2], self.db2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)

    def testTableDiff(self):
        """
        multiple changes, including the engine
        """
        tableName = 'mytable'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `objectType` smallint(6) unsigned NOT NULL,
  `objectNamespaceAndId` bigint(20) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        dmls = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)
        (cs1, cs2) = apply_table_change(self.cursor,
                                        tableName,
                                        self.db1,
                                        self.db2)

        self.assertEqual(cs1, cs2)


class TestDiffDatabases(SchemaDiffTest):
    db1 = 'TestCreateTable_old'
    db2 = 'TestCreateTable_new'

#    def testAddTable(self):
#        tableName = 'mytable'
#
#        s = """CREATE TABLE `%(table)s` (
#  `author` varchar(128) DEFAULT NULL,
#  `objectProductOverrideId` bigint(20) NOT NULL,
#  `createDate` datetime NOT NULL
#) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }
#
#        self.cursor.execute("use %s" % self.db2)
#        self.cursor.execute(s)
#
#        dmls = schemadiff.diff_databases(
#            self.cursor, self.db1, self.db2)
#        (cs1, cs2) = apply_table_change(self.cursor,
#                                        tableName,
#                                        self.db1,
#                                        self.db2)
#        self.assertEqual(cs1, cs2)
#    
#    def testDropTable(self):
#        tableName = 'mytable'
#
#        s = """CREATE TABLE `%(table)s` (
#  `author` varchar(128) DEFAULT NULL,
#  `objectProductOverrideId` bigint(20) NOT NULL,
#  `createDate` datetime NOT NULL
#) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }
#
#        self.cursor.execute("use %s" % self.db1)
#        self.cursor.execute(s)
#
#        dmls = schemadiff.diff_databases(
#            self.cursor, self.db1, self.db2)
#        (cs1, cs2) = apply_table_change(self.cursor,
#                                        tableName,
#                                        self.db1,
#                                        self.db2)
#        self.assertEqual(cs1, cs2)
    
if __name__ == '__main__':
    FORMAT = "%(asctime)-15s %(funcName)s %(levelname)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    filterwarnings('ignore', category = MySQLdb.Warning)
    unittest.main()
        
