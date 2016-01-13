#!/usr/bin/env python

import MySQLdb
# docs at http://mysql-python.sourceforge.net/MySQLdb.html

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
    (drop, add, diffs,
     index_drop, index_add, index_diffs) = schemadiff.diff_table(
        cursor, tableName, db1, db2)

    dml = schemadiff.construct_altertable(cursor, db1, db2,
                                          tableName,
                                          drop=drop,
                                          add=add,
                                          diffs=diffs,
                                          index_drop=index_drop,
                                          index_add=index_add,
                                          index_diffs=index_diffs)

    cursor.execute(dml)

    cs1 = schemadiff.dbchecksum(db1)
    cs2 = schemadiff.dbchecksum(db2)

    return (cs1, cs2)


class TestBasic(unittest.TestCase):

    def test_normalize(self):
        result = schemadiff.normalize('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        self.assertEqual('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz0123456789', result)
        result = schemadiff.shacmd(result)
        self.assertEqual('106d564d4ccd3ce4dc111457baf36f99ba634d45', result)

    def test_basic(self):
        s = """CREATE TABLE `objectProductOverride` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        self.assertEquals('6ab29987db01aeeecad2beedbc02774526194c54', schemadiff.shacmd(schemadiff.normalize(s)))


class TestSetup(unittest.TestCase):

    db1 = 'schemadiff_testsetup_old'
    db2 = 'schemadiff_testsetup_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })
    
    def testDbSetup(self):
        # make sure the databases are there
        conn1 = dsn.getConnection(self.db1)
        conn2 = dsn.getConnection(self.db2)

        conn2.close()
        conn1.close()

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
    

class TestTableDiff(unittest.TestCase):

    db1 = 'schemadiff_testtablediff_old'
    db2 = 'schemadiff_testtablediff_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })
    
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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNone(diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName)
        self.assertIsNone(dml)

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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNotNone(drop)
        self.assertIsNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(drop) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop=drop)
        control = "ALTER TABLE %(db)s.%(table)s DROP COLUMN objectId" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dml)

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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, diffs=diffs)
        control = "ALTER TABLE %(db)s.%(table)s MODIFY COLUMN objectType smallint(6) NOT NULL" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dml)

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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, diffs=diffs)
        control = "ALTER TABLE %(db)s.%(table)s MODIFY COLUMN objectType int(11) NULL" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dml)
                                                   
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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, diffs=diffs)
        control = "ALTER TABLE %(db)s.%(table)s MODIFY COLUMN objectType int(11) NOT NULL" % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dml)
                                                   
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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(add) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, add=add)
        control = "ALTER TABLE %(db)s.%(table)s ADD COLUMN objectId bigint(20) NOT NULL" % {
            "db" : self.db1,
            "table" : tableName
            }

        self.assertEqual(control, dml)
        
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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(add) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, add=add)
        control = "ALTER TABLE %(db)s.%(table)s ADD COLUMN objectId bigint(20)" % {
            "db" : self.db1,
            "table" : tableName
            }

        self.assertEqual(control, dml)
        
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

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[0:3]

        self.assertIsNotNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(drop) > 0)
        self.assertTrue(len(add) > 0)
        self.assertTrue(len(diffs) > 0)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName,
                                              drop=drop, add=add, diffs=diffs)
        control = ("ALTER TABLE %(db)s.%(table)s "
                   "DROP COLUMN objectId, "
                   "ADD COLUMN objectNamespaceAndId bigint(20) NOT NULL, "
                   "MODIFY COLUMN objectType smallint(6) unsigned NOT NULL") % {
            "db" : self.db1,
            "table" : tableName
            }
        self.assertEqual(control, dml)

    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.close()
        self.dbconn.close()
    
class TestAlterTable(unittest.TestCase):
    """
    these tests actually execute the alter table statement that is generated.
    """

    db1 = 'schemadiff_testaltertable_old'
    db2 = 'schemadiff_testaltertable_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })

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

    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.close()
        self.dbconn.close()

class TestIndexDiffDML(unittest.TestCase):
    # changing has to be done as drop-then-add

    db1 = 'schemadiff_testindexdiffdml_old'
    db2 = 'schemadiff_testindexdiffdml_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })

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

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        drop_control = ast.literal_eval("{u'key_drop': {'key_columns': u'column3', 'index_name': u'key_drop', 'constraint_type': None}}")
        add_control = ast.literal_eval("{u'key_add': {'key_columns': u'column4', 'index_name': u'key_add', 'constraint_type': None}}")
        diffs_control = ast.literal_eval("{u'key_change': {'key_columns': u'column1,column2', 'index_name': u'key_change', 'constraint_type': None}}")

        self.assertDictEqual(drop_control, drop)
        self.assertDictEqual(add_control, add)
        self.assertDictEqual(diffs_control, diffs)

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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        self.assertIsNotNone(index_drop)
        self.assertIsNone(index_add)
        self.assertIsNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName, index_drop=index_drop)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "DROP INDEX key_drop") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)


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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        self.assertIsNone(index_drop)
        self.assertIsNotNone(index_add)
        self.assertIsNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName,
                                              index_add=index_add)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "ADD INDEX key_add (column3)") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)

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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName,
                                              index_diffs=index_diffs)

        self.assertIsNone(index_drop)
        self.assertIsNone(index_add)
        self.assertIsNotNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName,
                                              index_diffs=index_diffs)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "DROP INDEX key_add, "
                   "ADD INDEX key_add (column2,column3)"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)


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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        self.assertIsNotNone(index_drop)
        self.assertIsNone(index_add)
        self.assertIsNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName, index_drop=index_drop)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "DROP PRIMARY KEY") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)

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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        self.assertIsNone(index_drop)
        self.assertIsNotNone(index_add)
        self.assertIsNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName,
                                              index_add=index_add)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "ADD PRIMARY KEY (column1,column2)") % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)

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

        (index_drop, index_add, index_diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)[3:6]

        self.assertIsNone(index_drop)
        self.assertIsNone(index_add)
        self.assertIsNotNone(index_diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2,
                                              tableName,
                                              index_diffs=index_diffs)

        control = ("ALTER TABLE %(db)s.%(table)s "
                   "DROP PRIMARY KEY, "
                   "ADD PRIMARY KEY (column1,column2)"
                   ) % {
            "db" : self.db1,
            "table" : tableName }
        self.assertEqual(control, dml)

    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.close()
        self.dbconn.close()


class TestIndexDiff(unittest.TestCase):
    # changing has to be done as drop-then-add

    db1 = 'schemadiff_testindexdiff_old'
    db2 = 'schemadiff_testindexdiff_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })

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

    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.close()
        self.dbconn.close()


class TestUniqueIndex(unittest.TestCase):
    db1 = 'schemadiff_testuniqueindex_old'
    db2 = 'schemadiff_testuniqueindex_new'
    dbconn = None
    cursor = None

    def setUp(self):
        self.dbconn = dsn.getConnection()
        self.cursor = self.dbconn.cursor()

        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db1 })
        self.cursor.execute("create database %(db)s" % { "db" : self.db2 })

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

    def tearDown(self):
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })

        self.cursor.close()
        self.dbconn.close()

class TestFK(unittest.TestCase):
    db1 = 'schemadiff_testforeignkey_old'
    db2 = 'schemadiff_testforeignkey_new'
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

        self.cursor.close()
        self.dbconn.close()


if __name__ == '__main__':
    filterwarnings('ignore', category = MySQLdb.Warning)
    unittest.main()
        
