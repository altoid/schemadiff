#!/usr/bin/env python

import MySQLdb
# docs at http://mysql-python.sourceforge.net/MySQLdb.html

import sys
import time
import os
import string
import unittest
import schemadiff
import dsn
from warnings import filterwarnings

class TestBasic(unittest.TestCase):

    def test_normalize(self):
        result = schemadiff.normalize('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        self.assertEqual('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz0123456789', result)
        result = schemadiff.shacmd(result)
        self.assertEqual('4dcd4f08ed97c050a52da10eb3eeaf1d9b28a9d3', result)

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
  `createDate` datetime NOT NULL,
  PRIMARY KEY (`objectProductOverrideId`),
  KEY `idx_oopp` (`objectId`,`objectType`,`productId`,`partnerId`),
  KEY `idx_p` (`partnerId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        self.assertEquals('504018a9e2d5f90067f4c3fb58d6055fcaa9dd4c', schemadiff.shacmd(schemadiff.normalize(s)))


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

    db1 = 'schemadiff_tablediff_old'
    db2 = 'schemadiff_tablediff_new'
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
  `createDate` datetime NOT NULL,
  PRIMARY KEY (`objectProductOverrideId`),
  KEY `idx_oopp` (`objectId`,`objectType`,`productId`,`partnerId`),
  KEY `idx_p` (`partnerId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(s)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(s)

        self.assertEqual(schemadiff.dbchecksum(self.db1), schemadiff.dbchecksum(self.db2))

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNone(diffs)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        self.assertIsNone(dml)

    def testDrop1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNotNone(drop)
        self.assertIsNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(drop) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        print dml

    def testChange1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` smallint NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        print dml

    def testMakeColumnNullable(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11),
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        print dml
                                                   
    def testMakeColumnNotNull(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11),
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(diffs) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        print dml
                                                   
    def testAdd1Column(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(add) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        control = "ALTER TABLE %(db)s.%(table)s ADD COLUMN objectId bigint NOT NULL" % {
            "db" : self.db1,
            "table" : tableName
            }

        self.assertEqual(control, dml)
        
    def testAdd1ColumnNullable(self):
        tableName = 'deletedObject'

        t1 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `objectType` int(11) NOT NULL,
  `objectId` bigint(20),
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNone(diffs)

        self.assertTrue(len(add) > 0)
        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        control = "ALTER TABLE %(db)s.%(table)s ADD COLUMN objectId bigint" % {
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
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`objectType`,`objectId`),
  KEY `idx_deleteDate` (`deleteDate`),
  KEY `idx_objectType_deleteDate` (`objectType`,`deleteDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        t2 = """CREATE TABLE `%(table)s` (
  `deleteDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `objectType` smallint(6) unsigned NOT NULL,
  `objectNamespaceAndId` bigint(20) NOT NULL,
  PRIMARY KEY (`deleteDate`,`objectType`,`objectNamespaceAndId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % { "table" : tableName }

        self.cursor.execute("use %s" % self.db1)
        self.cursor.execute(t1)

        self.cursor.execute("use %s" % self.db2)
        self.cursor.execute(t2)

        (drop, add, diffs) = schemadiff.diff_table(
            self.cursor, tableName, self.db1, self.db2)

        self.assertIsNotNone(drop)
        self.assertIsNotNone(add)
        self.assertIsNotNone(diffs)

        self.assertTrue(len(drop) > 0)
        self.assertTrue(len(add) > 0)
        self.assertTrue(len(diffs) > 0)

        dml = schemadiff.construct_altertable(self.cursor, self.db1, self.db2, tableName, drop, add, diffs)
        print dml

    def tearDown(self):
#        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db1 })
#        self.cursor.execute("drop database if exists %(db)s" % { "db" : self.db2 })
#
#        self.cursor.close()
#        self.dbconn.close()
        pass
    


if __name__ == '__main__':
    filterwarnings('ignore', category = MySQLdb.Warning)
    unittest.main()
        
