package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.kernel.EmbeddedGraphDatabase
import java.util.HashMap;
import org.neo4j.onlinebackup.Neo4jBackup

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File

/**
 * Integration smoketest for the neo4j-online-backup component.
 *
 */
class OnlineBackupSpec extends SpecificationWithJUnit {

  "neo4j online backup" should {


    val dbname = "db_to_backup"
    val expected_backupname = "backup_of_db"
    val expected_backupdir = new File(expected_backupname)
    var graphdb:EmbeddedGraphDatabase = null

    doBefore {
      val dbconfig = new HashMap[String,String]
      dbconfig.put("keep_logical_logs", "nioneodb")
      graphdb = new EmbeddedGraphDatabase( dbname, dbconfig )
      val tx = graphdb.beginTx
      graphdb.createNode
      tx.success
      tx.finish
      graphdb.shutdown

      // TODO: copy the database to the backup dir, as in: new File(dbname).copyAll(expected_backupdir)
      
      graphdb = new EmbeddedGraphDatabase( dbname, dbconfig )
    }

    doAfter {
      new File(dbname).deleteAll
    }

    "backup graph database" in {

      val backup = Neo4jBackup.neo4jDataSource(graphdb, expected_backupname)
      backup.doBackup

      expected_backupdir must exist

      expected_backupdir.deleteAll

    }

  }

}