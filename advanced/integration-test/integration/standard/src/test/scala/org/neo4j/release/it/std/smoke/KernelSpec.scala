package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.kernel.EmbeddedGraphDatabase

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File

/**
 * Integration smoketest for the neo4j-kernel component.
 *
 */
class KernelSpec extends SpecificationWithJUnit {

  "neo4j kernel" should {
    "create a graph database" in {
      val dbname = "kerneldb"
      val graphdb = new EmbeddedGraphDatabase( dbname )

      graphdb must notBeNull

      graphdb.shutdown
      new File(dbname).deleteAll
    }
    
    "populate a directory" in {
      val expected_dbname = "kerneldb"
      val expected_dir = new File(expected_dbname)
      val graphdb = new EmbeddedGraphDatabase( expected_dbname )

      expected_dir must exist
      expected_dir must beDirectory

      graphdb.shutdown
      expected_dir.deleteAll
    }
    "create a node" in {
      val dbname = "kerneldb"
      val graphdb = new EmbeddedGraphDatabase( dbname )

      val tx = graphdb.beginTx

      val createdNode = graphdb.createNode
      createdNode must notBeNull

      tx.success
      tx.finish

      graphdb.shutdown
      new File(dbname).deleteAll
    }
  }


}


