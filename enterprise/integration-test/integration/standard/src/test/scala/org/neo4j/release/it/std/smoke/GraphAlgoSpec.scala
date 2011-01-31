package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.EmbeddedGraphDatabase

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File;

/**
 * Integration smoketest for the neo4j-graph-algo component.
 *
 */
class GraphAlgoSpec extends SpecificationWithJUnit {

  "neo4j graph algo" should {

    val dbname = "algodb"
    var graphdb:EmbeddedGraphDatabase = null
    var tx:Transaction = null

    doBefore {
      graphdb = new EmbeddedGraphDatabase( dbname )
      tx = graphdb.beginTx
    }
    doAfter {
      tx.success
      tx.finish
      graphdb.shutdown
      new File(dbname).deleteAll
    }

    "find shortest path" in {
      
    }

  }

}