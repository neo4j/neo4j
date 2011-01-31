package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File
import org.neo4j.index.lucene.LuceneIndexService
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.graphdb.Transaction

/**
 * Integration smoketest for the neo4j-index component.
 *
 */
class IndexSpec extends SpecificationWithJUnit {

  "neo4j index" should {

    val dbname = "indexdb"
    var graphdb:EmbeddedGraphDatabase = null

    doBefore {
      graphdb = new EmbeddedGraphDatabase( dbname )
    }
    doAfter {
      graphdb.shutdown
      new File(dbname).deleteAll
    }

    "add an index to a graphdb" in {
      val index = new LuceneIndexService( graphdb );

      index must notBeNull

      index.shutdown
    }
    "index nodes" in {
      val index = new LuceneIndexService( graphdb );

      val tx = graphdb.beginTx
      val expectedNode = graphdb.createNode
      expectedNode.setProperty( "name", "Smoke Test" );

      index.index( expectedNode, "name", expectedNode.getProperty( "name" ) );

      val foundNode = index.getSingleNode("name", "Smoke Test")

      foundNode.getId must be(expectedNode.getId)

      tx.success
      tx.finish

      index.shutdown
    }
  }
}