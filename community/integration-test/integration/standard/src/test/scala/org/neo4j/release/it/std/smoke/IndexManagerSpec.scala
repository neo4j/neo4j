package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File
import org.neo4j.index.lucene.LuceneIndexService
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.graphdb.Transaction

/**
 * Integration smoketest for the new (as of neo4j 1.2.M01)
 * indexing component.
 *
 */
class IndexSpecManager extends SpecificationWithJUnit {

  "neo4j managed index" should {

    val dbname = "managed-indexdb"
    var graphdb:EmbeddedGraphDatabase = null

    doBefore {
      graphdb = new EmbeddedGraphDatabase( dbname )
    }
    doAfter {
      graphdb.shutdown
      new File(dbname).deleteAll
    }

    "add a node index to a graphdb" in {
      val nodeIndex = graphdb.index.forNodes( "users" ) 

      nodeIndex must notBeNull

      val tx = graphdb.beginTx
      val expectedNode = graphdb.createNode
      expectedNode.setProperty( "name", "Smoke Test" );

      nodeIndex.add( expectedNode, "name", expectedNode.getProperty( "name" ) );

      val foundNode = nodeIndex.get("name", "Smoke Test").getSingle

      foundNode.getId must be(expectedNode.getId)

      tx.success
      tx.finish

    }
  }
}