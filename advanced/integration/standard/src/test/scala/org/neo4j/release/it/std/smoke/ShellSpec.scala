package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File
import org.neo4j.index.lucene.LuceneIndexService
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.shell.kernel.GraphDatabaseShellServer

/**
 * Integration smoketest for the neo4j-index component.
 *
 */
class ShellSpec extends SpecificationWithJUnit {

  "neo4j shell" should {

    val dbname = "shelldb"
    var graphdb:EmbeddedGraphDatabase = null

    doBefore {
      graphdb = new EmbeddedGraphDatabase( dbname )
    }
    doAfter {
      graphdb.shutdown
      new File(dbname).deleteAll
    }

    "attach to a graphdb" in {
      val shell = new GraphDatabaseShellServer( graphdb )

      shell must notBeNull

      shell.shutdown
    }
  }
  
}