/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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