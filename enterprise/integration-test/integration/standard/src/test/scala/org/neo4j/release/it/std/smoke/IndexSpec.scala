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