/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.io.File

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.io.fs.FileUtils
import org.neo4j.test.TestGraphDatabaseFactory

class AutoIndexAcceptanceTest extends ExecutionEngineFunSuite {

  test("should auto-index node on property set, even if the value does not change") {
    val file = new File("test")

    createDB(file)

    val db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder(file)
      .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
      .setConfig(GraphDatabaseSettings.node_keys_indexable, "name")
      .newGraphDatabase()

    try {
      val setSameQuery = "MATCH (p) WITH p, p.name as name SET p.name = name RETURN count(p)"
      runExpectARowAndClose(db, setSameQuery)
      val startQuery = "START i=node:node_auto_index('name:test') return i limit 1"
      runExpectARowAndClose(db, startQuery) // should find the index and not fail
    } finally {
      db.shutdown()
      FileUtils.deleteRecursively(file)
    }
  }

  private def createDB(file: File): Unit = {
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(file)

    val tx = db.beginTx()
    try {
      db.execute("CREATE ({name:'test'}), ({name:'test2'})").close()
      tx.success()
    } finally {
      tx.close()
      db.shutdown()
    }
  }

  private def runExpectARowAndClose(db: GraphDatabaseService, query: String): Unit = {
    val tx = db.beginTx()
    try {
      val result = db.execute(query)
      result.hasNext should be(true) // we expect one row
      result.close()
      tx.success()
    } finally {
      tx.close()
    }
  }
}
