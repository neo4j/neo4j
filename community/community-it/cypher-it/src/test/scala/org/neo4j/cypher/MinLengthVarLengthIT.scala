/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.MapHasAsScala

class MinLengthVarLengthIT extends CypherITTestSuite {

  private var managementService: DatabaseManagementService = _
  private var db: GraphDatabaseService = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)
  }

  override protected def afterEach(): Unit = {
    managementService.shutdown()
    super.afterEach()
  }

  private def run(query: String): Seq[Map[String, AnyRef]] = {
    val tx = db.beginTx()
    try {
      val rows = ArrayBuffer.empty[Map[String, AnyRef]]
      val result = tx.execute(query)
      while (result.hasNext) rows += result.next().asScala.toMap
      rows.toSeq
    } finally tx.close()
  }

  test("min(length(path)) over variable-length pattern grouped by start node") {
    val tx = db.beginTx()
    tx.execute("""CREATE (a:P3 {k: 0}), (b:P3 {k: 1}), (c:P3 {k: 2}),
                 |       (a)-[:P3T]->(b), (b)-[:P3T]->(c), (a)-[:P3T]->(c)""".stripMargin).close()
    tx.commit()

    val rows = run("MATCH q = (a:P3)-[:P3T*1..2]->(c) RETURN a.k AS k, min(length(q)) AS m ORDER BY k")
    rows.map(r => (r("k"), r("m"))) should equal(Seq((0L, 1L), (1L, 1L)))
  }
}
