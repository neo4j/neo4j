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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.database.Database
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.scalatest.BeforeAndAfterAll

class LastCommittedTxIdProviderTest extends CypherFunSuite with BeforeAndAfterAll {

  var managementService: DatabaseManagementService = _
  var graph: GraphDatabaseService = _
  var db: GraphDatabaseCypherService = _
  var lastCommittedTxIdProvider: LastCommittedTxIdProvider = _

  override protected def beforeAll(): Unit = {
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    graph = managementService.database(DEFAULT_DATABASE_NAME)
    db = new GraphDatabaseCypherService(graph)
    lastCommittedTxIdProvider = LastCommittedTxIdProvider(db)
  }

  override protected def afterAll(): Unit = managementService.shutdown()

  test("should return correct last committed tx id") {
    val startingTxId = lastCommittedTxIdProvider()

    (1 to 42).foreach(_ => createNode())

    val endingTxId = lastCommittedTxIdProvider()
    endingTxId shouldBe (startingTxId + 42)
  }

  test("should return correct last committed tx id after datasource restart") {
    val startingTxId = lastCommittedTxIdProvider()

    restartDataSource()
    (1 to 42).foreach(_ => createNode())

    val endingTxId = lastCommittedTxIdProvider()
    endingTxId shouldBe (startingTxId + 42)
  }

  private def createNode(): Unit = {
    val tx = db.beginTransaction(Type.EXPLICIT, AnonymousContext.write())
    try {
      tx.createNode()
      tx.commit()
    } finally {
      tx.close()
    }
  }

  private def restartDataSource(): Unit = {
    val ds = db.getDependencyResolver.resolveDependency(classOf[Database])
    ds.stop()
    ds.start()
  }
}
