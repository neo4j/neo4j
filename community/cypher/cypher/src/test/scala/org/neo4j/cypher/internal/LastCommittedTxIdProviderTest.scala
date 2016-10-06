/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.kernel.NeoStoreDataSource
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.SecurityContext
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.BeforeAndAfterAll

class LastCommittedTxIdProviderTest extends CypherFunSuite with BeforeAndAfterAll {

  var db: GraphDatabaseCypherService = null
  var lastCommittedTxIdProvider: LastCommittedTxIdProvider = null

  override protected def beforeAll(): Unit = {
    db = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
    lastCommittedTxIdProvider = LastCommittedTxIdProvider(db)
  }

  override protected def afterAll(): Unit = db.getGraphDatabaseService.shutdown()

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
    val tx = db.beginTransaction( KernelTransaction.Type.explicit, SecurityContext.Static.WRITE )
    try {
      db.createNode()
      tx.success()
    }
    finally {
      tx.close()
    }
  }

  private def restartDataSource(): Unit = {
    val ds = db.getDependencyResolver.resolveDependency(classOf[NeoStoreDataSource])
    ds.stop()
    ds.start()
  }
}
