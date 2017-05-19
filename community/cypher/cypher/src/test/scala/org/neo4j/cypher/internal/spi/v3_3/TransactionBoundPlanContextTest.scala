/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v3_3

import java.util.Collections

import org.neo4j.cypher.internal.frontend.v3_3.phases.devNullLogger
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, RelTypeId}
import org.neo4j.cypher.internal.ir.v3_3.Cardinality
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.KernelTransaction.Type._
import org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.test.TestGraphDatabaseFactory

class TransactionBoundPlanContextTest extends CypherFunSuite {

  var database:GraphDatabaseService = _

  private def createTransactionContext(graphDatabaseCypherService: GraphDatabaseCypherService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseCypherService, new PropertyContainerLocker)
    contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, "no query", Collections.emptyMap())
  }

  override protected def initTest(): Unit = {
    database = new TestGraphDatabaseFactory().newImpermanentDatabase()
  }

  override protected def afterEach(): Unit = {
    database.shutdown()
  }

  test("statistics should default to single cardinality on empty db") {
    val graph = new GraphDatabaseCypherService(database)
    val transaction = graph.beginTransaction(explicit, AUTH_DISABLED)
    val transactionalContext = createTransactionContext(graph, transaction)
    val planContext = new TransactionBoundPlanContext(TransactionalContextWrapper(transactionalContext), devNullLogger)
    val statistics = planContext.statistics

    // label stats
    statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality.SINGLE)
    statistics.nodesWithLabelCardinality(None) should equal(Cardinality.SINGLE)

    // pattern stats
    Set(Some(LabelId(0)), None).foreach { label1 =>
      Set(Some(LabelId(1)), None).foreach { label2 =>
        statistics.cardinalityByLabelsAndRelationshipType(label1, Some(RelTypeId(0)), label2) should equal(Cardinality.SINGLE)
        statistics.cardinalityByLabelsAndRelationshipType(label1, None, label2) should equal(Cardinality.SINGLE)
      }
    }

    transaction.close()
  }

  test("statistics should default to single cardinality for unknown counts on nonempty db") {
    val graph = new GraphDatabaseCypherService(database)
    graph.getGraphDatabaseService.execute("UNWIND range(1, 100) AS i CREATE (:L1)-[:T]->()")

    val transaction = graph.beginTransaction(explicit, AUTH_DISABLED)
    val transactionalContext = createTransactionContext(graph, transaction)
    val planContext = new TransactionBoundPlanContext(TransactionalContextWrapper(transactionalContext), devNullLogger)
    val statistics = planContext.statistics

    // label stats
    statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality(100))
    statistics.nodesWithLabelCardinality(Some(LabelId(1))) should equal(Cardinality.SINGLE)
    statistics.nodesWithLabelCardinality(None) should equal(Cardinality(200))

    // pattern stats
    statistics.cardinalityByLabelsAndRelationshipType(
      Some(LabelId(0)), Some(RelTypeId(0)), Some(LabelId(1))) should equal(Cardinality.SINGLE)
    statistics.cardinalityByLabelsAndRelationshipType(
      Some(LabelId(0)), Some(RelTypeId(0)), None) should equal(Cardinality(100))
    statistics.cardinalityByLabelsAndRelationshipType(
      Some(LabelId(0)), None, Some(LabelId(1))) should equal(Cardinality.SINGLE)
    statistics.cardinalityByLabelsAndRelationshipType(
      Some(LabelId(0)), None, None) should equal(Cardinality(100))
    statistics.cardinalityByLabelsAndRelationshipType(
      None, None, None) should equal(Cardinality(100))
    statistics.cardinalityByLabelsAndRelationshipType(
      None, Some(RelTypeId(0)), None) should equal(Cardinality(100))

    transaction.close()
  }
}
