/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util.concurrent.TimeUnit.SECONDS

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_5.spi.{IndexDescriptor, IndexLimitation, SlowContains}
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType, Transaction}
import org.neo4j.internal.kernel.api.Transaction.Type._
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.cypher.internal.v3_5.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util._

class TransactionBoundPlanContextTest extends CypherFunSuite {

  private var database: GraphDatabaseService = _
  private var graph: GraphDatabaseCypherService = _

  private def createTransactionContext(graphDatabaseCypherService: GraphDatabaseCypherService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseCypherService, new PropertyContainerLocker)
    contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, "no query", EMPTY_MAP)
  }

  override protected def initTest(): Unit = {
    database = new TestGraphDatabaseFactory().newImpermanentDatabase()
    graph = new GraphDatabaseCypherService(database)
  }

  override protected def afterEach(): Unit = {
    database.shutdown()
  }

  test("statistics should default to single cardinality on empty db") {

    inTx(planContext => {
      val statistics = planContext.statistics

      // label stats
      statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality.SINGLE)
      statistics.nodesAllCardinality() should equal(Cardinality.SINGLE)

      // pattern stats
      Set(Some(LabelId(0)), None).foreach { label1 =>
        Set(Some(LabelId(1)), None).foreach { label2 =>
          statistics.cardinalityByLabelsAndRelationshipType(label1, Some(RelTypeId(0)), label2) should equal(Cardinality.SINGLE)
          statistics.cardinalityByLabelsAndRelationshipType(label1, None, label2) should equal(Cardinality.SINGLE)
        }
      }
    })
  }

  test("statistics should default to single cardinality for unknown counts on nonempty db") {

    // given
    inTx(_ => {
      for ( i <- 0 until 100 ) {
        val n1 = database.createNode(Label.label("L1"))
        val n2 = database.createNode()
        n1.createRelationshipTo(n2, RelationshipType.withName("T"))
      }
    })

    // then
    inTx(planContext => {
      val statistics = planContext.statistics

      // label stats
      statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality(100))
      statistics.nodesWithLabelCardinality(Some(LabelId(1))) should equal(Cardinality.SINGLE)
      statistics.nodesAllCardinality() should equal(Cardinality(200))

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
    })
  }

  test("indexPropertyExistsSelectivity of empty label index should be 0") {
    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop1id = planContext.getPropertyKeyId("prop")
      val index = IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop1id)))
      planContext.statistics.indexPropertyExistsSelectivity(index) should be(Some(Selectivity.ZERO))
    })
  }

  test("uniqueValueSelectivity of empty index should be 0") {
    inTx(_ => {
      database.createNode(Label.label("L1"))
    })

    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop1id = planContext.getPropertyKeyId("prop")
      val index = IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop1id)))
      planContext.statistics.uniqueValueSelectivity(index) should be(Some(Selectivity.ZERO))
    })
  }

  test("indexesGetForLabel should return both regular and unique indexes") {
    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
      database.schema().indexFor(Label.label("L2")).on("prop").create()
      database.schema().constraintFor(Label.label("L1")).assertPropertyIsUnique("prop2").create()
      database.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop1id = planContext.getPropertyKeyId("prop")
      val prop2id = planContext.getPropertyKeyId("prop2")
      planContext.indexesGetForLabel(l1id).toSet should equal(Set(
        IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop1id)), Set[IndexLimitation](SlowContains)),
        IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop2id)), Set[IndexLimitation](SlowContains))
      ))
    })
  }

  test("uniqueIndexesGetForLabel should return only unique indexes") {
    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
      database.schema().indexFor(Label.label("L2")).on("prop").create()
      database.schema().constraintFor(Label.label("L1")).assertPropertyIsUnique("prop2").create()
      database.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop2id = planContext.getPropertyKeyId("prop2")
      planContext.uniqueIndexesGetForLabel(l1id).toSet should equal(Set(
        IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop2id)), Set[IndexLimitation](SlowContains))
      ))
    })
  }

  test("indexExistsForLabel should return true for both regular and unique indexes") {
    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
      database.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx(_ => {
      database.createNode(Label.label("L3"))
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val l2id = planContext.getLabelId("L2")
      val l3id = planContext.getLabelId("L3")
      planContext.indexExistsForLabel(l1id) should be(true)
      planContext.indexExistsForLabel(l2id) should be(true)
      planContext.indexExistsForLabel(l3id) should be(false)
    })
  }

  test("indexExistsForLabelAndProperties should return true for both regular and unique indexes") {
    inTx(_ => {
      database.schema().indexFor(Label.label("L1")).on("prop").create()
      database.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx(_ => {
      database.createNode(Label.label("L3"))
    })

    inTx(planContext => {
      database.schema().awaitIndexesOnline(10, SECONDS)
      planContext.indexExistsForLabelAndProperties("L1", Seq("prop")) should be(true)
      planContext.indexExistsForLabelAndProperties("L1", Seq("prop2")) should be(false)
      planContext.indexExistsForLabelAndProperties("L2", Seq("prop")) should be(false)
      planContext.indexExistsForLabelAndProperties("L2", Seq("prop2")) should be(true)
      planContext.indexExistsForLabelAndProperties("L3", Seq("prop")) should be(false)
      planContext.indexExistsForLabelAndProperties("L3", Seq("prop2")) should be(false)
    })
  }

  def inTx(f: TransactionBoundPlanContext => Unit) = {
    val tx = graph.beginTransaction(explicit, AUTH_DISABLED)
    val transactionalContext = createTransactionContext(graph, tx)
    val planContext = TransactionBoundPlanContext(TransactionalContextWrapper(transactionalContext), devNullLogger)

    try {
      f(planContext)
      transactionalContext.close(true)
      tx.success()
    } catch {
      case _: Throwable =>
        transactionalContext.close(false)
    } finally {
      tx.close()
    }
  }
}
