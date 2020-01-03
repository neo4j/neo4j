/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.spi.{IndexDescriptor, IndexLimitation, SlowContains}
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.v4_0.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.{GraphDatabaseService, Label, RelationshipType}
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.api.KernelTransaction.Type._
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class TransactionBoundPlanContextTest extends CypherFunSuite {

  private var managementService: DatabaseManagementService = _
  private var database: GraphDatabaseService = _
  private var graph: GraphDatabaseCypherService = _

  private def createTransactionContext(graphDatabaseCypherService: GraphDatabaseCypherService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseCypherService)
    contextFactory.newContext(transaction, "no query", EMPTY_MAP)
  }

  override protected def initTest(): Unit = {
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    database = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(database)
  }

  override protected def afterEach(): Unit = {
    managementService.shutdown()
  }

  test("statistics should default to minimum cardinality on empty db") {

    inTx((planContext,_) => {
      val statistics = planContext.statistics

      // all nodes
      statistics.nodesAllCardinality() should equal(Cardinality(10.0))

      // nodes with label
      statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality(10.0))

      // pattern stats
      Set(Some(LabelId(0)), None).foreach { label1 =>
        Set(Some(LabelId(1)), None).foreach { label2 =>
          statistics.patternStepCardinality(label1, Some(RelTypeId(0)), label2) should equal(Cardinality.SINGLE)
          statistics.patternStepCardinality(label1, None, label2) should equal(Cardinality.SINGLE)
        }
      }
    })
  }

  test("statistics should default to minimum cardinality for unknown counts on nonempty db") {

    // given
    inTx( (_, tx) => {
      for ( i <- 0 until 100 ) {
        val n1 = tx.createNode(Label.label("L1"))
        val n2 = tx.createNode()
        n1.createRelationshipTo(n2, RelationshipType.withName("T"))
      }
    })

    // then
    inTx( (planContext, _) => {
      val statistics = planContext.statistics

      // label stats
      statistics.nodesWithLabelCardinality(Some(LabelId(0))) should equal(Cardinality(100))
      statistics.nodesWithLabelCardinality(Some(LabelId(1))) should equal(Cardinality(10))
      statistics.nodesAllCardinality() should equal(Cardinality(200))

      // pattern stats
      statistics.patternStepCardinality(
        Some(LabelId(0)), Some(RelTypeId(0)), Some(LabelId(1))) should equal(Cardinality.SINGLE)
      statistics.patternStepCardinality(
        Some(LabelId(0)), Some(RelTypeId(0)), None) should equal(Cardinality(100))
      statistics.patternStepCardinality(
        Some(LabelId(0)), None, Some(LabelId(1))) should equal(Cardinality.SINGLE)
      statistics.patternStepCardinality(
        Some(LabelId(0)), None, None) should equal(Cardinality(100))
      statistics.patternStepCardinality(
        None, None, None) should equal(Cardinality(100))
      statistics.patternStepCardinality(
        None, Some(RelTypeId(0)), None) should equal(Cardinality(100))
    })
  }

  test("indexPropertyExistsSelectivity of empty label index should be 0") {
    inTx( (_, tx) => {
      tx.schema().indexFor(Label.label("L1")).on("prop").create()
    })

    inTx( ( planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop1id = planContext.getPropertyKeyId("prop")
      val index = IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop1id)))
      planContext.statistics.indexPropertyExistsSelectivity(index) should be(Some(Selectivity.ZERO))
    })
  }

  test("uniqueValueSelectivity of empty index should be 0") {
    inTx(( planContext, tx) => {
      tx.createNode(Label.label("L1"))
    })

    inTx((planContext, tx) => {
      tx.schema().indexFor(Label.label("L1")).on("prop").create()
    })

    inTx((planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop1id = planContext.getPropertyKeyId("prop")
      val index = IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop1id)))
      planContext.statistics.uniqueValueSelectivity(index) should be(Some(Selectivity.ZERO))
    })
  }

  test("indexesGetForLabel should return both regular and unique indexes") {
    inTx((planContext, tx) => {
      val schema = tx.schema()
      schema.indexFor(Label.label("L1")).on("prop").create()
      schema.indexFor(Label.label("L2")).on("prop").create()
      schema.constraintFor(Label.label("L1")).assertPropertyIsUnique("prop2").create()
      schema.constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx((planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
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
    inTx((planContext, tx) => {
      tx.schema().indexFor(Label.label("L1")).on("prop").create()
      tx.schema().indexFor(Label.label("L2")).on("prop").create()
      tx.schema().constraintFor(Label.label("L1")).assertPropertyIsUnique("prop2").create()
      tx.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx((planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val prop2id = planContext.getPropertyKeyId("prop2")
      planContext.uniqueIndexesGetForLabel(l1id).toSet should equal(Set(
        IndexDescriptor(LabelId(l1id), Seq(PropertyKeyId(prop2id)), Set[IndexLimitation](SlowContains))
      ))
    })
  }

  test("indexExistsForLabel should return true for both regular and unique indexes") {
    inTx((planContext, tx) => {
      tx.schema().indexFor(Label.label("L1")).on("prop").create()
      tx.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx((planContext, tx) => {
      tx.createNode(Label.label("L3"))
    })

    inTx((planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
      val l1id = planContext.getLabelId("L1")
      val l2id = planContext.getLabelId("L2")
      val l3id = planContext.getLabelId("L3")
      planContext.indexExistsForLabel(l1id) should be(true)
      planContext.indexExistsForLabel(l2id) should be(true)
      planContext.indexExistsForLabel(l3id) should be(false)
    })
  }

  test("indexExistsForLabelAndProperties should return true for both regular and unique indexes") {
    inTx((planContext, tx) => {
      tx.schema().indexFor(Label.label("L1")).on("prop").create()
      tx.schema().constraintFor(Label.label("L2")).assertPropertyIsUnique("prop2").create()
    })

    inTx((planContext, tx) => {
      tx.createNode(Label.label("L3"))
    })

    inTx((planContext, tx) => {
      tx.schema().awaitIndexesOnline(10, SECONDS)
      planContext.indexExistsForLabelAndProperties("L1", Seq("prop")) should be(true)
      planContext.indexExistsForLabelAndProperties("L1", Seq("prop2")) should be(false)
      planContext.indexExistsForLabelAndProperties("L2", Seq("prop")) should be(false)
      planContext.indexExistsForLabelAndProperties("L2", Seq("prop2")) should be(true)
      planContext.indexExistsForLabelAndProperties("L3", Seq("prop")) should be(false)
      planContext.indexExistsForLabelAndProperties("L3", Seq("prop2")) should be(false)
    })
  }

  def inTx(f: (TransactionBoundPlanContext,InternalTransaction) => Unit) = {
    val tx = graph.beginTransaction(explicit, AUTH_DISABLED)
    val transactionalContext = createTransactionContext(graph, tx)
    val planContext = TransactionBoundPlanContext(TransactionalContextWrapper(transactionalContext), devNullLogger)

    try {
      f(planContext, tx)
      transactionalContext.close()
      tx.commit()
    } catch {
      case t: Throwable =>
        transactionalContext.close()
        tx.rollback()
        throw t
    } finally {
      tx.close()
    }
  }
}
