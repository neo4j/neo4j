/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.LogProvider

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

abstract class TransactionForeachTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) with SideEffectingInputStream[CONTEXT] {

  override protected def createRuntimeTestSupport(graphDb: GraphDatabaseService,
                                                  edition: Edition[CONTEXT],
                                                  workloadMode: Boolean,
                                                  logProvider: LogProvider): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, workloadMode, logProvider, debugOptions) {
      override def getTransactionType: Type = Type.IMPLICIT
    }
  }

  test("should create data from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe 2
  }

  def inputStreamWithSideEffectInNewTxn(inputValues: InputDataStream, sideEffect: (InternalTransaction, Long) => Unit): InputDataStream = {
    new OnNextInputDataStream(inputValues) {
      override protected def onNext(offset: Long): Unit = withNewTx(sideEffect(_, offset))
    }
  }

  test("should create data in different transactions when using transactionForeach") {
    val numberOfIterations = 30
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val stream = inputStreamWithSideEffectInNewTxn(
      inputValues(inputRows: _*).stream(),
      (tx, offset) => tx.getAllNodes.stream().count() shouldEqual offset
    )

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, stream)

    consume(runtimeResult)

    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe numberOfIterations
  }

  test("should not create data in different transactions when using subqueryForeach") {
    val numberOfIterations = 30
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .subqueryForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val stream = inputStreamWithSideEffectInNewTxn(
      inputValues(inputRows: _*).stream(),
      (tx, offset) => tx.getAllNodes.stream().count() shouldEqual 0
    )

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, stream)

    consume(runtimeResult)

    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions when using transactionForeach and see previous changes") {
    val numberOfIterations = 8
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    given {
      nodeGraph(1, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val stream = inputStreamWithSideEffectInNewTxn(
      inputValues(inputRows: _*).stream(),
      (tx, offset) => tx.getAllNodes.stream().count() shouldEqual Math.pow(2, offset)
    )

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, stream)

    consume(runtimeResult)

    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test("should create data in different transactions when using transactionForeach with index and see previous changes") {
    val numberOfIterations = 8
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    given {
      nodeIndex("Label", "prop")
      nodePropertyGraph(1, {case _ => Map[String, Any]("prop" -> 2)},"Label")
    }


    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNodeWithProperties("b", Seq("Label"), "{prop: 2}"))
      .|.nodeIndexOperator("a:Label(prop=2)")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val stream = inputStreamWithSideEffectInNewTxn(
      inputValues(inputRows: _*).stream(),
      (tx, offset) => tx.getAllNodes.stream().count() shouldEqual Math.pow(2, offset)
    )

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, stream)

    consume(runtimeResult)

    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test("should create data in different transactions when using transactionForeach and see previous changes (also from other transactionForeach)") {
    val numberOfIterations = 4
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    given {
      nodeGraph(1, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val stream = inputStreamWithSideEffectInNewTxn(
      inputValues(inputRows: _*).stream(),
      (tx, offset) => tx.getAllNodes.stream().count() shouldEqual Math.pow(2, 2 * offset)
    )

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, stream)

    consume(runtimeResult)

    val nodes = tx.getAllNodes.asScala.toList
    nodes.size shouldBe Math.pow(2, 2 * numberOfIterations)
  }

  test("statistics should report data creation from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq(1, 2)))
      .withStatistics(nodesCreated = 2, labelsAdded = 2)
  }

  test("statistics should report data creation from subqueries while profiling") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq(1, 2)))
      .withStatistics(nodesCreated = 2, labelsAdded = 2)
  }

  test("Should not throw exception when reading before call subquery") {
    given {
      nodeGraph(sizeHint) ++ nodeGraph(2, "X")
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.allNodeScan("m")
      .nodeByLabelScan("x", "X")
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime)
    noException should be thrownBy consume(runtimeResult)
  }

  test("Make sure transaction state is empty before opening inner transaction") {
    given {
      nodeGraph(sizeHint)
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.allNodeScan("m")
      .create(createNode("node"))
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime)

    val exception = the[Exception] thrownBy consume(runtimeResult)
    exception.getMessage should include("Expected transaction state to be empty when calling transactional subquery.")
  }

  test("Index seeks should see data created from previous transactions") {
    given {
      nodeIndex("N", "prop")
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.nodeIndexOperator("n:N(prop)")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Index seeks should see data created in same transaction") {
    given {
      nodeIndex("N", "prop")
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.union()
      .|.|.emptyResult()
      .|.|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.|.nodeIndexOperator("n:N(prop)")
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("[100, 101, 102] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs(
      (0 until 10) ++ // nodes from given
        (100 to 102) ++ // nodes from LHS of Union
        (11 to 15 by 2) // nodes from RHS of Union
    )
  }

  test("Label scans should see data created from previous transactions") {
    given {
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.nodeByLabelScan("n", "N")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Label scans should see data created in same transaction") {
    given {
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.union()
      .|.|.emptyResult()
      .|.|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.|.nodeByLabelScan("n", "N")
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("[100, 101, 102] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs(
      (0 until 10) ++ // nodes from given
        (100 to 102) ++ // nodes from LHS of Union
        (11 to 15 by 2) // nodes from RHS of Union
      )
  }

  test("Relationship type scans should see data created from previous transactions") {
    given {
      val nodes = nodeGraph(10)
      connectWithProperties(nodes, nodes.indices.map(i => (i, i, "R", Map("prop" -> i))))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.emptyResult()
      .|.create(nodes = Seq(createNode("n"), createNode("m")),
                relationships = Seq(createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: c}"))))
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.relationshipTypeScan("(a)-[r:R]->(b)")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nRels = tx.findRelationships(RelationshipType.withName("R")).asScala.toList
    nRels.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Relationship type scans should see data created in same transaction") {
    given {
      val nodes = nodeGraph(10)
      connectWithProperties(nodes, nodes.indices.map(i => (i, i, "R", Map("prop" -> i))))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach()
      .|.union()
      .|.|.emptyResult()
      .|.|.create(nodes = Seq(createNode("n"), createNode("m")),
                  relationships = Seq(createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: c}"))))
      .|.|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.|.relationshipTypeScan("(a)-[r:R]->(b)")
      .|.create(nodes = Seq(createNode("n"), createNode("m")),
      relationships = Seq(createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: x}"))))
      .|.argument("x")
      .unwind("[100, 101, 102] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns().withNoRows
    val nRels = tx.findRelationships(RelationshipType.withName("R")).asScala.toList
    nRels.map(_.getProperty("prop")) should contain theSameElementsAs(
      (0 until 10) ++ // rels from given
        (100 to 102) ++ // rels from LHS of Union
        (11 to 15 by 2) // rels from RHS of Union
      )
  }
}
