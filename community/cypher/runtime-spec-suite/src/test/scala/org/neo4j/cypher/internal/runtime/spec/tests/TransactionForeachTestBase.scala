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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.LogProvider

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
}
