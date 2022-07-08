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
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingProbe
import org.neo4j.cypher.internal.runtime.spec.RecordingRowsProbe
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.logging.InternalLogProvider
import org.scalatest.Assertion

abstract class TransactionApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) with SideEffectingInputStream[CONTEXT] {

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](
      graphDb,
      edition,
      workloadMode,
      logProvider,
      debugOptions,
      defaultTransactionType = Type.IMPLICIT
    )
  }

  test("batchSize 0") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(0)
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(query, runtime))
    }
    exception.getMessage should include("Must be a positive integer")
  }

  test("batchSize -1") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(-1)
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(query, runtime))
    }
    exception.getMessage should include("Must be a positive integer")
  }

  test("batchSize -1 on an empty input") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(-1)
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(query, runtime))
    }
    exception.getMessage should include("Must be a positive integer")
  }

  test("should create data from returning subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe 3
  }

  test("should read from LHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(3)
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 10, transactionsCommitted = 5)
  }

  test("should work with aggregation on top") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(prop) AS c"))
      .projection("n.prop AS prop")
      .transactionApply(3)
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("c")
      .withSingleRow(10)
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 10, transactionsCommitted = 5)
  }

  test("should work with aggregation on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .transactionApply(3)
      .|.aggregation(Seq.empty, Seq("count(i) AS c"))
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("c")
      .withRows(singleColumn(1 to 10))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 10, transactionsCommitted = 5)
  }

  test("data from returning subqueries should be accessible") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .setProperty("n", "prop", "17")
      .transactionApply()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("prop").withRows(singleColumn(List(17, 17, 17)))

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe 3
  }

  test("should be possible to write to variables returned from subquery") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .setProperty("n", "prop", "n.prop + 1")
      .eager()
      .transactionApply(3)
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(2 to 11))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 20, transactionsCommitted = 5)
  }

  test("should create data in different transactions when using transactionApply") {
    val numberOfIterations = 30
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val probe = recordingProbe(
      "n",
      queryStatistics => {
        queryStatistics.getNodesCreated shouldEqual 1
        queryStatistics.getLabelsAdded shouldEqual 1
      }
    )
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(1)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("n").withRows(rowCount(numberOfIterations))

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions in batches when using transactionApply") {
    val numberOfIterations = 30
    val batchSize = 7
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val probe = recordingProbe(
      "n",
      queryStatistics => {
        queryStatistics.getNodesCreated shouldEqual batchSize
        queryStatistics.getLabelsAdded shouldEqual batchSize
      }
    )
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(batchSize)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("n").withRows(rowCount(numberOfIterations))

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions when using transactionApply and see previous changes") {
    val numberOfIterations = 8
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    given {
      nodeGraph(1, "N")
    }

    var nodeCount: Long = 1
    val probe = recordingProbe(
      "n",
      queryStatistics => {
        val _nodeCount = nodeCount
        nodeCount = tx.findNodes(Label.label("N")).stream().count()
        queryStatistics.getNodesCreated shouldEqual _nodeCount
        queryStatistics.getLabelsAdded shouldEqual _nodeCount
      }
    )
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(1)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("n").withRows(rowCount(Math.pow(2, numberOfIterations).toInt - 1))

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test("should create data in different transactions when using transactionApply with index and see previous changes") {
    val numberOfIterations = 8
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    given {
      nodeIndex("Label", "prop")
      nodePropertyGraph(1, { case _ => Map[String, Any]("prop" -> 2) }, "Label")
    }

    var nodeCount: Long = 1
    val probe = recordingProbe(
      "b",
      queryStatistics => {
        val _nodeCount = nodeCount
        nodeCount = tx.findNodes(Label.label("Label"), "prop", 2).stream().count()
        queryStatistics.getNodesCreated shouldEqual _nodeCount
        queryStatistics.getLabelsAdded shouldEqual _nodeCount
      }
    )
    val query = new LogicalQueryBuilder(this)
      .produceResults("b")
      .transactionApply(1)
      .|.prober(probe)
      .|.create(createNodeWithProperties("b", Seq("Label"), "{prop: 2}"))
      .|.nodeIndexOperator("a:Label(prop=2)")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("b").withRows(rowCount(Math.pow(2, numberOfIterations).toInt - 1))

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test("statistics should report data creation from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(1)
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 10, transactionsCommitted = 11)
  }

  test("statistics should report data creation from subqueries in batches") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(3)
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, propertiesSet = 10, transactionsCommitted = 5)
  }

  test("statistics should report data creation from subqueries while profiling") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply()
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    consume(runtimeResult)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(1, 2)))
      .withStatistics(nodesCreated = 2, labelsAdded = 2, propertiesSet = 2, transactionsCommitted = 2)
  }

  test("should handle RHS with R/W dependencies - with Filter (cancels rows) under Apply") {
    // given
    val sizeHint = 16
    val inputValsToCancel = (0 until sizeHint).map(_ => sizeHint).toArray
    val inputValsToPassThrough = (0 until sizeHint).toArray
    val inputVals = inputValsToCancel ++ inputValsToPassThrough ++ inputValsToCancel
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val batchSize = sizeHint / 4

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(batchSize)
      .|.eager()
      .|.create(createNode("n"))
      .|.argument("x")
      .filter(s"x <> $sizeHint")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)

    val expected = inputValsToPassThrough
    val expectedTxCommitted = inputValsToPassThrough.length / batchSize + 1

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withStatistics(
      nodesCreated = expected.length,
      transactionsCommitted = expectedTxCommitted
    )
  }

  test("should handle RHS with R/W dependencies - with Filter under Apply and Sort on RHS") {
    // given
    val sizeHint = 16
    val inputValsToCancel = (0 until sizeHint).map(_ => sizeHint).toArray
    val inputValsToPassThrough = (0 until sizeHint).toArray
    val inputVals = inputValsToCancel ++ inputValsToPassThrough ++ inputValsToCancel
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val batchSize = sizeHint / 2

    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(batchSize)
      .|.sort(Seq(Ascending("y")))
      .|.create(createNode("n"))
      .|.eager()
      .|.allNodeScan("y", "x")
      .filter(s"x <> $sizeHint")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)

    val expected = inputValsToPassThrough.flatMap(i => (0 until Math.pow(2, i).toInt).map(_ => i))
    val expectedTxCommitted = inputValsToPassThrough.length / batchSize + 1
    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withStatistics(
      nodesCreated = expected.length,
      transactionsCommitted = expectedTxCommitted
    )
  }

  protected def recordingProbe(
    variable: String,
    assertion: QueryStatistics => Assertion
  ): Prober.Probe with RecordingRowsProbe = {
    val probe = new Probe {
      private var _prevTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty
      private var _thisTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty
      private var _transactionsCommitted = 0

      override def onRow(row: AnyRef, queryStatistics: QueryStatistics, transactionsCommitted: Int): Unit = {
        if (_transactionsCommitted != transactionsCommitted) {
          assertion(_thisTxQueryStatistics.-(_prevTxQueryStatistics))
          _transactionsCommitted = transactionsCommitted
          _prevTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty.+(_thisTxQueryStatistics)
        }
        _thisTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics(queryStatistics)
      }
    }
    new RecordingProbe(variable)(probe)
  }
}
