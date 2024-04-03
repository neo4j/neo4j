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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.IteratorInputStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.GraphCreation.ComplexGraph
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.cypher.internal.runtime.spec.rewriters.RussianRoulette
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.runtime.spec.tests.RandomisedTransactionForEachTests.genRandomTestSetup
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionApplyTestBase.ComplexRhsTestSetup
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.bottomUpWithParent
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.LoneElement

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

abstract class TransactionApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime, testPlanCombinationRewriterHints = Set(NoRewrites))
    with SideEffectingInputStream[CONTEXT]
    with RandomisedTransactionApplyTests[CONTEXT]
    with RandomValuesTestSupport
    with LoneElement {

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](
      graphDb,
      edition,
      runtime,
      workloadMode,
      logProvider,
      debugOptions,
      defaultTransactionType = Type.IMPLICIT
    )
  }

  test("batchSize 0") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(0, onErrorBehaviour = randomErrorBehaviour())
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
      .transactionApply(-1, onErrorBehaviour = randomErrorBehaviour())
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
      .transactionApply(-1, onErrorBehaviour = randomErrorBehaviour())
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
      .transactionApply(onErrorBehaviour = randomErrorBehaviour())
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
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should work with aggregation on top") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(prop) AS c"))
      .projection("n.prop AS prop")
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("c")
      .withSingleRow(10)
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should work with aggregation on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.aggregation(Seq.empty, Seq("count(i) AS c"))
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("c")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should work with grouping aggregation on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.aggregation(Seq("1 AS group"), Seq("count(i) AS c"))
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("c")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should work with top on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("i")
      .transactionApply(3, onErrorBehaviour = OnErrorContinue)
      .|.top(2, "i DESC")
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("i")
      .withRows(singleColumn(Seq(1, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 8, 7, 9, 8, 10, 9)))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should handle RHS with R/W dependencies on both branches of union - with aggregation on top") {
    // given
    val nodeCountA = 7
    val nodeCountB = 5
    val nodeCountC = 3
    val batchSize = 2
    givenGraph {
      for (_ <- 0 until nodeCountA) yield runtimeTestSupport.tx.createNode(Label.label("A"))
      for (_ <- 0 until nodeCountB) yield runtimeTestSupport.tx.createNode(Label.label("B"))
      for (_ <- 0 until nodeCountC) yield runtimeTestSupport.tx.createNode(Label.label("C"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .aggregation(Seq.empty, Seq("count(*) AS x"))
      .transactionApply(batchSize = batchSize, onErrorBehaviour = randomErrorBehaviour())
      .|.union()
      .|.|.create(createNode("cc", "C"))
      .|.|.eager()
      .|.|.nodeByLabelScan("c", "C")
      .|.create(createNode("bb", "B"))
      .|.eager()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = (nodeCountB + nodeCountC) * (Math.pow(2, nodeCountA).toInt - 1)
    val expectedCommits = (nodeCountA + (nodeCountA % batchSize)) / batchSize + 1 // +1 is for outer transaction
    runtimeResult should beColumns("x")
      .withSingleRow(expectedCount)
      .withStatistics(
        nodesCreated = expectedCount,
        labelsAdded = expectedCount,
        transactionsStarted = expectedCommits,
        transactionsCommitted = expectedCommits
      )
  }

  test("data from returning subqueries should be accessible") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .setProperty("n", "prop", "17")
      .transactionApply(onErrorBehaviour = randomErrorBehaviour())
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
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(2 to 11))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 20,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("should create data in different transactions when using transactionApply") {
    val numberOfIterations = 30
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val probe = queryStatisticsProbe(queryStatistics => {
      queryStatistics.getNodesCreated shouldEqual 1
      queryStatistics.getLabelsAdded shouldEqual 1
    })

    var committedCount = 0L
    val txProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual committedCount
      committedCount += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(1, onErrorBehaviour = randomErrorBehaviour())
      .|.prober(txProbe)
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

    val probe = queryStatisticsProbe(queryStatistics => {
      queryStatistics.getNodesCreated shouldEqual batchSize
      queryStatistics.getLabelsAdded shouldEqual batchSize
    })

    var committedCount = 0L
    var inputRow = 0L
    val txProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual committedCount
      inputRow += 1
      if (inputRow % batchSize == 0) {
        committedCount = inputRow
      }
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(batchSize, onErrorBehaviour = randomErrorBehaviour())
      .|.prober(txProbe)
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

    givenGraph {
      nodeGraph(1, "N")
    }

    var nodeCount: Long = 1
    val probe = queryStatisticsProbe(queryStatistics => {
      val _nodeCount = nodeCount
      nodeCount = tx.findNodes(Label.label("N")).stream().count()
      queryStatistics.getNodesCreated shouldEqual _nodeCount
      queryStatistics.getLabelsAdded shouldEqual _nodeCount
    })

    var committedCount = 1L
    var rhsRowsRemaining = 1L // initial node count
    val txProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual committedCount
      rhsRowsRemaining -= 1
      if (rhsRowsRemaining == 0) {
        committedCount *= 2
        rhsRowsRemaining = committedCount
      }
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(1, onErrorBehaviour = randomErrorBehaviour())
      .|.prober(txProbe)
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

    givenGraph {
      nodeIndex("Label", "prop")
      nodePropertyGraph(1, { case _ => Map[String, Any]("prop" -> 2) }, "Label")
    }

    var nodeCount: Long = 1
    val probe = queryStatisticsProbe(queryStatistics => {
      val _nodeCount = nodeCount
      nodeCount = tx.findNodes(Label.label("Label"), "prop", 2).stream().count()
      queryStatistics.getNodesCreated shouldEqual _nodeCount
      queryStatistics.getLabelsAdded shouldEqual _nodeCount
    })

    var committedCount = 1L
    var rhsRowsRemaining = 1L // initial node count
    val txProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual committedCount
      rhsRowsRemaining -= 1
      if (rhsRowsRemaining == 0) {
        committedCount *= 2
        rhsRowsRemaining = committedCount
      }
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("b")
      .transactionApply(1, onErrorBehaviour = randomErrorBehaviour())
      .|.prober(txProbe)
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
      .transactionApply(1, onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 11,
        transactionsCommitted = 11
      )
  }

  test("statistics should report data creation from subqueries in batches") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(3, onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsStarted = 5,
        transactionsCommitted = 5
      )
  }

  test("statistics should report data creation from subqueries while profiling") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(onErrorBehaviour = randomErrorBehaviour())
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(1, 2)))
      .withStatistics(
        nodesCreated = 2,
        labelsAdded = 2,
        propertiesSet = 2,
        transactionsStarted = 2,
        transactionsCommitted = 2
      )
  }

  test("should handle RHS with R/W dependencies - with Filter (cancels rows) under Apply") {
    // given
    val sizeHint = 16
    val inputValsToCancel = (0 until sizeHint).map(_ => sizeHint).toArray
    val inputValsToPassThrough = (0 until sizeHint).toArray
    val inputVals = inputValsToCancel ++ inputValsToPassThrough ++ inputValsToCancel
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val batchSize = sizeHint / 4
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(batchSize, onErrorBehaviour = randomErrorBehaviour())
      .|.eager()
      .|.create(createNode("n"))
      .|.argument("x")
      .filter(s"x <> $sizeHint")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)

    val expected = inputValsToPassThrough
    val expectedTxCommitted = inputValsToPassThrough.length / batchSize + 1

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withStatistics(
      nodesCreated = expected.length,
      transactionsStarted = expectedTxCommitted,
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
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(batchSize, onErrorBehaviour = randomErrorBehaviour())
      .|.sort("y ASC")
      .|.create(createNode("n"))
      .|.eager()
      .|.allNodeScan("y", "x")
      .filter(s"x <> $sizeHint")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)

    val expected = inputValsToPassThrough.flatMap(i => (0 until Math.pow(2, i).toInt).map(_ => i))
    val expectedTxCommitted = inputValsToPassThrough.length / batchSize + 1
    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withStatistics(
      nodesCreated = expected.length,
      transactionsStarted = expectedTxCommitted,
      transactionsCommitted = expectedTxCommitted
    )
  }

  test("should discard columns") {
    assume(runtime.name != "interpreted" && runtime.name != "slotted")

    val probe = recordingProbe("keepLhs", "discardLhs", "keepRhs", "discardRhs")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keepLhs", "keepRhs", "comesFromDiscarded")
      .prober(probe)
      .nonFuseable() // Needed because of limitation in prober
      // Discarded but should not be removed because there's no eager buffer after this point
      .projection("0 as hello")
      .transactionApply(onErrorBehaviour = randomErrorBehaviour())
      .|.projection("discardLhs + discardRhs as comesFromDiscarded")
      .|.projection("keepRhs as keepRhs")
      .|.projection("'bla' + (a+2) as keepRhs", "'blö' + (a+3) as discardRhs")
      .|.argument()
      .projection("keepLhs as keepLhs")
      .projection("'bla' + (a) as keepLhs", "'blö' + (a+1) as discardLhs")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    result should beColumns("keepLhs", "keepRhs", "comesFromDiscarded")
      .withRows(inOrder(
        Range.inclusive(0, sizeHint)
          .map(i => Array(s"bla$i", s"bla${i + 2}", s"blö${i + 1}blö${i + 3}"))
      ))

    probe.seenRows.map(_.toSeq).toSeq shouldBe
      Range.inclusive(0, sizeHint).map { i =>
        Seq(stringValue(s"bla$i"), null, stringValue(s"bla${i + 2}"), null)
      }
  }

  // TransactionApply do not break in slotted, so should not discard
  test("should not discard columns (slotted)") {
    assume(runtime.name == "slotted")

    val probe = recordingProbe("keepLhs", "discardLhs", "keepRhs", "discardRhs")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keepLhs", "keepRhs", "comesFromDiscarded")
      .prober(probe)
      // Discarded but should not be removed because there's no eager buffer after this point
      .projection("0 as hello")
      .transactionApply(onErrorBehaviour = randomErrorBehaviour())
      .|.projection("discardLhs + discardRhs as comesFromDiscarded")
      .|.projection("keepRhs as keepRhs")
      .|.projection("'bla' + (a+2) as keepRhs", "'blö' + (a+3) as discardRhs")
      .|.argument()
      .projection("keepLhs as keepLhs")
      .projection("'bla' + (a) as keepLhs", "'blö' + (a+1) as discardLhs")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    result should beColumns("keepLhs", "keepRhs", "comesFromDiscarded")
      .withRows(inOrder(
        Range.inclusive(0, sizeHint)
          .map(i => Array(s"bla$i", s"bla${i + 2}", s"blö${i + 1}blö${i + 3}"))
      ))

    probe.seenRows.map(_.toSeq).toSeq shouldBe
      Range.inclusive(0, sizeHint).map { i =>
        Seq(
          stringValue(s"bla$i"),
          stringValue(s"blö${i + 1}"),
          stringValue(s"bla${i + 2}"),
          stringValue(s"blö${i + 3}")
        )
      }
  }

  private def defaultComplexRhsSetup(): ComplexRhsTestSetup = {
    val iterations = 3
    val batchSize = random.nextInt(iterations + 1) + 1
    val concurrency = TransactionConcurrency.Concurrent(Some(literalInt(1)))
    ComplexRhsTestSetup(batchSize, concurrency, iterations, OnErrorFail, None)
  }

  private def setupComplexRhsTest(
    graph: ComplexGraph,
    setup: ComplexRhsTestSetup = defaultComplexRhsSetup()
  ): (LogicalQueryBuilder, Seq[Seq[Array[Object]]]) = {
    val expectedRhsResult = OrderedTrailTestBase.complexGraphAndPartiallyOrderedExpectedResult(graph)
    val expected = Range.inclusive(1, setup.iterations).flatMap(i => expectedRhsResult.map(_.map(_ :+ longValue(i))))
    val produce =
      Seq("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2", "iteration") ++ setup.status

    val planBuilder = new LogicalQueryBuilder(this)
      .produceResults(produce: _*)
      .transactionApply(setup.batchSize, setup.concurrency, setup.onError, setup.status)
      .|.valueHashJoin("left=right")

      // Join RHS (identical to LHS)
      .|.|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      // insert distinct to "cancel out" the effect of unioning two identical inputs together
      .|.|.distinct(
        "start AS start",
        "middle AS middle",
        "end AS end",
        "a AS a",
        "b AS b",
        "r1 AS r1",
        "c AS c",
        "d AS d",
        "r2 AS r2"
      )
      .|.|.union()
      // (on RHS of Join) Union RHS (identical to Union LHS)
      .|.|.|.optional("end")
      .|.|.|.filter("end:LOOP")
      .|.|.|.apply()
      .|.|.|.|.trail(TrailTestBase.`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.|.optional("middle")
      .|.|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.|.argument("middle", "c_inner")
      .|.|.|.|.argument("middle")
      .|.|.|.sort("foo ASC")
      .|.|.|.projection("start.foo AS foo")
      .|.|.|.filter("middle:MIDDLE:LOOP")
      .|.|.|.trail(TrailTestBase.`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.|.allNodeScan("b_inner")
      .|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.|.optional("start")
      .|.|.|.|.argument("firstMiddle", "a_inner")
      .|.|.|.trail(TrailTestBase.`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.|.argument("start", "anon_start_inner")
      .|.|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      // (on RHS of Join) Union LHS (identical to Union RHS)
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.trail(TrailTestBase.`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.argument("middle")
      .|.|.sort("foo ASC")
      .|.|.projection("start.foo AS foo")
      .|.|.filter("middle:MIDDLE:LOOP")
      .|.|.trail(TrailTestBase.`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(TrailTestBase.`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)

    // Join LHS (identical to RHS)
    planBuilder
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(TrailTestBase.`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.limit(Long.MaxValue)
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.limit(Long.MaxValue)
      .|.sort("foo ASC")
      .|.projection("start.foo AS foo")
      .|.filter("middle:MIDDLE:LOOP")
      .|.trail(TrailTestBase.`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.trail(TrailTestBase.`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)
      .unwind(s"range(1, ${setup.iterations}) as iteration")
      .argument()

    (planBuilder, expected)
  }

  test("complex case: complex RHS") {
    // given
    val graph = givenGraph(complexGraph())
    val setup = defaultComplexRhsSetup().copy(onError = randomErrorBehaviour())
    val (planBuilder, expected) = setupComplexRhsTest(graph, setup)

    val query = planBuilder.build()

    val result = execute(query, runtime)
    result should
      beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2", "iteration")
        .withRows(inPartialOrder(expected))
  }

  test("complex case: random failures with complex RHS") {
    // given
    val graph = givenGraph(complexGraph())
    val (planBuilder, _) = setupComplexRhsTest(graph)

    val query = planBuilder.build()

    val rewriter = RussianRoulette(0.005, 0.25, planBuilder.idGen, random)
    val rewritten = query.logicalPlan.endoRewrite(rewriter)

    Try(executeAndConsume(query.copy(logicalPlan = rewritten), runtime)) match {
      case Failure(error) =>
        withClue(
          s"""Unexpected error:
             |${ExceptionUtils.getStackTrace(error)}
             |Rewritten plan:
             |$rewritten
             |""".stripMargin
        ) {
          error.getMessage should include("/ by zero")
          val cause = error.getCause
          if (cause != null) {
            cause shouldBe an[org.neo4j.exceptions.ArithmeticException]
            cause.getMessage shouldBe "/ by zero"
            cause.getCause shouldBe null
            cause.getSuppressed.isEmpty shouldBe true
          }
          error.getSuppressed.isEmpty shouldBe true
        }
      case Success(_) =>
        cancel("Query did not fail")
    }
  }

  test("complex case: random failures with complex RHS ON ERROR CONTINUE") {
    // given
    val graph = givenGraph(complexGraph())
    val iterations = random.nextInt(8) + 1
    val batchSize = random.nextInt(iterations + 1) + 1
    val setup = ComplexRhsTestSetup(
      batchSize = batchSize,
      concurrency = TransactionConcurrency.Serial,
      iterations = iterations,
      onError = OnErrorContinue,
      status = Some("s")
    )
    val (planBuilder, expected) = setupComplexRhsTest(graph, setup)

    val query = planBuilder.build()

    val rewritten = query.logicalPlan.endoRewrite(bottomUpWithParent(
      RewriterWithParent.lift {
        case (rhs: LogicalPlan, Some(parent: TransactionApply)) if parent.right == rhs =>
          rhs.endoRewrite(RussianRoulette(0.0005, 0.25, planBuilder.idGen))
      }
    ))

    // The result Seqs represent 1) tx batch, 2) rows in tx batch 3) columns in row
    def batches(rows: Seq[Array[_ <: AnyRef]]): Seq[Seq[Seq[AnyValue]]] = {
      rows
        .map(_.toSeq.map(ValueUtils.asAnyValue))
        .groupBy(r => r(10).asInstanceOf[LongValue].longValue()).toSeq // Result by iteration
        .sortBy { case (iteration, _) => iteration }
        .map { case (_, rows) => rows }
        .grouped(setup.batchSize)
        .map(_.flatten)
        .toSeq
    }

    val resultBatches = batches(executeAndConsume(query.copy(logicalPlan = rewritten), runtime).awaitAll())
    val expectedBatches = batches(expected.flatten)

    withClue(
      s"""Test setup: $setup
         |Rewritten plan:
         |$rewritten
         |""".stripMargin
    ) {
      resultBatches.size shouldBe expectedBatches.size
      resultBatches.zip(expectedBatches).foreach {
        case (result, expected) =>
          val status = result
            .map(_.last.asInstanceOf[MapValue])
            .toSet
            .loneElement // All rows in batch should have the same status

          val committed = status.get("committed").asInstanceOf[BooleanValue].booleanValue()
          val resultWithoutStatus = result.map(rows => rows.take(rows.length - 1))
          if (committed) {
            resultWithoutStatus should contain theSameElementsAs expected
          } else {
            val iterations = expected.map(_.last.asInstanceOf[LongValue]).distinct
            resultWithoutStatus shouldBe iterations.map(i => Seq.fill(10)(NO_VALUE) :+ i)
          }
      }
    }
  }

  test("should fail on downstream errors") {
    val rows = sizeHint / 4
    val batchSize = random.nextInt(rows + 20) + 1
    val failAtRow = random.nextInt(rows)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "status", "bang", "hello")
      .projection(s"1 / (x - $failAtRow) as bang")
      .transactionApply(
        batchSize,
        onErrorBehaviour = randomAmong(Seq(OnErrorContinue, OnErrorBreak)),
        maybeReportAs = Some("status")
      )
      .|.projection("'im innocent' as hello")
      .|.argument()
      .unwind(s"range(0, ${rows - 1}) as x")
      .argument()
      .build()

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(logicalQuery, runtime))
    }
    exception.getMessage should include("/ by zero")
  }

  test("should fail on upstream errors") {
    val rows = sizeHint / 4
    val batchSize = random.nextInt(rows + 20) + 1
    val failAtRow = random.nextInt(rows)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "status", "bang", "hello")
      .transactionApply(
        batchSize,
        onErrorBehaviour = randomAmong(Seq(OnErrorContinue, OnErrorBreak)),
        maybeReportAs = Some("status")
      )
      .|.projection("'im innocent' as hello")
      .|.argument()
      .projection(s"1 / (x - $failAtRow) as bang")
      .unwind(s"range(0, ${rows - 1}) as x")
      .argument()
      .build()

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(logicalQuery, runtime))
    }
    exception.getMessage should include("/ by zero")
  }

  test("error handling with plan that evaluates expressions inside buffers in pipelined") {
    // given
    val batchSize = random.nextInt(17) + 1
    val input = Range(0, sizeHint)
      .grouped(batchSize)
      .flatMap { batch =>
        val canBatchFail = random.nextBoolean()
        if (canBatchFail) {
          batch.map(_ => (random.nextDouble(), random.nextInt(3)))
        } else {
          batch.map(_ => (random.nextDouble(), random.nextInt(2) + 1))
        }
      }
      .toSeq

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "committed", "started", "error", "hiFromRhs")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .transactionApply(batchSize, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
      .|.projection("x*y as hiFromRhs")
      .|.selectOrSemiApply("y / y + y = 2")
      .|.|.filter("x > 0.5")
      .|.|.argument()
      .|.argument()
      .input(variables = Seq("x", "y"))
      .build()

    // Test integrity
    if (runtime.name == "Pipelined") {
      testIntegrityFailInsideBuffer()
    }

    // then
    val inputRows = input.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))

    val expected = input
      .grouped(batchSize)
      .flatMap { batch =>
        val committed = !batch.map(_._2).contains(0)
        if (committed) {
          batch.flatMap {
            case (x, y) =>
              val useLhs = y / y + y == 2
              val rhsEmpty = x <= 0.5
              if (!useLhs && rhsEmpty) Seq()
              else Seq(Array[Any](x, y, true, true, null, x * y))
          }
        } else {
          batch.map(x => Array[Any](x._1, x._2, committed, true, "/ by zero", null))
        }
      }
      .toSeq

    runtimeResult should beColumns("x", "y", "committed", "started", "error", "hiFromRhs")
      .withRows(expected)
  }

  private def testIntegrityFailInsideBuffer() {
    val throwingPlan = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .transactionApply(1, onErrorBehaviour = OnErrorFail)
      .|.selectOrSemiApply("y / y + y = 2")
      .|.|.filter("x > 0.5")
      .|.|.argument()
      .|.argument()
      .input(variables = Seq("x", "y"))
      .build()
    val failingInput = Array[Any](0.5, 0)
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(throwingPlan, runtime, inputValues(failingInput)))
    }
    exception.getMessage should include("/ by zero")
    val prettyTrace = ExceptionUtils.getStackTrace(exception)
    withClue(s"\nException was not caused by ConditionalApplyBuffer.put:\n$prettyTrace\n") {
      // The following assertion is here to make sure we fail in a buffer put,
      // if the assertion fails we should find another plan that fails in put to keep coverage
      assert(exception.getCause.getStackTrace.exists { e =>
        e.getClassName.endsWith("ConditionalApplyBuffer") && e.getMethodName.equals("put")
      })
    }
  }

  test("error handling with limit on all sides") {
    // given
    val batchSize = random.nextInt(9) + 1
    val input = Range(0, sizeHint / 3)
      .grouped(batchSize)
      .flatMap { batch =>
        val canBatchFail = random.nextBoolean()
        val size = random.nextInt(6)
        if (canBatchFail) {
          batch.map(_ => Range(0, size).map(_ => random.nextInt(3)).toArray)
        } else {
          batch.map(_ => Range.inclusive(1, size).toArray)
        }
      }
      .toSeq

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xs", "x", "committed", "started", "error", "maybeBang")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .limit(input.size / 2)
      .transactionApply(batchSize, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
      .|.projection("1 / x as maybeBang")
      .|.limit(3)
      .|.unwind("xs as x")
      .|.argument()
      .limit(input.size - 3)
      .input(variables = Seq("xs"))
      .build()

    // then
    val inputRows = input.map { x => Array[Any](x) }
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))

    val expected = input
      .take(input.size - 3)
      .grouped(batchSize)
      .flatMap { batch =>
        val committed = !batch.exists(_.take(3).contains(0))
        if (committed) {
          batch.flatMap { xs =>
            xs.take(3).map(x => Array[Any](xs, x, true, true, null, 1 / x))
          }
        } else {
          batch.map { xs =>
            Array[Any](xs, null, false, true, "/ by zero", null)
          }
        }
      }
      .take(input.size / 2)
      .toSeq

    runtimeResult should beColumns("xs", "x", "committed", "started", "error", "maybeBang")
      .withRows(expected)
  }

  private def setupCommitErrorHandlingTest(
    errorBehaviour: InTransactionsOnErrorBehaviour,
    status: Boolean = true
  ): (Seq[MapValue], Int, LogicalQuery) = {
    givenGraph {
      nodeConstraint("Dog") { creator =>
        creator.assertPropertyExists("tail")
      }
    }

    val rows = math.max(sizeHint / 4, 50)
    val batchSize = if (random.nextBoolean()) random.nextInt(rows + 3) + 1 else random.nextInt(10) + 1
    val rowFailureProbability = 0.1
    val input = Range(0, rows).map { i =>
      val props = new MapValueBuilder()
      props.add("id", intValue(i))
      if (random.nextDouble() >= rowFailureProbability) {
        props.add("tail", stringValue("wagging"))
      }
      props.build()
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("props", "committed", "started", "error", "idFromRhs")
      .projection(
        (if (status) Seq(
           "status.committed as committed",
           "status.started as started",
           "right(status.errorMessage, 46) as error"
         )
         else Seq(
           "null as committed",
           "null as started",
           "null as error"
         )): _*
      )
      .transactionApply(
        batchSize,
        onErrorBehaviour = errorBehaviour,
        maybeReportAs = if (status) Some("status") else None
      )
      .|.projection("d.id as idFromRhs")
      .|.create(createNodeWithProperties("d", Seq("Dog"), "props"))
      .|.argument()
      .input(variables = Seq("props"))
      .build()

    (input, batchSize, query)
  }

  test("test integrity: handle errors during commit") {
    assume(runtime.name != "interpreted")
    val (_, _, query) = setupCommitErrorHandlingTest(OnErrorFail, status = false)

    // Test integrity
    val failingInput = Array[Any](MapValue.EMPTY)
    val exception = intercept[ConstraintViolationException] {
      consume(execute(query, runtime, new InputValues().and(failingInput)))
    }
    exception.getMessage should include("with label `Dog` must have the property `tail`")
    val prettyTrace = ExceptionUtils.getStackTrace(exception)
    withClue(s"\nStacktrace did not include commit from kernel package:\n$prettyTrace\n") {
      // if the following assertion fails you might need to find another plan that fails during commit
      assert(exception.getStackTrace.exists { e =>
        e.getClassName.startsWith("org.neo4j.kernel") && e.getMethodName.equals("commit")
      })
    }
  }

  test("handle errors during commit ON ERROR CONTINUE") {
    assume(runtime.name != "interpreted")
    val (input, batchSize, query) = setupCommitErrorHandlingTest(OnErrorContinue)

    // then
    val queryInput = input.map(r => Array[Any](r))
    val result = execute(query, runtime, new InputValues().and(queryInput: _*))
    val expected = input
      .grouped(batchSize)
      .flatMap { batch =>
        val batchSuccessful = batch.forall(_.containsKey("tail"))
        if (batchSuccessful)
          batch.map(r => Array[Any](r, true, true, null, r.get("id")))
        else
          batch.map(r => Array[Any](r, false, true, "with label `Dog` must have the property `tail`", null))
      }

    result should beColumns("props", "committed", "started", "error", "idFromRhs")
      .withRows(inOrder(expected.toSeq))
  }

  test("handle errors during commit ON ERROR BREAK") {
    assume(runtime.name != "interpreted")
    val (input, batchSize, query) = setupCommitErrorHandlingTest(OnErrorBreak)

    // then
    val queryInput = input.map(r => Array[Any](r))
    val result = execute(query, runtime, new InputValues().and(queryInput: _*))
    val firstFailure = input
      .find(r => !r.containsKey("tail"))
      .map(_.get("id").asInstanceOf[IntValue].intValue())
    val expected = input
      .grouped(batchSize)
      .flatMap { batch =>
        val batchFirstId = batch.head.get("id").asInstanceOf[IntValue].intValue()
        val batchLastId = batch.last.get("id").asInstanceOf[IntValue].intValue()
        val shouldFail = firstFailure.exists(first => first >= batchFirstId && first <= batchLastId)
        val hasFailedPreviousBatch = firstFailure.exists(first => batchFirstId > first)
        if (shouldFail)
          batch.map(r => Array[Any](r, false, true, "with label `Dog` must have the property `tail`", null))
        else if (hasFailedPreviousBatch) batch.map(r => Array[Any](r, false, false, null, null))
        else batch.map(r => Array[Any](r, true, true, null, r.get("id")))
      }

    result should beColumns("props", "committed", "started", "error", "idFromRhs")
      .withRows(inOrder(expected.toSeq))
  }

  test("handle errors with union on RHS") {
    givenGraph {
      uniqueNodeIndex("Animal", "id")
      nodePropertyGraph(1, { case _ => Map("id" -> 0) }, "Animal")
    }

    val batchSize = random.nextInt(16) + 1
    val inputRows = math.max(sizeHint / 5, 50)
    val idGen = new AtomicLong(1)

    def generateIdsBatch(thisBatchSize: Int, fail: Boolean): Array[Array[Long]] = {
      val failAtBatch = if (fail) random.nextInt(thisBatchSize) else -1
      Range(0, thisBatchSize).toArray.map { batch =>
        val size = if (fail) random.nextInt(3) + 1 else random.nextInt(4)
        val failAtRow = if (fail) random.nextInt(size) else -1
        Range(0, size).toArray.map {
          case row if batch == failAtBatch && row == failAtRow => 0L
          case _                                               => idGen.getAndIncrement()
        }
      }
    }
    val inputBatches = Range(0, inputRows)
      .grouped(batchSize)
      .map { batch =>
        val failLhs = random.nextDouble() < 0.25
        val failRhs = random.nextDouble() < 0.25
        val lhs = generateIdsBatch(batch.size, failLhs)
        val rhs = generateIdsBatch(batch.size, failRhs)
        lhs.zip(rhs).map { case (lhs, rhs) => Array[Array[Long]](lhs, rhs) }
      }
      .toSeq

    val query = new LogicalQueryBuilder(this)
      .produceResults("lhsIds", "rhsIds", "id", "labels", "committed", "started", "expectedError")
      .projection("isErr1 or isErr2 as expectedError")
      .projection(
        "status.committed as committed",
        "status.started as started",
        "status.errorMessage contains 'does not satisfy Constraint' as isErr1",
        "status.errorMessage contains 'already exists with label' as isErr2",
        "labels(animal) as labels"
      )
      .transactionApply(batchSize, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
      .|.union()
      .|.|.create(createNodeWithProperties("animal", Seq("Animal", "Shark"), "{id: id}"))
      .|.|.unwind("rhsIds AS id")
      .|.|.argument()
      .|.create(createNodeWithProperties("animal", Seq("Animal", "Dog"), "{id: id}"))
      .|.unwind("lhsIds AS id")
      .|.argument()
      .input(variables = Seq("lhsIds", "rhsIds"))
      .build()

    val input = inputBatches.flatten.map(_.asInstanceOf[Array[Any]])
    val result = execute(query, runtime, inputValues(input: _*))

    val expected = inputBatches
      .flatMap { batch =>
        val batchShouldFail = batch.exists(row => row.exists(_.contains(0L)))
        if (batchShouldFail) {
          batch.toSeq.map { row =>
            Array[Any](row(0), row(1), null, null, false, true, true)
          }
        } else {
          batch.toSeq.flatMap { row =>
            val idAndLabel = row(0).map(id => id -> "Dog") ++ row(1).map(id => id -> "Shark")
            idAndLabel.map {
              case (id, label) =>
                Array[Any](row(0), row(1), id, Array("Animal", label), true, true, null)
            }
          }
        }
      }

    result should beColumns("lhsIds", "rhsIds", "id", "labels", "committed", "started", "expectedError")
      .withRows(inAnyOrder(expected))
  }

  test("no resource leaks with continuations on rhs") {
    val size = 10
    val relCount = givenGraph {
      val (as, bs) = bipartiteGraph(size, "A", "B", "R")
      val rIdGen = new AtomicInteger(0)
      for {
        a <- as
        rel <- a.getRelationships().asScala
      } {
        rel.setProperty("id", rIdGen.getAndIncrement())
      }
      Int.box(rIdGen.get())
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("committed")
      .projection("status.committed AS committed")
      .transactionApply(1, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
      .|.create(createNodeWithProperties("n", Seq("N"), "{p:sum}"))
      .|.aggregation(Seq(), Seq("sum(p) as sum"))
      .|.projection("1 / (x - r.id) AS p")
      .|.nonFuseable()
      .|.expandAll("(a)-[r]->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("a", "A", "x")
      .unwind(s"range(0, ${relCount - 1}) AS x")
      .argument()
      .build(readOnly = false)

    val result = execute(query, runtime)

    // then
    // Main assertion is that execute succeeds without exceptions
    val expectedRow = Array[Any](false)
    result should beColumns("committed")
      .withRows(inOrder(Range(0, relCount).map(_ => expectedRow)))
  }

  test("transaction apply with index seeks that are not used in all batches") {
    val nodes = givenGraph {
      nodeIndex("N")(_.withIndexType(IndexType.FULLTEXT).on("fullText").withName("fullTextIndex"))
      nodeIndex(IndexType.TEXT, "N", "text")
      nodeIndex(IndexType.POINT, "N", "point")
      nodeIndex(IndexType.RANGE, "N", "range")
      nodeIndex(IndexType.RANGE, "N", "compositeA", "compositeB")
      nodeIndex(IndexType.FULLTEXT, "N", "fullTextAndRange")
      nodeIndex(IndexType.RANGE, "N", "fullTextAndRange")

      nodePropertyGraph(
        nNodes = 1,
        properties = { case _ =>
          Map(
            "fullText" -> "value",
            "text" -> "value",
            "point" -> Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0),
            "range" -> 1,
            "compositeA" -> 1,
            "compositeB" -> 2,
            "fullTextAndRange" -> "value"
          )
        },
        labels = "N"
      )
    }

    val rowIn = Range.inclusive(0, 99).filter(_ => random.nextDouble() < 0.25)
    val batchSize = random.nextInt(20) + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "n1", "n2", "n3", "n4", "n5")
      .transactionApply(batchSize)
      .|.apply()
      .|.|.procedureCall("db.index.fulltext.queryNodes('fullTextIndex', 'value') YIELD node AS n5")
      .|.|.argument()
      .|.apply()
      .|.|.nodeIndexOperator("n4:N(text = 'value')", indexType = IndexType.TEXT)
      .|.apply()
      .|.|.nodeIndexOperator("n3:N(point = ???)", paramExpr = Some(point(1.0, 2.0)), indexType = IndexType.POINT)
      .|.apply()
      .|.|.nodeIndexOperator("n2:N(range = 1)", indexType = IndexType.RANGE)
      .|.apply()
      .|.|.nodeIndexOperator("n1:N(compositeA = 1, compositeB = 2)", indexType = IndexType.RANGE)
      .|.filter(s"i IN ${rowIn.mkString("[", ",", "]")}")
      .|.argument()
      .unwind("range(0, 99) AS i")
      .projection(
        "'value' AS fullText",
        "'value' AS text",
        "point({x:1.0, y:2.0}) AS point",
        "1 AS range",
        "1 AS compositeA",
        "2 AS compositeB",
        "'value' AS all"
      )
      .argument()
      .build(readOnly = false)

    val expected = rowIn.flatMap(i => nodes.map(n => Array[Any](i, n, n, n, n, n)))
    withClue(s"batchSize=$batchSize, rowIn=${rowIn.mkString("[", ",", "]")}\n") {
      execute(buildPlan(logicalQuery, runtime), readOnly = false) should beColumns("i", "n1", "n2", "n3", "n4", "n5")
        .withRows(inOrder(expected))
    }
  }

  private def executeAndConsume(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT]) = {
    val result = execute(logicalQuery, runtime)
    consume(result)
    result
  }

  protected def txAssertionProbe(assertion: InternalTransaction => Unit): Prober.Probe = {
    new Probe {
      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        withNewTx(assertion(_))
      }
    }
  }

  private def randomErrorBehaviour(): InTransactionsOnErrorBehaviour =
    randomAmong(Seq(OnErrorFail, OnErrorContinue, OnErrorBreak))
}

object TransactionApplyTestBase {

  case class ComplexRhsTestSetup(
    batchSize: Int,
    concurrency: TransactionConcurrency,
    iterations: Int,
    onError: InTransactionsOnErrorBehaviour,
    status: Option[String]
  )
}

/**
 * Tests transaction foreach in queries like.
 * 
 * .produceResult()
 * .transactionApply()
 * .|.create("(n {props})")
 * .|.unwind("randomProps AS props")
 * .input("randomProps")
 * 
 * With random:
 * 
 * - Number of input rows.
 * - Size of rhs (the unwind list)
 * - Failures
 * - Transaction batch size
 */
trait RandomisedTransactionApplyTests[CONTEXT <: RuntimeContext]
    extends CypherScalaCheckDrivenPropertyChecks { self: RuntimeTestSuite[CONTEXT] =>

  def sizeHint: Int

  // We don't allow `on error fail` and `report status`, semantically, but some runtimes
  // supports it and it makes it easier to test.
  test("should handle random failures with ON ERROR FAIL REPORT STATUS") {
    assume(runtime.name != "Pipelined")
    givenGraph {
      uniqueNodeIndex("N", "p")
      val node = runtimeTestSupport.tx.createNode(Label.label("N"))
      node.setProperty("p", 42)
    }

    forAll(genRandomTestSetup(sizeHint), minSuccessful(100)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "i2", "started", "committed", "errorMessage")
        .projection(
          "i AS i",
          "i2 AS i2",
          "status.started AS started",
          "status.committed AS committed",
          "status.errorMessage AS errorMessage"
        )
        .transactionApply(
          onErrorBehaviour = OnErrorFail,
          batchSize = setup.txBatchSize,
          maybeReportAs = Some("status")
        )
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.projection("i AS i", "i+1 as i2", "rhsRows AS rhsRows")
        .|.argument("i", "rhsRows")
        .input(variables = Seq("i", "rhsRows"))
        .build(readOnly = false)

      val failParams = VirtualValues.map(Array("p"), Array(Values.intValue(42)))
      val successParams = MapValue.EMPTY
      val input = new IteratorInputStream(setup.generateRows(successParams, failParams))

      val statsBefore = txStats()

      val expectedCommittedInnerTxs = setup.batches()
        .takeWhile(txBatch => !txBatch.exists(_.shouldFail) || txBatch.isEmpty)
        .size

      if (setup.shouldFail) {
        assertThrows[Exception](execute(query, runtime, input).awaitAll())
        val statsAfter = txStats()
        val expectedStats = statsBefore.add(TxStats(expectedCommittedInnerTxs + 1, expectedCommittedInnerTxs, 1))
        statsAfter shouldBe expectedStats
      } else {
        val runtimeResult = execute(query, runtime, input)

        val expected = setup.input
          .flatMap(row => row.rhsUnwind.map(rhs => Array[Any](row.i, row.i + 1, true, true, null)))

        val expectedNodes = setup.input.iterator
          .map(row => row.rhsUnwind.size)
          .sum

        runtimeResult should beColumns("i", "i2", "started", "committed", "errorMessage")
          .withRows(inOrder(expected))
          .withStatistics(
            nodesCreated = expectedNodes,
            labelsAdded = expectedNodes,
            transactionsStarted = expectedCommittedInnerTxs + 1,
            transactionsCommitted = expectedCommittedInnerTxs + 1
          )

        val statsAfter = txStats()
        val expectedStats = statsBefore.add(TxStats(expectedCommittedInnerTxs, expectedCommittedInnerTxs, 0))
        statsAfter shouldBe expectedStats
      }
    }
  }

  test("should handle random failures with ON ERROR FAIL") {
    givenGraph {
      uniqueNodeIndex("N", "p")
      val node = runtimeTestSupport.tx.createNode(Label.label("N"))
      node.setProperty("p", 42)
    }

    forAll(genRandomTestSetup(sizeHint), minSuccessful(100)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "i2")
        .transactionApply(
          batchSize = setup.txBatchSize,
          onErrorBehaviour = OnErrorFail
        )
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.projection("i AS i", "i+1 as i2", "rhsRows AS rhsRows")
        .|.argument("i", "rhsRows")
        .input(variables = Seq("i", "rhsRows"))
        .build(readOnly = false)

      val failParams = VirtualValues.map(Array("p"), Array(Values.intValue(42)))
      val successParams = MapValue.EMPTY
      val input = new IteratorInputStream(setup.generateRows(successParams, failParams))

      val statsBefore = txStats()

      val expectedCommittedInnerTxs = setup.batches()
        .takeWhile(txBatch => !txBatch.exists(_.shouldFail) || txBatch.isEmpty)
        .size

      if (setup.shouldFail) {
        assertThrows[Exception](execute(query, runtime, input).awaitAll())
        val statsAfter = txStats()
        val expectedStats = statsBefore.add(TxStats(expectedCommittedInnerTxs + 1, expectedCommittedInnerTxs, 1))
        statsAfter shouldBe expectedStats
      } else {
        val runtimeResult = execute(query, runtime, input)

        val expected = setup.input
          .flatMap(row => row.rhsUnwind.map(rhs => Array(row.i, row.i + 1)))

        val expectedNodes = setup.input.iterator
          .map(row => row.rhsUnwind.size)
          .sum

        runtimeResult should beColumns("i", "i2")
          .withRows(inOrder(expected))
          .withStatistics(
            nodesCreated = expectedNodes,
            labelsAdded = expectedNodes,
            transactionsStarted = expectedCommittedInnerTxs + 1,
            transactionsCommitted = expectedCommittedInnerTxs + 1
          )

        val statsAfter = txStats()
        val expectedStats = statsBefore.add(TxStats(expectedCommittedInnerTxs, expectedCommittedInnerTxs, 0))
        statsAfter shouldBe expectedStats
      }
    }
  }

  test("should handle random failures with ON ERROR BREAK") {
    givenGraph {
      uniqueNodeIndex("N", "p")
      val node = runtimeTestSupport.tx.createNode(Label.label("N"))
      node.setProperty("p", 42)
    }

    forAll(genRandomTestSetup(sizeHint), minSuccessful(100)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "i2", "started", "committed")
        .projection(
          "i AS i",
          "i2 AS i2",
          "status.started AS started",
          "status.committed AS committed",
          "status.errorMessage AS errorMessage"
        )
        .transactionApply(
          onErrorBehaviour = OnErrorBreak,
          batchSize = setup.txBatchSize,
          maybeReportAs = Some("status")
        )
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.projection("i AS i", "i+1 AS i2", "rhsRows AS rhsRows")
        .|.argument("i", "rhsRows")
        .input(variables = Seq("i", "rhsRows"))
        .build(readOnly = false)

      val failParams: AnyValue = VirtualValues.map(Array("p"), Array(Values.intValue(42)))
      val successParams: AnyValue = MapValue.EMPTY
      val input = new IteratorInputStream(setup.generateRows(successParams, failParams))

      val statsBefore = txStats()

      val expectedCommittedInnerTxs = setup.batches()
        .takeWhile(txBatch => !txBatch.exists(_.shouldFail) || txBatch.isEmpty)
        .size

      val expectedFailedInnerTxs = if (setup.shouldFail) 1 else 0

      val runtimeResult = execute(query, runtime, input)

      val expected = setup.batches().flatMap {
        var hasFailed = false
        batch => {
          if (hasFailed) {
            batch.map(row => Array[Any](row.i, null, false, false))
          } else if (batch.exists(_.shouldFail)) {
            hasFailed = true
            batch.map(row => Array[Any](row.i, null, true, false))
          } else {
            batch.flatMap(row => row.rhsUnwind.map(_ => Array[AnyVal](row.i, row.i + 1, true, true)))
          }
        }
      }

      val expectedNodes = setup.input.iterator
        .takeWhile(r => !r.shouldFail)
        .map(row => row.rhsUnwind.size)
        .sum

      runtimeResult should beColumns("i", "i2", "started", "committed")
        .withRows(inOrder(expected.toSeq))
        .withStatistics(
          nodesCreated = expectedNodes,
          labelsAdded = expectedNodes,
          transactionsStarted = expectedCommittedInnerTxs + 1 + expectedFailedInnerTxs,
          transactionsCommitted = expectedCommittedInnerTxs + 1,
          transactionsRolledBack = expectedFailedInnerTxs
        )

      val statsAfter = txStats()
      val expectedStats = statsBefore.add(TxStats(
        expectedCommittedInnerTxs + expectedFailedInnerTxs,
        expectedCommittedInnerTxs,
        expectedFailedInnerTxs
      ))
      statsAfter shouldBe expectedStats
    }
  }

  test("should handle random failures with ON ERROR CONTINUE") {
    givenGraph {
      uniqueNodeIndex("N", "p")
      val node = runtimeTestSupport.tx.createNode(Label.label("N"))
      node.setProperty("p", 42)
    }

    // TODO Are we leaking memory, got failure when I turned up minSuccessful?
    forAll(genRandomTestSetup(sizeHint), minSuccessful(50)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "i2", "started", "committed")
        .projection(
          "i AS i",
          "i2 AS i2",
          "status.started AS started",
          "status.committed AS committed",
          "status.errorMessage AS errorMessage"
        )
        .transactionApply(
          onErrorBehaviour = OnErrorContinue,
          batchSize = setup.txBatchSize,
          maybeReportAs = Some("status")
        )
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.projection("i AS i", "i+1 AS i2", "rhsRows AS rhsRows")
        .|.argument("i", "rhsRows")
        .input(variables = Seq("i", "rhsRows"))
        .build(readOnly = false)

      val failParams: AnyValue = VirtualValues.map(Array("p"), Array(Values.intValue(42)))
      val successParams: AnyValue = MapValue.EMPTY
      val input = new IteratorInputStream(setup.generateRows(successParams, failParams))

      val statsBefore = txStats()

      val expectedCommittedInnerTxs = setup.batches()
        .count(txBatch => !txBatch.exists(_.shouldFail) || txBatch.isEmpty)

      val expectedFailedInnerTxs = setup.batches().size - expectedCommittedInnerTxs

      val runtimeResult = execute(query, runtime, input)

      val expected = setup.batches().flatMap {
        case batch if batch.exists(_.shouldFail) =>
          batch.map(r => Array[Any](r.i, null, true, false))
        case batch =>
          batch.flatMap(r => r.rhsUnwind.map(_ => Array[Any](r.i, r.i + 1, true, true)))
      }

      val expectedNodes = setup.input.iterator
        .filterNot(_.shouldFail)
        .map(row => row.rhsUnwind.size)
        .sum

      runtimeResult should beColumns("i", "i2", "started", "committed")
        .withRows(inOrder(expected.toSeq))
        .withStatistics(
          nodesCreated = expectedNodes,
          labelsAdded = expectedNodes,
          transactionsStarted = expectedCommittedInnerTxs + 1 + expectedFailedInnerTxs,
          transactionsCommitted = expectedCommittedInnerTxs + 1,
          transactionsRolledBack = expectedFailedInnerTxs
        )

      val statsAfter = txStats()
      val expectedStats = statsBefore.add(TxStats(
        expectedCommittedInnerTxs + expectedFailedInnerTxs,
        expectedCommittedInnerTxs,
        expectedFailedInnerTxs
      ))
      statsAfter shouldBe expectedStats
    }
  }

  private def txStats(): TxStats = {
    val stats = graphDb.asInstanceOf[GraphDatabaseFacade].getDependencyResolver
      .resolveDependency(classOf[DatabaseTransactionStats])
    TxStats(
      stats.getNumberOfStartedTransactions,
      stats.getNumberOfCommittedTransactions,
      stats.getNumberOfRolledBackTransactions
    )
  }
}
