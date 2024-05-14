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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Serial
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExtendedQueryStatistics
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RowsMatcher
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.internal.helpers.MathUtil
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.test.Barrier
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValue
import org.scalatest.LoneElement

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class ConcurrentTransactionApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int,
  concurrency: TransactionConcurrency = TransactionConcurrency.Serial
) extends RuntimeTestSuite[CONTEXT](edition, runtime, testPlanCombinationRewriterHints = Set(NoRewrites))
    with SideEffectingInputStream[CONTEXT]
    with LoneElement
    with TimeLimitedCypherTest {

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

  def inExpectedOrder(rows: Iterable[Array[_]]): RowsMatcher = inOrder(rows, concurrency != Serial)

  test("batchSize 0") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(0, concurrency = concurrency)
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
      .transactionApply(-1, concurrency = concurrency)
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
      .transactionApply(-1, concurrency = concurrency)
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
      .transactionApply(concurrency = concurrency)
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
      .transactionApply(3, concurrency = concurrency)
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
      .transactionApply(3, concurrency = concurrency)
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
    val nRows = 10
    val nBatchSize = 3
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .transactionApply(nBatchSize, concurrency = concurrency)
      .|.aggregation(Seq.empty, Seq("count(i) AS c"))
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind(s"range(1, $nRows) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("c")
      .withRows(singleColumn(1 to nRows))
      .withStatistics(
        nodesCreated = nRows,
        labelsAdded = nRows,
        propertiesSet = nRows,
        transactionsStarted = MathUtil.ceil(nRows, nBatchSize) + 1,
        transactionsCommitted = MathUtil.ceil(nRows, nBatchSize) + 1
      )
  }

  test("should work with grouping aggregation on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .transactionApply(3, concurrency = concurrency)
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
      .transactionApply(3, concurrency = concurrency, onErrorBehaviour = OnErrorContinue)
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
      .transactionApply(batchSize = batchSize, concurrency = TransactionConcurrency.Concurrent(1))
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
    val expectedCommits = (nodeCountA + (nodeCountA % batchSize)) / batchSize + 1 // +1 is for outer transaction
    runtimeResult should beColumns("x")
      .withPartialStatistics(
        transactionsStarted = expectedCommits,
        transactionsCommitted = expectedCommits
      )
  }

  test("data from returning subqueries should be accessible") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .setProperty("n", "prop", "17")
      .transactionApply(concurrency = concurrency)
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
      .transactionApply(3, concurrency = concurrency)
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

    val numberOfTransactions = new AtomicInteger(0)
    val txCountingProbe = countingProbe(numberOfTransactions)
    val txCountAssertionProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual numberOfTransactions.get()
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .prober(txCountAssertionProbe)
      .eager()
      .transactionApply(1, concurrency)
      .|.prober(txCountingProbe)
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
    val numBatches = MathUtil.ceil(numberOfIterations, batchSize)
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply(batchSize, concurrency = concurrency)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("n").withRows(rowCount(numberOfIterations)).withStatistics(
      nodesCreated = numberOfIterations,
      labelsAdded = numberOfIterations,
      transactionsStarted = numBatches + 1,
      transactionsCommitted = numBatches + 1
    )

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("statistics should report data creation from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("n.prop AS prop")
      .transactionApply(1, concurrency = concurrency)
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
      .transactionApply(3, concurrency = concurrency)
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
      .transactionApply(concurrency = concurrency)
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
      .transactionApply(batchSize, concurrency = concurrency)
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
      .transactionApply(batchSize, concurrency = concurrency)
      .|.sort("y ASC")
      .|.create(createNode("n"))
      .|.eager()
      .|.allNodeScan("y", "x")
      .filter(s"x <> $sizeHint")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime, input)

    val expectedTxCommitted = inputValsToPassThrough.length / batchSize + 1
    // then
    runtimeResult should beColumns("x").withPartialStatistics(
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
      .transactionApply(concurrency = concurrency)
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
      .withRows(inExpectedOrder(
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
      .transactionApply(concurrency = concurrency)
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
      .withRows(inExpectedOrder(
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
      .transactionApply(1, concurrency, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
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

  test("on error continue behaviour") {
    val batchSize = 5
    val input = Range.inclusive(1, 100)
    val inputRows = input.map { x => Array[Any](x) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xs", "x", "committed", "started", "error", "bang")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .transactionApply(batchSize, concurrency, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("status"))
      .|.projection("1 / (x - 7) as bang")
      .|.limit(3)
      .|.unwind("xs as x")
      .|.argument()
      .input(variables = Seq("xs"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))

    val expected = input
      .grouped(batchSize)
      .flatMap { batch =>
        val committed = !batch.exists(_ == 7)
        if (committed) {
          batch.map { x =>
            Array[Any](x, x, true, true, null, 1 / (x - 7))
          }
        } else {
          batch.map { x =>
            Array[Any](x, null, false, true, "/ by zero", null)
          }
        }
      }
      .toSeq

    runtimeResult should beColumns("xs", "x", "committed", "started", "error", "bang")
      .withRows(expected)
  }

  test("on error break behaviour") {
    val batchSize = 5
    val input = Range.inclusive(1, 100)
    val inputRows = input.map { x => Array[Any](x) }

    val startBarrier = new Barrier.Control
    val finishBarrier = new Barrier.Control
    var firstErrorReached: Boolean = false

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("committed", "started", "error")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .prober((row, _) => {
        if (!firstErrorReached) {
          val cypherRow = row.asInstanceOf[CypherRow]
          val status = cypherRow.getByName("status")
          if (status != null) {
            val errorValue = status.asInstanceOf[MapValue].get("errorMessage")
            if (errorValue != Values.NO_VALUE) {
              firstErrorReached = true
              finishBarrier.reached()
            }
          }
        }
      })
      .transactionApply(batchSize, concurrency, onErrorBehaviour = OnErrorBreak, maybeReportAs = Some("status"))
      .|.prober((row, _) => {
        val cypherRow = row.asInstanceOf[CypherRow]
        val x = ValueUtils.asLongValue(cypherRow.getByName("x")).value()
        val shouldBeInterrupted = x == 11L
        val shouldThrow = x == 16L
        if (shouldThrow) {
          startBarrier.reached()
          throw new CypherTypeException("I hate the number you chose")
        }
        if (shouldBeInterrupted) {
          startBarrier.await()
          startBarrier.release()
          finishBarrier.await()
          finishBarrier.release()
        }
      })
      .|.unwind("xs as x")
      .|.argument()
      .input(variables = Seq("xs"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult.runtimeResult.consumeAll()

    // then
    val queryStatistics = runtimeResult.runtimeResult.queryStatistics().asInstanceOf[ExtendedQueryStatistics]
    val expectedCommitted = List.fill(queryStatistics.getTransactionsCommitted * batchSize)(Array(true, true, null))
    val expectedFailed = List.fill(batchSize)(Array(false, true, "I hate the number you chose"))
    val expectedInterrupted = List.fill((queryStatistics.getTransactionsRolledBack - 1) * batchSize)(Array(
      false,
      true,
      "The batch was interrupted and the transaction was rolled back because another batch failed"
    ))
    val expectedNotStarted =
      List.fill(input.size - queryStatistics.getTransactionsStarted * batchSize)(Array(false, false, null))
    val expected = expectedCommitted ++ expectedFailed ++ expectedInterrupted ++ expectedNotStarted

    assert(expectedFailed.nonEmpty, "No transactions failed")
    assert(expectedInterrupted.nonEmpty, "No transactions were interrupted")
    runtimeResult should beColumns("committed", "started", "error").withRows(inAnyOrder(expected))
  }

  test("on error fail behaviour") {
    val batchSize = 5
    val input = Range.inclusive(1, 100)
    val inputRows = input.map { x => Array[Any](x) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xs", "x", "committed", "started", "error", "bang")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .transactionApply(batchSize, concurrency, onErrorBehaviour = OnErrorFail, maybeReportAs = Some("status"))
      .|.projection("1 / (x - 7) as bang")
      .|.unwind("xs as x")
      .|.argument()
      .input(variables = Seq("xs"))
      .build()

    // then
    val exception = intercept[StatusWrapCypherException] {
      consume(execute(logicalQuery, runtime, inputValues(inputRows: _*)))
    }
    exception.getMessage should include("/ by zero")
  }

  protected def txAssertionProbe(assertion: InternalTransaction => Unit): Prober.Probe = {
    new Probe {
      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        withNewTx(assertion(_))
      }
    }
  }

  protected def countingProbe(atomicIncr: AtomicInteger): Prober.Probe = {
    new Probe {
      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        atomicIncr.getAndAdd(1)
      }
    }
  }
}
