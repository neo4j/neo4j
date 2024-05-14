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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.traversal.Paths
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.InternalLogProvider

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class ConcurrentTransactionForeachTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int,
  concurrency: TransactionConcurrency = TransactionConcurrency.Serial
) extends RuntimeTestSuite[CONTEXT](edition, runtime, testPlanCombinationRewriterHints = Set(NoRewrites))
    with SideEffectingInputStream[CONTEXT]
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

  test("batchSize 0") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(0, concurrency = concurrency)
      .|.emptyResult()
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
      .transactionForeach(-1, concurrency = concurrency)
      .|.emptyResult()
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
      .transactionForeach(-1, concurrency = concurrency)
      .|.emptyResult()
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

  test("should create data from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency, onErrorBehaviour = OnErrorContinue)
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe 2
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
      .transactionForeach(batchSize = batchSize, concurrency = concurrency)
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
    val expectedRhsCount = (nodeCountB + nodeCountC) * (Math.pow(2, nodeCountA).toInt - 1)
    val expectedCommits = (nodeCountA + (nodeCountA % batchSize)) / batchSize + 1 // +1 is for outer transaction
    runtimeResult should beColumns("x")
      .withPartialStatistics(
        transactionsStarted = expectedCommits,
        transactionsCommitted = expectedCommits
      )
  }

  test("should create data in different transactions when using transactionForeach") {
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
      .produceResults()
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions in batches when using transactionForeach") {
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
      .produceResults()
      .transactionForeach(batchSize, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions in one batch when using transactionForeach") {
    val numberOfIterations = 30
    val batchSize = 70
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    val probe = queryStatisticsProbe(queryStatistics => {
      queryStatistics.getNodesCreated shouldEqual 0
      queryStatistics.getLabelsAdded shouldEqual 0
      Iterables.count(tx.getAllNodes) shouldEqual 0
    })

    val txProbe = txAssertionProbe(tx => Iterables.count(tx.getAllNodes) shouldEqual 0)

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach(batchSize, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.argument()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe numberOfIterations
  }

  test("should create data in different transactions when using transactionForeach and see previous changes") {
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
      .produceResults()
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test(
    "should create data in different transactions in batches when using transactionForeach and see previous changes"
  ) {
    val numberOfIterations = 8
    val batchSize = 3
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
      queryStatistics.getNodesCreated shouldEqual nodeCount - _nodeCount
      queryStatistics.getLabelsAdded shouldEqual nodeCount - _nodeCount
    })

    var committedCount = 1L
    var inputRow = 0L
    var rhsRowsRemaining = Math.pow(2, inputRow).toLong
    val txProbe = txAssertionProbe(tx => {
      Iterables.count(tx.getAllNodes) shouldEqual committedCount
      rhsRowsRemaining -= 1
      if (rhsRowsRemaining == 0) {
        inputRow += 1
        rhsRowsRemaining = Math.pow(2, inputRow).toLong
        if (inputRow % batchSize == 0) {
          committedCount = Math.pow(2, inputRow).toLong
        }
      }
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach(batchSize, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test(
    "should create data in different transactions when using transactionForeach with index and see previous changes"
  ) {
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
      .produceResults()
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNodeWithProperties("b", Seq("Label"), "{prop: 2}"))
      .|.nodeIndexOperator("a:Label(prop=2)")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, numberOfIterations)
  }

  test(
    "should create data in different transactions when using transactionForeach and see previous changes (also from other transactionForeach)"
  ) {
    val numberOfIterations = 4
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

    // assertion assumes scheduling/execution order
    var committedCount = 1L
    var rhsRowsRemaining = 1L // initial node count
    val txProbe = txAssertionProbe(tx => {
      val actual = Iterables.count(tx.getAllNodes)
      actual shouldEqual committedCount
      rhsRowsRemaining -= 1
      if (rhsRowsRemaining == 0) {
        committedCount *= 2
        rhsRowsRemaining = committedCount
      }
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .eager()
      .transactionForeach(1)
      .|.emptyResult()
      .|.prober(txProbe)
      .|.prober(probe)
      .|.create(createNode("n", "N"))
      .|.nodeByLabelScan("y", "N")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())

    consume(runtimeResult)

    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe Math.pow(2, 2 * numberOfIterations)
  }

  test("should pass LHS slot configuration onto downstream pipeline") {
    // This test was added to recreate a bug encountered by customers
    // See: https://trello.com/c/O7LN98Bd
    val inputRows = Seq(Array[Any](42))

    givenGraph {
      nodeGraph(1, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1)
      .|.argument()
      .distinct("x AS x")
      .transactionForeach(1)
      .|.allNodeScan("n")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val runtimeResult = execute(query, runtime, inputValues(inputRows: _*).stream())
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("statistics should report data creation from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to 10))
      .withStatistics(nodesCreated = 10, labelsAdded = 10, transactionsCommitted = 11, transactionsStarted = 11)
  }

  test("statistics should report data creation from subqueries in batches") {
    val batchSize = 3
    val rangeSize = 10
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(batchSize, concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind(s"range(1, $rangeSize) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    val expectedTransactionCount = Math.ceil(rangeSize / batchSize.toDouble).toInt
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to rangeSize))
      .withStatistics(
        nodesCreated = rangeSize,
        labelsAdded = rangeSize,
        transactionsCommitted = expectedTransactionCount + 1,
        transactionsStarted = expectedTransactionCount + 1
      )
  }

  test("statistics should report data creation from subqueries while profiling") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(Seq(1, 2)))
      .withStatistics(nodesCreated = 2, labelsAdded = 2, transactionsCommitted = 2, transactionsStarted = 2)
  }

  test("Should not throw exception when reading before call subquery") {
    givenGraph {
      nodeGraph(sizeHint) ++ nodeGraph(2, "X")
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.allNodeScan("m")
      .nodeByLabelScan("x", "X")
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime)
    noException should be thrownBy consume(runtimeResult)
  }

  test("Make sure transaction state is empty before opening inner transaction") {
    givenGraph {
      nodeGraph(sizeHint)
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
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
    givenGraph {
      nodeIndex("N", "prop")
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.nodeIndexOperator("n:N(prop)")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns().withNoRows()
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Index seeks should see data created in same transaction") {
    givenGraph {
      nodeIndex("N", "prop")
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
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
    runtimeResult should beColumns().withNoRows()
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (
      (0 until 10) ++ // nodes from given
        (100 to 102) ++ // nodes from LHS of Union
        (11 to 15 by 2) // nodes from RHS of Union
    )
  }

  test("Label scans should see data created from previous transactions") {
    givenGraph {
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.create(createNodeWithProperties("newN", Seq("N"), "{prop: c}"))
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.nodeByLabelScan("n", "N")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns().withNoRows()
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Label scans should see data created in same transaction") {
    givenGraph {
      nodePropertyGraph(10, { case i => Map("prop" -> i) }, "N")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
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
    runtimeResult should beColumns().withNoRows()
    val nNodes = tx.findNodes(label("N")).asScala.toList
    nNodes.map(_.getProperty("prop")) should contain theSameElementsAs (
      (0 until 10) ++ // nodes from given
        (100 to 102) ++ // nodes from LHS of Union
        (11 to 15 by 2) // nodes from RHS of Union
    )
  }

  test("Relationship type scans should see data created from previous transactions") {
    givenGraph {
      val nodes = nodeGraph(10)
      connectWithProperties(nodes, nodes.indices.map(i => (i, i, "R", Map("prop" -> i))))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.create(
        createNode("n"),
        createNode("m"),
        createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: c}"))
      )
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.relationshipTypeScan("(a)-[r:R]->(b)")
      .unwind("[1, 2, 3] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns().withNoRows()
    val nRels = tx.findRelationships(RelationshipType.withName("R")).asScala.toList
    nRels.map(_.getProperty("prop")) should contain theSameElementsAs (0 until (10 + 3))
  }

  test("Relationship type scans should see data created in same transaction") {
    givenGraph {
      val nodes = nodeGraph(10)
      connectWithProperties(nodes, nodes.indices.map(i => (i, i, "R", Map("prop" -> i))))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(concurrency = concurrency)
      .|.union()
      .|.|.emptyResult()
      .|.|.create(
        createNode("n"),
        createNode("m"),
        createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: c}"))
      )
      .|.|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.|.relationshipTypeScan("(a)-[r:R]->(b)")
      .|.create(
        createNode("n"),
        createNode("m"),
        createRelationship("newR", "n", "R", "m", OUTGOING, Some("{prop: x}"))
      )
      .|.argument("x")
      .unwind("[100, 101, 102] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns().withNoRows()
    val nRels = tx.findRelationships(RelationshipType.withName("R")).asScala.toList
    nRels.map(_.getProperty("prop")) should contain theSameElementsAs (
      (0 until 10) ++ // rels from given
        (100 to 102) ++ // rels from LHS of Union
        (11 to 15 by 2) // rels from RHS of Union
    )
  }

  test("should allow node entity values as params") {
    val nodes = givenGraph {
      val n = runtimeTestSupport.tx.createNode()
      n.setProperty("prop", 1L)
      val m = runtimeTestSupport.tx.createNode()
      m.setProperty("prop", 1L)
      Seq(m, n)
    }

    var x = 0
    val probe = txAssertionProbe(tx => {
      x match {
        case 0 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 1L)
        case 1 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 2L)
      }
      x += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("m.prop AS prop")
      .apply()
      .|.allNodeScan("m")
      .eager()
      .prober(probe) // pipelined: probe placement still depends on lazy scheduling order
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.setProperty("n", "prop", "2")
      .|.argument("n")
      .input(variables = Seq("n"))
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime, inputStream = inputValues(nodes.map(n => Array[Any](n)): _*).stream())

    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(2L, 2L, 2L, 2L)))
  }

  test("should allow relationship entity values as params") {
    val relationships = givenGraph {
      val n = runtimeTestSupport.tx.createNode()
      val m = runtimeTestSupport.tx.createNode()
      val r = n.createRelationshipTo(m, RelationshipType.withName("R"))
      r.setProperty("prop", 1L)
      val s = n.createRelationshipTo(m, RelationshipType.withName("R"))
      s.setProperty("prop", 1L)
      Seq(s, r)
    }

    var x = 0
    val probe = txAssertionProbe(tx => {
      x match {
        case 0 =>
          checkExternalAndRuntimeRelationships(tx, runtimeTestSupport, 1L)
        case 1 =>
          checkExternalAndRuntimeRelationships(tx, runtimeTestSupport, 2L)
      }
      x += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("s.prop AS prop")
      .apply()
      .|.relationshipTypeScan("(a)-[s:R]->(b)")
      .eager()
      .prober(probe) // pipelined: probe placement still depends on lazy scheduling order
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.setProperty("r", "prop", "2")
      .|.argument("r")
      .input(variables = Seq("r"))
      .build(readOnly = false)

    val runtimeResult =
      execute(query, runtime, inputStream = inputValues(relationships.map(r => Array[Any](r)): _*).stream())

    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(2L, 2L, 2L, 2L)))
  }

  test("should allow path entity values as params") {
    val paths = givenGraph {
      val n = runtimeTestSupport.tx.createNode()
      n.setProperty("prop", 1L)
      val p = Paths.singleNodePath(n)
      val m = runtimeTestSupport.tx.createNode()
      m.setProperty("prop", 1L)
      val q = Paths.singleNodePath(m)
      Seq(q, p)
    }

    var x = 0
    val probe = txAssertionProbe(tx => {
      x match {
        case 0 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 1L)
        case 1 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 2L)
      }
      x += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("m.prop AS prop")
      .apply()
      .|.allNodeScan("m")
      .eager()
      .prober(probe) // pipelined: probe placement still depends on lazy scheduling order
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.setProperty("n", "prop", "2")
      .|.unwind("nodes(p) AS n")
      .|.argument("p")
      .input(variables = Seq("p"))
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime, inputStream = inputValues(paths.map(p => Array[Any](p)): _*).stream())

    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(2L, 2L, 2L, 2L)))
  }

  test("should allow lists of node entity values as params") {
    val nodeRows = givenGraph {
      val n = runtimeTestSupport.tx.createNode()
      n.setProperty("prop", 1L)
      val m = runtimeTestSupport.tx.createNode()
      m.setProperty("prop", 1L)
      Seq(Array[Any](Array(m)), Array[Any](Array(n)))
    }

    var x = 0
    val probe = txAssertionProbe(tx => {
      x match {
        case 0 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 1L)
        case 1 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 2L)
      }
      x += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("m.prop AS prop")
      .apply()
      .|.allNodeScan("m")
      .eager()
      .prober(probe) // pipelined: probe placement still depends on lazy scheduling order
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.setProperty("n", "prop", "2")
      .|.unwind("l AS n")
      .|.argument("l")
      .input(variables = Seq("l"))
      .build(readOnly = false)

    val runtimeResult = execute(query, runtime, inputStream = inputValues(nodeRows: _*).stream())

    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(2L, 2L, 2L, 2L)))
  }

  test("should allow maps of node entity values as params") {
    val nodes = givenGraph {
      val n = runtimeTestSupport.tx.createNode()
      n.setProperty("prop", 1L)
      val m = runtimeTestSupport.tx.createNode()
      m.setProperty("prop", 1L)
      Seq(m, n)
    }

    var x = 0
    val probe = txAssertionProbe(tx => {
      x match {
        case 0 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 1L)
        case 1 =>
          checkExternalAndRuntimeNodes(tx, runtimeTestSupport, 2L)
      }
      x += 1
    })

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("o.prop AS prop")
      .apply()
      .|.allNodeScan("o")
      .eager()
      .prober(probe) // pipelined: probe placement still depends on lazy scheduling order
      .transactionForeach(1, concurrency = concurrency)
      .|.emptyResult()
      .|.setProperty("n", "prop", "2")
      .|.projection("m.n AS n")
      .|.argument("m")
      .input(variables = Seq("m"))
      .build(readOnly = false)

    val runtimeResult = execute(
      query,
      runtime,
      inputStream = inputValues(nodes.map(n => Array[Any](java.util.Map.of("n", n))): _*).stream()
    )

    runtimeResult should beColumns("prop")
      .withRows(singleColumn(Seq(2L, 2L, 2L, 2L)))
  }

  test("should work with grouping aggregation on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(3, concurrency = concurrency)
      .|.aggregation(Seq("1 AS group"), Seq("count(i) AS c"))
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    runtimeResult should beColumns("x")
      .withRows(singleColumn(1 to 10))
      .withStatistics(
        nodesCreated = 10,
        labelsAdded = 10,
        propertiesSet = 10,
        transactionsCommitted = 5,
        transactionsStarted = 5
      )
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
      .transactionForeach(
        1,
        concurrency = concurrency,
        onErrorBehaviour = OnErrorContinue,
        maybeReportAs = Some("status")
      )
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

  private def checkExternalAndRuntimeNodes(
    externalTx: InternalTransaction,
    runtimeTestSupport: RuntimeTestSupport[CONTEXT],
    firstItemVal: Long
  ): Unit = {
    val extAllNodes = externalTx.getAllNodes
    try {
      val runtimeAllNodes = runtimeTestSupport.tx.getAllNodes
      try {
        val externallyVisible = extAllNodes.asScala.map(_.getProperty("prop")).toList
        val parentVisible = runtimeAllNodes.asScala.map(_.getProperty("prop")).toList
        externallyVisible shouldEqual List(firstItemVal, 2L)
        parentVisible shouldEqual List(firstItemVal, 2L)
      } finally {
        runtimeAllNodes.close()
      }
    } finally {
      extAllNodes.close()
    }
  }

  private def checkExternalAndRuntimeRelationships(
    externalTx: InternalTransaction,
    runtimeTestSupport: RuntimeTestSupport[CONTEXT],
    firstItemVal: Long
  ): Unit = {
    val extAllRels = externalTx.getAllRelationships
    try {
      val runtimeAllRels = runtimeTestSupport.tx.getAllRelationships
      try {
        val externallyVisible = extAllRels.asScala.map(_.getProperty("prop")).toList
        val parentVisible = runtimeAllRels.asScala.map(_.getProperty("prop")).toList
        externallyVisible shouldEqual List(firstItemVal, 2L)
        parentVisible shouldEqual List(firstItemVal, 2L)
      } finally {
        runtimeAllRels.close()
      }
    } finally {
      extAllRels.close()
    }
  }

  protected def txAssertionProbe(assertion: InternalTransaction => Unit): Prober.Probe = {
    new Probe {
      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        withNewTx(assertion(_))
      }
    }
  }
}
