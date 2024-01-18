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
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.IteratorInputStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.QueryStatisticsProbe
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.cypher.internal.runtime.spec.tests.RandomisedTransactionForEachTests.genRandomTestSetup
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.traversal.Paths
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import org.scalacheck.Gen
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class TransactionForeachTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with SideEffectingInputStream[CONTEXT]
    with RandomValuesTestSupport
    with RandomisedTransactionForEachTests[CONTEXT] {

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
      .transactionForeach(0, randomErrorBehavior())
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
      .transactionForeach(-1, randomErrorBehavior())
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
      .transactionForeach(-1, randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = OnErrorContinue)
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
    val nodeCountB = 3
    val nodeCountC = 5
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
      .transactionForeach(batchSize = batchSize, randomErrorBehavior())
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
      .withSingleRow(nodeCountA)
      .withStatistics(
        nodesCreated = expectedRhsCount,
        labelsAdded = expectedRhsCount,
        transactionsCommitted = expectedCommits,
        transactionsStarted = expectedCommits
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(batchSize, randomErrorBehavior())
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
      .transactionForeach(batchSize, randomErrorBehavior())
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

  test("should not create data in different transactions when using subqueryForeach") {
    val numberOfIterations = 30
    val inputRows = (0 until numberOfIterations).map { i =>
      Array[Any](i.toLong)
    }

    var nodesCreated = 0
    val probe = queryStatisticsProbe(queryStatistics => {
      nodesCreated += 1
      queryStatistics.getNodesCreated shouldEqual nodesCreated
      queryStatistics.getLabelsAdded shouldEqual nodesCreated
    })

    val txProbe = txAssertionProbe(tx => Iterables.count(tx.getAllNodes) shouldEqual 0)

    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .subqueryForeach()
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(batchSize, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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

  test("statistics should report data creation from subqueries") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(batchSize, randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(onErrorBehaviour = randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(1, randomErrorBehavior())
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
      .transactionForeach(3, randomErrorBehavior())
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

  test("should fail on downstream errors") {
    val rows = sizeHint / 4
    val batchSize = random.nextInt(rows + 20) + 1
    val failAtRow = random.nextInt(rows)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "status", "bang")
      .projection(s"1 / (x - $failAtRow) as bang")
      .transactionForeach(batchSize, randomErrorHandlingBehavior(), Some("status"))
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
      .produceResults("x", "status", "bang")
      .transactionForeach(batchSize, randomErrorHandlingBehavior(), Some("status"))
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
      .produceResults("x", "y", "committed", "started", "error")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .transactionForeach(batchSize, OnErrorContinue, Some("status"))
      .|.selectOrSemiApply("y / y + y = 2")
      .|.|.filter("x > 0.5")
      .|.|.argument()
      .|.argument()
      .input(variables = Seq("x", "y"))
      .build()

    // then
    val inputRows = input.map { case (x, y) => Array[Any](x, y) }
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))

    val expected = input
      .grouped(batchSize)
      .flatMap { batch =>
        val committed = !batch.map(_._2).contains(0)
        val started = true
        val error = if (committed) null else "/ by zero"
        batch.map(x => Array[Any](x._1, x._2, committed, started, error))
      }
      .toSeq

    runtimeResult should beColumns("x", "y", "committed", "started", "error")
      .withRows(expected)
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
      .produceResults("xs", "committed", "started", "error")
      .projection("status.committed as committed", "status.started as started", "status.errorMessage as error")
      .limit(input.size / 2)
      .transactionForeach(batchSize, OnErrorContinue, Some("status"))
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
        val started = true
        val error = if (committed) null else "/ by zero"
        batch.map(x => Array[Any](x, committed, started, error))
      }
      .take(input.size / 2)
      .toSeq

    runtimeResult should beColumns("xs", "committed", "started", "error")
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
      .produceResults("props", "committed", "started", "error")
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
      .transactionForeach(batchSize, errorBehaviour, if (status) Some("status") else None)
      .|.create(createNodeWithProperties("d", Seq("Dog"), "props"))
      .|.argument()
      .input(variables = Seq("props"))
      .build()

    (input, batchSize, query)
  }

  // If this test fails, the other tests that use the same setup might have lost coverage
  // and might need a new plan that fails during commit (or this assertion needs to update)
  test("test integrity, handle errors during commit") {
    assume(runtime.name != "interpreted")
    val (_, _, query) = setupCommitErrorHandlingTest(OnErrorFail, status = false)

    // Test integrity
    val failingInput = Array[Any](MapValue.EMPTY)
    val exception = intercept[ConstraintViolationException] {
      consume(execute(query, runtime, new InputValues().and(failingInput)))
    }
    exception.getMessage should include("with label `Dog` must have the property `tail`")
    withClue(
      s"""
         |Stacktrace did not include commit from kernel package:
         |${ExceptionUtils.getStackTrace(exception)}
         |""".stripMargin
    ) {
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
          batch.map(r => Array[Any](r, true, true, null))
        else
          batch.map(r => Array[Any](r, false, true, "with label `Dog` must have the property `tail`"))
      }

    result should beColumns("props", "committed", "started", "error")
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
        if (shouldFail) batch.map(r => Array[Any](r, false, true, "with label `Dog` must have the property `tail`"))
        else if (hasFailedPreviousBatch) batch.map(r => Array[Any](r, false, false, null))
        else batch.map(r => Array[Any](r, true, true, null))
      }

    result should beColumns("props", "committed", "started", "error")
      .withRows(inOrder(expected.toSeq))
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
      .transactionForeach(1, OnErrorContinue, Some("status"))
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

  private def randomErrorBehavior(): InTransactionsOnErrorBehaviour =
    randomAmong(Seq(OnErrorFail, OnErrorContinue, OnErrorBreak))

  private def randomErrorHandlingBehavior(): InTransactionsOnErrorBehaviour =
    randomAmong(Seq(OnErrorContinue, OnErrorBreak))
}

/**
 * Tests transaction apply in queries like.
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
trait RandomisedTransactionForEachTests[CONTEXT <: RuntimeContext]
    extends CypherScalaCheckDrivenPropertyChecks { self: RuntimeTestSuite[CONTEXT] =>

  def sizeHint: Int

  test("should handle random failures with ON ERROR FAIL") {
    givenGraph {
      uniqueNodeIndex("N", "p")
      val node = runtimeTestSupport.tx.createNode(Label.label("N"))
      node.setProperty("p", 42)
    }

    forAll(genRandomTestSetup(sizeHint), minSuccessful(50)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i")
        .transactionForeach(
          onErrorBehaviour = OnErrorFail,
          batchSize = setup.txBatchSize,
          maybeReportAs = None
        )
        .|.emptyResult()
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.argument("rhsRows")
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

        val expected = setup.input.map(row => Array(row.i))

        val expectedNodes = setup.input.iterator
          .map(row => row.rhsUnwind.size)
          .sum

        runtimeResult should beColumns("i")
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

    forAll(genRandomTestSetup(sizeHint), minSuccessful(50)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "started", "committed")
        .projection(
          "i AS i",
          "status.started AS started",
          "status.committed AS committed",
          "status.errorMessage AS errorMessage"
        )
        .transactionForeach(
          onErrorBehaviour = OnErrorBreak,
          batchSize = setup.txBatchSize,
          maybeReportAs = Some("status")
        )
        .|.emptyResult()
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.argument("rhsRows")
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
            batch.map(row => Array[AnyVal](row.i, false, false))
          } else if (batch.exists(_.shouldFail)) {
            hasFailed = true
            batch.map(row => Array[AnyVal](row.i, true, false))
          } else {
            batch.map(row => Array[AnyVal](row.i, true, true))
          }
        }
      }

      val expectedNodes = setup.input.iterator
        .takeWhile(r => !r.shouldFail)
        .map(row => row.rhsUnwind.size)
        .sum

      runtimeResult should beColumns("i", "started", "committed")
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

    forAll(genRandomTestSetup(sizeHint), minSuccessful(50)) { setup =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("i", "started", "committed")
        .projection(
          "i AS i",
          "status.started AS started",
          "status.committed AS committed",
          "status.errorMessage AS errorMessage"
        )
        .transactionForeach(
          onErrorBehaviour = OnErrorContinue,
          batchSize = setup.txBatchSize,
          maybeReportAs = Some("status")
        )
        .|.emptyResult()
        .|.create(createNodeWithProperties("n", Seq("N"), "rhs"))
        .|.unwind("rhsRows AS rhs")
        .|.argument("rhsRows")
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
        case batch if batch.exists(_.shouldFail) => batch.map(r => Array[Any](r.i, true, false))
        case batch                               => batch.map(r => Array[Any](r.i, true, true))
      }

      val expectedNodes = setup.input.iterator
        .filterNot(_.shouldFail)
        .map(row => row.rhsUnwind.size)
        .sum

      runtimeResult should beColumns("i", "started", "committed")
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

object RandomisedTransactionForEachTests {
  sealed trait RhsRow
  case object ShouldSucceed extends RhsRow
  case object ShouldFail extends RhsRow

  case class InputRow(i: Int, rhsUnwind: IndexedSeq[RhsRow]) {
    lazy val shouldFail: Boolean = rhsUnwind.contains(ShouldFail)
  }

  case class TestSetup(txBatchSize: Int, input: IndexedSeq[InputRow]) {
    def batches(): Iterator[Iterable[InputRow]] = input.grouped(txBatchSize)

    def generateRows(successValue: AnyValue, failValue: AnyValue): Iterator[Array[AnyValue]] = {
      input.iterator
        .map { row =>
          val rhs = row.rhsUnwind.map {
            case ShouldSucceed => successValue
            case ShouldFail    => failValue
          }
          Array(Values.longValue(row.i), VirtualValues.list(rhs: _*))
        }
    }

    lazy val shouldFail: Boolean = input.exists(_.shouldFail)
  }

  /*
   * Generates test data that is random in the following dimensions:
   * - input row count
   * - rhs row count factor
   * - call in transaction batch size
   * - if rows will fail transaction
   */
  def genRandomTestSetup(sizeHint: Int): Gen[TestSetup] = {
    for {
      maxInputRowCount <- Gen.choose(1, sizeHint)
      txBatchSize <- Gen.chooseNum(1, sizeHint)
      probabilityOfTxBatchSuccess <- Gen.oneOf(1.0, 0.5) // Probability to have a committed tx batch
      txBatchSuccess <- Gen.infiniteStream(Gen.prob(probabilityOfTxBatchSuccess))
      rhsSizeGen = Gen.choose(0, sizeHint / 10)
      successRows <- Gen.infiniteStream {
        rhsSizeGen.map(size => IndexedSeq.fill(size)(ShouldSucceed))
      }
      failRows <- Gen.infiniteStream {
        rhsSizeGen
          .flatMap(size => Gen.buildableOfN[IndexedSeq[RhsRow], RhsRow](size, Gen.oneOf(ShouldSucceed, ShouldFail)))
          .map { rhsRow =>
            // Ugly hack to guarantee batch will fail
            if (rhsRow.contains(ShouldFail)) rhsRow else rhsRow :+ ShouldFail
          }
      }
    } yield {
      val successRowGenerator = successRows.iterator
      val failRowGenerator = failRows.iterator
      val rows = txBatchSuccess
        .flatMap { isSuccessBatch =>
          // Generate rows for each batch
          val rowIterator = if (isSuccessBatch) successRowGenerator else failRowGenerator
          Range(0, txBatchSize).map(_ => rowIterator.next()).toIndexedSeq
        }
        .take(maxInputRowCount)
        .takeWhile {
          // Limit rows further to never have more than sizeHint creates
          var createCount = 0
          row =>
            val rowCreates = row.iterator.takeWhile(rhsRow => rhsRow == ShouldSucceed).size
            createCount += rowCreates
            createCount <= sizeHint
        }
        .zipWithIndex
        .map { case (row, i) => InputRow(i, row) }
        .toIndexedSeq

      TestSetup(txBatchSize, rows)
    }
  }
}

case class TxStats(started: Long, committed: Long, rollbacked: Long) {

  def add(stats: TxStats): TxStats =
    copy(started + stats.started, committed + stats.committed, rollbacked + stats.rollbacked)

  override def toString: String = s"TxStats(started = $started, committed = $committed, rollbacked: $rollbacked)"
}
