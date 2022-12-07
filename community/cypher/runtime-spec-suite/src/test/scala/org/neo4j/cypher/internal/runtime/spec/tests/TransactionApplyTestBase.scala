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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.runtime.IteratorInputStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.cypher.internal.runtime.spec.tests.RandomisedTransactionForEachTests.genRandomTestSetup
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

abstract class TransactionApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) with SideEffectingInputStream[CONTEXT] {

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
      .transactionApply(3)
      .|.aggregation(Seq("1 AS group"), Seq("count(i) AS c"))
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
      .transactionApply(3)
      .|.top(Seq(Descending("i")), 2)
      .|.unwind("range(1, x) AS i")
      .|.create(createNodeWithProperties("n", Seq("N"), "{prop: x}"))
      .|.argument("x")
      .unwind("range(1, 10) AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
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

    given {
      for (_ <- 0 until nodeCountA) yield runtimeTestSupport.tx.createNode(Label.label("A"))
      for (_ <- 0 until nodeCountB) yield runtimeTestSupport.tx.createNode(Label.label("B"))
      for (_ <- 0 until nodeCountC) yield runtimeTestSupport.tx.createNode(Label.label("C"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .aggregation(Seq.empty, Seq("count(*) AS x"))
      .transactionApply(batchSize = batchSize)
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
    consume(runtimeResult)

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

    val probe = newProbe(queryStatistics => {
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
      .transactionApply(1)
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

    val probe = newProbe(queryStatistics => {
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
      .transactionApply(batchSize)
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

    given {
      nodeGraph(1, "N")
    }

    var nodeCount: Long = 1
    val probe = newProbe(queryStatistics => {
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
      .transactionApply(1)
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

    given {
      nodeIndex("Label", "prop")
      nodePropertyGraph(1, { case _ => Map[String, Any]("prop" -> 2) }, "Label")
    }

    var nodeCount: Long = 1
    val probe = newProbe(queryStatistics => {
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
      .transactionApply(1)
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
      transactionsStarted = expectedTxCommitted,
      transactionsCommitted = expectedTxCommitted
    )
  }

  protected def txAssertionProbe(assertion: InternalTransaction => Unit): Prober.Probe = {
    new Probe {
      override def onRow(row: AnyRef, queryStatistics: QueryStatistics, transactionsCommitted: Int): Unit = {
        withNewTx(assertion(_))
      }
    }
  }

  protected def newProbe(
    assertion: QueryStatistics => Assertion
  ): Prober.Probe = {
    new Probe {
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
  }
}

/**
 * Tests transaction foreach in queries like.
 * 
 * .produceResult()
 * .transactionForeach()
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
    extends ScalaCheckPropertyChecks { self: RuntimeTestSuite[CONTEXT] =>

  def sizeHint: Int

  test("should handle random failures with ON ERROR FAIL") {
    given {
      uniqueIndex("N", "p")
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
        consume(runtimeResult)

        val expected = setup.input
          .flatMap(row => row.rhsUnwind.map(rhs => Array(row.i, row.i + 1, true, true, null)))

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

  test("should handle random failures with ON ERROR BREAK") {
    given {
      uniqueIndex("N", "p")
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
      consume(runtimeResult)

      val expected = setup.batches().flatMap {
        var hasFailed = false
        batch => {
          if (hasFailed) {
            batch.map(row => Array(row.i, null, false, false))
          } else if (batch.exists(_.shouldFail)) {
            hasFailed = true
            batch.map(row => Array(row.i, null, true, false))
          } else {
            batch.flatMap(row => row.rhsUnwind.map(_ => Array(row.i, row.i + 1, true, true)))
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
    given {
      uniqueIndex("N", "p")
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
      consume(runtimeResult)

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
