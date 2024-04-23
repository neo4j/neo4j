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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.api.KernelTransactions
import org.neo4j.kernel.impl.util.ReadAndDeleteTransactionConflictException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import java.util.UUID

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Random

object MemoryDeallocationTestBase {
  // The configured max memory per transaction in Bytes
  val maxMemory: Long = ByteUnit.mebiBytes(3)
}

/**
 * These tests are designed to compare two executions of queries with the same input data
 * and assert that they do not differ significantly in the amount of estimated heap usage.
 *
 * Typically for eager operators, executing 3 in a row should not differ significantly from 2 in a row,
 * since we should be able to release the memory for a previous eager operator.
 *
 * E.g.
 * Query1: Scan -> ... -> Eager1 -> ... -> Eager2 -> ... -> ProduceResult
 * Query2: Scan -> ... -> Eager1 -> ... -> Eager2 -> ... -> Eager3 -> ... -> ProduceResult
 *
 * When we stream rows between Eager2 and Eager3 in Query2, we should have been able to release
 * the memory used by Eager1.
 * If the eagers have the same peak memory usage between Query1 and Query2,
 * the overall peak memory usage should not differ.
 *
 * The execution and assertion happens by calling one of the following test helpers:
 * compareMemoryUsage, compareMemoryUsageWithInputRows or compareMemoryUsageWithInputStreams
 */
abstract class MemoryDeallocationTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseSettings.memory_transaction_max_size -> Long.box(MemoryManagementTestBase.maxMemory),
        GraphDatabaseInternalSettings.initial_transaction_heap_grab_size_per_worker -> Long.box(
          MemoryManagementTestBase.perWorkerGrabSize
        )
      ),
      runtime,
      testPlanCombinationRewriterHints = Set(TestPlanCombinationRewriter.NoRewrites)
    )
    with InputStreams[CONTEXT]
    with RandomValuesTestSupport {

  test("should deallocate memory between grouping aggregation - many groups") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHint

    // then
    compareMemoryUsageWithInputRows(
      logicalQuery1,
      logicalQuery2,
      nRows,
      toleratedDeviation = 0.05
    ) // Pipelined is not exact
  }

  test("should deallocate memory between grouping aggregation - one large group") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("d")
      .aggregation(Seq("y AS y"), Seq("collect(y) AS d"))
      .unwind("c as y")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("e")
      .aggregation(Seq("z AS z"), Seq("collect(z) AS e"))
      .unwind("d as z")
      .aggregation(Seq("y AS y"), Seq("collect(y) AS d"))
      .unwind("c as y")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHint
    val input1 = finiteInput(nRows, Some(_ => Array(0)))
    val input2 = finiteInput(nRows, Some(_ => Array(0)))

    // then
    compareMemoryUsageWithInputStreams(
      logicalQuery1,
      logicalQuery2,
      input1,
      input2,
      toleratedDeviation = 0.05
    ) // Pipelined is not exact
  }

  test("should deallocate memory between eager") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .eager()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHint

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows, 0.035) // Pipelined is not exact
  }

  test("should deallocate memory between sort") {
    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x DESC")
      .sort("x ASC")
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x DESC")
      .sort("x ASC")
      .sort("x DESC")
      .sort("x ASC")
      .input(variables = Seq("x"))
      .build()

    val nRows = sizeHint

    // then
    // Unfortunately adding two extra plans make the GrowingArray holding operator memory tracker grow, so we need a tiny bit of tolerance here
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows, toleratedDeviation = 0.01)
  }

  test("should deallocate memory between top") {
    val nRows = sizeHint

    // given
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Descending(varFor("x"))), nRows / 2)
      .top(Seq(Ascending(varFor("x"))), nRows / 2)
      .input(variables = Seq("x"))
      .build()

    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Descending(varFor("x"))), nRows / 2)
      .top(Seq(Ascending(varFor("x"))), nRows / 2)
      .top(Seq(Descending(varFor("x"))), nRows / 2)
      .top(Seq(Ascending(varFor("x"))), nRows / 2)
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputRows(logicalQuery1, logicalQuery2, nRows, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between single node hash joins") {
    val nNodes = sizeHint

    givenGraph {
      nodeGraph(nNodes)
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between multi node hash joins") {
    val nNodes = sizeHint

    val paths = givenGraph { chainGraphs(nNodes, "R") }
    val random = new Random(seed = 1337)
    val data = (0 until nNodes).map { i => Array[Any](paths(i).startNode, paths(i).endNode()) }
    val shuffledData = random.shuffle(data).toArray
    val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
    val input1 = finiteInput(nNodes, Some(dataFunction))
    val input2 = finiteInput(nNodes, Some(dataFunction))

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .input(nodes = Seq("a", "b"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .nodeHashJoin("a", "b")
      .|.expandAll("(a)-[r:R]->(b)")
      .|.allNodeScan("a")
      .input(nodes = Seq("a", "b"))
      .build()

    // then
    val toleratedDeviation = runtimeUsed match {
      case Interpreted => 0.2 // TODO: Improve accuracy of interpreted
      case _           => 0.1
    }
    compareMemoryUsage(logicalQuery1, logicalQuery2, () => input1, () => input2, toleratedDeviation)
  }

  test("should deallocate memory between left outer hash joins") {
    val nNodes = sizeHint

    val n = nodeGraph(nNodes / 2)

    def input(): InputDataStream = {
      val nodes = givenGraph { n }
      val random = new Random(seed = 1337)
      val payload: Array[ListValue] = (1 to nNodes).map { _ =>
        VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)
      }.toArray
      val data = (0 until nNodes / 2).map { i => Array[Any](nodes(i), payload(i)) }
      val nullData = (nNodes / 2 until nNodes).map { i => Array[Any](VirtualValues.node(-1L), payload(i)) }
      val shuffledData = random.shuffle(data ++ nullData).toArray
      val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
      finiteInput(nNodes, Some(dataFunction))
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .leftOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, input, input, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between right outer hash joins") {
    val nNodes = sizeHint

    val n = nodeGraph(nNodes / 2)
    def input(): InputDataStream = {
      val nodes = givenGraph { n }
      val random = new Random(seed = 1337)
      val payload: Array[ListValue] = (1 to nNodes).map { _ =>
        VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)
      }.toArray
      val data = (0 until nNodes / 2).map { i => Array[Any](nodes(i), payload(i)) }
      val nullData = (nNodes / 2 until nNodes).map { i => Array[Any](VirtualValues.node(-1L), payload(i)) }
      val shuffledData = random.shuffle(data ++ nullData).toArray
      val dataFunction = (i: Long) => shuffledData(i.toInt - 1)
      finiteInput(nNodes, Some(dataFunction))
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a", "a2")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .rightOuterHashJoin("a")
      .|.projection("a AS a2")
      .|.allNodeScan("a")
      .input(nodes = Seq("a"), variables = Seq("payload"))
      .build()

    // then
    compareMemoryUsage(logicalQuery1, logicalQuery2, input, input, toleratedDeviation = 0.1)
  }

  test("should deallocate memory between value hash joins") {
    val nNodes = sizeHint

    givenGraph {
      val nodes = nodeGraph(nNodes)
      nodes.foreach(n => n.setProperty("prop", n.getId))
    }

    // when
    val logicalQuery1 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    // when
    val logicalQuery2 = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    // then
    val toleratedDeviation = runtimeUsed match {
      case Interpreted => 0.2 // TODO: Improve accuracy of interpreted
      case _           => 0.1
    }
    compareMemoryUsage(logicalQuery1, logicalQuery2, toleratedDeviation = toleratedDeviation)
  }

  test("should deallocate memory for distinct on RHS of apply") {
    val ys = Seq(1, 1, 2, 3, 4, 4, 5, 1, 3, 2, 5, 4, 1)
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.distinct("y as y")
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for single primitive distinct on RHS of apply") {
    givenGraph {
      nodeGraph(5)
    }

    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.distinct("y as y")
      .|.allNodeScan("y")
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for multiple primitive distinct on RHS of apply") {
    givenGraph {
      nodeGraph(5)
    }

    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .apply()
      .|.distinct("y as y", "y as z")
      .|.allNodeScan("y")
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty distinct on RHS of apply") {
    val ys = Seq()
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.distinct("y as y")
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty single primitive distinct on RHS of apply") {
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.distinct("y as y")
      .|.allNodeScan("y")
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty multiple primitive distinct on RHS of apply") {
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .apply()
      .|.distinct("y as y", "y as z")
      .|.allNodeScan("y")
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for limit on RHS of apply") {
    val ys = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.limit(5)
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty limit on RHS of apply") {
    val ys = Seq()
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.limit(5)
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for skip on RHS of apply") {
    val ys = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.skip(5)
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty skip on RHS of apply") {
    val ys = Seq()
    val nRows = sizeHint
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.skip(5)
      .|.unwind(s"[${ys.mkString(",")}] AS y")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for partial top on RHS of apply") {
    assume(!isParallel)
    val nRows = sizeHint
    val topLimit = 17
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialTop(Seq(Ascending(varFor("b"))), Seq(Ascending(varFor("c"))), topLimit)
      .|.unwind("range(a, b, -1) AS c")
      .|.unwind("range(0, a) AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate memory for empty partial top on RHS of apply") {
    assume(!isParallel)
    val nRows = sizeHint
    val topLimit = 17
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows * 3)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialTop(Seq(Ascending(varFor("b"))), Seq(Ascending(varFor("c"))), topLimit)
      .|.unwind("[] AS c")
      .|.unwind("[] AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    // then
    compareMemoryUsageWithInputStreams(logicalQuery, logicalQuery, input1, input2, 0.01) // Pipelined is not exact
  }

  test("should deallocate discarded slots with eager") {
    // Parallel runtime does not yet support tracking query heap high water mark that is used for the assertion
    assume(!isParallel && runtime.name != "interpreted")

    val nRows = sizeHint
    val inputListSize = 4
    def createInputRow(): Array[Array[String]] =
      Array(Range(0, inputListSize)
        .map(_ => UUID.randomUUID().toString).toArray)
    def createInput(): InputDataStream =
      finiteInput(
        nRows,
        data = Some(_ => createInputRow().asInstanceOf[Array[Any]])
      )

    def plan(discard: Boolean) = {
      val produce = if (discard) Seq("res") else Seq("res", "x")
      if (runtime.name == "slotted") {
        new LogicalQueryBuilder(this)
          .produceResults(produce: _*)
          .eager()
          // Limitation of discarding in slotted makes us need an extra pipeline break
          .unwind("[0] as needExtraPipelineBreak")
          .projection("size(x) as res")
          .input(variables = Seq("x"))
          .build()
      } else {
        new LogicalQueryBuilder(this)
          .produceResults(produce: _*)
          .eager()
          .projection("size(x) as res")
          .input(variables = Seq("x"))
          .build()
      }
    }

    // given
    val planWithoutDiscard = plan(discard = false)
    val planWithDiscard = plan(discard = true)

    // then
    val maxMemWithoutDiscard = maxAllocatedMem(planWithoutDiscard, createInput)
    val maxMemWithDiscard = maxAllocatedMem(planWithDiscard, createInput)

    val expectedSavingsPerRow = Values.of(createInputRow()(0)).estimatedHeapUsage()
    val expectedSavings = nRows * expectedSavingsPerRow
    expectedSavings should be > 10000L // ensure test integrity

    val actualSavings = maxMemWithoutDiscard - maxMemWithDiscard
    actualSavings should be > (expectedSavings * 0.9).toLong
  }

  test("should deallocate discarded slots with sort") {
    // Parallel runtime does not yet support tracking query heap high water mark that is used for the assertion
    assume(!isParallel && runtime.name != "interpreted" && runtime.name != "slotted")

    val nRows = sizeHint
    val inputListSize = 4
    def createInputRow(): Array[Array[String]] =
      Array(Range(0, inputListSize)
        .map(_ => UUID.randomUUID().toString).toArray)
    def createInput(): InputDataStream =
      finiteInput(
        nRows,
        data = Some(_ => createInputRow().asInstanceOf[Array[Any]])
      )

    def plan(discard: Boolean) = {
      val produce = if (discard) Seq("res") else Seq("res", "x")
      new LogicalQueryBuilder(this)
        .produceResults(produce: _*)
        .sort("res ASC")
        .projection("size(x) as res")
        .input(variables = Seq("x"))
        .build()
    }

    // given
    val planWithoutDiscard = plan(false)
    val planWithDiscard = plan(true)

    // then
    val maxMemWithoutDiscard = maxAllocatedMem(planWithoutDiscard, createInput)
    val maxMemWithDiscard = maxAllocatedMem(planWithDiscard, createInput)

    val expectedSavingsPerRow = Values.of(createInputRow()(0)).estimatedHeapUsage()
    val expectedSavings = nRows * expectedSavingsPerRow
    expectedSavings should be > 10000L // ensure test integrity

    val actualSavings = maxMemWithoutDiscard - maxMemWithDiscard
    actualSavings should be > (expectedSavings * 0.9).toLong
  }

  test("should deallocate memory after transaction foreach") {
    assume(!isParallel)
    val errorBehaviour = randomAmong(Seq(OnErrorFail, OnErrorBreak, OnErrorContinue))
    val status = if (errorBehaviour != OnErrorFail && random.nextBoolean()) Some("status") else None
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .transactionForeach(10, onErrorBehaviour = errorBehaviour, maybeReportAs = status)
      .|.projection("i * 2 as i2")
      .|.argument()
      .unwind(s"range(1, $rows) as i")
      .argument()
      .build()

    // then
    compareMemoryUsageImplicitTx(query(rows = 100), query(rows = 10000))
  }

  test("should deallocate memory after transaction apply") {
    assume(!isParallel)
    val errorBehaviour = randomAmong(Seq(OnErrorFail, OnErrorBreak, OnErrorContinue))
    val status = if (errorBehaviour != OnErrorFail && random.nextBoolean()) Some("status") else None
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults("i", "i2")
      .transactionApply(10, onErrorBehaviour = errorBehaviour, maybeReportAs = status)
      .|.projection("i * 2 as i2")
      .|.argument()
      .unwind(s"range(1, $rows) as i")
      .argument()
      .build()

    // then
    compareMemoryUsageImplicitTx(query(rows = 100), query(rows = 10000))
  }

  test("should deallocate memory after eager under apply") {
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults("aaa", "i")
      .apply()
      .|.eager()
      .|.projection(s"'${"a".repeat(128)}' as aaa")
      .|.unwind("range(1, 32) as i")
      .|.argument()
      .unwind(s"range(1, $rows) as i")
      .argument()
      .build()

    // then
    compareMemoryUsage(query(rows = 10), query(rows = 100))
  }

  test("should deallocate memory after aggregation under apply") {
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults("z")
      .apply()
      .|.aggregation(Seq.empty, Seq("collect(x+y) as z"))
      .|.unwind("range (1, 128) as y")
      .|.argument()
      .unwind(s"range(1, $rows) as x")
      .argument()
      .build()

    compareMemoryUsage(query(rows = 10), query(rows = 100))
  }

  test("should deallocate memory after sort under apply") {
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults("aaa")
      .apply()
      .|.sort("aaa DESC")
      .|.projection(s"'${"a".repeat(128)}' + i as aaa")
      .|.unwind("range(1, 32) as i")
      .|.argument()
      .unwind(s"range(1, $rows) as i")
      .argument()
      .build()

    // then
    compareMemoryUsage(query(rows = 10), query(rows = 100))
  }

  test("should deallocate memory after top under apply") {
    def query(rows: Int) = new LogicalQueryBuilder(this)
      .produceResults("aaa")
      .apply()
      .|.top(6, "aaa DESC")
      .|.projection(s"'${"a".repeat(128)}' + i as aaa")
      .|.unwind("range(1, 32) as i")
      .|.argument()
      .unwind(s"range(1, $rows) as i")
      .argument()
      .build()

    // then
    compareMemoryUsage(query(rows = 10), query(rows = 100))
  }

  test("should deallocate memory after skip with out of order arguments in pipelined runtime") {
    def query(skip: Int) = {
      val limit = 128
      new LogicalQueryBuilder(this)
        .produceResults("i", "j")
        .skip(skip)
        .limit(skip + limit)
        .apply()
        .|.distinct("j as j")
        .|.union()
        .|.|.unwind("range(0,75) as j")
        .|.|.argument()
        .|.unwind("range(25,100) as j")
        .|.argument()
        .unwind(s"range(0, 1000) as i")
        .argument()
        .build()
    }

    compareMemoryUsage(query(skip = 1000), query(skip = 10000), toleratedDeviation = 0.5)
  }

  test("should account for memory allocated in value population") {
    val nodes = givenGraph {
      // Some nodes with memory hungry properties and labels
      nodePropertyGraphFunctional(
        nNodes = 32,
        properties = { i =>
          Range(0, 128).map(p => s"p$p" -> s"$i:$p:${"bla".repeat(24)}").toMap
        },
        labels = { i =>
          Range(0, 128).map(l => s"Label-$i-$l")
        }
      )
    }

    val estHeapPropsAndLabels = nodes
      .map { n =>
        val propValueHeap = n.getAllProperties.asScala.values.map(v => Values.of(v).estimatedHeapUsage()).sum
        val labelsHeap = n.getLabels.asScala.map(l => Values.of(l.name()).estimatedHeapUsage()).sum
        propValueHeap + labelsHeap
      }
      .sum

    val queryWithNodeRefs = new LogicalQueryBuilder(this)
      .produceResults("nodes")
      .aggregation(Seq.empty, Seq("collect(n) as nodes"))
      .allNodeScan("n")
      .build()

    val queryWithPropsAndLabels = new LogicalQueryBuilder(this)
      .produceResults("props", "labels")
      .aggregation(Seq.empty, Seq("collect(properties(n)) as props", "collect(labels(n)) as labels"))
      .allNodeScan("n")
      .build()

    compareMemoryUsage(
      queryWithNodeRefs,
      queryWithPropsAndLabels,
      toleratedDeviation = 0.20,
      minAllocated = estHeapPropsAndLabels
    )
  }

  test("should account for memory allocated in value population with deeply nested nodes") {
    val nodes = givenGraph {
      nodePropertyGraphFunctional(
        nNodes = 32,
        properties = { i => Map("prop" -> i) },
        labels = { _ => Seq("L") }
      )
    }

    val estHeapPropsAndLabels = nodes
      .map { n =>
        val propValueHeap = n.getAllProperties.asScala.values.map(v => Values.of(v).estimatedHeapUsage()).sum
        val labelsHeap = n.getLabels.asScala.map(l => Values.of(l.name()).estimatedHeapUsage()).sum
        propValueHeap + labelsHeap
      }
      .sum

    val queryWithNestedNodeRefs = new LogicalQueryBuilder(this)
      .produceResults("nestedNodes")
      .projection("[[[[[[[[[[nodes]]]]]]]]]] as nestedNodes")
      .aggregation(Seq.empty, Seq("collect(n) as nodes"))
      .allNodeScan("n")
      .build()

    val queryWithPropsAndLabels = new LogicalQueryBuilder(this)
      .produceResults("props", "labels")
      .aggregation(Seq.empty, Seq("collect(properties(n)) as props", "collect(labels(n)) as labels"))
      .allNodeScan("n")
      .build()

    compareMemoryUsage(
      queryWithNestedNodeRefs,
      queryWithPropsAndLabels,
      toleratedDeviation = 0.20,
      minAllocated = estHeapPropsAndLabels
    )
  }

  protected def compareMemoryUsageImplicitTx(
    logicalQuery1: LogicalQuery,
    logicalQuery2: LogicalQuery,
    toleratedDeviation: Double = 0.0d
  ): Unit = {
    compareMemoryUsage(
      logicalQuery1,
      logicalQuery2,
      () => NoInput,
      () => NoInput,
      toleratedDeviation,
      txType = Some(KernelTransaction.Type.IMPLICIT)
    )
  }

  protected def compareMemoryUsageWithInputRows(
    logicalQuery1: LogicalQuery,
    logicalQuery2: LogicalQuery,
    nRows: Int,
    toleratedDeviation: Double = 0.0d
  ): Unit = {
    val input1 = finiteInput(nRows)
    val input2 = finiteInput(nRows)
    compareMemoryUsage(logicalQuery1, logicalQuery2, () => input1, () => input2, toleratedDeviation)
  }

  protected def compareMemoryUsageWithInputStreams(
    logicalQuery1: LogicalQuery,
    logicalQuery2: LogicalQuery,
    input1: InputDataStream,
    input2: InputDataStream,
    toleratedDeviation: Double = 0.0d
  ): Unit = {
    compareMemoryUsage(logicalQuery1, logicalQuery2, () => input1, () => input2, toleratedDeviation)
  }

  private def compareMemoryUsage(
    logicalQuery1: LogicalQuery,
    logicalQuery2: LogicalQuery,
    input1: () => InputDataStream = () => NoInput,
    input2: () => InputDataStream = () => NoInput,
    toleratedDeviation: Double = 0.0,
    minAllocated: Long = 0,
    txType: Option[KernelTransaction.Type] = None
  ): Unit = {
    val maxMem1 = maxAllocatedMem(logicalQuery1, input1, txType)
    val maxMem2 = maxAllocatedMem(logicalQuery2, input2, txType)
    val memDiff = Math.abs(maxMem2 - maxMem1)

    maxMem1 should be > minAllocated
    maxMem2 should be > minAllocated

    val deviation = Math.abs(maxMem1 - maxMem2) / maxMem1.toDouble
    val deviationPercentage = Math.round(deviation * 100)
    val toleratedDeviationPercentage = Math.round(toleratedDeviation * 100)
    val deviationMessage =
      s"$deviationPercentage%${if (toleratedDeviation > 0.0d) s" is more than tolerated ${toleratedDeviationPercentage}%"
        else ""}"

    withClue(
      s"Query 1 used $maxMem1 bytes and Query 2 used $maxMem2 bytes ($memDiff bytes difference, $deviationMessage):\n"
    ) {
      if (toleratedDeviation == 0.0d) {
        maxMem1 shouldEqual maxMem2
      } else {
        deviation <= toleratedDeviation shouldBe true
      }
    }
  }

  private def maxAllocatedMem(
    query: LogicalQuery,
    input: () => InputDataStream,
    txTypeOpt: Option[KernelTransaction.Type] = None
  ): Long = {
    txTypeOpt match {
      case Some(txType) => restartTx(txType)
      case _            => restartTx()
    }
    val runtimeResult = profile(query, runtime, input())
    try {
      consume(runtimeResult)
    } catch {
      case _: ReadAndDeleteTransactionConflictException =>
      // some node is missing, ignore
    }

    if (isParallel) {
      // TODO: Parallel runtime does not yet support heap high watermark through query profile
      //       For now, create a very rough estimate from how much heap was grabbed from the transaction memory pool
      val kernelTransactions =
        graphDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[KernelTransactions])
      val activeTransactions = kernelTransactions.activeTransactions()
      activeTransactions.size() should be(1)
      activeTransactions.iterator().next().transactionStatistic().getEstimatedUsedHeapMemory
    } else {
      runtimeResult.runtimeResult.queryProfile().maxAllocatedMemory()
    }
  }
}
