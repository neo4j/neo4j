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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.options.CypherRuntimeOption.slotted
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.ArrayUtil
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.values.virtual.VirtualValues

import java.util.Locale

object MemoryManagementTestBase {
  // The configured max memory per transaction in Bytes
  val maxMemory: Long = ByteUnit.mebiBytes(6)
  val perWorkerGrabSize: Long = ByteUnit.kibiBytes(8)
}

trait InputStreams[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  /**
   * Infinite iterator.
   *
   * @param rowSize the size of a row in Bytes
   * @param data    optionally a function to create data. If non-empty, the result of passing the current row number will be returned in every call to `next`.
   *                If empty, the iterator returns integer values.
   */
  protected def infiniteInput(rowSize: Long, data: Option[Long => Array[Any]] = None): InputDataStream = {
    iteratorInput(iterate(data, None, nodeInput = false, rowSize))
  }

  /**
   * Infinite iterator.
   *
   * @param rowSize the size of a row in Bytes
   * @param data    optionally a function to create data. If non-empty, the result of passing the current row number will be returned in every call to `next`.
   *                If empty, the iterator returns node values.
   */
  protected def infiniteNodeInput(rowSize: Long, data: Option[Long => Array[Any]] = None): InputDataStream = {
    iteratorInput(iterate(data, None, nodeInput = true, rowSize))
  }

  /**
   * Finite iterator.
   *
   * @param limit   the iterator will be exhausted after the given amount of rows
   * @param rowSize the size of a row in Bytes
   * @param data    optionally a function to create data. If non-empty, the result of passing the current row number will be returned in every call to `next`..
   *                If empty, the iterator returns integer values.
   */
  protected def finiteInputWithRowSize(
    limit: Int,
    rowSize: Long,
    data: Option[Long => Array[Any]] = None
  ): InputDataStream = {
    iteratorInput(iterate(data, Some(limit), nodeInput = false, rowSize))
  }

  /**
   * Finite iterator.
   *
   * @param limit the iterator will be exhausted after the given amount of rows
   * @param data  optionally a function to create data. If non-empty, the result of passing the current row number will be returned in every call to `next`..
   *              If empty, the iterator returns integer values.
   */
  protected def finiteInput(limit: Int, data: Option[Long => Array[Any]] = None): InputDataStream = {
    iteratorInput(iterate(data, Some(limit), nodeInput = false, -1))
  }

  /**
   * Determine after how many rows to kill the query and fail the test, given the size of a row in the operator under test.
   * @param rowSize the size of a row in Bytes.
   */
  protected def killAfterNRows(rowSize: Long): Long = {
    ((MemoryManagementTestBase.maxMemory / rowSize) * 1.2).toLong // An extra of 20% rows to account for mis-estimation and batching
  }

  sealed trait ValueToEstimate
  // a single int column
  case object E_INT extends ValueToEstimate
  // a single int column used in DISTINCT
  case object E_INT_IN_DISTINCT extends ValueToEstimate
  // a single node column, which can be stored in a long-slot in slotted
  case object E_NODE_PRIMITIVE extends ValueToEstimate
  // a single node column, which cannot be stored in a long-slot in slotted
  case object E_NODE_VALUE extends ValueToEstimate

  /**
   * Estimate the size of an object after converting it into a Neo4j value.
   */
  protected def estimateSize(data: ValueToEstimate): Long = {
    data match {
      case E_INT => ValueUtils.of(0L).estimatedHeapUsage()
      case E_INT_IN_DISTINCT =>
        ValueUtils.of(java.util.Arrays.asList(0L)).estimatedHeapUsage() // We wrap the columns in a list
      case E_NODE_PRIMITIVE => VirtualValues.node(0).estimatedHeapUsage()
      case E_NODE_VALUE     => VirtualValues.node(0).estimatedHeapUsage()
    }
  }

  /**
   * Create an iterator.
   *
   * @param data      an optionally empty array. If non-empty, it will be returned in every call to `next`
   * @param limit     if defined, the iterator will be exhausted after the given amount of rows
   * @param nodeInput if true, and data is empty, the iterator returns node values.
   *                  If false, and data is empty, the iterator returns integer values.
   * @param rowSize   the size of a row in the operator under test. This value determines when to fail the test if the query is not killed soon enough.
   */
  protected def iterate(
    data: Option[Long => Array[Any]],
    limit: Option[Int],
    nodeInput: Boolean,
    rowSize: Long
  ): Iterator[Array[Any]] = new Iterator[Array[Any]] {
    private val killThreshold = killAfterNRows(rowSize)
    private var i = 0L
    override def hasNext: Boolean = limit.fold(true)(i < _)

    override def next(): Array[Any] = {
      i += 1
      if (limit.isEmpty && i > killThreshold) {
        fail("The query was not killed even though it consumed too much memory.")
      }
      data match {
        case None =>
          // Make sure that if you ever call this in parallel, you cannot just create nodes here and need to redesign the test.
          val value =
            if (nodeInput) {
              tx.createNode()
            } else {
              i
            }
          Array(value)
        case Some(func) =>
          func(i)
      }
    }
  }
}

abstract class MemoryManagementDisabledTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseSettings.memory_transaction_max_size -> Long.box(0L)
      ),
      runtime
    ) with InputStreams[CONTEXT] {

  test("should not kill memory eating query") {
    // given
    val input = finiteInput(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .input(variables = Seq("x"))
      .build()

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }
}

abstract class MemoryManagementTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseSettings.memory_transaction_max_size -> Long.box(MemoryManagementTestBase.maxMemory),
        GraphDatabaseInternalSettings.initial_transaction_heap_grab_size_per_worker -> Long.box(
          MemoryManagementTestBase.perWorkerGrabSize
        ),
        GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(6),
        GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(6)
      ),
      runtime,
      testPlanCombinationRewriterHints = Set(NoRewrites)
    ) with InputStreams[CONTEXT] {

  test("should kill sort query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill partial sort query before it runs out of memory") {
    assume(!isParallel)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT) * 2, Some(i => Array(1, i.toInt)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill partial sort query with distinct ordered rows") {
    assume(!isParallel)
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val input = for (i <- 0 to 100000) yield Array[Any](i, i)

    // then
    val result = execute(logicalQuery, runtime, inputValues(input: _*).stream())
    consume(result)
  }

  test("should kill distinct query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT_IN_DISTINCT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill count aggregation query") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = finiteInput(100000)

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should not kill stdDev aggregation query") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("stdev(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = finiteInput(100000)

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill collect aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - one large group") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT, Some(0L))
    val input = infiniteInput(expectedRowSize, Some(_ => Array(0L)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - many groups") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node hash join query before it runs out of memory") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = infiniteInput(expectedRowSize, Some(_ => Array(nodes.head)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill hash join query with large RHS") {
    // given
    val nodesBatch1 = givenGraph { nodeGraph(5000) }
    val nodesBatch2 = givenGraph { nodeGraph(5000) }
    val nodes = nodesBatch1 ++ nodesBatch2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = finiteInputWithRowSize(1, expectedRowSize, Some(_ => Array(nodes.head)))

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill multi-column node hash join query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.allNodeScan("x")
      .input(nodes = Seq("x", "y"))
      .build()

    // when
    val (nodes, _) = givenGraph { circleGraph(1) }
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE) * 2, Some(_ => Array(nodes.head, nodes.head)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill value hash join query before it runs out of memory") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        1,
        {
          case i => Map("prop" -> i)
        }
      )
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .input(nodes = Seq("a"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = infiniteInput(expectedRowSize, Some(_ => Array(nodes.head)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill double collect aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c1", "c2")
      .aggregation(Seq.empty, Seq("collect(x) AS c1", "collect(x) AS c2"))
      .input(variables = Seq("x"))
      .build()

    val estimatedRowSize =
      estimateRowSize(logicalQuery, nRows = 100) // Need more sample rows to amortize initial overhead of collections

    // when
    val input = infiniteInput(estimatedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct + node hash join query before it runs out of memory") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .distinct("x AS x")
      .input(nodes = Seq("x"))
      .build()

    val estimatedRowSize = estimateRowSize(logicalQuery, Some(nodes.head))

    // when
    val input = infiniteNodeInput(estimatedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill top query with low limit") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(10, "x ASC")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT, estimateFunction = estimateTopTableSize(10))
    val input = finiteInput(1000000, Some(_ => Array(expectedRowSize)))

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should not kill cartesian product query") {
    // given
    givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .cartesianProduct()
      .|.allNodeScan("a")
      .input(variables = Seq("x"))
      .build()

    // when
    // Not running assertTotalAllocatedMemory since interpreted and slotted do not eagerize at all
    val expectedRowSize = estimateSize(E_INT) + estimateSize(E_NODE_PRIMITIVE)
    val input = finiteInput(1000, Some(_ => Array(expectedRowSize)))

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should not kill cartesian product query with large RHS") {
    // given
    givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .cartesianProduct()
      .|.allNodeScan("a")
      .input(variables = Seq("x"))
      .build()

    // when
    // Not running assertTotalAllocatedMemory since interpreted and slotted do not eagerize at all
    val expectedRowSize = estimateSize(E_INT) + estimateSize(E_NODE_PRIMITIVE)
    val input = finiteInput(1, Some(_ => Array(expectedRowSize)))

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill top n query before it runs out of memory, where n < max array size") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(ArrayUtil.MAX_ARRAY_SIZE - 1L, "x ASC")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill top n query before it runs out of memory, where n is the maximum array size") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(ArrayUtil.MAX_ARRAY_SIZE, "x ASC")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill top n query before it runs out of memory, where n > max array size") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(ArrayUtil.MAX_ARRAY_SIZE + 1L, "x ASC")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill ordered distinct query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(variables = Seq("x", "y"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT_IN_DISTINCT), Some(i => Array(1, i.toInt)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill ordered distinct query with distinct values in ordered column") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(variables = Seq("x", "y"))
      .build()

    val input = for (i <- 0 to 100000) yield Array[Any](i, i)

    // then
    val result = execute(logicalQuery, runtime, inputValues(input: _*).stream())
    consume(result)
  }

  test("should kill partial top query before it runs out of memory") {
    assume(!isParallel)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialTop(100000, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT) * 2, Some(i => Array(1, i.toInt)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill partial top query with distinct ordered rows") {
    assume(!isParallel)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialTop(100000, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val input = for (i <- 0 to 100000) yield Array[Any](i, i)

    // then
    val result = execute(logicalQuery, runtime, inputValues(input: _*).stream())
    consume(result)
  }

  // we decided not to use `infiniteNodeInput` with an estimated size here since it is tricky to
  // get it to work with the internal cache in expand(into). DO NOT copy-paste this test when
  // adding support to the memory manager, prefer tests that use `infiniteNodeInput` instead.
  test("should kill pruning-var-expand before it runs out of memory") {
    // given
    getConfig.setDynamic(GraphDatabaseSettings.memory_transaction_max_size, Long.box(ByteUnit.mebiBytes(115)), "Test")
    restartTx()
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .pruningVarExpand("(x)<-[*6..6]-(y)")
      .nodeByLabelScan("x", "C", IndexOrderNone)
      .build()

    // when
    nestedStarGraphCenterOnly(6, 6, "C", "L")

    // Creating the graph needs more memory than querying it, so we need to lower the max size to trigger the MemoryLimitExceeded exception
    getConfig.setDynamic(GraphDatabaseSettings.memory_transaction_max_size, Long.box(ByteUnit.mebiBytes(1)), "Test")
    restartTx()

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }

  test("should kill distinct-pruning-var-expand before it runs out of memory") {
    // given
    getConfig.setDynamic(GraphDatabaseSettings.memory_transaction_max_size, Long.box(ByteUnit.mebiBytes(115)), "Test")
    restartTx()
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .bfsPruningVarExpand("(x)<-[*1..6]-(y)")
      .nodeByLabelScan("x", "C", IndexOrderNone)
      .build()

    // when
    nestedStarGraphCenterOnly(6, 6, "C", "L")

    // Creating the graph needs more memory than querying it, so we need to lower the max size to trigger the MemoryLimitExceeded exception
    getConfig.setDynamic(GraphDatabaseSettings.memory_transaction_max_size, Long.box(ByteUnit.mebiBytes(1)), "Test")
    restartTx()

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }

  test("should kill distinct aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    // when
    val (nodes, _) = givenGraph { circleGraph(1) } // Just for size estimation
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill ordered count aggregation query") {
    assume(!isParallel)
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(*) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val input = for (i <- 0 to 100000) yield Array[Any](i, i)

    // then
    val result = execute(logicalQuery, runtime, inputValues(input: _*).stream())
    consume(result)
  }

  test("should not kill ordered collect aggregation query") {
    assume(!isParallel)
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("collect(y) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val input = for (i <- 0 to 100000) yield Array[Any](i, i)

    // then
    val result = execute(logicalQuery, runtime, inputValues(input: _*).stream())
    consume(result)
  }

  test("should kill nested plan collect expression") {
    // given
    val n =
      (MemoryManagementTestBase.maxMemory / estimateSize(
        E_INT
      )) * 1.2 // Should fill the size of memory with n values alone + 20% extra margin
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1")
      .nestedPlanCollectExpressionProjection("x1", "i")
      .|.unwind(s"range(1,$n) as i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // when
    val input = finiteInput(1, Some((i: Long) => Array(i)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill nested plan exists expression") {
    val n =
      (MemoryManagementTestBase.maxMemory / estimateSize(
        E_INT
      )) * 1.2 // Should fill the size of memory with n values alone + 20% extra margin
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1")
      .nestedPlanExistsExpressionProjection("x1")
      .|.unwind(s"range(1,$n) as i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    // when
    val input = finiteInput(1, Some((i: Long) => Array(i)))

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill eager query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .input(variables = Seq("x"))
      .build()

    // when
    val estimatedRowSize = assertHeapHighWaterMark(logicalQuery, E_INT)
    val input = infiniteInput(estimatedRowSize)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileDisc aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileDisc(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT), Some(_ => Array(5L)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileCont aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileCont(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT), Some(_ => Array(5L)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node left outer hash join query before it runs out of memory") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .leftOuterHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = infiniteInput(expectedRowSize, Some(_ => Array(nodes.head)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node right outer hash join query before it runs out of memory") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .rightOuterHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertHeapHighWaterMark(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = infiniteInput(expectedRowSize, Some(_ => Array(nodes.head)))

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill var-length query with long pattern before it runs out of memory") {
    // given
    givenGraph {
      var start = tx.createNode(Label.label("START"))
      var i = 1
      while (i <= 1000000) {
        val next = tx.createNode()
        start.createRelationshipTo(next, RelationshipType.withName("R"))
        start = next

        if (i % 100 == 0) {
          restartTx()
          start = tx.getNodeById(start.getId)
        }
        i += 1
      }
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    // then
    a[MemoryLimitExceededException] should be thrownBy { countRows(logicalQuery, runtime) }
  }

  test("should not count duplicated memory in slotted eager") {
    // Pipelined only passes this test with large morsel sizes
    assume(runtime.correspondingRuntimeOption.contains(slotted))
    // given
    val sizeHint = 2000
    val aaa = "a".repeat(2048)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aaa")
      .eager()
      .unwind(s"range(1, $sizeHint) as i")
      .projection(s"'$aaa' as aaa")
      .argument()
      .build()

    val result = profile(logicalQuery, runtime)

    val row = Array[Any](aaa)
    val expected = Seq.fill(sizeHint)(row)
    result should beColumns("aaa").withRows(inOrder(expected))

    result.runtimeResult.queryProfile().maxAllocatedMemory() should be < 200000L
  }

  test("should not count duplicated memory in slotted sort") {
    // Pipelined only passes this test with large morsel sizes
    assume(runtime.correspondingRuntimeOption.contains(slotted))
    // given
    val sizeHint = 2000
    val aaa = "a".repeat(2048)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "aaa")
      .sort("i DESC")
      .unwind(s"range(1, $sizeHint) as i")
      .projection(s"'$aaa' as aaa")
      .argument()
      .build()

    val result = profile(logicalQuery, runtime)
    val expected = Range.inclusive(1, sizeHint).reverse.map(i => Array[Any](i, aaa))
    result should beColumns("i", "aaa").withRows(inOrder(expected))

    result.runtimeResult.queryProfile().maxAllocatedMemory() should be < 200000L
  }

  protected def assertHeapHighWaterMark(
    logicalQuery: LogicalQuery,
    valueToEstimate: ValueToEstimate,
    sampleValue: Option[Any] = None,
    estimateFunction: (LogicalQuery, Option[Any], Int) => Long = estimateRowSize
  ): Long = {
    // TODO: Improve this to be a bit more reliable
    val expectedValueSize = estimateSize(valueToEstimate)
    val expectedRowSize = if (isParallel) {
      // TODO: Estimate the constant overhead instead, e.g. by running two estimates with different number of rows
      expectedValueSize / 2
    } else {
      expectedValueSize
    }
    val estimatedRowSize = estimateFunction(logicalQuery, sampleValue, 1000)
    estimatedRowSize should be >= expectedRowSize
    runtime.name.toLowerCase(Locale.ROOT) match {
      case "pipelined" =>
        estimatedRowSize should be < expectedRowSize * 60 // in pipelined we have lots of overhead for some operators in corner cases
      case "parallel" =>
        estimatedRowSize should be < expectedRowSize * 100 // in parallel we also have overhead of upfront memory grab per worker
      case _ =>
        estimatedRowSize should be < expectedRowSize * 20
    }
    expectedRowSize
  }

  protected def estimateRowSize(logicalQuery: LogicalQuery, sampleValue: Option[Any] = None, nRows: Int = 8): Long = {
    val result = execute(logicalQuery, runtime, inputColumns(1, nRows, i => sampleValue.getOrElse(i.toLong)))
    consume(result)
    if (isParallel) {
      tx.kernelTransaction().memoryTracker().heapHighWaterMark() / nRows
    } else {
      result.runtimeResult.heapHighWaterMark() / nRows
    }
  }

  protected def estimateTopTableSize(nTableRows: Int)(
    logicalQuery: LogicalQuery,
    sampleValue: Option[Any] = None,
    nRows: Int = 8
  ): Long = {
    val result = execute(logicalQuery, runtime, inputColumns(1, nTableRows, i => sampleValue.getOrElse(i.toLong)))
    consume(result)
    if (isParallel) {
      tx.kernelTransaction().memoryTracker().heapHighWaterMark() / nTableRows
    } else {
      result.runtimeResult.heapHighWaterMark() / nTableRows
    }
  }
}

/**
 * Tests for runtime with full language support
 */
trait FullSupportMemoryManagementTestBase[CONTEXT <: RuntimeContext] {
  self: MemoryManagementTestBase[CONTEXT] =>

  // expandInto and optionalExpandInto _are_ supported by pipelined.
  // But since the cache is shorter lived there, it does not make sense to have this test for pipelined.

  // we decided not to use `infiniteNodeInput` with an estimated here size since it is tricky to
  // get it to work with the internal cache in expand(into). DO NOT copy-paste this test when
  // adding support to the memory manager, prefer tests that use `infiniteNodeInput` instead.
  test("should kill caching expand-into query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expandInto("(x)-->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    // when
    circleGraph(1500)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }

  // we decided not to use `infiniteNodeInput` with an estimated here size since it is tricky to
  // get it to work with the internal cache in expand(into). DO NOT copy-paste this test when
  // adding support to the memory manager, prefer tests that use `infiniteNodeInput` instead.
  test("should kill caching optional expand-into query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .optionalExpandInto("(x)-->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    // when
    circleGraph(1500)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }
}

/**
 * Tests for runtimes with support for TransactionForeach
 */
trait TransactionForeachMemoryManagementTestBase[CONTEXT <: RuntimeContext] {
  self: MemoryManagementTestBase[CONTEXT] =>

  def concurrency: TransactionConcurrency = TransactionConcurrency.Serial

  test("should kill transactional subquery before it runs out of memory") {
    runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.sort("y ASC")
      .|.unwind("range(1, 100000000) as y")
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }

  test("should kill transactional subquery before it runs out of memory - grouping aggregation") {
    runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.aggregation(Seq("x as x"), Seq("collect(y) as z"))
      .|.unwind("range(1, 100000000) as y")
      .|.argument()
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime))
    }
  }

  test("should not kill transaction foreach subquery if both inner and outer together exceed the limit - sort") {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted                                             => 32000
      case Slotted if concurrency eq TransactionConcurrency.Serial => 52000
      case Slotted                                                 => 32000
      case Pipelined                                               => 70000
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.sort("y ASC")
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .limit(1)
      .sort("x ASC")
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .sort("x ASC")
        .unwind(s"range(1, ${2 * rowCount}) as x") // When doubling the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .subqueryForeach()
        .|.emptyResult()
        .|.sort("y ASC")
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .limit(1)
        .sort("x ASC")
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should not kill transaction foreach subquery if both inner and outer together exceed the limit - grouping aggregation"
  ) {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted                                             => 12000
      case Slotted if concurrency eq TransactionConcurrency.Serial => 13000
      case Slotted                                                 => 12000
      case Pipelined                                               => 15000
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = concurrency)
      .|.emptyResult()
      .|.aggregation(Seq("y as y"), Seq("collect(y) as ys"))
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .limit(1)
      .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
        .unwind(s"range(1, ${2 * rowCount}) as x") // When doubling the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .subqueryForeach()
        .|.emptyResult()
        .|.aggregation(Seq("y as y"), Seq("collect(y) as ys"))
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .limit(1)
        .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should not kill transaction foreach subquery with limit and distinct if both inner and outer together exceed the limit"
  ) {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted => 37000
      case Slotted     => 53000
      case Pipelined   => 70000
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1, concurrency = TransactionConcurrency.Serial)
      .|.emptyResult()
      .|.distinct("y AS y")
      .|.limit(100)
      .|.sort("y ASC")
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .distinct("x AS x")
      .limit(100)
      .sort("x ASC")
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .distinct("x AS x")
        .limit(100)
        .sort("x ASC")
        .unwind(s"range(1, ${2 * rowCount}) as x") // When doubling the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .subqueryForeach()
        .|.emptyResult()
        .|.distinct("y AS y")
        .|.limit(100)
        .|.sort("y ASC")
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .distinct("x AS x")
        .limit(100)
        .sort("x ASC")
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }

  ignore(
    "should not kill concurrent transaction foreach subquery with limit and distinct if both inner and outer together exceed the limit"
  ) {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted => 37000
      case Slotted     => 18750
      case Pipelined   => 70000
    }

    val concurrencyInt = 2
    val rowCountMultiplier = concurrencyInt + 1

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1, concurrency = TransactionConcurrency.Concurrent(concurrencyInt))
      .|.emptyResult()
      .|.distinct("y AS y")
      .|.limit(100)
      .|.sort("y ASC")
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .distinct("x AS x")
      .limit(100)
      .sort("x ASC")
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .distinct("x AS x")
        .limit(100)
        .sort("x ASC")
        .unwind(
          s"range(1, ${rowCountMultiplier * rowCount}) as x"
        ) // When multiplying the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .subqueryForeach()
        .|.emptyResult()
        .|.union()
        .|.|.distinct("y AS y")
        .|.|.limit(100)
        .|.|.sort("y ASC")
        .|.|.unwind(s"range(1, $rowCount) as y")
        .|.|.argument()
        .|.distinct("y AS y")
        .|.limit(100)
        .|.sort("y ASC")
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .distinct("x AS x")
        .limit(100)
        .sort("x ASC")
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("should not kill transaction apply subquery if both inner and outer together exceed the limit") {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted                                             => 37000
      case Slotted if concurrency eq TransactionConcurrency.Serial => 53000
      case Slotted                                                 => 32000
      case Pipelined                                               => 79000
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(concurrency = concurrency)
      .|.limit(1)
      .|.sort("y ASC")
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .limit(1)
      .sort("x ASC")
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .limit(1)
        .sort("x ASC")
        .unwind(s"range(1, ${2 * rowCount}) as x") // When doubling the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .apply()
        .|.limit(1)
        .|.sort("y ASC")
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .limit(1)
        .sort("x ASC")
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should not kill transaction apply subquery if both inner and outer together exceed the limit - grouping aggregation"
  ) {
    // Determined empirically
    val rowCount = runtimeUsed match {
      case Interpreted => 12000
      case Slotted     => 13000
      case Pipelined   => 15000
    }

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(concurrency = concurrency)
      .|.limit(1)
      .|.aggregation(Seq("y as y"), Seq("collect(y) as ys"))
      .|.unwind(s"range(1, $rowCount) as y")
      .|.argument()
      .limit(1)
      .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
      .unwind(s"range(1, $rowCount) as x")
      .argument()
      .build(readOnly = false)

    // Used to check that we choose the right amount of rows:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
        .unwind(s"range(1, ${2 * rowCount}) as x") // When doubling the rows we should consume too much
        .argument()
        .build()

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // Used to check that the same query without IN TRANSACTIONS runs out of memory:
    {
      val checkQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .apply()
        .|.limit(1)
        .|.aggregation(Seq("y as y"), Seq("collect(y) as ys"))
        .|.unwind(s"range(1, $rowCount) as y")
        .|.argument()
        .limit(1)
        .aggregation(Seq("x as x"), Seq("collect(x) as xs"))
        .unwind(s"range(1, $rowCount) as x")
        .argument()
        .build(readOnly = false)

      a[MemoryLimitExceededException] should be thrownBy {
        consume(execute(checkQuery, runtime))
      }
      // Restart tx here to reset memory usage
      runtimeTestSupport.restartTx(KernelTransaction.Type.IMPLICIT)
    }

    // then
    noException should be thrownBy consume(execute(logicalQuery, runtime))
  }
}
