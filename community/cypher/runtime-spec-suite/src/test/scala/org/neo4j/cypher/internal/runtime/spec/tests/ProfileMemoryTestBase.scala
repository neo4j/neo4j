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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Concurrent
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.helpers.ArrayUtil
import org.neo4j.kernel.api.KernelTransaction

abstract class ProfileMemoryTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  protected val SIZE: Int = 10

  test("should profile memory of sort") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of partial sort") {
    // partialSort not supported in parallel
    assume(!isParallel)
    val input = for (i <- 0 to SIZE) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    // then
    assertOnMemory(logicalQuery, inputValues(input: _*), 3, 1)
  }

  test("should profile memory of distinct") {
    givenGraph {
      nodePropertyGraph(SIZE, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xp")
      .distinct("x.p AS xp")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of collect aggregation") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 0, 1)
  }

  test("should profile memory of grouping aggregation - one large group") {
    givenGraph {
      nodePropertyGraph(SIZE, { case _ => Map("p" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x.p AS xp"), Seq("collect(x.p) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of grouping aggregation - many groups") {
    givenGraph {
      nodePropertyGraph(SIZE, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x.p AS xp"), Seq("collect(x.p) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of node hash join") {
    givenGraph {
      nodeGraph(SIZE)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1)
  }

  test("should profile memory of multi-column node hash join") {
    givenGraph {
      bipartiteGraph(SIZE, "X", "Y", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expand("(y)--(x)")
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .expand("(x)--(y)")
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 6, 1)
  }

  test("should profile memory of top n, where n < max array size") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending(varFor("x"))), ArrayUtil.MAX_ARRAY_SIZE - 1L)
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of top n, where n > max array size") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending(varFor("x"))), ArrayUtil.MAX_ARRAY_SIZE + 1L)
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of top1WithTies") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top1WithTies("x ASC")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of partial top n, where n < max array size") {
    // partialTop not supported in parallel
    assume(!isParallel)
    val input = for (i <- 0 to SIZE) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), ArrayUtil.MAX_ARRAY_SIZE - 1L)
      .input(variables = Seq("x", "y"))
      .build()

    // then
    assertOnMemory(logicalQuery, inputValues(input: _*), 3, 1)
  }

  test("should profile memory of partial top n, where n > max array size") {
    // partialTop not supported in parallel
    assume(!isParallel)
    val input = for (i <- 0 to SIZE) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), ArrayUtil.MAX_ARRAY_SIZE + 1L)
      .input(variables = Seq("x", "y"))
      .build()

    // then
    assertOnMemory(logicalQuery, inputValues(input: _*), 3, 1)
  }

  test("should profile memory of ordered distinct") {
    // orderedDistinct not supported in parallel
    assume(!isParallel)
    val input = for (i <- 0 to SIZE) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(variables = Seq("x", "y"))
      .build()

    // then
    assertOnMemory(logicalQuery, inputValues(input: _*), 3, 1)
  }

  test("should profile memory of ordered aggregation") {
    // orderedAggregation not supported in parallel
    assume(!isParallel)
    val input = for (i <- 0 to SIZE) yield Array[Any](1, i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("collect(y) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input: _*))
    consume(runtimeResult)

    // then
    assertOnMemory(logicalQuery, inputValues(input: _*), 3, 1)
  }

  test("should profile memory of var-length-expand") {
    // given
    givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*2..4]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    execute(logicalQuery, runtime)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1, 2)
  }

  test("should profile memory of distinct var-length-expand") {
    // given
    givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .bfsPruningVarExpand("(x)-[*1..4]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    execute(logicalQuery, runtime)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of IN") {
    givenGraph {
      // we need a bigger value than SIZE here since each worker has a separate InCache
      // and there is a delay before we start to cache things
      nodeGraph(1000)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.projection("43 IN list AS y")
      .|.allNodeScan("x", "list")
      .projection(s"range(1, 256) AS list")
      .argument()
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 6, 2)
  }

  test("should profile memory of varExpand") {
    givenGraph {
      circleGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*1..4]->(y)")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  // noinspection SameParameterValue
  protected def assertOnMemory(
    logicalQuery: LogicalQuery,
    input: InputValues,
    numOperators: Int,
    allocatingOperators: Int*
  ): Unit = {
    require(logicalQuery.logicalPlan.isInstanceOf[ProduceResult])
    val produceResultId = logicalQuery.logicalPlan.id
    val runtimeResult = profile(logicalQuery, runtime, input.stream())
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    var i = 0
    while (i < numOperators) {
      withClue(s"Memory allocations of plan $i: ") {
        if (allocatingOperators.contains(i)) {
          queryProfile.operatorProfile(i).maxAllocatedMemory() should be > 0L
        } else if (i == produceResultId.x) {
          queryProfile.operatorProfile(i).maxAllocatedMemory() should (be(OperatorProfile.NO_DATA) or be(0))
        } else {
          queryProfile.operatorProfile(i).maxAllocatedMemory() should be(OperatorProfile.NO_DATA)
        }
      }
      i += 1
    }
    queryProfile.maxAllocatedMemory() should be > 0L
  }
}

/**
 * Tests for runtime with full language support
 */
trait FullSupportProfileMemoryTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileMemoryTestBase[CONTEXT] =>

  test("should profile memory of eager") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of percentileDisc aggregation") {
    givenGraph {
      nodePropertyGraph(SIZE, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileDisc(x.p, 0.1) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of percentileCont aggregation") {
    givenGraph {
      nodePropertyGraph(SIZE, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileCont(x.p, 0.1) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of distinct aggregation") {
    givenGraph {
      nodeGraph(SIZE)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x) AS c"))
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 3, 1)
  }

  test("should profile memory of expand(into)") {
    givenGraph {
      bipartiteGraph(SIZE, "X", "Y", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 5, 1)
  }

  test("should profile memory of node left outer hash join") {
    givenGraph {
      nodeGraph(SIZE)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .leftOuterHashJoin("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1)
  }

  test("should profile memory of node right outer hash join") {
    givenGraph {
      nodeGraph(SIZE)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .rightOuterHashJoin("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1)
  }

  test("should profile memory of value hash join") {
    // given
    givenGraph {
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 4, 1)
  }

  test("should profile memory of operators inside transactionForeach") {
    // given
    givenWithTransactionType(
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      ),
      KernelTransaction.Type.IMPLICIT
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.expandInto("(m)-->(m)")
      .|.allNodeScan("m")
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 8, 1, 4)
  }

  test("should profile memory of operators inside transactionApply") {
    // given
    givenWithTransactionType(
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      ),
      KernelTransaction.Type.IMPLICIT
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply()
      .|.create(createNode("n", "N"))
      .|.expandInto("(m)-->(m)")
      .|.allNodeScan("m")
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 7, 1, 3)
  }

  test("should profile memory of operators inside concurrent transactionForeach") {
    // given
    givenWithTransactionType(
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      ),
      KernelTransaction.Type.IMPLICIT
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(concurrency = Concurrent(None))
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.expandInto("(m)-->(m)")
      .|.allNodeScan("m")
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 8, 1, 4)
  }

  test("should profile memory of operators inside concurrent transactionApply") {
    // given
    givenWithTransactionType(
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      ),
      KernelTransaction.Type.IMPLICIT
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(concurrency = Concurrent(None))
      .|.create(createNode("n", "N"))
      .|.expandInto("(m)-->(m)")
      .|.allNodeScan("m")
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    assertOnMemory(logicalQuery, NO_INPUT, 7, 1, 3)
  }
}
