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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.interpreted.profiler.PageCacheStats
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.kernel.api.KernelTransaction
import org.scalatest.funsuite.AnyFunSuiteLike

abstract class ProfilePageCacheStatsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(GraphDatabaseSettings.pagecache_memory -> Long.box(164480)), // 20 pages
      runtime
    ) {

  // This needs to be big enough to trigger some page cache hits & misses
  protected val SIZE = 5000

  val PageCacheIsNotUsed: PageCacheAssertionOp = PageCacheAssertionOp.Equal(PageCacheStats(0, 0))

  val NoEntryInPageCacheStat: PageCacheAssertionOp =
    PageCacheAssertionOp.Equal(PageCacheStats(-1, -1))

  test("should profile page cache stats of linear plan") {
    givenGraph {
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      )
      () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("x.prop AS p")
      .filter("x.prop > 0")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
      if (canFuse && !isParallel) {
        Map(
          0 -> NoEntryInPageCacheStat, // ProduceResults is part of a fused pipeline
          1 -> NoEntryInPageCacheStat, // Projection is part of a fused pipeline
          2 -> NoEntryInPageCacheStat
        ) // Filer is part of a fused pipeline
      } else if (canFuse) {
        Map(
          0 -> PageCacheIsNotUsed, // ProduceResults is not part of a fused pipeline
          1 -> NoEntryInPageCacheStat, // Projection is part of a fused pipeline
          2 -> NoEntryInPageCacheStat
        ) // Filer is part of a fused pipeline
      } else {
        Map(0 -> PageCacheIsNotUsed) // Projection of a previous row should not access store
      }
    checkProfilerStatsMakeSense(runtimeResult, 4, expectedOperatorPageCacheStats)
  }

  test("should profile page cache stats of linear plan with breaks") {
    givenGraph {
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        }
      )
      () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("p AS p"), Seq("count(*) AS c"))
      .projection("x.prop AS p")
      .filter("x.prop > 0")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
      if (canFuse) {
        Map(
          1 -> NoEntryInPageCacheStat, // Aggregation is part of a fused pipeline
          2 -> NoEntryInPageCacheStat, // Projection is part of a fused pipeline
          3 -> NoEntryInPageCacheStat // Filter is part of a fused pipeline
        )
      } else {
        Map.empty
      }
    checkProfilerStatsMakeSense(runtimeResult, 5, expectedOperatorPageCacheStats)
  }

  test("should profile page cache stats of branched plan") {
    givenGraph {
      nodeIndex("M", "prop")
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        },
        "N",
        "M"
      )
      () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n") // Populates results, thus can have page cache hits & misses
      .filter("n.prop > 0")
      .nodeHashJoin("n")
      .|.apply()
      .|.|.aggregation(Seq("n AS n"), Seq("count(*) AS c"))
      .|.|.argument("n")
      .|.nodeIndexOperator("n:M(prop = 1)")
      .nodeByLabelScan("n", "N", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
      if (canFuse) {
        Map(
          2 -> PageCacheIsNotUsed, // A join should not access store
          3 -> PageCacheIsNotUsed, // Apply does not do anything
          4 -> NoEntryInPageCacheStat, // Aggregation is part of a fused pipeline
          5 -> PageCacheIsNotUsed // Argument does not do anything
        )
      } else {
        Map(
          2 -> PageCacheIsNotUsed, // A join should not access store
          3 -> PageCacheIsNotUsed, // Apply does not do anything
          4 -> PageCacheIsNotUsed, // Aggregation should not access store
          5 -> PageCacheIsNotUsed // Argument does not do anything
        )
      }
    checkProfilerStatsMakeSense(runtimeResult, 8, expectedOperatorPageCacheStats)
  }

  test("should profile page cache stats of plan with apply over aggregation") {
    givenGraph {
      nodeIndex("M", "prop")
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        },
        "N",
        "M"
      )
      () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
      if (canFuse) {
        Map(
          0 -> PageCacheIsNotUsed, // Produce result should not access store
          1 -> PageCacheIsNotUsed // Apply does not do anything
          // TODO Shouldn't it be like this: 3 -> NoEntryInPageCacheStat // Expand all is part of a fused pipeline
        )
      } else {
        Map(
          0 -> PageCacheIsNotUsed, // Produce result should not access store
          1 -> PageCacheIsNotUsed // Apply does not do anything
        )
      }
    checkProfilerStatsMakeSense(runtimeResult, 8, expectedOperatorPageCacheStats)
  }

  sealed trait PageCacheAssertionOp {
    def check(hits: Long, misses: Long): Unit
  }

  object PageCacheAssertionOp {

    case class Equal(expected: PageCacheStats) extends PageCacheAssertionOp {

      override def check(hits: Long, misses: Long): Unit = {
        withClue("hits:") {
          hits shouldBe expected.hits
        }
        withClue("missed:") {
          misses shouldBe expected.misses
        }
      }
    }

    case class GreaterThanOrEqual(expected: PageCacheStats) extends PageCacheAssertionOp {

      override def check(hits: Long, misses: Long): Unit = {
        withClue("hits:") {
          hits should be >= expected.hits
        }
        withClue("misses:") {
          misses should be >= expected.misses
        }
      }
    }

    case class SumGreaterThanOrEqual(expected: Long) extends PageCacheAssertionOp {

      override def check(hits: Long, misses: Long): Unit = {
        withClue(s"$hits + $misses >= $expected?:") {
          hits + misses should be >= expected
        }
      }
    }
  }

  protected def checkProfilerStatsMakeSense(
    runtimeResult: RecordingRuntimeResult,
    numberOfOperators: Int,
    expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] = Map.empty,
    isOnlyOneTransaction: Boolean = true
  ): Unit = {
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    var accHits = 0L
    var accMisses = 0L
    for (i <- 0 until numberOfOperators) {
      val op = queryProfile.operatorProfile(i)
      val hits = op.pageCacheHits()
      val misses = op.pageCacheMisses()

      withClue(s"Incorrect page cache stats for operator $i.") {
        if (expectedOperatorPageCacheStats.contains(i)) {
          val expectedBehaviour = expectedOperatorPageCacheStats(i)
          withClue(s"hits=$hits, misses=$misses)") {
            expectedBehaviour.check(hits, misses)
          }
        } else {
          withClue(s"hits: (hits=$hits, misses=$misses)") {
            hits should be >= 0L
          }
          withClue(s"misses: (hits=$hits, misses=$misses)") {
            misses should be >= 0L
          }
        }
        if (hits > 0) {
          accHits += hits
        }
        if (misses > 0) {
          accMisses += misses
        }
      }
    }

    // This sanity check can only be done when the query is executed with just one transaction.
    // When executing with more transactions, the runtimeResult.pageCacheHits/Misses will return only the stats for the outermost transaction.
    if (isOnlyOneTransaction) {
      val totalHits = runtimeResult.pageCacheHits
      val totalMisses = runtimeResult.pageCacheMisses

      if (isParallel) {
        // when using parallel scans atm we don't account the
        // page hits/misses happening in nextTask for partitioned scans
        accHits should be <= totalHits
        accMisses should be <= totalMisses
      } else {
        accHits should be(totalHits)
        accMisses should be(totalMisses)
      }
    }
  }

  // private def isParallel: Boolean = runtime.name.toLowerCase(Locale.ROOT) == "parallel"
}

trait UpdatingProfilePageCacheStatsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfilePageCacheStatsTestBase[CONTEXT] =>

  test("should profile page cache stats of create with new label") {
    givenGraph {
      uniqueNodeIndex("M", "prop")
      nodePropertyGraph(
        SIZE,
        {
          case i => Map("prop" -> i)
        },
        "N",
        "M"
      )
      () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a") // Populates results, thus can have page cache hits & misses
      .create(createNode("a", "A"))
      .nodeIndexOperator("m:M(prop > 0)", argumentIds = Set("a"))
      .build()

    val runtimeResult: RecordingRuntimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    checkProfilerStatsMakeSense(runtimeResult, 3)
  }
}

trait TransactionForeachPageCacheStatsTestBase[CONTEXT <: RuntimeContext] extends AnyFunSuiteLike {
  self: ProfilePageCacheStatsTestBase[CONTEXT] =>

  Seq(
    TransactionConcurrency.Serial,
    TransactionConcurrency.Concurrent(2),
    TransactionConcurrency.Concurrent(None)
  ).foreach { concurrency =>
    test(s"should profile page cache stats of plan with transactionForeach concurrency=$concurrency") {
      givenWithTransactionType(
        {
          nodePropertyGraph(
            SIZE,
            {
              case i => Map("prop" -> i)
            }
          )
          () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
        },
        KernelTransaction.Type.IMPLICIT
      )

      // when
      val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
        .produceResults("i")
        .transactionForeach(concurrency = concurrency, batchSize = 1)
        .|.emptyResult()
        .|.allNodeScan("x")
        .unwind("range(1,20) AS i")
        .argument()
        .build()

      val runtimeResult = profile(logicalQuery, runtime)
      consume(runtimeResult)

      // then
      val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
        Map(
          0 -> PageCacheIsNotUsed, // produceResult
          1 -> PageCacheIsNotUsed, // transactionForeach
          2 -> PageCacheIsNotUsed, // emptyResult
          3 -> PageCacheAssertionOp.SumGreaterThanOrEqual(20), // allNodeScan (at least one per incoming from unwind)
          4 -> PageCacheIsNotUsed, // unwind
          5 -> PageCacheIsNotUsed // argument
        )

      checkProfilerStatsMakeSense(runtimeResult, 6, expectedOperatorPageCacheStats, isOnlyOneTransaction = false)
    }

    test(s"should profile page cache stats of plan with transactionApply concurrency=$concurrency") {
      givenWithTransactionType(
        {
          nodePropertyGraph(
            SIZE,
            {
              case i => Map("prop" -> i)
            }
          )
          () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
        },
        KernelTransaction.Type.IMPLICIT
      )

      // when
      val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
        .produceResults("i")
        .transactionApply(concurrency = concurrency, batchSize = 1)
        .|.allNodeScan("x")
        .unwind("range(1,20) AS i")
        .argument()
        .build()

      val runtimeResult = profile(logicalQuery, runtime)
      consume(runtimeResult)

      // then
      val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
        Map(
          0 -> PageCacheIsNotUsed, // produceResult
          1 -> PageCacheIsNotUsed, // transactionApply
          3 -> PageCacheAssertionOp.SumGreaterThanOrEqual(2), // allNodeScan (at least one per incoming from unwind)
          3 -> PageCacheIsNotUsed, // unwind
          4 -> PageCacheIsNotUsed // argument
        )

      checkProfilerStatsMakeSense(runtimeResult, 5, expectedOperatorPageCacheStats, isOnlyOneTransaction = false)
    }

    test(
      s"should profile page cache stats of plan with transactionForeach and commit-phase hits/misses concurrency=$concurrency"
    ) {
      givenWithTransactionType(
        {
          nodePropertyGraph(
            100,
            {
              case i => Map("prop" -> i)
            },
            "A"
          )
          () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
        },
        KernelTransaction.Type.IMPLICIT
      )

      // when
      val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
        .produceResults("i")
        .transactionForeach(concurrency = TransactionConcurrency.Concurrent(None), batchSize = 1)
        .|.emptyResult()
        .|.create(createNode("b", "B"))
        .|.nodeByLabelScan("a", "A")
        .unwind("range(1,10) AS i")
        .argument()
        .build()

      val runtimeResult = profile(logicalQuery, runtime)
      consume(runtimeResult)

      // then
      val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
        Map(
          0 -> PageCacheIsNotUsed, // produceResult
          1 ->
            PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 1)), // transactionForeach
          2 -> PageCacheIsNotUsed, // emptyResult
          3 -> PageCacheIsNotUsed, // create
          4 ->
            PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 0)), // nodeByLabelScan
          5 -> PageCacheIsNotUsed, // unwind
          6 -> PageCacheIsNotUsed // argument
        )

      checkProfilerStatsMakeSense(runtimeResult, 7, expectedOperatorPageCacheStats, isOnlyOneTransaction = false)
    }

    test(
      s"should profile page cache stats of plan with two consecutive transactionForeach and commit-phase hits/misses concurrency=$concurrency"
    ) {
      givenWithTransactionType(
        {
          nodePropertyGraph(
            1000,
            {
              case i => Map("prop" -> i)
            },
            "A"
          )
          () // This makes sure we don't reattach the nodes to the new transaction, since that would create additional page cache hits/misses
        },
        KernelTransaction.Type.IMPLICIT
      )

      // when
      val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
        .produceResults("i")
        .transactionForeach(concurrency = concurrency, batchSize = 1)
        .|.emptyResult()
        .|.create(createNode("c", "C"))
        .|.nodeByLabelScan("a", "A")
        .transactionForeach(concurrency = concurrency, batchSize = 1)
        .|.emptyResult()
        .|.create(createNode("b", "B"))
        .|.nodeByLabelScan("a", "A")
        .unwind("range(1,100) AS i")
        .argument()
        .build()

      val runtimeResult = profile(logicalQuery, runtime)
      consume(runtimeResult)

      // then
      val expectedOperatorPageCacheStats: Map[Int, PageCacheAssertionOp] =
        Map(
          0 -> PageCacheIsNotUsed, // produceResult
          1 -> PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 1)), // transactionForeach
          2 -> PageCacheIsNotUsed, // emptyResult
          3 -> PageCacheIsNotUsed, // create
          4 -> PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 0)), // nodeByLabelScan
          5 -> PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 1)), // transactionForeach
          6 -> PageCacheIsNotUsed, // emptyResult
          7 -> PageCacheIsNotUsed, // create
          8 -> PageCacheAssertionOp.GreaterThanOrEqual(PageCacheStats(1, 0)), // nodeByLabelScan
          9 -> PageCacheIsNotUsed, // unwind
          10 -> PageCacheIsNotUsed // argument
        )

      checkProfilerStatsMakeSense(runtimeResult, 11, expectedOperatorPageCacheStats, isOnlyOneTransaction = false)
    }
  }
}
