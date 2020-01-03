/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ProfilePageCacheStatsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                        runtime: CypherRuntime[CONTEXT]
                                                                       ) extends RuntimeTestSuite[CONTEXT](
  edition.copyWith(GraphDatabaseSettings.pagecache_memory -> "164480"), // 20 pages
  runtime) {

  // This needs to be big enough to trigger some page cache hits & misses
  private val SIZE = 5000

  test("should profile page cache stats of linear plan") {
    given {
      nodePropertyGraph(SIZE, {
        case i => Map("prop" -> i)
      })
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
    checkProfilerStatsMakeSense(runtimeResult, 4,
      Seq(0) // Projection of a previous row should not access store
    )
  }

  test("should profile page cache stats of branched plan") {
    given {
      index("M", "prop")
      nodePropertyGraph(SIZE, {
        case i => Map("prop" -> i)
      }, "N", "M")
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
      .nodeByLabelScan("n", "N")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    checkProfilerStatsMakeSense(runtimeResult, 8,
      Seq(2, // A join should not access store
        3, // Apply does not do anything
        5, // Argument does not do anything
      )
    )
  }

  private def checkProfilerStatsMakeSense(runtimeResult: RecordingRuntimeResult,
                                          numberOfOperators: Int,
                                          idSOfOperatorsThatShouldNotHaveAnyStats: Seq[Int] = Seq.empty): Unit = {
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    var accHits = 0L
    var accMisses = 0L
    for(i <- 0 until numberOfOperators) {
      val op = queryProfile.operatorProfile(i)
      val hits = op.pageCacheHits()
      val misses = op.pageCacheMisses()

      if (idSOfOperatorsThatShouldNotHaveAnyStats.contains(i)) {
        hits should be(0L)
        misses should be(0L)
      } else {
        hits should be >= 0L
        misses should be >= 0L
      }

      accHits += hits
      accMisses += misses
    }

    val totalHits = runtimeResult.pageCacheHits
    val totalMisses = runtimeResult.pageCacheMisses

    accHits should be(totalHits)
    accMisses should be(totalMisses)
  }
}
