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

import org.neo4j.configuration.GraphDatabaseSettings.{cypher_pipelined_batch_size_big, cypher_pipelined_batch_size_small, cypher_worker_count}
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultStressTestBase.{MORSEL_SIZE, WORKERS}
import org.neo4j.cypher.internal.runtime.spec.{Edition, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CypherRuntime, LogicalQuery, RuntimeContext}
import org.scalatest.concurrent.Eventually

import scala.util.Random
object ReactiveResultStressTestBase {
  val MORSEL_SIZE = 17
  val WORKERS = 13
}

abstract class ReactiveResultStressTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                       runtime: CypherRuntime[CONTEXT],
                                                                       sizeHint: Int)
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
    cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
    cypher_worker_count -> Integer.valueOf(WORKERS)), runtime) with Eventually {
  private val random = new Random(seed = 31)

  test("should handle allNodeScan") {
    given { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    //then
    oneAtaTimeCount(logicalQuery) should equal(sizeHint)
    randomCount(logicalQuery) should equal(sizeHint)
  }

  test("should handle expand") {
    given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    oneAtaTimeCount(logicalQuery) should equal(2 * sizeHint)
    randomCount(logicalQuery) should equal(2 * sizeHint)
  }

  test("should handle input") {
    // given, an empty graph

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .input(variables = Seq("x"))
      .build()

    oneAtaTimeCount(logicalQuery) should equal(sizeHint)
    randomCount(logicalQuery) should equal(sizeHint)
  }

  private def randomCount(logicalQuery: LogicalQuery): Int =
    count(logicalQuery,
          () => intBetween(MORSEL_SIZE - MORSEL_SIZE/2, MORSEL_SIZE + MORSEL_SIZE/2))

  private def oneAtaTimeCount(logicalQuery: LogicalQuery): Int = count(logicalQuery, () => 1)

  private def count(logicalQuery: LogicalQuery, request: () => Int): Int = {
    val subscriber = new TestSubscriber
    val data = inputValues((1 to sizeHint).map(Array[Any](_)):_*)
    val runtimeResult = execute(logicalQuery, runtime, data.stream(), subscriber)
    var hasMore = true
    var totalNumberOfRequests = 0
    while (hasMore) {
      val requested = request()
      runtimeResult.request(requested)
      totalNumberOfRequests += requested
      hasMore = runtimeResult.await()

      if (!hasMore) {
        subscriber.isCompleted should equal(true)
      }
      subscriber.numberOfSeenResults should be <= totalNumberOfRequests
    }
    subscriber.allSeen.size
  }

  def intBetween(min: Int, max: Int): Int = min + random.nextInt(max - min + 1)
}
