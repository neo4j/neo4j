/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.configuration.GraphDatabaseSettings.{cypher_morsel_size, cypher_worker_count}
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultStressTestBase.{MORSEL_SIZE, WORKERS}
import org.neo4j.cypher.internal.runtime.spec.{Edition, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CypherRuntime, LogicalQuery, RuntimeContext}

import scala.util.Random
object ReactiveResultStressTestBase {
  val MORSEL_SIZE = 17
  val WORKERS = 13
}

abstract class ReactiveResultStressTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                       runtime: CypherRuntime[CONTEXT],
                                                                       sizeHint: Int)
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    cypher_morsel_size -> MORSEL_SIZE.toString,
    cypher_worker_count -> WORKERS.toString), runtime) {
  private val random = new Random(seed = 31)

  test("should handle allNodeScan") {
    // given
    nodeGraph(sizeHint)

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
    // given
    circleGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

   oneAtaTimeCount(logicalQuery) should equal(2 * sizeHint)
   randomCount(logicalQuery) should equal(2 * sizeHint)
  }

  private def randomCount(logicalQuery: LogicalQuery): Int =
    count(logicalQuery,
          () => intBetween(MORSEL_SIZE - MORSEL_SIZE/2, MORSEL_SIZE + MORSEL_SIZE/2))

  private def oneAtaTimeCount(logicalQuery: LogicalQuery): Int = count(logicalQuery, () => 1)

  private def count(logicalQuery: LogicalQuery, request: () => Int): Int = {
    val subscriber = new TestSubscriber
    val runtimeResult = execute(logicalQuery, runtime, subscriber)
    var hasMore = true
    while (hasMore) {
      val requested = request()
      runtimeResult.request(requested)
      hasMore = runtimeResult.await()
      subscriber.isCompleted should equal(!hasMore)
      subscriber.resultsInLastBatch should be <= requested
    }
    subscriber.allSeen.size
  }

  def intBetween(min: Int, max: Int): Int = min + random.nextInt(max - min + 1)
}
