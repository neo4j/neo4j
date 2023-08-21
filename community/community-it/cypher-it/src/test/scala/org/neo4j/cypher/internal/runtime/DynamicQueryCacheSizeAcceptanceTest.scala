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
package org.neo4j.cypher.internal.runtime

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.util.CountingCacheTracer
import org.neo4j.graphdb.config.Setting

import java.lang.Boolean.TRUE
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DynamicQueryCacheSizeAcceptanceTest extends ExecutionEngineFunSuite {

  override def databaseConfig(): Map[Setting[_], Object] = {
    Map(
      GraphDatabaseSettings.query_cache_size -> Int.box(1),
      GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> TRUE
    )
  }

  Seq(
    CypherQueryCaches.AstCache,
    CypherQueryCaches.LogicalPlanCache,
    CypherQueryCaches.ExecutableQueryCache,
    CypherQueryCaches.PreParserCache,
    CypherQueryCaches.ExecutionPlanCache
  ).foreach { cache =>
    test(s"should resize the ${cache.getClass.getSimpleName} dynamically") {
      // given
      val counter = cache.addMonitorListener(kernelMonitors, new LatchTracer[cache.Key])

      counter.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      counter.expectEvents(misses = 1, discards = 1) {
        runNovelQuery()
      }

      setCacheSize(2)

      counter.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      counter.expectEvents(misses = 0, discards = 1) {
        setCacheSize(1)
      }
    }
  }

  private val queries = Iterator.from(1).map("return 1 as a" + _)

  private def runNovelQuery(): Unit = {
    execute(queries.next())
  }

  protected def setCacheSize(size: Int): Unit = {
    graph.config().setDynamic(GraphDatabaseSettings.query_cache_size, Int.box(size), "dynamic query cache size test")
  }

  // the latch helps guard against potential race conditions which might cause flakiness in the test
  class LatchTracer[KEY] extends CountingCacheTracer[KEY] {
    private var latch: CountDownLatch = _

    override def cacheMiss(queryKey: KEY, metaData: String): Unit = {
      super.cacheMiss(queryKey, metaData)
      latch.countDown()
    }

    override def discard(key: KEY, metaData: String): Unit = {
      super.discard(key, metaData)
      latch.countDown()
    }

    def expectEvents(misses: Int, discards: Int)(block: => Unit): Unit = {
      val expected = misses + discards
      this.misses.set(0)
      this.discards.set(0)

      latch = new CountDownLatch(expected)
      block
      if (!latch.await(10, TimeUnit.SECONDS)) {
        fail(
          s"Expected $expected events but only received ${expected - latch.getCount} (${this.misses.get} misses; ${this.discards.get} discards)"
        )
      }

      withClue(s"Expected $misses cache misses") {
        this.misses.get shouldBe misses
      }
      withClue(s"Expected $discards cache discards") {
        this.discards.get shouldBe discards
      }
    }
  }
}
