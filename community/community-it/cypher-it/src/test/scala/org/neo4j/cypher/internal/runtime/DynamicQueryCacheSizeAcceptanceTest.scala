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
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheCompanion
import org.neo4j.cypher.util.CacheCountsTestSupport
import org.neo4j.graphdb.config.Setting
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import java.lang.Boolean.TRUE

class DynamicQueryCacheSizeAcceptanceTest extends ExecutionEngineFunSuite with CacheCountsTestSupport with Eventually {

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
      val wrapper = new TestWrapper(cache)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 1, discards = 1) {
        runNovelQuery()
      }

      setCacheSize(2)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 0, discards = 1) {
        setCacheSize(1)
      }
    }
  }

  Seq(
    CypherQueryCaches.LogicalPlanCache,
    CypherQueryCaches.ExecutableQueryCache
  ).foreach(cache => {
    test(s"should resize the strong ${cache.getClass.getSimpleName} dynamically") {
      restartWithConfig(Map(
        GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> TRUE,
        GraphDatabaseInternalSettings.cypher_soft_cache_enabled -> TRUE,
        GraphDatabaseInternalSettings.query_cache_strong_size -> Int.box(1),
        GraphDatabaseInternalSettings.query_cache_soft_size -> Int.box(0)
      ))

      val wrapper = new TestWrapper(cache)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 1, discards = 1) {
        runNovelQuery()
      }

      setCacheSize(2, 0)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 0, discards = 1) {
        setCacheSize(1, 0)
      }

    }

  })

  Seq(
    CypherQueryCaches.LogicalPlanCache,
    CypherQueryCaches.ExecutableQueryCache
  ).foreach(cache => {
    test(s"should resize the soft ${cache.getClass.getSimpleName} dynamically") {
      restartWithConfig(Map(
        GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> TRUE,
        GraphDatabaseInternalSettings.cypher_soft_cache_enabled -> TRUE,
        GraphDatabaseInternalSettings.query_cache_strong_size -> Int.box(1),
        GraphDatabaseInternalSettings.query_cache_soft_size -> Int.box(1)
      ))

      val wrapper = new TestWrapper(cache)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      setCacheSize(1, 2)

      wrapper.expectEvents(misses = 1, discards = 0) {
        runNovelQuery()
      }

      wrapper.expectEvents(misses = 0, discards = 1) {
        setCacheSize(1, 1)
      }
    }

  })

  private val queries = Iterator.from(1).map("return 1 as a" + _)

  private def runNovelQuery(): Unit = {
    execute(queries.next())
  }

  protected def setCacheSize(size: Int): Unit = {
    graph.config().setDynamic(GraphDatabaseSettings.query_cache_size, Int.box(size), "dynamic query cache size test")
  }

  implicit override def patienceConfig: PatienceConfig = PatienceConfig(Span(10, Seconds))

  protected def setCacheSize(strongSize: Int, softSize: Int): Unit = {
    graph.config().setDynamic(
      GraphDatabaseInternalSettings.query_cache_strong_size,
      Int.box(strongSize),
      "dynamic query cache size test"
    )
    graph.config().setDynamic(
      GraphDatabaseInternalSettings.query_cache_soft_size,
      Int.box(softSize),
      "dynamic query cache size test"
    )
  }

  private class TestWrapper(cache: CacheCompanion) {

    def expectEvents(misses: Int, discards: Int)(block: => Unit): Unit = {
      cacheCountsDiffFor(cache) { cacheCounts =>
        block

        eventually {
          val counts = cacheCounts()

          withClue(s"Expected $misses cache misses") {
            counts.misses shouldBe misses
          }
          withClue(s"Expected $discards cache discards") {
            counts.discards shouldBe discards
          }
        }
      }
    }
  }
}
