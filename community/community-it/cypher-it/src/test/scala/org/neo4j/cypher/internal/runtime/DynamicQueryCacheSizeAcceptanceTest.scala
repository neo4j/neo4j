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
package org.neo4j.cypher.internal.runtime

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CountingCacheTracer
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.graphdb.config.Setting
import org.scalatest.concurrent.Eventually.eventually

import java.lang.Boolean.TRUE

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
      val counter = cache.addMonitorListener(kernelMonitors, new CountingCacheTracer[cache.Key])

      runNovelQuery()
      counter.counts.discards shouldBe 0

      runNovelQuery()
      eventually { // the cache is asynchronous; occasionally this line will be hit before the discard
        counter.counts.discards shouldBe 1
      }

      // when
      setCacheSize(2)

      // then
      runNovelQuery()
      counter.counts.discards shouldBe 1

      // when
      setCacheSize(1)

      // then
      counter.counts.discards shouldBe 2
    }
  }

  private val queries = Iterator.from(1).map("return 1 as a" + _)

  private def runNovelQuery(): Unit = {
    execute(queries.next())
  }

  private def setCacheSize(size: Int): Unit = {
    graph.config().setDynamic(GraphDatabaseSettings.query_cache_size, Int.box(size), "dynamic query cache size test")
  }
}
