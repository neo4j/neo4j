/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher

import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api
import org.neo4j.logging.AssertableLogProvider

import scala.collection.Map

class CypherCompilerStringCacheMonitoringAcceptanceTest extends ExecutionEngineFunSuite {

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0, evicted: Int = 0) {
    override def toString = s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted"
  }

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends StringCacheMonitor {
    override def cacheMiss(key: String) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    override def cacheHit(key: String) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    override def cacheFlushDetected(justBeforeKey: api.Statement) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }

    override def cacheDiscard(key: String) {
      counts = counts.copy(evicted = counts.evicted + 1)
    }
  }

  override def databaseConfig(): Map[String,String] = Map(GraphDatabaseSettings.cypher_min_replan_interval.name() -> "0")

  test("should monitor cache miss") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should monitor cache misses and hits") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 2, misses = 1, flushes = 1))
  }

  test("should monitor cache flushes") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("create constraint on (n:Person) assert n.id is unique").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 3, misses = 3, flushes = 2))
  }

  test("should monitor cache evictions") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)
    val query = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    execute(query).toList

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    execute(query).toList

    // then
    counter.counts should equal(CacheCounts(hits = 3, misses = 2, flushes = 1, evicted = 1))
  }

  test("should log on cache evictions") {
    // given
    val logProvider = new AssertableLogProvider( )
    val engine = new ExecutionEngine( graph, logProvider )
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)
    val query = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    engine.execute(query).toList

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    engine.execute(query).toList

    // then
    logProvider.assertAtLeastOnce(
      AssertableLogProvider.inLog( classOf[ExecutionEngine] ).info( s"Discarded stale query from the query cache: CYPHER 2.3 $query" )
    )
  }
}

