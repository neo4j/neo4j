/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.api

class CypherCompilerStringCacheMonitoringAcceptanceTest extends ExecutionEngineFunSuite {

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0)

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends StringCacheMonitor {
    def cacheMiss(key: String) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    def cacheHit(key: String) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    def cacheFlushDetected(justBeforeKey: api.Statement) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }
  }

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
}

