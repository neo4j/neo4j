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
package org.neo4j.cypher.internal

import org.mockito.Mockito.{atLeastOnce, verify}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class QueryCacheStressTest extends CypherFunSuite {

  import QueryCacheTest._

  import scala.concurrent.ExecutionContext.Implicits.global

  test("should recompile at least once when running from multiple threads") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val futures = Future.sequence((1 to 100).map(_ => Future {
      cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    }))

    // Then
    Await.ready(futures, 60.seconds)
    verify(tracer, atLeastOnce()).queryCacheRecompile(key, "")
  }

  test("should hit at least once when running from multiple threads") {
    // Given
    val cache = newCache()
    val key = newKey("foo")

    // When
    val futures = Future.sequence((1 to 100).map(_ => Future {
      cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    }))

    // Then
    val (hits, misses) = Await.result(futures, 60.seconds).partition {
      case _: CacheHit[_] => true
      case _: CacheMiss[_] => false
      case _ => fail("we only expect hits and misses")
    }

    misses.size should be >= 1
    hits.size should be >= 1
  }
}
