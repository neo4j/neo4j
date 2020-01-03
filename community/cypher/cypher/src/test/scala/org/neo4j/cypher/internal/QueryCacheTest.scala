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
package org.neo4j.cypher.internal

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions, when}
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.scalatest.mock.MockitoSugar

class QueryCacheTest extends CypherFunSuite {
  import QueryCacheTest._

  test("first time accessing the cache should be a cache miss") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    // Then
    valueFromCache should equal(CacheMiss(valueFromKey(key)))
    valueFromCache.executableQuery.recompiled should equal(false)
    verify(tracer).queryCacheMiss(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("accessing the cache with two different keys should both result in cache misses") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key1 = newKey("key1")
    val key2 = newKey("key2")


    // When
    val value1FromCache = cache.computeIfAbsentOrStale(key1, TC, compileKey(key1), recompile(key1))
    val value2FromCache = cache.computeIfAbsentOrStale(key2, TC, compileKey(key2), recompile(key2))

    // Then
    value1FromCache should equal(CacheMiss(valueFromKey(key1)))
    value2FromCache should equal(CacheMiss(valueFromKey(key2)))
    value1FromCache.executableQuery.recompiled should equal(false)
    value2FromCache.executableQuery.recompiled should equal(false)

    verify(tracer).queryCacheMiss(key1, "")
    verify(tracer).queryCacheMiss(key2, "")
    verifyNoMoreInteractions(tracer)
  }

  test("second time accessing the cache should be a cache hit") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")
    val _ = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))

    // Then
    valueFromCache should equal(CacheHit(valueFromKey(key)))
    valueFromCache.executableQuery.recompiled should equal(false)
    verify(tracer).queryCacheMiss(key, "")
    verify(tracer).queryCacheHit(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("if item is stale we should miss the cache") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val cache = newCache(tracer, alwaysStale(secondsSinceReplan))
    val key = newKey("foo")
    val _ = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))

    // Then
    valueFromCache should equal(CacheMiss(valueFromKey(key)))
    valueFromCache.executableQuery.recompiled should equal(false)

    verify(tracer, times(2)).queryCacheMiss(key, "")
    verify(tracer).queryCacheStale(key, secondsSinceReplan, "")
    verifyNoMoreInteractions(tracer)
  }

  test("should trigger recompile when hot") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key))

    // Then
    valueFromCache should equal(CacheHit(valueFromKey(key)))
    valueFromCache.executableQuery.recompiled should equal(true)

    verify(tracer).queryCacheMiss(key, "")
    verify(tracer, times(3)).queryCacheHit(key, "")
    verify(tracer).queryCacheRecompile(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("should only trigger recompile once") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    (1 to 100).foreach(_ => cache.computeIfAbsentOrStale(key, TC, compileKey(key), recompile(key)))

    // Then
    verify(tracer).queryCacheMiss(key, "")
    verify(tracer, times(99)).queryCacheHit(key, "")
    verify(tracer).queryCacheRecompile(key, "")
    verifyNoMoreInteractions(tracer)
  }
}

  object QueryCacheTest extends MockitoSugar {
    case class MyValue(key: String)(val recompiled: Boolean) {
      def shouldBeCached: Boolean = true
    }

    private val RECOMPILE_LIMIT = 2
    def recompile(key: Key): (Int) => Option[MyValue] = (count: Int) => {
      if (count > RECOMPILE_LIMIT) Some(MyValue(key.first())(recompiled = true))
      else None
    }

    val TC: TransactionalContext = mock[TransactionalContext]
    type Tracer = CacheTracer[Pair[String, ParameterTypeMap]]
    type Key = Pair[String, Map[String, Class[_]]]

    def compileKey(key: Key): () => MyValue = () => valueFromKey(key)

    def newKey(string: String): Key = Pair.of(string, Map.empty[String, Class[_]])

   def newCache(tracer: Tracer = newTracer(), stalenessCaller:PlanStalenessCaller[MyValue] = neverStale()): QueryCache[String, Pair[String, ParameterTypeMap], MyValue] = {
    new QueryCache[String, Pair[String, ParameterTypeMap], MyValue](10, stalenessCaller, tracer)
  }

   def newTracer(): Tracer = mock[Tracer]

   def neverStale(): PlanStalenessCaller[MyValue] = {
    val stalenessCaller: PlanStalenessCaller[MyValue] = mock[PlanStalenessCaller[MyValue]]
    when(stalenessCaller.staleness(any[TransactionalContext], any[MyValue])).thenReturn(NotStale)
    stalenessCaller
  }

  private def alwaysStale(seconds: Int): PlanStalenessCaller[MyValue] = {
    val stalenessCaller: PlanStalenessCaller[MyValue] = mock[PlanStalenessCaller[MyValue]]
    when(stalenessCaller.staleness(any[TransactionalContext], any[MyValue])).thenReturn(Stale(seconds))
    stalenessCaller
  }

  private def valueFromKey(key: Key): MyValue = MyValue(key.first())(recompiled = false)
}
