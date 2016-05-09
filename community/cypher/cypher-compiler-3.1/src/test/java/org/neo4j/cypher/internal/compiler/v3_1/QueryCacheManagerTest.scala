/*
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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class QueryCacheManagerTest extends CypherFunSuite {

  val NOT_STALE = (ignored: T) => false
  val STALE = (ignored: T) => true

  test("empty cache should pass a value straight through") {
    // Given
    val accessor = new PassThroughCacheAccessor
    val cache = new QueryCacheManager[String, T](accessor, emptyCache())

    // When
    val (result: T, planned: Boolean) = cache.getOrElseUpdate("A", "A", NOT_STALE, new T)

    // Then
    result should not be null
    planned should equal(true)
  }

  test("if query already in cache and not stale, return it") {
    // Given
    val accessor = new PassThroughCacheAccessor
    val t1 = new T
    val cache = cacheWith("A" -> t1)
    val queryCacheManager = new QueryCacheManager[String, T](accessor, cache)

    // When
    val (result: T, planned: Boolean) = queryCacheManager.getOrElseUpdate("A", "A", NOT_STALE, new T)

    // Then
    result should be theSameInstanceAs t1
    planned should equal(false)
  }

  test("if query already in cache but has gone stale, re-plan before returning a plan") {
    // Given
    val accessor = new PassThroughCacheAccessor
    val t1 = new T
    val cache = cacheWith("A" -> t1)
    val queryCacheManager = new QueryCacheManager[String, T](accessor, cache)

    // When
    val (result: T, planned: Boolean) = queryCacheManager.getOrElseUpdate("A", "A", STALE, new T)

    // Then
    result shouldNot be theSameInstanceAs t1
    planned should equal(true)
  }

  private def emptyCache() = {
    new LRUCache[String, T](10)
  }

  private def cacheWith(objects: (String, T)*): LRUCache[String, T] = {
    val cache = emptyCache()
    objects foreach { case (k, v) =>
      cache.put(k, v)
    }
    cache
  }
}

class T

class PassThroughCacheAccessor extends CacheAccessor[String, T] {
  override def getOrElseUpdate(cache: LRUCache[String, T])(key: String, f: => T): T = {
    cache.getOrElseUpdate(key, f)
  }

  override def remove(cache: LRUCache[String, T])(key: String, userKey: String): Unit = {
    cache.remove(key)
  }
}
