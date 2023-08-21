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
package org.neo4j.cypher.internal.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener

import java.util.concurrent.ConcurrentMap

/**
 * Simple thread-safe cache with a least-frequently-used eviction policy.
 */
class LFUCache[K <: AnyRef, V <: AnyRef](
  cacheFactory: CaffeineCacheFactory,
  initialSize: CacheSize,
  tracer: CacheTracer[K]
) {

  def this(cacheFactory: CaffeineCacheFactory, initialSize: CacheSize) =
    this(cacheFactory, initialSize, new CacheTracer[K] {})

  def this(cacheFactory: CaffeineCacheFactory, initialSize: Int) =
    this(cacheFactory, CacheSize.Static(initialSize), new CacheTracer[K] {})

  val removalListener: RemovalListener[K, V] = (key: K, value: V, cause: RemovalCause) => tracer.discard(key, "")

  private val inner: Cache[K, V] = cacheFactory.createCache(initialSize, removalListener)

  def computeIfAbsent(key: K, f: => V): V = {
    var hit = true

    val result = inner.get(key, (_: K) => { hit = false; f })

    if (hit) {
      tracer.cacheHit(key, "")
    } else {
      tracer.cacheMiss(key, "")
    }

    result
  }

  def get(key: K): Option[V] = {
    val res = Option(inner.getIfPresent(key))
    if (res.isEmpty) tracer.cacheMiss(key, "")
    else tracer.cacheHit(key, "")

    res
  }

  def put(key: K, value: V): Unit = inner.put(key, value)

  def estimatedSize(): Long = inner.estimatedSize()

  def asMap(): ConcurrentMap[K, V] = inner.asMap()

  def invalidate(key: K): Unit = inner.invalidate(key)

  def clear(): Long = {
    val priorSize = inner.estimatedSize()
    inner.invalidateAll()
    inner.cleanUp()
    tracer.cacheFlush(priorSize)
    priorSize
  }
}
