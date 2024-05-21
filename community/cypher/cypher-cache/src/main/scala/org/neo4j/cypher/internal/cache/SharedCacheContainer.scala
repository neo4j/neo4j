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
import com.github.benmanes.caffeine.cache.Policy
import com.github.benmanes.caffeine.cache.stats.CacheStats

import java.lang
import java.util
import java.util.Optional
import java.util.OptionalInt
import java.util.OptionalLong
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.function
import java.util.function.BiFunction
import java.util.function.Consumer

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * The SharedCacheContainer is used for a caching architecture where multiple containers share a single backing cache. Thus multiple 'SharedCacheContainer's are
 * expected to exist where they all share the same 'inner' cache.
 *
 * A 'SharedCacheContainer' should at creation be assigned a unique 'id'. When relaying method calls from 'SharedCacheContainer' to 'inner', keys are
 * transformed such that 'key -> (id, key)'. This ensures that a 'SharedCacheContainer' that share the same backing cache with other containers won't observe
 * entries in the backing cache from the other containers.
 *
 * @param inner A backing Cache that is expected to be the 'inner' cache of other containers as well.
 * @param id A Int assigned to the SharedCacheContainer which is assumed to be unique in order to differentiate between different containers and their entires
 *           in the backing 'inner' cache.
 * @param tracer Tracer for the shared cache.
 * @tparam K The key type of the cache.
 * @tparam V The value type of the cache.
 */
case class SharedCacheContainer[K, V](inner: Cache[(Int, K), V], id: Int, tracer: CacheTracer[K]) extends Cache[K, V] {

  override def get(key: K, mappingFunction: function.Function[_ >: K, _ <: V]): V = {
    var hit = true

    val result = inner.get((id, key), _ => { hit = false; mappingFunction(key) })

    if (hit) {
      tracer.cacheHit(key, "")
    } else {
      tracer.cacheMiss(key, "")
    }

    result
  }

  override def getIfPresent(key: K): V = {
    val result = inner.getIfPresent((id, key))
    Option(result) match {
      case None => tracer.cacheMiss(key, "")
      case _    => tracer.cacheHit(key, "")
    }

    result
  }

  override def put(key: K, value: V): Unit = inner.put((id, key), value)
  override def invalidate(key: K): Unit = inner.invalidate((id, key))

  override def estimatedSize(): Long = {
    var count = 0
    forEachKey(_ => count += 1)
    count
  }
  override def cleanUp(): Unit = inner.cleanUp()
  override def stats(): CacheStats = inner.stats()

  override def invalidateAll(): Unit =
    forEachKey(inner.invalidate)

  private def forEachKey(f: ((Int, K)) => Unit): Unit = {
    val consumer = new Consumer[(Int, K)]() {
      override def accept(key: (Int, K)): Unit = if (key._1 == id) f(key)
    }
    inner.asMap().keySet().forEach(consumer)
  }

  // These could very well be implemented using different approaches. However, we should know the use-case first before choosing an implementation.
  override def getAllPresent(keys: lang.Iterable[_ <: K]): util.Map[K, V] = throw new UnsupportedOperationException

  override def getAll(
    keys: lang.Iterable[_ <: K],
    mappingFunction: function.Function[_ >: util.Set[_ <: K], _ <: util.Map[_ <: K, _ <: V]]
  ): util.Map[K, V] = throw new UnsupportedOperationException
  override def putAll(map: util.Map[_ <: K, _ <: V]): Unit = throw new UnsupportedOperationException
  override def invalidateAll(keys: lang.Iterable[_ <: K]): Unit = throw new UnsupportedOperationException

  override def asMap(): ConcurrentMap[K, V] = new ConcurrentMap[K, V]() {
    private val innerMap = inner.asMap()

    override def size(): Int = throw new UnsupportedOperationException()

    override def isEmpty: Boolean = throw new UnsupportedOperationException()

    override def containsKey(key: Any): Boolean = throw new UnsupportedOperationException()

    override def containsValue(value: Any): Boolean = throw new UnsupportedOperationException()

    override def get(key: Any): V = throw new UnsupportedOperationException()

    override def put(key: K, value: V): V = throw new UnsupportedOperationException()

    override def remove(key: Any): V = throw new UnsupportedOperationException()

    override def putAll(m: util.Map[_ <: K, _ <: V]): Unit = throw new UnsupportedOperationException()

    override def clear(): Unit = throw new UnsupportedOperationException()

    override def keySet(): util.Set[K] = throw new UnsupportedOperationException()

    override def values(): util.Collection[V] = throw new UnsupportedOperationException()

    override def entrySet(): util.Set[java.util.Map.Entry[K, V]] = throw new UnsupportedOperationException()

    override def computeIfAbsent(key: K, mappingFunction: function.Function[_ >: K, _ <: V]): V =
      throw new UnsupportedOperationException()

    override def computeIfPresent(key: K, remappingFunction: BiFunction[_ >: K, _ >: V, _ <: V]): V =
      throw new UnsupportedOperationException()

    override def compute(key: K, remappingFunction: BiFunction[_ >: K, _ >: V, _ <: V]): V =
      throw new UnsupportedOperationException()

    override def merge(key: K, value: V, remappingFunction: BiFunction[_ >: V, _ >: V, _ <: V]): V =
      throw new UnsupportedOperationException()

    override def replaceAll(function: BiFunction[_ >: K, _ >: V, _ <: V]): Unit =
      throw new UnsupportedOperationException()

    override def remove(key: Any, value: Any): Boolean = throw new UnsupportedOperationException()

    override def putIfAbsent(key: K, value: V): V = throw new UnsupportedOperationException()

    override def replace(key: K, oldValue: V, newValue: V): Boolean = {
      innerMap.replace((id, key), oldValue, newValue)
    }

    override def replace(key: K, value: V): V = throw new UnsupportedOperationException()
  }

  override def policy(): Policy[K, V] = new SharedCacheContainer.Policy(inner.policy(), id)
}

object SharedCacheContainer {
  import com.github.benmanes.caffeine.cache.{Policy => CaffeinePolicy}

  private def convertMap[K, V](id: Int, map: util.Map[(Int, K), V]): util.Map[K, V] =
    map
      .asScala
      .collect { case ((i, k), v) if i == id => k -> v }
      .asJava

  private class Policy[K, V](inner: CaffeinePolicy[(Int, K), V], id: Int) extends CaffeinePolicy[K, V] {
    def isRecordingStats: Boolean = inner.isRecordingStats

    def getIfPresentQuietly(key: K): V = inner.getIfPresentQuietly((id, key))

    def refreshes(): util.Map[K, CompletableFuture[V]] = convertMap(id, inner.refreshes())

    def eviction(): Optional[Policy.Eviction[K, V]] =
      inner.eviction().map(new Eviction(_, id))

    def expireAfterAccess(): Optional[Policy.FixedExpiration[K, V]] =
      inner.expireAfterAccess().map(new FixedExpiration(_, id))

    def expireAfterWrite(): Optional[Policy.FixedExpiration[K, V]] =
      inner.expireAfterWrite().map(new FixedExpiration(_, id))

    def expireVariably(): Optional[Policy.VarExpiration[K, V]] =
      inner.expireVariably().map(new VarExpiration(_, id))

    def refreshAfterWrite(): Optional[Policy.FixedRefresh[K, V]] =
      inner.refreshAfterWrite().map(new FixedRefresh(_, id))
  }

  private class Eviction[K, V](inner: Policy.Eviction[(Int, K), V], id: Int) extends Policy.Eviction[K, V] {
    def isWeighted: Boolean = inner.isWeighted
    def weightOf(key: K): OptionalInt = inner.weightOf((id, key))
    def weightedSize(): OptionalLong = inner.weightedSize()
    def getMaximum: Long = inner.getMaximum
    def setMaximum(maximum: Long): Unit = inner.setMaximum(maximum)
    def coldest(limit: Int): util.Map[K, V] = convertMap(id, inner.coldest(limit))
    def hottest(limit: Int): util.Map[K, V] = convertMap(id, inner.hottest(limit))
  }

  private class FixedExpiration[K, V](inner: Policy.FixedExpiration[(Int, K), V], id: Int)
      extends Policy.FixedExpiration[K, V] {
    def ageOf(key: K, unit: TimeUnit): OptionalLong = inner.ageOf((id, key), unit)
    def getExpiresAfter(unit: TimeUnit): Long = inner.getExpiresAfter(unit)
    def setExpiresAfter(duration: Long, unit: TimeUnit): Unit = inner.setExpiresAfter(duration, unit)
    def oldest(limit: Int): util.Map[K, V] = convertMap(id, inner.oldest(limit))
    def youngest(limit: Int): util.Map[K, V] = convertMap(id, inner.youngest(limit))
  }

  private class VarExpiration[K, V](inner: Policy.VarExpiration[(Int, K), V], id: Int)
      extends Policy.VarExpiration[K, V] {
    def getExpiresAfter(key: K, unit: TimeUnit): OptionalLong = inner.getExpiresAfter((id, key), unit)
    def setExpiresAfter(key: K, duration: Long, unit: TimeUnit): Unit = inner.setExpiresAfter((id, key), duration, unit)
    def putIfAbsent(key: K, value: V, dura: Long, unit: TimeUnit): V = inner.putIfAbsent((id, key), value, dura, unit)
    def put(key: K, value: V, duration: Long, unit: TimeUnit): V = inner.put((id, key), value, duration, unit)
    def oldest(limit: Int): util.Map[K, V] = convertMap(id, inner.oldest(limit))
    def youngest(limit: Int): util.Map[K, V] = convertMap(id, inner.youngest(limit))
  }

  private class FixedRefresh[K, V](inner: Policy.FixedRefresh[(Int, K), V], id: Int) extends Policy.FixedRefresh[K, V] {
    def ageOf(key: K, unit: TimeUnit): OptionalLong = inner.ageOf((id, key), unit)
    def getRefreshesAfter(unit: TimeUnit): Long = inner.getRefreshesAfter(unit)
    def setRefreshesAfter(duration: Long, unit: TimeUnit): Unit = inner.setRefreshesAfter(duration, unit)
  }
}
