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
package org.neo4j.cypher.internal.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Policy
import com.github.benmanes.caffeine.cache.Ticker
import com.github.benmanes.caffeine.cache.stats.CacheStats

import java.lang
import java.util
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function
import java.util.function.Consumer

import scala.collection.concurrent.TrieMap

object ExecutorBasedCaffeineCacheFactory {

  def createCache[K <: AnyRef, V <: AnyRef](executor: Executor, size: Int): Cache[K, V] = {
    Caffeine
      .newBuilder()
      .executor(executor)
      .maximumSize(size)
      .build[K, V]()
  }

  def createCache[K <: AnyRef, V <: AnyRef](executor: Executor, size: Int, ttlAfterAccess: Long): Cache[K, V] = {
    Caffeine
      .newBuilder()
      .executor(executor)
      .maximumSize(size)
      .expireAfterAccess(ttlAfterAccess, TimeUnit.MILLISECONDS)
      .build[K, V]()
  }

  def createCache[K <: AnyRef, V <: AnyRef](
    executor: Executor,
    ticker: Ticker,
    ttlAfterWrite: Long,
    size: Int
  ): Cache[K, V] =
    Caffeine
      .newBuilder()
      .executor(executor)
      .maximumSize(size)
      .ticker(ticker)
      .expireAfterWrite(ttlAfterWrite, TimeUnit.MILLISECONDS)
      .build[K, V]()
}

trait CacheFactory {
  def resolveCacheKind(kind: String): CaffeineCacheFactory
}

trait CaffeineCacheFactory extends CacheFactory {
  def createCache[K <: AnyRef, V <: AnyRef](size: Int): Cache[K, V]
  def createCache[K <: AnyRef, V <: AnyRef](size: Int, ttlAfterAccess: Long): Cache[K, V]
  def createCache[K <: AnyRef, V <: AnyRef](ticker: Ticker, ttlAfterWrite: Long, size: Int): Cache[K, V]
}

class ExecutorBasedCaffeineCacheFactory(executor: Executor) extends CaffeineCacheFactory {

  override def createCache[K <: AnyRef, V <: AnyRef](size: Int): Cache[K, V] = {
    ExecutorBasedCaffeineCacheFactory.createCache(executor, size)
  }

  override def createCache[K <: AnyRef, V <: AnyRef](size: Int, ttlAfterAccess: Long): Cache[K, V] = {
    ExecutorBasedCaffeineCacheFactory.createCache(executor, size, ttlAfterAccess)
  }

  override def createCache[K <: AnyRef, V <: AnyRef](ticker: Ticker, ttlAfterWrite: Long, size: Int): Cache[K, V] =
    ExecutorBasedCaffeineCacheFactory.createCache(executor, ticker, ttlAfterWrite, size)

  override def resolveCacheKind(kind: String): CaffeineCacheFactory = this
}

class SharedExecutorBasedCaffeineCacheFactory(executor: Executor) extends CacheFactory {
  self =>

  private val caches: TrieMap[String, Cache[_, _]] = scala.collection.concurrent.TrieMap()

  def getCacheSizeOf(kind: String): Long = {
    caches.get(kind) match {
      case Some(cache) => cache.estimatedSize()
      case None        => 0L
    }
  }

  def createCache[K <: AnyRef, V <: AnyRef](size: Int, cacheKind: String): Cache[K, V] = {
    SharedCacheContainer(
      caches.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache[(Int, K), V](executor, size)
      ).asInstanceOf[Cache[(Int, K), V]]
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](size: Int, ttlAfterAccess: Long, cacheKind: String): Cache[K, V] = {
    SharedCacheContainer(
      caches.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache(executor, size, ttlAfterAccess)
      ).asInstanceOf[Cache[(Int, K), V]]
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](
    ticker: Ticker,
    ttlAfterWrite: Long,
    size: Int,
    cacheKind: String
  ): Cache[K, V] = {
    SharedCacheContainer(
      caches.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache(executor, ticker, ttlAfterWrite, size)
      ).asInstanceOf[Cache[(Int, K), V]]
    )
  }

  override def resolveCacheKind(kind: String): CaffeineCacheFactory = new CaffeineCacheFactory {
    override def createCache[K <: AnyRef, V <: AnyRef](size: Int): Cache[K, V] = self.createCache(size, kind)

    override def createCache[K <: AnyRef, V <: AnyRef](size: Int, ttlAfterAccess: Long): Cache[K, V] =
      self.createCache(size, ttlAfterAccess, kind)

    override def createCache[K <: AnyRef, V <: AnyRef](ticker: Ticker, ttlAfterWrite: Long, size: Int): Cache[K, V] =
      self.createCache(ticker, ttlAfterWrite, size, kind)
    override def resolveCacheKind(kind: String): CaffeineCacheFactory = this
  }
}

object SharedCacheContainer {
  private val id = new AtomicInteger(0)

  def apply[K, V](inner: Cache[(Int, K), V]): SharedCacheContainer[K, V] = new SharedCacheContainer(inner, id.getAndIncrement())
}

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
 * @tparam K The key type of the cache.
 * @tparam V The value type of the cache.
 */
private class SharedCacheContainer[K, V](inner: Cache[(Int, K), V], id: Int) extends Cache[K, V] {

  override def get(key: K, mappingFunction: function.Function[_ >: K, _ <: V]): V =
    inner.get((id, key), _ => mappingFunction(key))
  override def getIfPresent(key: K): V = inner.getIfPresent((id, key))
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
  override def asMap(): ConcurrentMap[K, V] = throw new UnsupportedOperationException
  override def policy(): Policy[K, V] = throw new UnsupportedOperationException
}
