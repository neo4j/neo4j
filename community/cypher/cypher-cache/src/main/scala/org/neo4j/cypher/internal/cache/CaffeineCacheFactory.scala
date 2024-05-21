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
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import com.github.benmanes.caffeine.cache.Ticker

import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap

object ExecutorBasedCaffeineCacheFactory {

  /**
   * Please note that this constructor creates a cache with a synchronous executor. Caffeine would normally perform
   * a number of background operations such as expiring entries and compacting the cache which it would ideally perform
   * on a pool of background threads. It is recommended that you use [[org.neo4j.scheduler.MonitoredJobExecutor]] for
   * this as it would give users observability into the thread pool and your cache, as well as improve cache
   * performance. Instead, use this constructor when all you need is a Map with a size limit and evictions.
   */
  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize): Cache[K, V] = {
    val currentThreadExecutor = new Executor {
      def execute(command: Runnable): Unit = command.run()
    }
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(currentThreadExecutor)
        .maximumSize(size)
        .build[K, V]()
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](executor: Executor, size: CacheSize): Cache[K, V] =
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(executor)
        .maximumSize(size)
        .build[K, V]()
    )

  def createCache[K <: AnyRef, V <: AnyRef](
    executor: Executor,
    removalListener: RemovalListener[K, V],
    size: CacheSize
  ): Cache[K, V] =
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(executor)
        .maximumSize(size)
        .evictionListener(removalListener)
        .build[K, V]()
    )

  def createSoftValuesCache[K <: AnyRef, V <: AnyRef](
    executor: Executor,
    removalListener: RemovalListener[K, V],
    size: CacheSize
  ): Cache[K, V] = {
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(executor)
        .maximumSize(size)
        .evictionListener(removalListener)
        .softValues()
        .build[K, V]()
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](executor: Executor, size: CacheSize, ttlAfterAccess: Long): Cache[K, V] = {
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(executor)
        .maximumSize(size)
        .expireAfterAccess(ttlAfterAccess, TimeUnit.MILLISECONDS)
        .build[K, V]()
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](
    executor: Executor,
    ticker: Ticker,
    ttlAfterWrite: Long,
    size: CacheSize
  ): Cache[K, V] =
    size.withSize[K, V, Cache[K, V]](size =>
      Caffeine
        .newBuilder()
        .executor(executor)
        .maximumSize(size)
        .ticker(ticker)
        .expireAfterWrite(ttlAfterWrite, TimeUnit.MILLISECONDS)
        .build[K, V]()
    )

}

trait CacheFactory {
  def resolveCacheKind(kind: String): CaffeineCacheFactory
}

trait CaffeineCacheFactory extends CacheFactory {
  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize): Cache[K, V]
  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, removalListener: RemovalListener[K, V]): Cache[K, V]

  def createWithSoftBackingCache[K <: AnyRef, V <: AnyRef](
    primarySize: CacheSize,
    secondarySize: CacheSize,
    removalListener: RemovalListener[K, V]
  ): Cache[K, V]

  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, ttlAfterAccess: Long): Cache[K, V]
  def createCache[K <: AnyRef, V <: AnyRef](ticker: Ticker, ttlAfterWrite: Long, size: CacheSize): Cache[K, V]
}

trait CacheTracerRepository {
  def tracerForCacheKind(cacheKind: String): CacheTracer[_]
}

class ExecutorBasedCaffeineCacheFactory(executor: Executor) extends CaffeineCacheFactory {

  override def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize): Cache[K, V] = {
    ExecutorBasedCaffeineCacheFactory.createCache(executor, size)
  }

  override def createCache[K <: AnyRef, V <: AnyRef](
    size: CacheSize,
    removalListener: RemovalListener[K, V]
  ): Cache[K, V] = {
    ExecutorBasedCaffeineCacheFactory.createCache(executor, removalListener, size)
  }

  override def createWithSoftBackingCache[K <: AnyRef, V <: AnyRef](
    strongSize: CacheSize,
    softSize: CacheSize,
    removalListener: RemovalListener[K, V]
  ): Cache[K, V] = {
    val secondary = ExecutorBasedCaffeineCacheFactory.createSoftValuesCache(executor, removalListener, softSize)
    val primary = ExecutorBasedCaffeineCacheFactory.createCache(
      executor,
      TwoLayerCache.evictionListener(secondary),
      strongSize
    )
    new TwoLayerCache[K, V](primary, secondary)
  }

  override def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, ttlAfterAccess: Long): Cache[K, V] = {
    ExecutorBasedCaffeineCacheFactory.createCache(executor, size, ttlAfterAccess)
  }

  override def createCache[K <: AnyRef, V <: AnyRef](
    ticker: Ticker,
    ttlAfterWrite: Long,
    size: CacheSize
  ): Cache[K, V] =
    ExecutorBasedCaffeineCacheFactory.createCache(executor, ticker, ttlAfterWrite, size)

  override def resolveCacheKind(kind: String): CaffeineCacheFactory = this
}

class SharedExecutorBasedCaffeineCacheFactory(executor: Executor, val cacheTracerRepository: CacheTracerRepository)
    extends CacheFactory {
  self =>

  case class InternalRemovalListener[K, V](listener: RemovalListener[K, V]) extends RemovalListener[(Int, K), V] {
    private val externalListeners: TrieMap[Int, RemovalListener[K, V]] = scala.collection.concurrent.TrieMap()
    private val sharedCacheListener: RemovalListener[K, V] = listener

    override def onRemoval(key: (Int, K), value: V, cause: RemovalCause): Unit = key match {
      case (id, innerKey) =>
        externalListeners.get(id).foreach(_.onRemoval(innerKey, value, cause))
        sharedCacheListener.onRemoval(innerKey, value, cause)
    }

    def registerExternalListener(id: Int, listener: RemovalListener[K, V]): Unit =
      externalListeners.update(id, listener)
  }

  private val cacheKindToCache: TrieMap[String, Cache[_, _]] = scala.collection.concurrent.TrieMap()

  private val cacheKindToListener: TrieMap[String, InternalRemovalListener[_, _]] =
    scala.collection.concurrent.TrieMap()

  def getCacheSizeOf(kind: String): Long = {
    cacheKindToCache.get(kind) match {
      case Some(cache) =>
        cache.cleanUp()
        cache.estimatedSize()
      case None => 0L
    }
  }

  /**
   * Call this to get better cache statistics
   */
  def cleanUpCache(kind: String): Unit = {
    cacheKindToCache.get(kind).foreach(_.cleanUp())
  }

  /**
   * For testing purposes only.
   */
  def invalidateAllEntries(kind: String): Unit = {
    cacheKindToCache.get(kind).foreach(_.invalidateAll())
  }

  private def tracer[K <: AnyRef](cacheKind: String): CacheTracer[K] = {
    cacheTracerRepository.tracerForCacheKind(cacheKind).asInstanceOf[CacheTracer[K]]
  }

  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, cacheKind: String): Cache[K, V] = {
    SharedCacheContainer(
      cacheKindToCache.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache[(Int, K), V](executor, size)
      ).asInstanceOf[Cache[(Int, K), V]],
      SharedCacheContainerIdGen.getNewId,
      tracer(cacheKind)
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](
    size: CacheSize,
    removalListener: RemovalListener[K, V],
    cacheKind: String
  ): Cache[K, V] = {
    val id = SharedCacheContainerIdGen.getNewId
    val globalTracer: CacheTracer[K] = tracer(cacheKind)
    val globalRemovalListener: RemovalListener[K, V] =
      (key: K, value: V, cause: RemovalCause) => globalTracer.discard(key, "")
    val internalRemovalListener =
      cacheKindToListener.getOrElseUpdate(cacheKind, InternalRemovalListener(globalRemovalListener)).asInstanceOf[
        InternalRemovalListener[K, V]
      ]
    internalRemovalListener.registerExternalListener(id, removalListener)

    SharedCacheContainer(
      cacheKindToCache.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache[(Int, K), V](executor, internalRemovalListener, size)
      ).asInstanceOf[Cache[(Int, K), V]],
      id,
      globalTracer
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, ttlAfterAccess: Long, cacheKind: String): Cache[K, V] = {
    SharedCacheContainer(
      cacheKindToCache.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache(executor, size, ttlAfterAccess)
      ).asInstanceOf[Cache[(Int, K), V]],
      SharedCacheContainerIdGen.getNewId,
      tracer(cacheKind)
    )
  }

  def createCache[K <: AnyRef, V <: AnyRef](
    ticker: Ticker,
    ttlAfterWrite: Long,
    size: CacheSize,
    cacheKind: String
  ): Cache[K, V] = {
    SharedCacheContainer(
      cacheKindToCache.getOrElseUpdate(
        cacheKind,
        ExecutorBasedCaffeineCacheFactory.createCache(executor, ticker, ttlAfterWrite, size)
      ).asInstanceOf[Cache[(Int, K), V]],
      SharedCacheContainerIdGen.getNewId,
      tracer(cacheKind)
    )
  }

  def createWithSoftBackingCache[K <: AnyRef, V <: AnyRef](
    strongSize: CacheSize,
    softSize: CacheSize,
    removalListener: RemovalListener[K, V],
    cacheKind: String
  ): Cache[K, V] = {

    val id = SharedCacheContainerIdGen.getNewId
    val globalTracer: CacheTracer[K] = tracer(cacheKind)
    val globalRemovalListener: RemovalListener[K, V] =
      (key: K, value: V, cause: RemovalCause) => globalTracer.discard(key, "")
    val internalRemovalListener =
      cacheKindToListener.getOrElseUpdate(cacheKind, InternalRemovalListener(globalRemovalListener)).asInstanceOf[
        InternalRemovalListener[K, V]
      ]
    internalRemovalListener.registerExternalListener(id, removalListener)

    def newCache: Cache[(Int, K), V] = {
      val secondary = ExecutorBasedCaffeineCacheFactory.createSoftValuesCache[(Int, K), V](
        executor,
        internalRemovalListener,
        softSize
      )
      val primary = ExecutorBasedCaffeineCacheFactory.createCache[(Int, K), V](
        executor,
        TwoLayerCache.evictionListener(secondary),
        strongSize
      )
      new TwoLayerCache[(Int, K), V](primary, secondary)
    }
    SharedCacheContainer(
      cacheKindToCache.getOrElseUpdate(
        cacheKind,
        newCache
      ).asInstanceOf[Cache[(Int, K), V]],
      id,
      globalTracer
    )
  }

  override def resolveCacheKind(kind: String): CaffeineCacheFactory = new CaffeineCacheFactory {
    override def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize): Cache[K, V] = self.createCache(size, kind)

    override def createCache[K <: AnyRef, V <: AnyRef](
      size: CacheSize,
      removalListener: RemovalListener[K, V]
    ): Cache[K, V] =
      self.createCache(size, removalListener, kind)

    override def createCache[K <: AnyRef, V <: AnyRef](size: CacheSize, ttlAfterAccess: Long): Cache[K, V] =
      self.createCache(size, ttlAfterAccess, kind)

    override def createCache[K <: AnyRef, V <: AnyRef](
      ticker: Ticker,
      ttlAfterWrite: Long,
      size: CacheSize
    ): Cache[K, V] =
      self.createCache(ticker, ttlAfterWrite, size, kind)
    override def resolveCacheKind(kind: String): CaffeineCacheFactory = this

    override def createWithSoftBackingCache[K <: AnyRef, V <: AnyRef](
      primarySize: CacheSize,
      secondarySize: CacheSize,
      removalListener: RemovalListener[K, V]
    ): Cache[K, V] =
      self.createWithSoftBackingCache(primarySize, secondarySize, removalListener, kind)
  }
}

object SharedCacheContainerIdGen {
  private val id = new AtomicInteger(0)

  def getNewId: Int = id.getAndIncrement()
}
