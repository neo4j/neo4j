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
import com.github.benmanes.caffeine.cache.Policy
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import com.github.benmanes.caffeine.cache.Ticker
import com.github.benmanes.caffeine.cache.stats.CacheStats

import java.lang
import java.util
import java.util.Optional
import java.util.OptionalInt
import java.util.OptionalLong
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function
import java.util.function.Consumer

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

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
  override def asMap(): ConcurrentMap[K, V] = throw new UnsupportedOperationException
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
