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
import com.github.benmanes.caffeine.cache.Ticker
import org.neo4j.cypher.internal.cache.CacheSize.Static
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory.CacheConf
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.CacheOp
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.CountingRemovalListener
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.Fill
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.Get
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.GetIfPresent
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.InvalidateAll
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.InvalidateExisting
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.InvalidateKey
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.Put
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.Replace
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.ReplaceExisting
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.StringCache
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.TestTicker
import org.neo4j.cypher.internal.cache.SharedCachePropertyTest.TestedCache
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.util.concurrent.Executor
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Random
import scala.util.Using

class SharedCachePropertyTest extends CypherFunSuite with ScalaFutures {
  val rand = new Random()

  test("estimatedSize after random operations") {
    assertSizeEstAfterRandomOperations() { case (f, size, _) => f.createCache(size) }
  }

  test("estimatedSize after random operations with listener") {
    assertSizeEstAfterRandomOperations() { case (f, size, _) => f.createCache(size, new CountingRemovalListener) }
  }

  test("estimatedSize after random operations with ttl") {
    // Sometimes off by one, for unknown reason.
    // It's ok to be off by one:
    // - We currently don't even use shared cache with ttl.
    // - we only test estimated size here, do not have to be exact.
    val tolerance = 5
    val ttl = rand.between(TimeUnit.MINUTES.toMillis(15), TimeUnit.HOURS.toMillis(1))
    assertSizeEstAfterRandomOperations(tolerance) { case (f, size, ticker) => f.createCache(ticker, ttl, size) }
  }

  test("estimatedSize after random operations with soft backing") {
    // Not exact for unknown reasons.
    // Ok because we only test estimated size here, do not have to be exact.
    val tolerance = 5
    assertSizeEstAfterRandomOperations(tolerance) { case (f, size, _) =>
      f.createWithSoftBackingCache(
        size,
        Static(rand.between(1, 2 * size.currentValue + 1)),
        new CountingRemovalListener
      )
    }
  }

  test("estimatedSize after write expiration") {
    val eviction = Option.when(rand.nextBoolean())(new CountingRemovalListener[String, String])
    val ttlAfterWrite = 1000L
    for {
      size <- Seq(2, 10)
      softValues <- Seq(false, true)
    } {
      Using.resource(new ForkJoinPool(2)) { executor =>
        val ticker = new TestTicker
        val conf =
          CacheConf(executor, Static(size), eviction, None, softValues, Some(ttlAfterWrite), Some(ticker))
        val cache = newFactory(executor).resolveCacheKind("kind").create(conf)

        def getSize = {
          awaitCacheMaintenance(cache, executor)
          cache.estimatedSize()
        }

        withClue(
          s"""size=$size
             |ttlAfterWrite=$ttlAfterWrite
             |softValues=$softValues
             |Cache conf: $conf
             |""".stripMargin
        ) {
          cache.estimatedSize() shouldBe 0

          cache.put("a", "a")
          ticker.addMs(ttlAfterWrite - 1)

          getSize shouldBe 1
          cache.getIfPresent("a") shouldBe "a"

          ticker.addMs(2)
          getSize shouldBe 0
          cache.getIfPresent("a") shouldBe null
        }
      }
    }
  }

  def cacheSizeFourOps = Seq[(StringCache => CacheOp, Int)](
    (GetIfPresent("a"), 0),
    (Get("a", "a"), 1),
    (InvalidateKey("b"), 1),
    (Get("a", "b"), 1),
    (Get("a", "a"), 1),
    (GetIfPresent("a"), 1),
    (Put("a", "a"), 1),
    (Put("a", "b"), 1),
    (Put("a", "a"), 1),
    (Replace("a", "a", "b"), 1),
    (Replace("a", "a", "b"), 1),
    (Replace("a", "b", "a"), 1),
    (Replace("b", "a", "b"), 1),
    (InvalidateKey("b"), 1),
    (InvalidateKey("a"), 0),
    (Put("a2", "a"), 1),
    (Put("b2", "a"), 2),
    (GetIfPresent("b2"), 2), // Avoid early eviction
    (Put("c", "a"), 3),
    (GetIfPresent("c", "c"), 3), // Avoid early eviction
    (Put("d", "a"), 4),
    (GetIfPresent("d", "d", "d"), 4), // Avoid early eviction
    (Put("e", "a"), 4),
    (GetIfPresent("e", "e", "e", "e"), 4), // Avoid early eviction
    (Put("f", "a"), 4),
    (GetIfPresent("f", "f", "f", "f", "f"), 4), // Avoid early eviction
    (Put("g", "a"), 4),
    (GetIfPresent("g", "g", "g", "g", "g", "g"), 4), // Avoid early eviction
    (Put("h", "a"), 4),
    (GetIfPresent("h", "h", "h", "h", "h", "h", "h"), 4), // Avoid early eviction
    (InvalidateKey("a"), 4),
    (InvalidateKey("b"), 4),
    (InvalidateKey("a2"), 4),
    (InvalidateKey("b2"), 4),
    (Replace("c", "a", "b"), 4),
    (Replace("d", "a", "b"), 4),
    (InvalidateKey("e"), 3),
    (InvalidateKey("f"), 2),
    (Get("g", "b"), 2),
    (Get("h", "b"), 2),
    (Get("a", "b"), 3),
    (Get("b", "b"), 4),
    (Get("c", "b"), 4),
    (Get("d", "b"), 4),
    (Get("e", "b"), 4),
    (InvalidateAll(), 0)
  )

  // It's hard to reason about evictions with soft backed cache, since the two caches have separate stats.
  // So the assertions are simplified here.
  def cacheSizeFourOpsSoft = Seq[(StringCache => CacheOp, Int)](
    (GetIfPresent("a"), 0),
    (Get("a", "a"), 1),
    (InvalidateKey("b"), 1),
    (Get("a", "b"), 1),
    (Get("a", "a"), 1),
    (GetIfPresent("a"), 1),
    (Put("a", "a"), 1),
    (Put("a", "b"), 1),
    (Put("a", "a"), 1),
    (Replace("a", "a", "b"), 1),
    (Replace("a", "a", "b"), 1),
    (Replace("a", "b", "a"), 1),
    (Replace("b", "a", "b"), 1),
    (InvalidateKey("b"), 1),
    (InvalidateKey("a"), 0),
    (Put("aa", "a"), 1),
    (Put("bb", "a"), 2),
    (Put("cc", "a"), 3),
    (Put("dd", "a"), 4),
    (Put("e", "a"), 4),
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (Put("f", "a"), 4),
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (Put("g", "a"), 4),
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (Put("h", "a"), 4),
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (GetIfPresent("e", "f", "g", "h"), 4), // Avoid early eviction
    (InvalidateKey("aa"), 4),
    (InvalidateKey("bb"), 4),
    (InvalidateKey("cc"), 4),
    (InvalidateKey("dd"), 4),
    (Replace("cc", "a", "b"), 4),
    (Replace("dd", "a", "b"), 4),
    (InvalidateKey("e"), 3),
    (InvalidateKey("f"), 2),
    (Get("g", "b"), 2),
    (Get("h", "b"), 2),
    (Get("a", "b"), 3),
    (Get("b", "b"), 4),
    (Get("c", "b"), 4),
    (Get("d", "b"), 4),
    (Get("e", "b"), 4),
    (InvalidateAll(), 0)
  )

  test("estimatedSize after various operations sized cache") {
    assertSizes(cacheSizeFourOps)(_.createCache(Static(4)))
    assertSizes(cacheSizeFourOpsSoft)(_.createCache(Static(4)))
  }

  test("estimatedSize after various operations sized cache with removal listener") {
    assertSizes(cacheSizeFourOps)(_.createCache(Static(4), new CountingRemovalListener))
    assertSizes(cacheSizeFourOpsSoft)(_.createCache(Static(4), new CountingRemovalListener))
  }

  test("estimatedSize after various operations sized cache with ttl") {
    assertSizes(cacheSizeFourOps)(_.createCache(new TestTicker, 1000L, Static(4)))
    assertSizes(cacheSizeFourOpsSoft)(_.createCache(new TestTicker, 1000L, Static(4)))
  }

  test("estimatedSize after various operations soft backed cache") {
    assertSizes(cacheSizeFourOpsSoft)(_.createWithSoftBackingCache(Static(2), Static(2), new CountingRemovalListener))
  }

  private def assertSizes(
    opsF: Seq[(StringCache => CacheOp, Int)]
  )(createCache: CaffeineCacheFactory => Cache[String, String]): Unit = {
    Using.resource(new ForkJoinPool(2)) { executor =>
      val factory = newFactory(executor)
      val cache = createCache(factory.resolveCacheKind("kind")).asInstanceOf[SharedCacheContainer[String, String]]
      val ops = opsF.map { case (f, expected) => f(cache) -> expected }
      ops.zipWithIndex.foreach { case ((op, expectedSize), i) =>
        op.run()
        awaitCacheMaintenance(cache, executor)
        val estimatedSize = cache.estimatedSize()
        if (estimatedSize != expectedSize) {
          val cacheState = cache.inner match {
            case c: TwoLayerCache[_, _] =>
              s"""
                 |  primary: ${c.getPrimary.asMap().asScala.mkString("\n  ", "\n  ", "")}
                 |  secondary: ${c.getSecondary.asMap().asScala.mkString("\n  ", "\n  ", "")}
                 |""".stripMargin
            case c => c.asMap().asScala.mkString("\n", "\n", "")
          }
          fail(
            s"""Estimated size is incorrect
               |estimatedSize=$estimatedSize
               |expected=$expectedSize
               |current op=$op
               |inner cache=$cacheState
               |previous ops=${ops.map(_._1).take(i).mkString(", ")}
               |executor=$executor
               |""".stripMargin
          )
        }
      }
    }
  }

  private def newFactory(executor: Executor) = {
    val repo = new CacheTracerRepository {
      override def tracerForCacheKind(kind: String): CacheTracer[?] = new CacheTracer[Any] {}
    }
    new SharedExecutorBasedCaffeineCacheFactory(executor, repo)
  }

  private def assertSizeEstAfterRandomOperations(
    tolerance: Int = 0
  )(
    createCache: (CaffeineCacheFactory, CacheSize, Ticker) => Cache[String, String]
  ): Unit = {
    val opsCount = 75_000
    val size = rand.between(20, 1024)
    val cacheCount = rand.between(1, 16)
    val keyCount = rand.between(1, size / 2)
    val valueCount = rand.between(1, size / 2)
    val kindCount = rand.between(1, 4)

    // Kept in memory to not cause soft value eviction at the point of assertions
    val values = Range(0, valueCount).map(i => s"value-$i")

    def randKey = s"key-${rand.nextInt(keyCount)}"
    def randValue = values(rand.nextInt(valueCount))
    def randKind = s"kind-${rand.nextInt(kindCount)}"

    val ticker = new TestTicker
    val tick = TimeUnit.HOURS.toMillis(24) / opsCount

    Using.resource(new ForkJoinPool()) { executor =>
      implicit val executionContext = ExecutionContext.fromExecutorService(executor)
      val factory = newFactory(executor)

      val caches = IndexedSeq.fill(cacheCount) {
        val kind = randKind
        val cache = createCache(factory.resolveCacheKind(kind), Static(size), ticker)
          .asInstanceOf[SharedCacheContainer[String, String]]
        TestedCache(kind, cache)
      }

      def randCache = caches(rand.nextInt(caches.size))

      val operations = IndexedSeq.fill(opsCount) {
        val testedCache = randCache
        val cache = testedCache.cache
        rand.nextInt(18) match {
          case 0  => Put(randKey, randValue)(cache)
          case 1  => Put(rand.nextLong().toString, randValue)(cache)
          case 2  => Put("static-key", randValue)(cache)
          case 3  => Put("static-key", "static-value")(cache)
          case 4  => Get(randKey, randValue)(cache)
          case 5  => Get(rand.nextLong().toString, randValue)(cache)
          case 6  => GetIfPresent(randKey)(cache)
          case 7  => GetIfPresent("static-key")(cache)
          case 8  => Replace(randKey, randValue, randValue)(cache)
          case 9  => Replace("static-key", randValue, randValue)(cache)
          case 10 => Replace("static-key", "static-value", randValue)(cache)
          case 11 => ReplaceExisting(randValue)(cache)
          case 12 => InvalidateKey(randKey)(cache)
          case 13 => InvalidateKey("static-key")(cache)
          case 14 => InvalidateExisting()(cache)
          case 15 => InvalidateAll()(cache)
          case 16 => Fill("key-", rand.nextInt(size * 2), randValue)(cache)
          case 17 => Fill("", rand.nextInt(size * 2), randValue)(cache)
        }
      }

      // Run each cache operation + clock tick
      Future.sequence(operations.map(op => Future(op.run()).map(_ => ticker.addMs(tick))))
        .isReadyWithin(15.minutes) shouldBe true

      // Perform and wait for db cleanup
      caches.foreach(cache => awaitCacheMaintenance(cache.cache, executor))

      val highestEntries = caches.view.map(_.cache.estimatedSize()).max
      // println("largest cache: " + highestEntries + " of " + size)
      withClue(
        s"""size = $size
           |cacheCount = $cacheCount
           |keyCount = $keyCount
           |valueCount = $valueCount
           |kindCount = $kindCount
           |opsCount = $opsCount
           |executor = $executor
           |largest cache = $highestEntries
           |""".stripMargin
      ) {
        caches.foreach { cache =>
          val backingCache = factory.backingCache(cache.kind).get

          def estimatedSize() = cache.cache.estimatedSize().toInt
          def expectedSize() = backingCache.cache.asMap().keySet().iterator().asScala.count(_._1 == cache.cache.id)

          var retries = 5
          while (retries > 0 && estimatedSize() != expectedSize()) {
            // In case iterating the cache keys cause additional evictions we perform clean-up again
            Thread.sleep(10) // I don't expect this to have any effect, just a desperate attempt to fix flakiness.
            awaitCacheMaintenance(cache.cache, executor)
            retries -= 1
          }
          if (retries <= 0) {
            if (tolerance == 0) estimatedSize() shouldBe expectedSize()
            else estimatedSize() shouldBe expectedSize() +- tolerance
          }
        }
      }
    }
  }

  private def awaitCacheMaintenance(cache: StringCache, executor: ForkJoinPool): Unit = {
    executor.awaitQuiescence(15, TimeUnit.MINUTES) shouldBe true // Await tasks that might add maintenance tasks
    cache.cleanUp()
    executor.awaitQuiescence(15, TimeUnit.MINUTES) shouldBe true // Await tasks triggered by cleanup
  }
}

object SharedCachePropertyTest {
  type StringCache = Cache[String, String]

  sealed trait CacheOp extends Runnable

  case class Put(key: String, value: String)(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.put(key, value)
  }

  case class PutAll(entries: Map[String, String])(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.putAll(entries.asJava)
  }

  case class Fill(prefix: String, entries: Int, value: String)(cache: StringCache) extends CacheOp {
    override def run(): Unit = Range(0, entries).foreach(i => cache.put(prefix + i.toString, value))
  }

  case class Get(key: String, value: String)(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.get(key, _ => value)
  }

  case class GetIfPresent(keys: String*)(cache: StringCache) extends CacheOp {
    override def run(): Unit = keys.foreach(cache.getIfPresent)
  }

  case class Replace(key: String, old: String, newValue: String)(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.asMap().replace(key, old, newValue)
  }

  case class ReplaceExisting(newValue: String)(cache: SharedCacheContainer[String, String]) extends CacheOp {

    override def run(): Unit = {
      cache.inner.asMap().entrySet().iterator().asScala
        .collectFirst { case e if e.getKey._1 == cache.id => e.getKey._2 -> e.getValue }
        .foreach { case (key, value) => cache.asMap().replace(key, value, newValue) }

    }
  }

  case class InvalidateKey(key: String)(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.invalidate(key)
  }

  case class InvalidateExisting()(cache: SharedCacheContainer[String, String]) extends CacheOp {

    override def run(): Unit = {
      cache.inner.asMap().keySet().iterator().asScala
        .collectFirst { case (id, key) if id == cache.id => key }
        .foreach(cache.invalidate)
    }
  }

  case class InvalidateAll()(cache: StringCache) extends CacheOp {
    override def run(): Unit = cache.invalidateAll()
  }

  class TestTicker extends Ticker {
    private val currentTimeNs = new AtomicLong()
    def addMs(delta: Long): Unit = currentTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(delta))
    override def read(): Long = currentTimeNs.get()

    override def toString: String = s"TestTicker(${currentTimeNs.get()})"
  }

  class CountingRemovalListener[K, V] extends RemovalListener[K, V] {
    val count = new LongAdder
    override def onRemoval(key: K, value: V, cause: RemovalCause): Unit = count.increment()
    override def toString: String = s"CountingRemovalListener($count)"
  }

  case class TestedCache(kind: String, cache: SharedCacheContainer[String, String])
}
