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
import org.mockito.Mockito
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.util.Collections
import java.util.concurrent.Executor

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsScala

class SharedCacheContainerTest extends CypherFunSuite {

  case class TestData(
    cacheContainer0: SharedCacheContainer[String, String],
    tracer0: CacheTracer[String],
    cacheContainer1: SharedCacheContainer[String, String],
    tracer1: CacheTracer[String],
    backingCache: Cache[(Int, String), String]
  )

  private def mockCacheTracerRepository: CacheTracerRepository = {
    new CacheTracerRepository {
      override def tracerForCacheKind(kind: String): CacheTracer[?] = mock[CacheTracer[String]]
    }
  }

  private def setup(): TestData = {
    val factory = new SharedExecutorBasedCaffeineCacheFactory(_.run(), mockCacheTracerRepository)
    val size = CacheSize.Static(10)
    val cache0 = factory.resolveCacheKind("a").createCache(size)
      .asInstanceOf[SharedCacheContainer[String, String]]
    val cache1 = factory.resolveCacheKind("a").createCache(size)
      .asInstanceOf[SharedCacheContainer[String, String]]
    cache0.inner shouldBe cache1.inner
    TestData(cache0, cache0.tracer, cache1, cache1.tracer, cache0.inner)
  }

  test("should support put") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    cc0.put("a", "a")
    cc1.put("b", "b")

    // Then
    bc.getIfPresent((cc0.id, "a")) should be("a")
    bc.getIfPresent((cc1.id, "a")) should be(null)
    bc.getIfPresent((cc0.id, "b")) should be(null)
    bc.getIfPresent((cc1.id, "b")) should be("b")

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSeq shouldBe Seq("a")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should support getIfPresent") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc1.id, "b"), "b")

    // Then
    cc0.getIfPresent("a") should be("a")
    cc0.getIfPresent("b") should be(null)
    cc1.getIfPresent("a") should be(null)
    cc1.getIfPresent("b") should be("b")

    val o0 = Mockito.inOrder(t0)
    o0.verify(t0).cacheHit("a", "")
    o0.verify(t0).cacheMiss("b", "")
    verifyNoMoreInteractions(t0)
    val o1 = Mockito.inOrder(t1)
    o1.verify(t1).cacheMiss("a", "")
    o1.verify(t1).cacheHit("b", "")
    verifyNoMoreInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSeq shouldBe Seq("a")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should support get") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc1.id, "b"), "b")

    // Then
    cc0.get("a", x => x) should be("a")
    cc0.get("b", x => x) should be("b")
    cc1.get("a", x => x) should be("a")
    cc1.get("b", x => x) should be("b")

    bc.getIfPresent((cc0.id, "a")) should be("a")
    bc.getIfPresent((cc1.id, "a")) should be("a")
    bc.getIfPresent((cc0.id, "b")) should be("b")
    bc.getIfPresent((cc1.id, "b")) should be("b")

    val o0 = Mockito.inOrder(t0)
    o0.verify(t0).cacheHit("a", "")
    o0.verify(t0).cacheMiss("b", "")
    verifyNoMoreInteractions(t0)
    val o1 = Mockito.inOrder(t1)
    o1.verify(t1).cacheMiss("a", "")
    o1.verify(t1).cacheHit("b", "")
    verifyNoMoreInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSet shouldBe Set("a", "b")
    cc1.asMap().values().iterator().asScala.toSet shouldBe Set("b", "a")
  }

  test("should support invalidate") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc1.id, "b"), "b")

    cc0.invalidate("a")
    cc1.invalidate("b")

    // Then
    bc.getIfPresent((cc0.id, "a")) should be(null)
    bc.getIfPresent((cc1.id, "a")) should be(null)
    bc.getIfPresent((cc0.id, "b")) should be(null)
    bc.getIfPresent((cc1.id, "b")) should be(null)

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSeq shouldBe Seq()
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq()
  }

  test("should support estimatedSize") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    cc0.put("a", "a")
    cc0.put("b", "b")
    cc1.put("b", "b")

    // Then
    cc0.estimatedSize() should be(2)
    cc1.estimatedSize() should be(1)

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSet shouldBe Set("a", "b")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should support cleanUp") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc0.id, "b"), "b")
    bc.put((cc1.id, "b"), "b")

    // Then
    noException should be thrownBy cc0.cleanUp()
    noException should be thrownBy cc1.cleanUp()

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSet shouldBe Set("a", "b")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should support stats") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc0.id, "b"), "b")
    bc.put((cc1.id, "b"), "b")

    // Then
    // Currently, the implementation simply forwards `stats` to the backing cache.
    // That is not accurate, therefore no real assertion is added here at the moment.
    noException should be thrownBy cc0.stats()
    noException should be thrownBy cc1.stats()

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSet shouldBe Set("a", "b")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should support invalidateAll") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc0.id, "b"), "b")
    bc.put((cc1.id, "b"), "b")

    cc0.invalidateAll()

    // Then
    bc.getIfPresent((cc0.id, "a")) should be(null)
    bc.getIfPresent((cc1.id, "a")) should be(null)
    bc.getIfPresent((cc0.id, "b")) should be(null)
    bc.getIfPresent((cc1.id, "b")) should be("b")

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSeq shouldBe Seq()
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("should throw on unsupported methods") {
    val TestData(cc0, _, _, _, _) = setup()

    // When
    an[UnsupportedOperationException] should be thrownBy cc0.getAllPresent(Seq("a", "b").asJava)
    an[UnsupportedOperationException] should be thrownBy cc0.getAll(
      Seq("a", "b").asJava,
      set => set.asScala.map(x => (x, x)).toMap.asJava
    )
    an[UnsupportedOperationException] should be thrownBy cc0.putAll(
      Collections.singletonMap("a", "a")
    )
    an[UnsupportedOperationException] should be thrownBy cc0.invalidateAll(Seq("a", "b").asJava)

    val map = cc0.asMap()
    an[UnsupportedOperationException] should be thrownBy map.size()
    an[UnsupportedOperationException] should be thrownBy map.isEmpty
    an[UnsupportedOperationException] should be thrownBy map.containsKey("a")
    an[UnsupportedOperationException] should be thrownBy map.containsValue("a")
    an[UnsupportedOperationException] should be thrownBy map.get("a")
    an[UnsupportedOperationException] should be thrownBy map.put("a", "a")
    an[UnsupportedOperationException] should be thrownBy map.remove("a")
    an[UnsupportedOperationException] should be thrownBy map.putAll(Collections.singletonMap("a", "a"))
    an[UnsupportedOperationException] should be thrownBy map.clear()
    an[UnsupportedOperationException] should be thrownBy map.keySet()
    an[UnsupportedOperationException] should be thrownBy map.entrySet()
    an[UnsupportedOperationException] should be thrownBy map.computeIfAbsent("a", x => x)
    an[UnsupportedOperationException] should be thrownBy map.computeIfPresent("a", (x, _) => x)
    an[UnsupportedOperationException] should be thrownBy map.compute("a", (x, _) => x)
    an[UnsupportedOperationException] should be thrownBy map.merge("a", "b", (x, _) => x)
    an[UnsupportedOperationException] should be thrownBy map.replaceAll((x, _) => x)
    an[UnsupportedOperationException] should be thrownBy map.remove("a")
    an[UnsupportedOperationException] should be thrownBy map.putIfAbsent("a", "a")
    an[UnsupportedOperationException] should be thrownBy map.replace("a", "a")
  }

  test("should support asMap().replace(K, V, V)") {
    val TestData(cc0, t0, cc1, t1, bc) = setup()

    // When
    bc.put((cc0.id, "a"), "a")
    bc.put((cc0.id, "b"), "b")
    bc.put((cc1.id, "b"), "b")

    cc0.asMap().replace("a", "a", "A") should be(true)
    cc0.asMap().replace("b", "a", "A") should be(false)

    // Then
    bc.getIfPresent((cc0.id, "a")) should be("A")
    bc.getIfPresent((cc1.id, "a")) should be(null)
    bc.getIfPresent((cc0.id, "b")) should be("b")
    bc.getIfPresent((cc1.id, "b")) should be("b")

    verifyNoInteractions(t0)
    verifyNoInteractions(t1)

    cc0.asMap().values().iterator().asScala.toSet shouldBe Set("A", "b")
    cc1.asMap().values().iterator().asScala.toSeq shouldBe Seq("b")
  }

  test("replacing value does not increment estimated size") {
    object executor extends Executor {
      val commands: mutable.Buffer[Runnable] = mutable.Buffer[Runnable]()
      override def execute(command: Runnable): Unit = commands.append(command)
    }

    val cache =
      new SharedExecutorBasedCaffeineCacheFactory(executor, mockCacheTracerRepository)
        .resolveCacheKind("cache-kind")
        .createCache(CacheSize.Static(10))
        .asInstanceOf[SharedCacheContainer[String, String]]

    cache.estimatedSize() shouldBe 0

    cache.put("key", "value")
    cache.estimatedSize() shouldBe 1

    cache.put("key", "new-value")
    cache.estimatedSize() shouldBe 1

    cache.put("key", "new-new-value")
    cache.estimatedSize() shouldBe 1

    // sends onRemoval notifications
    executor.commands.foreach(_.run())
    cache.estimatedSize() shouldBe 1
  }

  // policy method remains untested for now.
}
