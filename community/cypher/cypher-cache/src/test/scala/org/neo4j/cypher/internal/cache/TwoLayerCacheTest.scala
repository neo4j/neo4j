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
import com.github.benmanes.caffeine.cache.Ticker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicLong

class TwoLayerCacheTest extends CypherFunSuite {

  test("should put a value ") {
    val (cache, _, _, _) = setup()

    cache.put("first", "first")

    cache.getIfPresent("first") should be("first")
  }

  test("should putAll values") {
    val (cache, _, _, _) = setup()

    cache.putAll(util.Map.of("first", "first", "second", "second"))

    cache.getIfPresent("first") should be("first")
    cache.getIfPresent("second") should be("second")
  }

  test("should get values from primary and secondary cache") {
    val (cache, primary, secondary, _) = setup()

    primary.put("first", "first")
    secondary.put("second", "second")

    cache.getIfPresent("first") should be("first")
    cache.getIfPresent("second") should be("second")
  }

  test("should compute missing value") {
    val (cache, primary, _, _) = setup()

    primary.getIfPresent("first") should be(null)
    cache.get("first", (key) => "first") should be("first")
    primary.getIfPresent("first") should be("first")
  }

  test("should throw exception on unsupported methods") {
    val (cache, _, _, _) = setup()

    assertThrows[UnsupportedOperationException] {
      cache.getAllPresent(util.List.of("first", "second"))
    }

    assertThrows[UnsupportedOperationException] {
      cache.getAll(util.List.of("first", "second"), _ => util.Map.of())
    }

    val map = cache.asMap()
    assertThrows[UnsupportedOperationException] {
      map.put("key", "new value")
    }
    assertThrows[UnsupportedOperationException] {
      map.remove("key")
    }
    assertThrows[UnsupportedOperationException] {
      map.putAll(util.Map.of("key", "new value"))
    }
    assertThrows[UnsupportedOperationException] {
      map.clear()
    }
    assertThrows[UnsupportedOperationException] {
      map.computeIfAbsent("first", _ => "new value")
    }
    assertThrows[UnsupportedOperationException] {
      map.computeIfPresent("key", (_, _) => "new value")
    }
    assertThrows[UnsupportedOperationException] {
      map.merge("key", "new value", (_, _) => "merged value")
    }
    assertThrows[UnsupportedOperationException] {
      map.compute("key", (_, _) => "new value")
    }
    assertThrows[UnsupportedOperationException] {
      map.replaceAll((_, _) => "new value")
    }
    assertThrows[UnsupportedOperationException] {
      map.remove("key", "value")
    }
    assertThrows[UnsupportedOperationException] {
      map.putIfAbsent("key", "value")
    }
    assertThrows[UnsupportedOperationException] {
      map.replace("key", "new value")
    }
  }

  test("should evict from primary to secondary on maximum size == 0") {
    val (cache, primary, secondary, _) = setup(primarySize = 0, secondarySize = 1)

    cache.put("first", "first")

    cache.getIfPresent("first") should be("first")
    primary.getIfPresent("first") should be(null)
    secondary.getIfPresent("first") should be("first")
  }

  test("should evict from primary to secondary on size > 0") {
    val (cache, primary, secondary, _) = setup(primarySize = 1, secondarySize = 1)

    cache.put("first", "first")
    cache.put("second", "second")

    primary.getIfPresent("first") should be(null)
    primary.getIfPresent("second") should be("second")
    secondary.getIfPresent("first") should be("first")
  }

  test("should evict from secondary on maximum size == 0") {
    val (cache, primary, secondary, _) = setup(primarySize = 0, secondarySize = 0)

    cache.put("first", "first")

    cache.getIfPresent("first") should be(null)
    primary.getIfPresent("first") should be(null)
    secondary.getIfPresent("first") should be(null)
  }

  test("should evict from secondary on maximum size > 0 ") {
    val (cache, primary, secondary, _) = setup(primarySize = 1, secondarySize = 1)

    cache.put("first", "first")
    cache.put("second", "second")
    cache.put("third", "third")

    cache.getIfPresent("first") should be(null)
    cache.getIfPresent("second") should be("second")
    cache.getIfPresent("third") should be("third")

    primary.getAllPresent(util.List.of("first", "second", "third")) should be(util.Map.of("third", "third"))
    secondary.getAllPresent(util.List.of("first", "second", "third")) should be(util.Map.of("second", "second"))
  }

  test("should evict on ttl") {
    val (cache, primary, secondary, ticker) = setup()

    cache.put("first", "first")

    cache.getIfPresent("first") should be("first")
    primary.getIfPresent("first") should be("first")
    secondary.getIfPresent("first") should be(null)

    ticker.tick(1000)
    cache.cleanUp()

    cache.getIfPresent("first") should be("first")
    primary.getIfPresent("first") should be(null)
    secondary.getIfPresent("first") should be("first")

    ticker.tick(1000)
    cache.cleanUp()

    cache.getIfPresent("first") should be(null)
    primary.getIfPresent("first") should be(null)
    secondary.getIfPresent("first") should be(null)
  }

  test("should invalidate all") {
    val (cache, primary, secondary, _) = setup()

    primary.put("first", "first")
    secondary.put("second", "second")

    cache.getIfPresent("first") should be("first")
    cache.getIfPresent("second") should be("second")

    cache.invalidateAll()

    cache.getIfPresent("first") should be(null)
    cache.getIfPresent("second") should be(null)
  }

  test("should accumulate estimated size") {
    val (cache, primary, secondary, _) = setup()

    primary.put("first", "first")
    secondary.put("second", "second")

    cache.estimatedSize() should be(2)
  }

  test("should return all entries from asMap") {
    val (cache, primary, secondary, _) = setup()

    primary.put("first", "first")
    secondary.put("second", "second")
    secondary.put("first", "not first")

    cache.asMap() should be(util.Map.of("first", "first", "second", "second"))
  }

  test("asMap should support replace") {
    val (cache, primary, secondary, _) = setup(primarySize = 1, secondarySize = 1)

    cache.put("first", "first")
    cache.put("second", "second")

    // Replace value in primary cache
    cache.asMap().replace("second", "second", "2nd") should be(true)
    // Replace value in secondary cache
    cache.asMap().replace("first", "first", "1st") should be(true)

    // Check underlying caches are correct
    primary.getIfPresent("second") should be("2nd")
    secondary.getIfPresent("first") should be("1st")
  }

  private def setup(
    primarySize: Int = 10,
    secondarySize: Int = 10
  ): (TwoLayerCache[String, String], Cache[String, String], Cache[String, String], FakeTicker) = {

    val ticker = new FakeTicker()
    val secondary =
      Caffeine.newBuilder().maximumSize(secondarySize).ticker(ticker).expireAfterWrite(Duration.ofNanos(1000)).executor(
        (runnable: Runnable) => runnable.run()
      ).build[String, String]()

    val primary = Caffeine.newBuilder().maximumSize(primarySize).ticker(ticker).expireAfterWrite(
      Duration.ofNanos(1000)
    ).evictionListener(
      TwoLayerCache.evictionListener(secondary)
    ).executor((runnable: Runnable) => runnable.run()).build[String, String]()

    val cache = new TwoLayerCache(primary, secondary)
    (cache, primary, secondary, ticker)
  }

  class FakeTicker() extends Ticker {
    val nanos = new AtomicLong(0L)

    override def read(): Long = nanos.get()

    def tick(nanoseconds: Long): Unit = nanos.addAndGet(nanoseconds)
  }

}
