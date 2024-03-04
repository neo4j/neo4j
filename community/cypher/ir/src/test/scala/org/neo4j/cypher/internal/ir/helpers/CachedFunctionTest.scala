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
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.ir.helpers.CachedFunction.CacheKey
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CachedFunctionTest extends CypherFunSuite {

  test("1 argument") {
    var i = 0
    val f = (_: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(3)
    cachedF(3)
    cachedF(2)
    cachedF(2)
    cachedF(4)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("2 arguments") {
    var i = 0
    val f = (_: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2)
    cachedF(1, 2)
    cachedF(1, 3)
    cachedF(4, 5)
    cachedF(4, 5)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("3 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3)
    cachedF(1, 2, 3)
    cachedF(1, 3, 4)
    cachedF(4, 5, 1)
    cachedF(4, 5, 1)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("4 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3, 4)
    cachedF(1, 2, 2, 1)
    cachedF(1, 2, 2, 1)
    cachedF(1, 2, 3, 4)
    cachedF(4, 3, 2, 1)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("5 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3, 4, 5)
    cachedF(1, 2, 2, 1, 2)
    cachedF(1, 2, 2, 1, 2)
    cachedF(1, 2, 3, 4, 5)
    cachedF(4, 3, 2, 1, 0)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("6 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3, 4, 5, 6)
    cachedF(1, 2, 3, 4, 5, 6)
    cachedF(1, 1, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("7 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3, 4, 5, 6, 7)
    cachedF(1, 2, 3, 4, 5, 6, 7)
    cachedF(1, 1, 1, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1, 1)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("8 arguments") {
    var i = 0
    val f = (_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => {
      i += 1
      5
    }
    val cachedF = CachedFunction(f)

    cachedF(1, 2, 3, 4, 5, 6, 7, 8)
    cachedF(1, 2, 3, 4, 5, 6, 7, 8)
    cachedF(1, 1, 1, 1, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1, 1, 1)
    cachedF(4, 5, 1, 1, 1, 1, 1, 1)

    i should be(3)
    cachedF.cacheSize shouldBe i
  }

  test("should cache calls with different values that evaluate to the same cache key") {
    var i = 0
    def f(s: String): Int = {
      i += 1
      123 + s.length
    }
    def g(key: CacheKey[Int, String]) = f(key.value)
    val cachedG = CachedFunction(g _)
    def cacheKey(s: String) = CacheKey.computeFrom(s)(_.length)

    cachedG(cacheKey("abc"))
    cachedG(cacheKey("123"))
    cachedG(cacheKey("zzz"))
    cachedG(cacheKey("hello"))

    i shouldBe 2
  }
}
