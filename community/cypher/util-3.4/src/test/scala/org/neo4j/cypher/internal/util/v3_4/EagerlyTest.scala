/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

import scala.collection.{immutable, mutable}

class EagerlyTest extends CypherFunSuite {
  test("maps values of immutable maps to immutable maps") {
    val result = Eagerly.immutableMapValues(immutable.Map("a" -> 1, "b" ->2), (x: Int) => x * 2)
    val expectation = immutable.Map("a" -> 2, "b" -> 4)

    result should equal(expectation)
    result.getClass should equal(expectation.getClass)
  }

  test("maps values of mutable maps to immutable maps") {
    val result = Eagerly.immutableMapValues(mutable.Map("a" -> 1, "b" ->2), (x: Int) => x * 2)
    val expectation = immutable.Map("a" -> 2, "b" -> 4)

    result should equal(expectation)
    result.getClass should equal(expectation.getClass)
  }

  test("maps values of immutable maps to mutable maps") {
    val result = Eagerly.mutableMapValues(immutable.Map("a" -> 1, "b" ->2), (x: Int) => x * 2)
    val expectation = mutable.Map("a" -> 2, "b" -> 4)

    result should equal(expectation)
    result.getClass should equal(expectation.getClass)
  }

  test("maps values of mutable maps to mutable maps") {
    val result = Eagerly.mutableMapValues(mutable.Map("a" -> 1, "b" ->2), (x: Int) => x * 2)
    val expectation = mutable.Map("a" -> 2, "b" -> 4)

    result should equal(expectation)
    result.getClass should equal(expectation.getClass)
  }

  test("replaces keys on empty map") {
    val result = Eagerly.immutableReplaceKeys[Any, Any](Map.empty)("a" -> 1)

    result should be(empty)
  }

  test("replaces keys on non-empty map") {
    val result = Eagerly.immutableReplaceKeys(Map("a" -> 10))("a" -> "a", "a" -> "b")

    result should equal(Map("a" -> 10, "b" -> 10))
  }
}
