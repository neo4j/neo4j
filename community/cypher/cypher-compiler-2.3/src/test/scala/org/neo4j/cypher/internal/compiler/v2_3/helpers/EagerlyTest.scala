/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers


import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

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
