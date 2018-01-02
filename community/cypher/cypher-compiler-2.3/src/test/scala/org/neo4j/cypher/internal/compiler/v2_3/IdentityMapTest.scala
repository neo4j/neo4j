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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.IdentityMap
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class IdentityMapTest extends CypherFunSuite {

  case class Val()

  test("should store and retrieve based on object identity") {
    val x = Val()
    val y = Val()
    assert(x === y)

    val map = IdentityMap(x -> "x", y -> "y")
    assert(map.get(x) === Some("x"))
    assert(map.get(y) === Some("y"))
  }

  test("should not overwrite equal key") {
    val x = Val()
    val y = Val()
    assert(x === y)

    val map = IdentityMap(x -> "x")
    val updatedMap = map.updated(y, "y")
    assert(updatedMap.get(x) === Some("x"))
    assert(updatedMap.get(y) === Some("y"))
  }

  test("should overwrite eq key") {
    val x = Val()
    val map = IdentityMap(x -> "x")
    val updatedMap = map.updated(x, "x'")
    assert(updatedMap.get(x) === Some("x'"))
  }

  test("should be immutable when updating") {
    val x = Val()
    val y = Val()
    val map = IdentityMap(x -> "x")
    val updatedMap = map.updated(x, "x'").updated(y, "y")
    assert(map.get(x) === Some("x"))
    assert(map.get(y) === None)
    assert(updatedMap.get(x) === Some("x'"))
    assert(updatedMap.get(y) === Some("y"))
  }

  test("should know contained keys") {
    val k1 = Val()
    val k2 = Val()
    val map = IdentityMap(k1 -> "x")

    assert(map.contains(k1))
    assert(!map.contains(k2))
  }
}
