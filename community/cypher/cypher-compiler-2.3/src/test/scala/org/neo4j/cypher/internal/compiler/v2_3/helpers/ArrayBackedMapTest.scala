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

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ArrayBackedMapTest extends CypherFunSuite {

  test("updating and getting") {
    val map = ArrayBackedMap.apply[String, String]("name", "id")
    map.putValues(Array("neo", "123"))

    map.get("name") should equal(Some("neo"))
    map.get("id") should equal(Some("123"))
    map.get("other") should equal(None)
  }

  test("+ operator with existing key") {
    val map = ArrayBackedMap.apply[String, String]("name", "id")
    map.putValues(Array("neo", "123"))
    val updatedMap = map + ("name" -> "oen")

    map.get("name") should equal(Some("neo"))
    map.get("id") should equal(Some("123"))
    updatedMap.get("name") should equal(Some("oen"))
    updatedMap.get("id") should equal(Some("123"))
  }

  test("+ operator with non-existing key") {
    val map = ArrayBackedMap.apply[String, String]("name", "id")
    map.putValues(Array("neo", "123"))
    val updatedMap = map + ("age" -> "35")

    map.get("name") should equal(Some("neo"))
    map.get("id") should equal(Some("123"))
    map.get("age") should equal(None)

    updatedMap.get("name") should equal(Some("neo"))
    updatedMap.get("id") should equal(Some("123"))
    updatedMap.get("age") should equal(Some("35"))
  }

  test("- operator on existing key") {
    val map = ArrayBackedMap.apply[String, String]("name", "id")
    map.putValues(Array("neo", "123"))
    val updatedMap = map - "name"

    map.get("name") should equal(Some("neo"))
    map.get("id") should equal(Some("123"))
    updatedMap.get("name") should equal(None)
    updatedMap.get("id") should equal(Some("123"))
  }

  test("iterating over map") {
    val map = ArrayBackedMap.apply[String, String]("name", "id")
    map.putValues(Array("neo", "123"))
    map.iterator.toSet should equal(Set(("name", "neo"), ("id", "123")))
  }

  test("support one null key") {
    val map = ArrayBackedMap.apply[String, String]("name", null)
    map.putValues(Array("neo", "123"))

    map.get("name") should equal(Some("neo"))
    map.get(null) should equal(Some("123"))
  }

  test("support for null values") {
    val map = ArrayBackedMap.apply[String, String]("a", "b", "c")
    map.putValues(Array(null, "123", null))

    map.get("a") should equal(Some(null))
    map.get("b") should equal(Some("123"))
    map.get("c") should equal(Some(null))
  }

  test("support one null key mapped to null value") {
    val map = ArrayBackedMap.apply[String, String]("name", null)
    map.putValues(Array("neo", null))

    map.get("name") should equal(Some("neo"))
    map.get(null) should equal(Some(null))
  }

  test("having multiple nulls means that the last null is mapped") {
    val map = ArrayBackedMap.apply[String, String](null, null, null)
    map.putValues(Array("v1", "v2", "v3"))

    map.get(null) should equal(Some("v3"))
  }
}
