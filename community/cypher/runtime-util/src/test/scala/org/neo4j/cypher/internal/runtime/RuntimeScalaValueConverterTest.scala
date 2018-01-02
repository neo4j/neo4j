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
package org.neo4j.cypher.internal.runtime

import java.util
import java.util.Arrays.asList
import java.util.Collections.{emptyList, emptyMap, singletonMap, singleton => singletonSet}

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class RuntimeScalaValueConverterTest extends CypherFunSuite {

  val converter = new RuntimeScalaValueConverter(_ => false)

  test("should convert hash map") {
    val it = new util.HashMap[String, Any]()
    it.put("k1", 5)
    it.put("k2", 15)

    converter.asDeepScalaValue(it) should equal(Map("k1" -> 5, "k2" -> 15))
  }

  test("should convert singleton map") {
    val it = singletonMap("key", 12)

    converter.asDeepScalaValue(it) should equal(Map("key" -> 12))
  }

  test("should convert empty map") {
    val it = emptyMap()

    converter.asDeepScalaValue(it) should equal(Map.empty)
  }

  test("should convert nested map") {
    val it = new util.HashMap[String, Any]()
    it.put("k1", singletonMap("a", 2))
    it.put("k2", 15)

    converter.asDeepScalaValue(it) should equal(Map("k1" -> Map("a" -> 2), "k2" -> 15))
  }

  test("should convert linked list") {
    val it = new util.LinkedList[Any]()
    it.add(12)
    it.add(14)

    converter.asDeepScalaValue(it) should equal(List(12, 14))
  }

  test("should convert array list") {
    val it = new util.ArrayList[Any]()
    it.add(12)
    it.add(14)

    converter.asDeepScalaValue(it) should equal(List(12, 14))
  }

  test("should convert singleton set") {
    val it = singletonSet(3)

    converter.asDeepScalaValue(it) should equal(List(3))
  }

  test("should convert empty list") {
    val it = emptyList()

    converter.asDeepScalaValue(it) should equal(List.empty)
  }

  test("should convert nested data structures") {
    val it = singletonSet(asList(3, 4))

    converter.asDeepScalaValue(it) should equal(List(List(3, 4)))
  }

  test("should convert from set") {
    val it = new java.util.HashSet[String]
    it.add("Hello")

    converter.asDeepScalaValue(it) should equal(List("Hello"))
  }

  test("should convert traversable to Iterable") {
    val it = Stream[Any](1, 2, 3)

    converter.asDeepScalaValue(it) shouldBe an [Iterable[_]]
  }

  test("should preserve java lists without copying") {
    val javaList = util.Arrays.asList(1,2,3)

    val converted = converter.asDeepScalaValue(javaList)

    converted shouldBe a [JavaListWrapper[_]]
    converted.asInstanceOf[JavaListWrapper[_]].inner shouldBe an [util.ArrayList[_]]
  }
}
