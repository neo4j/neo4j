/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

class LongArrayHashMultiMapTest extends FunSuite with Matchers with RandomTester {
  test("basic") {
    val map = new LongArrayHashMultiMap[String](32, 3)
    map.add(Array(1L, 2L, 3L), "hello")
    map.add(Array(1L, 2L, 3L), "world")
    val iterator = map.get(Array(1L, 2L, 3L))

    iterator.asScala.toList should equal(List("world", "hello"))

    map.get(Array(6L, 6L, 6L)).hasNext should equal(false)
    map.isEmpty should equal(false)
  }

  test("isEmpty") {
    val map = new LongArrayHashMultiMap(32, 11)
    map.isEmpty should equal(true)
  }

  test("fill and doubleCapacity") {
    val map = new LongArrayHashMultiMap[String](8, 3)
    map.add(Array(0L, 8L, 1L), "hello")
    map.add(Array(0L, 7L, 2L), "is")
    map.add(Array(0L, 6L, 3L), "it")
    map.add(Array(0L, 5L, 4L), "me")
    map.add(Array(0L, 4L, 5L), "you")
    map.add(Array(0L, 3L, 6L), "are")
    map.add(Array(0L, 2L, 7L), "looking")
    map.add(Array(0L, 1L, 8L), "for")

    map.get(Array(0L, 7L, 2L)).asScala.toList should equal(List("is"))
    map.get(Array(0L, 6L, 3L)).asScala.toList should equal(List("it"))
    map.get(Array(0L, 5L, 4L)).asScala.toList should equal(List("me"))
    map.get(Array(0L, 4L, 5L)).asScala.toList should equal(List("you"))
    map.get(Array(0L, 3L, 6L)).asScala.toList should equal(List("are"))
    map.get(Array(0L, 2L, 7L)).asScala.toList should equal(List("looking"))
    map.get(Array(0L, 1L, 8L)).asScala.toList should equal(List("for"))
    map.isEmpty should equal(false)
  }

  test("multiple values with the same keys") {
    val map = new LongArrayHashMultiMap[String](8, 3)
    map.add(Array(0L, 0L, 1L), "hello")
    map.add(Array(0L, 0L, 2L), "is")
    map.add(Array(0L, 0L, 3L), "it")
    map.add(Array(0L, 0L, 1L), "me")
    map.add(Array(0L, 0L, 2L), "you")
    map.add(Array(0L, 0L, 3L), "are")
    map.add(Array(0L, 0L, 1L), "looking")
    map.add(Array(0L, 0L, 2L), "or")
    map.add(Array(0L, 0L, 3L), "what")

    map.get(Array(0L, 0L, 1L)).asScala.toList should equal(List("looking", "me", "hello"))
    map.get(Array(0L, 0L, 2L)).asScala.toList should equal(List("or", "you", "is"))
    map.get(Array(0L, 0L, 3L)).asScala.toList should equal(List("what", "are", "it"))
    map.isEmpty should equal(false)
  }

  test("getting a non existing value returns an empty iterator") {
    val map = new LongArrayHashMultiMap[String](32, 2)
    map.get(Array(0L, 0L)).asScala.toList should equal(List.empty)
  }

  randomTest { randomer =>
    val r = randomer.r
    val width = r.nextInt(10) + 2
    val size = r.nextInt(10000)
    val tested = new LongArrayHashMultiMap[String](16, width)
    val validator = new mutable.HashMap[Array[Long], mutable.ListBuffer[String]]()
    (0 to size) foreach { _ =>
      val key = new Array[Long](width)
      (0 until width) foreach { i => key(i) = randomer.randomLong() }
      val value = System.nanoTime().toString
      tested.add(key, value)
      val values: mutable.ListBuffer[String] = validator.getOrElseUpdate(key, new mutable.ListBuffer[String])
      values.append(value)
    }

    validator.foreach { case (key, expectedValues) =>
      val v = tested.get(key).asScala.toList
      v should equal(expectedValues.toList)
    }

    (0 to size) foreach { _ =>
      val tuple = new Array[Long](width)
      (0 until width) foreach { i => tuple(i) = randomer.randomLong() }
      val a = tested.get(tuple).asScala.toList
      val b = validator.getOrElse(tuple, List.empty)
      a should equal(b.toList)
    }
  }
}
