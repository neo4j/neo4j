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

import java.util.function.Supplier

import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class LongArrayHashMapTest extends FunSuite with Matchers with RandomTester {
  test("basic") {
    val map = new LongArrayHashMap[String](32, 3)
    map.computeIfAbsent(Array(1L, 2L, 3L), () => "hello") should equal("hello")
    map.computeIfAbsent(Array(1L, 2L, 3L), () => "world") should equal("hello")

    map.get(Array(1L, 2L, 3L)) should equal("hello")
    resultAsSet(map) should equal(Set(List(1L, 2L, 3L) -> "hello"))
    map.isEmpty should equal(false)
  }

  test("isEmpty") {
    val map = new LongArrayHashMap[String](32, 11)
    map.isEmpty should equal(true)
    resultAsSet(map) should equal(Set.empty)
  }

  test("fill and doubleCapacity") {
    val map = new LongArrayHashMap[String](8, 3)
    map.computeIfAbsent(Array(0L, 8L, 1L), () => "hello")
    map.computeIfAbsent(Array(0L, 7L, 2L), () => "is")
    map.computeIfAbsent(Array(0L, 6L, 3L), () => "it")
    map.computeIfAbsent(Array(0L, 5L, 4L), () => "me")
    map.computeIfAbsent(Array(0L, 4L, 5L), () => "you")
    map.computeIfAbsent(Array(0L, 3L, 6L), () => "are")
    map.computeIfAbsent(Array(0L, 2L, 7L), () => "looking")
    map.computeIfAbsent(Array(0L, 1L, 8L), () => "for")

    map.get(Array(0L, 8L, 1L)) should equal("hello")
    map.get(Array(0L, 7L, 2L)) should equal("is")
    map.get(Array(0L, 6L, 3L)) should equal("it")
    map.get(Array(0L, 5L, 4L)) should equal("me")
    map.get(Array(0L, 4L, 5L)) should equal("you")
    map.get(Array(0L, 3L, 6L)) should equal("are")
    map.get(Array(0L, 2L, 7L)) should equal("looking")
    map.get(Array(0L, 1L, 8L)) should equal("for")
    map.get(Array(6L, 6L, 6L)) should equal(null)

    resultAsSet(map) should equal(Set(
      List(0L, 8L, 1L) -> "hello",
      List(0L, 7L, 2L) -> "is",
      List(0L, 6L, 3L) -> "it",
      List(0L, 5L, 4L) -> "me",
      List(0L, 4L, 5L) -> "you",
      List(0L, 3L, 6L) -> "are",
      List(0L, 2L, 7L) -> "looking",
      List(0L, 1L, 8L) -> "for"
    ))
    map.isEmpty should equal(false)
  }

  private def resultAsSet(map: LongArrayHashMap[String]): Set[(List[Long], String)] = map.iterator().asScala.map {
    e =>
      (e.getKey.toList, e.getValue)
  }.toSet

  implicit def lambda2JavaFunction[T](f: () => T): Supplier[T] = new Supplier[T] {
    override def get(): T = f()
  }

  randomTest { randomer =>
    val r = randomer.r
    val width = r.nextInt(10) + 2
    val size = r.nextInt(10000)
    val tested = new LongArrayHashMap[String](16, width)
    val validator = new mutable.HashMap[Array[Long], String]()
    (0 to size) foreach { _ =>
      val key = new Array[Long](width)
      (0 until width) foreach { i => key(i) = randomer.randomLong() }
      tested.computeIfAbsent(key, () => key.toString)
      validator.getOrElseUpdate(key, key.toString)
    }

    validator foreach { case (key: Array[Long], expectedValue: String) =>
      val v = tested.get(key)
      v should equal(expectedValue)
    }

    (0 to size) foreach { _ =>
      val tuple = new Array[Long](width)
      (0 until width) foreach { i => tuple(i) = randomer.randomLong() }
      val a = tested.get(tuple)
      val b = validator.getOrElse(tuple, null)
      a should equal(b)
    }

  }
}

trait RandomTester {
  self: FunSuite =>
  def randomTest(f: Randomer => Unit): Unit = {
    val seed = System.nanoTime()
    val rand = new Random(seed)
    val input = new Randomer {
      override val r: Random = rand
    }

    (0 to 100) foreach { i =>
      test(s"random test with seed $seed uniquefier $i") {
        f(input)
      }
    }
  }

  trait Randomer {
    val r: Random

    def randomLong(): Long = {
      val x = r.nextLong()
      if (x == -1 || x == -2)
        randomLong()
      else
        x
    }
  }

}