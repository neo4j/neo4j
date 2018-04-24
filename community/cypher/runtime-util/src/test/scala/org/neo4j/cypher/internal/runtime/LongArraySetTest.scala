/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import java.util

import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class LongArraySetTest extends FunSuite with Matchers {

  val r = new Random()

  (0 to 100) foreach { i =>
    test(s"test #$i") {
      val width = r.nextInt(10) + 2
      val size = r.nextInt(10000)
      val tested = new LongArraySet(16, width)
      val validator = new mutable.HashSet[Array[Long]]()
      (0 to size) foreach { _ =>
        val tuple = new Array[Long](width)
        (0 until width) foreach { i => tuple(i) = r.nextLong() }
        tested.add(tuple)
        validator.add(tuple)
      }

      validator foreach { x =>
        if (!tested.contains(x))
          fail(s"Value was missing: ${util.Arrays.toString(x)}")
      }

      (0 to size) foreach { _ =>
        val tuple = new Array[Long](width)
        (0 until width) foreach { i => tuple(i) = r.nextLong() }
        val a = tested.contains(tuple)
        val b = validator.contains(tuple)

        if(a != b)
          fail(s"Value: ${util.Arrays.toString(tuple)} LongArraySet $a mutable.HashSet")
      }
    }
  }

  test("manual test to help with debugging") {
    val set = new LongArraySet(8, 3)
    set.add(Array(1, 2, 3))
    set.add(Array(2, 3, 4))
    set.add(Array(3, 6, 7))
    set.add(Array(45, 6, 7))
    set.add(Array(55, 6, 7))
    set.add(Array(65, 6, 7))
    set.add(Array(75, 6, 7))
    set.add(Array(85, 6, 7))
    set.add(Array(95, 6, 7))
    set.add(Array(66, 6, 7))
    set.add(Array(1, 2, 3))
    set.contains(Array(1, 2, 3)) should equal(true)
    set.contains(Array(2, 3, 4)) should equal(true)
    set.contains(Array(3, 6, 7)) should equal(true)
    set.contains(Array(45, 6, 7)) should equal(true)
    set.contains(Array(55, 6, 7)) should equal(true)
    set.contains(Array(65, 6, 7)) should equal(true)
    set.contains(Array(6, 6, 6)) should equal(false)
    set.contains(Array(75, 6, 7)) should equal(true)
    set.contains(Array(85, 6, 7)) should equal(true)
    set.contains(Array(95, 6, 7)) should equal(true)
    set.contains(Array(66, 6, 7)) should equal(true)
    set.contains(Array(6, 6, 6)) should equal(false)
  }
}