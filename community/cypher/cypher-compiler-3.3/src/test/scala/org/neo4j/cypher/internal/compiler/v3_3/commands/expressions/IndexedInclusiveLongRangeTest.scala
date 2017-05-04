/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

import scala.util.Random

class IndexedInclusiveLongRangeTest extends CypherFunSuite {

  test("single element") {
    IndexedInclusiveLongRange(0L, 0L, 1L).toIndexedSeq should equal(IndexedSeq(0L))
  }

  test("step length 1"){
    val range= IndexedInclusiveLongRange(0L, 11L, 1L)

    range.toIndexedSeq should equal(IndexedSeq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L))
    range should have length 12

    for (i <- 0 to 11) {
      range(i) should equal(i.toLong)
    }
  }

  test("step length 3"){
    val range= IndexedInclusiveLongRange(3L, 14L, 3L)

    range.toIndexedSeq should equal(IndexedSeq(3L, 6L, 9L, 12L))
    range should have length 4
    range(0) should equal(3L)
    range(1) should equal(6L)
    range(2) should equal(9L)
    range(3) should equal(12L)
  }

  test("negative step"){
    val range= IndexedInclusiveLongRange(14L, 3L, -3L)

    range.toIndexedSeq should equal(IndexedSeq(14L, 11L, 8L, 5L))
    range should have length 4
    range(0) should equal(14L)
    range(1) should equal(11L)
    range(2) should equal(8L)
    range(3) should equal(5L)
  }

  test("stress test") {
    val random = new Random
    for (i <- 1 to 100;
         a = random.nextInt(Int.MaxValue);
         b = random.nextInt(Int.MaxValue);
         c = random.nextInt(Int.MaxValue)
    ) {
      val input = Array(a,b,c).sorted
      val step = input(0)
      val start = input(1)
      val end = input(2)

      IndexedInclusiveLongRange(start, end, step).toIndexedSeq.map(_.toInt) should equal(start to end by step)
    }
  }

  test("should fail if using a too big range as indexed seq") {
    val range = IndexedInclusiveLongRange(0, Int.MaxValue + 1L, 1L)

    an [OutOfMemoryError] shouldBe thrownBy(range(2))
  }

  test("should handle big ranges as long as you only iterat") {
    val range = IndexedInclusiveLongRange(0, Int.MaxValue + 1L, 1L)

    var i = 0L
    val it = range.iterator
    while(it.hasNext && i < 1000L) {
      it.next() should equal(i)
      i += 1L
    }

    it.hasNext should be(true)
  }
}
