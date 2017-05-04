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
package org.neo4j.cypher.internal.compiler.v3_3.helpers

import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class CounterTest extends CypherFunSuite {

  test("counts up") {
    (Counter(0) += 1) should equal(1l)
    (Counter(0) += 3) should equal(3l)
    (Counter(7) += 1) should equal(8l)
  }

  test("counts down") {
    (Counter(7) -= 1) should equal(6l)
  }

  test("resets counter") {
    Counter(10).reset(5).counted should equal(5)
  }

  test("streams all values") {
    Counter().values.take(5).toList should equal(List(1l, 2l, 3l, 4l, 5l))
  }

  test("maps all values") {
    Counter().map(_ * 2).take(3).toList should equal(List(2l, 4l, 6l))
  }

  test("tracks iterators") {
    // given
    val iterator = Counter(10).track(1.to(5).iterator)

    // when
    iterator.toList

    // then
    iterator.counted should equal(15l)
  }

  test("limits tracked iterators") {
    an [Exception] should be thrownBy {
      Counter().track(1.to(5).iterator).limit(2) { counted => counted shouldBe 3; fail("Limit reached") }.toList
    }
  }
}
