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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class DDminTest extends CypherFunSuite with ReductionTestHelper {

  class TestDDInput(originalLength: Int) extends DDInput[Array[Int]](originalLength) {
    override def getCurrentCode: Array[Int] = activeTokens
  }

  class ThrowingDDInput(originalLength: Int) extends DDInput[Array[Int]](originalLength) {
    override def getCurrentCode: Array[Int] = {
      if (activeTokens.length == originalLength) {
        activeTokens
      } else {
        throw new IllegalSyntaxException()
      }
    }
  }

  test("should reduce subset") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 1), Reproduced),
      (Array(0), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestDDInput(4)
    val res = DDmin(input)(oracle)
    res should equal(Array(0))
    oracle.assertExhausted
  }

  test("should reduce to complement") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 1), NotReproduced),
      (Array(2, 3), Reproduced),
      (Array(2), NotReproduced),
      (Array(3), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestDDInput(4)
    val res = DDmin(input)(oracle)
    res should equal(Array(3))
    oracle.assertExhausted
  }

  test("should increase granularity") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 1), NotReproduced),
      (Array(2, 3), NotReproduced),
      (Array(0), NotReproduced),
      (Array(1), NotReproduced),
      (Array(2), NotReproduced),
      (Array(3), NotReproduced),
      (Array(1, 2, 3), NotReproduced),
      (Array(0, 2, 3), NotReproduced),
      (Array(0, 1, 3), NotReproduced),
      (Array(0, 1, 2), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestDDInput(4)
    val res = DDmin(input)(oracle)
    res should equal(Array(0, 1, 2, 3))
    oracle.assertExhausted
  }

  test("should combine subsets, complements, granularity increases and cache") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 1, 2, 3), NotReproduced),
      (Array(4, 5, 6, 7), NotReproduced),
      (Array(0, 1), NotReproduced),
      (Array(2, 3), NotReproduced),
      (Array(4, 5), NotReproduced),
      (Array(6, 7), NotReproduced),
      (Array(2, 3, 4, 5, 6, 7), Reproduced),
      (Array(2, 3, 6, 7), Reproduced),
      (Array(2), NotReproduced),
      (Array(3), NotReproduced),
      (Array(6), NotReproduced),
      (Array(7), NotReproduced),
      (Array(3, 6, 7), Reproduced),
      (Array(3, 7), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestDDInput(8)
    val res = DDmin(input)(oracle)
    res should equal(Array(3, 7))
    oracle.assertExhausted
  }

  test("should treat IllegalSyntaxExceptions correctly") {
    val expectedInvocationsAndResults = Seq()
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new ThrowingDDInput(4)
    val res = DDmin(input)(oracle)
    res should equal(Array(0, 1, 2, 3))
    oracle.assertExhausted
  }

  test("should work with odd-sized inputs") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 1), NotReproduced),
      (Array(2), NotReproduced),
      (Array(0), NotReproduced),
      (Array(1), NotReproduced),
      (Array(1, 2), NotReproduced),
      (Array(0, 2), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestDDInput(3)
    val res = DDmin(input)(oracle)
    res should equal(Array(0, 1, 2))
    oracle.assertExhausted
  }

}
