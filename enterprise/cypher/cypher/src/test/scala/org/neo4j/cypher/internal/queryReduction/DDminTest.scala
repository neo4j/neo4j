/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
