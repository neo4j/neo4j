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

class BTTest extends CypherFunSuite with ReductionTestHelper {

  class TestBTInput(array: Array[Int], withNewAssignments: Boolean) extends BTInput[Array[Int], Int] {
    override val domains: Array[BTDomain[Int]] = {
      array.map {
        i => new BTDomain[Int](Range.inclusive(0, i).reverse.map(j => BTAssignment(j, i - j)).toArray)
      }
    }

    override def convertToInput(objects: Seq[Int]): Array[Int] = objects.toArray

    override def getNewAssignments(assignment: BTAssignment[Int]): Seq[BTAssignment[Int]] = {
      if(withNewAssignments && assignment.obj == 0) Seq(BTAssignment(-1, assignment.gain + 1))
      else Seq.empty
    }
  }

  test("should reduce single element") {
    val expectedInvocationsAndResults = Seq(
      (Array(0), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(1), false)
    val res = BT(input)(oracle)
    res should equal(Array(0))
    oracle.assertExhausted
  }

  test("should reduce single element with new assignments") {
    val expectedInvocationsAndResults = Seq(
      (Array(0), Reproduced),
      (Array(-1), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(1), true)
    val res = BT(input)(oracle)
    res should equal(Array(-1))
    oracle.assertExhausted
  }

  test("should reduce single element with new assignments 2") {
    val expectedInvocationsAndResults = Seq(
      (Array(0), Reproduced),
      (Array(-1), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(1), true)
    val res = BT(input)(oracle)
    res should equal(Array(0))
    oracle.assertExhausted
  }

  test("should reduce two elements") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 3), Reproduced),
      (Array(0, 0), NotReproduced),
      (Array(0, 1), NotReproduced),
      (Array(0, 2), Reproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(2, 3), false)
    val res = BT(input)(oracle)
    res should equal(Array(0, 2))
    oracle.assertExhausted
  }

  test("should reduce two elements and iterate") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 3), NotReproduced),
      (Array(1, 3), Reproduced),
      (Array(1, 0), NotReproduced),
      (Array(1, 1), NotReproduced),
      (Array(1, 2), Reproduced),
      (Array(0, 2), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(2, 3), false)
    val res = BT(input)(oracle)
    res should equal(Array(1, 2))
    oracle.assertExhausted
  }

  test("should reduce two elements with new assignments") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 3), Reproduced),
      (Array(-1, 3), Reproduced),
      (Array(-1, 0), NotReproduced),
      (Array(-1, 1), NotReproduced),
      (Array(-1, 2), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(2, 3), true)
    val res = BT(input)(oracle)
    res should equal(Array(-1, 3))
    oracle.assertExhausted
  }

  test("should reduce two elements with new assignments and iterate") {
    val expectedInvocationsAndResults = Seq(
      (Array(0, 3), Reproduced),
      (Array(-1, 3), NotReproduced),
      (Array(0, 0), NotReproduced),
      (Array(0, 1), Reproduced),
      (Array(-1, 1), Reproduced),
      (Array(-1, 0), Reproduced),
      (Array(-1, -1), NotReproduced)
    )
    val oracle = getOracle(expectedInvocationsAndResults)
    val input = new TestBTInput(Array(2, 3), true)
    val res = BT(input)(oracle)
    res should equal(Array(-1, 0))
    oracle.assertExhausted
  }

}
