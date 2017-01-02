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
package org.neo4j.cypher

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EqualsTest extends ExecutionEngineFunSuite {

  test("should not throw semantic check error for number-typed integer comparison") {
    createNode()

    // the parameter is used to disable auto-parametrization
    val queryWithInt =
      """PROFILE
        |WITH {parameter} AS p
        |WITH collect([0, 0.0]) as numbers
        |UNWIND numbers as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE id(n)=expected
        |RETURN n""".stripMargin
    val result = execute(queryWithInt, "parameter" -> "placeholder")

    result.toList.length shouldBe 1
  }

  test("should not throw semantic check error for number-typed float comparison") {
    createNode()

    // the parameter is used to disable auto-parametrization
    val queryWithFloat =
      """PROFILE
        |WITH {parameter} AS p
        |WITH collect([0.5, 0]) as numbers
        |UNWIND numbers as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE id(n)=expected
        |RETURN n""".stripMargin
    val result = execute(queryWithFloat, "parameter" -> "placeholder")

    result.toList shouldBe empty
  }

  test("should not throw semantic check error for any-typed string comparison") {
    createNode()

    // the parameter is used to disable auto-parametrization
    val queryWithString =
      """PROFILE
        |WITH {parameter} AS p
        |WITH collect(["0", 0]) as things
        |UNWIND things as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE id(n)=expected
        |RETURN n""".stripMargin
    val result = execute(queryWithString, "parameter" -> "placeholder")

    result.toList shouldBe empty
  }

  test("should prohibit equals between node and parameter") {
    // given
    createLabeledNode("Person")

    evaluating {
      execute("MATCH (b) WHERE b = {param} RETURN b", "param" -> Map("name" -> "John Silver"))
    } should produce[IncomparableValuesException]
  }

  test("should prohibit equals between parameter and node") {
    // given
    createLabeledNode("Person")

    evaluating {
      execute("MATCH (b) WHERE {param} = b RETURN b", "param" -> Map("name" -> "John Silver"))
    } should produce[IncomparableValuesException]
  }

  test("should allow equals between node and node") {
    // given
    createLabeledNode("Person")

    // when
    val result = executeScalar[Number]("MATCH (a) WITH a MATCH (b) WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }

  test("should reject equals between node and property") {
    // given
    createLabeledNode(Map("val"->17), "Person")

    evaluating {
      execute("MATCH (a) WHERE a = a.val RETURN count(a)")
    } should produce[IncomparableValuesException]
  }


  test("should allow equals between relationship and relationship") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    // when
    val result = executeScalar[Number]("MATCH ()-[a]->() WITH a MATCH ()-[b]->() WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }

  test("should reject equals between node and relationship") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    evaluating {
      execute("MATCH (a)-[b]->() RETURN a = b")
    } should produce[IncomparableValuesException]
  }

  test("should reject equals between relationship and node") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    evaluating {
      execute("MATCH (a)-[b]->() RETURN b = a")
    } should produce[IncomparableValuesException]
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = {param}", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN {param} = 'a'", "param" -> 'a') shouldBe true
  }
}
