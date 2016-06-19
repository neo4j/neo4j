/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport}

class EqualsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  // TCK'd
  test("does not lose precision") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Number]("match (p:Label) return p.id")

    result should equal(4611686018427387905L)
  }

  // TCK'd
  test("equality takes the full value into consideration 1") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (p:Label {id: 4611686018427387905}) return p")

    result should not be empty
  }

  // TCK'd
  test("equality takes the full value into consideration 2") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (p:Label) where p.id = 4611686018427387905 return p")

    result should not be empty
  }

  // TCK'd
  test("equality takes the full value into consideration 3") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (p:Label) where p.id = 4611686018427387900 return p")

    result should be(empty)
  }

  // TCK'd
  test("equality takes the full value into consideration 4") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (p:Label {id : 4611686018427387900}) return p")

    result should be(empty)
  }

  // TCK'd
  test("should not throw semantic check error for number-typed integer comparison") {
    createNode("id" -> 0)

    // the parameter is used to disable auto-parametrization
    val queryWithInt =
      """WITH {parameter} AS p
        |WITH collect([0, 0.0]) as numbers
        |UNWIND numbers as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE toInt(n.id) = expected
        |RETURN n""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(queryWithInt, "parameter" -> "placeholder")

    result.toList.length shouldBe 1
  }

  // TCK'd
  test("should not throw semantic check error for number-typed float comparison") {
    createNode("id" -> 0)

    // the parameter is used to disable auto-parametrization
    val queryWithFloat =
      """WITH {parameter} AS p
        |WITH collect([0.5, 0]) as numbers
        |UNWIND numbers as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE toInt(n.id) = expected
        |RETURN n""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(queryWithFloat, "parameter" -> "placeholder")

    result.toList shouldBe empty
  }

  // TCK'd
  test("should not throw semantic check error for any-typed string comparison") {
    createNode("id" -> 0)

    // the parameter is used to disable auto-parametrization
    val queryWithString =
      """WITH {parameter} AS p
        |WITH collect(["0", 0]) as things
        |UNWIND things as arr
        |WITH arr[0] as expected
        |MATCH (n) WHERE toInt(n.id) = expected
        |RETURN n""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(queryWithString, "parameter" -> "placeholder")

    result.toList shouldBe empty
  }

  // TCK'd
  test("should prohibit equals between node and parameter") {
    // given
    createLabeledNode("Person")

    intercept[IncomparableValuesException](executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (b) WHERE b = {param} RETURN b",
      "param" -> Map("name" -> "John Silver")))
  }

  // TCK'd
  test("should prohibit equals between parameter and node") {
    // given
    createLabeledNode("Person")

    intercept[IncomparableValuesException](executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (b) WHERE {param} = b RETURN b", "param" -> Map("name" -> "John Silver")))
  }

  // TCK'd
  test("should allow equals between node and node") {
    // given
    createLabeledNode("Person")

    // when
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Number]("MATCH (a) WITH a MATCH (b) WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }

  // TCK'd
  test("should reject equals between node and property") {
    // given
    createLabeledNode(Map("val"->17), "Person")

    intercept[IncomparableValuesException](executeWithAllPlannersAndCompatibilityMode("MATCH (a) WHERE a = a.val RETURN count(a)"))
  }

  // TCK'd
  test("should allow equals between relationship and relationship") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    // when
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Number]("MATCH ()-[a]->() WITH a MATCH ()-[b]->() WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }

  // TCK'd
  test("should reject equals between node and relationship") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    intercept[IncomparableValuesException](executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a)-[b]->() RETURN a = b"))
  }

  // TCK'd
  test("should reject equals between relationship and node") {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    intercept[IncomparableValuesException](executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a)-[b]->() RETURN b = a"))
  }

  // Not TCK material below; sending graph types or characters as parameters is not supported

  test("should be able to send in node via parameter") {
    // given
    val node = createLabeledNode("Person")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (b) WHERE b = {param} RETURN b", "param" -> node)
    result.toList should equal(List(Map("b" -> node)))
  }

  test("should be able to send in relationship via parameter") {
    // given
    val rel = relate(createLabeledNode("Person"), createLabeledNode("Person"))

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (:Person)-[r]->(:Person) WHERE r = {param} RETURN r", "param" -> rel)
    result.toList should equal(List(Map("r" -> rel)))
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = {param}", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN {param} = 'a'", "param" -> 'a') shouldBe true
  }
}
