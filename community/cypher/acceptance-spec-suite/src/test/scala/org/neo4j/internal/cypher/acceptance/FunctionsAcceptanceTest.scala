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

import org.neo4j.cypher._

class FunctionsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("toInt should accept type Any") {
    // When
    val query = "WITH [2, 2.9, '1.7'] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("int_numbers" -> List(2, 2, 1))))
  }

  test("toInt should fail statically on type boolean") {
    val query = "RETURN toInt(true)"

    a [SyntaxException] should be thrownBy {
      executeWithAllPlanners(query)
    }
  }

  test("toFloat should work on type Any") {
    // When
    val query = "WITH [3.4, 3, '5'] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("float_numbers" -> List(3.4, 3.0, 5.0))))
  }

  test("toFloat should fail statically on type Boolean") {
    // When
    val query = "RETURN toFloat(false)"

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

  test("toFloat should work on string collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH ['1', '2', 'foo'] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers")

    // Then
    result.toList should equal(List(Map("float_numbers" -> List(1.0, 2.0, null))))
  }

  test("id on a node should work in both runtimes")  {
    // GIVEN
    val expected = createNode().getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN id(n)")

    // THEN
    result.toList should equal(List(Map("id(n)" -> expected)))

  }

  test("id on a rel should work in both runtimes")  {
    // GIVEN
    val expected = relate(createNode(), createNode()).getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() RETURN id(r)")

    // THEN
    result.toList should equal(List(Map("id(r)" -> expected)))
  }

  test("type() should accept type Any") {
    relate(createNode(), createNode(), "T")

    val query = """
      |MATCH (a)-[r]->()
      |WITH [a, r, 1] AS list
      |RETURN type(list[1]) AS t
    """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("t" -> "T")))
  }

  test("type() should fail statically when given type Node") {
    relate(createNode(), createNode(), "T")

    val query = """
      |MATCH (a)-[r]->()
      |RETURN type(a) AS t
    """.stripMargin

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

  test("type() should fail at runtime when given type Any but bad value")  {
    relate(createNode(), createNode(), "T")

    val query = """
                  |MATCH (a)-[r]->()
                  |WITH [a, r, 1] AS list
                  |RETURN type(list[0]) AS t
                """.stripMargin

    a [ParameterWrongTypeException] should be thrownBy executeWithAllPlanners(query)
  }

  test("labels() should accept type Any") {
    createLabeledNode("Foo")
    createLabeledNode("Foo", "Bar")

    val query = """
      |MATCH (a)
      |WITH [a, 1] AS list
      |RETURN labels(list[0]) AS l
    """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("l" -> List("Foo")), Map("l" -> List("Foo", "Bar"))))
  }

  test("labels() should fail statically on type Path") {
    createLabeledNode("Foo")
    createLabeledNode("Foo", "Bar")

    val query = """
      |MATCH p = (a)
      |RETURN labels(p) AS l
    """.stripMargin

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

  test("labels() should fail at runtime on type Any with bad values") {
    createLabeledNode("Foo")
    createLabeledNode("Foo", "Bar")

    val query = """
                  |MATCH (a)
                  |WITH [a, 1] AS list
                  |RETURN labels(list[1]) AS l
                """.stripMargin

    a [ParameterWrongTypeException] should be thrownBy executeWithAllPlanners(query)
  }
}
