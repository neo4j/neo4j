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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  // TODO: These are TCK'd and should be removed when TCK gets updated
  test("IN should work with nested list subscripting") {
    val query = """WITH [[1, 2, 3]] AS list
                  |RETURN 3 IN list[0] AS r
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> true)))
  }

  test("IN should work with nested literal list subscripting") {
    val query = "RETURN 3 IN [[1, 2, 3]][0] AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> true)))
  }

  test("IN should work with list slices") {
    val query = """WITH [1, 2, 3] AS list
                  |RETURN 3 IN list[0..1] AS r
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> false)))
  }

  test("IN should work with literal list slices") {
    val query = "RETURN 3 IN [1, 2, 3][0..1] AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> false)))
  }
  // End of above TODO

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeScalarWithAllPlanners[Any]("MATCH (n) RETURN n{.foo,.bar,.baz}") should equal(
      Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeScalarWithAllPlanners[Any]("WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}") should equal(
      Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.foo,.bar}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*, .baz}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*, bar:'apatisk'}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting from a null identifier produces a null value") {
    executeWithAllPlanners("OPTIONAL MATCH (n) RETURN n{.foo, .bar}").toList should equal(
      List(Map("n" -> null)))
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    executeWithAllPlanners(
      """MATCH (actor:Actor)-->(movie:Movie)
        |RETURN actor{ .name, movies: collect(movie{.title}) }""".stripMargin).toList should equal(
      List(Map("actor" ->
        Map("name" -> "Actor 1", "movies" -> Seq(
          Map("title" -> "Movie 2"),
          Map("title" -> "Movie 1"))))))
  }
}
