/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "MATCH (n) RETURN n{.foo,.bar,.baz}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "MATCH (n) RETURN n{.foo,.bar}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*, .baz}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*, bar:'apatisk'}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting from a null identifier produces a null value") {

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, "OPTIONAL MATCH (n) RETURN n{.foo, .bar}")

    result.toList should equal(List(Map("n" -> null)))
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, """MATCH (actor:Actor)-->(movie:Movie)
            |RETURN actor{ .name, movies: collect(movie{.title}) }""".stripMargin)
    result.toList should equal(
      List(Map("actor" ->
        Map("name" -> "Actor 1", "movies" -> Seq(
          Map("title" -> "Movie 2"),
          Map("title" -> "Movie 1"))))))
  }

  test("prepending item to a list should behave correctly in all runtimes") {
    val query = "CYPHER WITH {a:[1,2,3]} AS x RETURN 'a:' + x.a AS r"

    val result = executeWith(Configs.All, query)

    result.toList.head("r") should equal(List("a:", 1, 2, 3))
  }

  test("appending item to a list should behave correctly in all runtimes") {
    val query = "CYPHER WITH {a:[1,2,3]} AS x RETURN x.a + 'a:' AS r"

    val result = executeWith(Configs.All, query)

    result.toList.head("r") should equal(List(1, 2, 3, "a:"))
  }
}
