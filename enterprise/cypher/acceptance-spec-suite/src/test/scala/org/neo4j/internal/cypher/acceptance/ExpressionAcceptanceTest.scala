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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "MATCH (n) RETURN n{.foo,.bar,.baz}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "MATCH (n) RETURN n{.foo,.bar}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*, .baz}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "MATCH (n) RETURN n{.*, bar:'apatisk'}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting from a null identifier produces a null value") {

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "OPTIONAL MATCH (n) RETURN n{.foo, .bar}")

    result.toList should equal(List(Map("n" -> null)))
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, """MATCH (actor:Actor)-->(movie:Movie)
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

  test("not(), when right of a =, should give a helpful error message") {
    val query = "RETURN true = not(42 = 32)"

    val config =  Configs.AbsolutelyAll - Configs.Version2_3

    failWithError(config, query,
      List("Unknown function 'not'. If you intended to use the negation expression, surround it with parentheses."))
  }

  test("NOT(), when right of a =, should give a helpful error message") {
    val query = "RETURN true = NOT(42 = 32)"

    // this should have the right error message for 3.1 after the next patch releases
    val config =  Configs.AbsolutelyAll- Configs.Version3_1 - Configs.Version2_3 - Configs.AllRulePlanners

    failWithError(config, query,
      List("Unknown function 'NOT'. If you intended to use the negation expression, surround it with parentheses."))
  }
}
