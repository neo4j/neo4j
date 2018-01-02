/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CustomMatchers
import org.neo4j.cypher.{ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport, SyntaxException}

class OrderByAcceptanceTest extends ExecutionEngineFunSuite with CustomMatchers with NewPlannerTestSupport {

  test("should support ORDER BY") {
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)
    val result = executeWithAllPlanners("match (n) return n.prop AS prop ORDER BY n.prop")
    result.toList should equal(List(
      Map("prop" -> -5),
      Map("prop" -> 1),
      Map("prop" -> 3)
    ))
  }

  test("should support ORDER BY DESC") {
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)
    val result = executeWithAllPlanners("match (n) return n.prop AS prop ORDER BY n.prop DESC")
    result.toList should equal(List(
      Map("prop" -> 3),
      Map("prop" -> 1),
      Map("prop" -> -5)
    ))
  }

  test("ORDER BY of an column introduced in RETURN should work well") {
    executeWithAllPlanners("WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows UNWIND prows AS p UNWIND qrows[p] AS q WITH p, count(q) AS rng RETURN p ORDER BY rng").toList should
      equal(List(Map("p" -> 0), Map("p" -> 1)))
  }

  test("renaming columns before ORDER BY is not confusing") {
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)

    executeWithAllPlanners("MATCH n RETURN n.prop AS n ORDER BY n + 2").toList should
      equal(List(
        Map("n" -> -5),
        Map("n" -> 1),
        Map("n" -> 3)
      ))
  }

  test("Properly handle projections and ORDER BY (GH#4937)") {
    val crew1 = createLabeledNode(Map("name" -> "Neo", "rank" -> 1), "Crew")
    val crew2 = createLabeledNode(Map("name" -> "Neo", "rank" -> 2), "Crew")
    val crew3 = createLabeledNode(Map("name" -> "Neo", "rank" -> 3), "Crew")
    val crew4 = createLabeledNode(Map("name" -> "Neo", "rank" -> 4), "Crew")
    val crew5 = createLabeledNode(Map("name" -> "Neo", "rank" -> 5), "Crew")

    val query = """MATCH (crew:Crew {name: 'Neo'})
                  |WITH crew, 0 AS relevance RETURN crew
                  |ORDER BY relevance, crew.rank""".stripMargin

    executeWithAllPlanners(query).toList should equal(List(
      Map("crew" -> crew1),
      Map("crew"-> crew2),
      Map("crew" -> crew3),
      Map("crew" -> crew4),
      Map("crew" -> crew5)
    ))
  }

  test("Order by with limit zero or negative should not generate errors") {
    createLabeledNode(Map("name" -> "Steven"), "Person")
    createLabeledNode(Map("name" -> "Craig"), "Person")
    executeWithAllPlanners("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 1").length should equal(1)
    executeWithAllPlanners("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 0").length should equal(0)
    executeWithAllPlanners("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT {limit}", "limit" -> -1).length should equal(0)
    a [SyntaxException] should be thrownBy executeWithAllPlanners("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT -1")
  }

  test("should be able to order booleans") {
    val query = "UNWIND [true, false] AS bools RETURN bools ORDER BY bools"

    val expected = List(Map("bools" -> false), Map("bools" -> true))
    executeWithAllPlanners(query).toList should equal(expected)
    executeWithAllPlanners(s"$query DESC").toList should equal(expected.reverse)
  }

  test("should be able to order strings") {
    val query = "UNWIND ['.*', '', ' ', 'one'] AS strings RETURN strings ORDER BY strings"

    val expected = List(Map("strings" -> ""), Map("strings" -> " "), Map("strings" -> ".*"), Map("strings" -> "one"))
    executeWithAllPlanners(query).toList should equal(expected)
    executeWithAllPlanners(s"$query DESC").toList should equal(expected.reverse)
  }

  test("should be able to order ints") {
    val query = "UNWIND [1,3,2] as ints return ints order by ints"

    val expected = List(Map("ints" -> 1), Map("ints" -> 2), Map("ints" -> 3))
    executeWithAllPlanners(query).toList should equal(expected)
    executeWithAllPlanners(s"$query DESC").toList should equal(expected.reverse)
  }

  test("should be able to order floats") {
    val query = "UNWIND [1.5,1.3,999.99] as floats return floats order by floats"

    val expected = List(Map("floats" -> 1.3), Map("floats" -> 1.5), Map("floats" -> 999.99))
    executeWithAllPlanners(query).toList should equal(expected)
    executeWithAllPlanners(s"$query DESC").toList should equal(expected.reverse)
  }

  test("should provide sensible error message when ordering by mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform ORDER BY on mixed types.")
    }
  }

  test("should provide sensible error message when ordering by list values") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List(List("a"),List("b")))))
      exception.getMessage should startWith("Cannot perform ORDER BY on lists, consider using UNWIND.")
    }
  }
}
