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

import org.neo4j.cypher.{QueryStatisticsTestSupport, ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node


class UnwindAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with QueryStatisticsTestSupport {

  // TCK'd
  test("unwind list returns individual values") {

    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND [1, 2, 3] AS x RETURN x"
    )

    result.columnAs[Long]("x").toList should equal(List(1, 2, 3))
  }

  // TCK'd
  test("unwind a range") {

    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND range(1, 3) AS x RETURN x"
    )

    result.columnAs[Long]("x").toList should equal(List(1, 2, 3))
  }

  // TCK'd
  test("unwind a concatenation of lists") {

    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH [1, 2, 3] AS first, [4, 5, 6] AS second UNWIND (first + second) AS x RETURN x"
    )

    result.columnAs[Long]("x").toList should equal(List(1, 2, 3, 4, 5, 6))
  }

  // TCK'd
  test("unwind a collected unwound expression") {

    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND RANGE(1, 2) AS row WITH collect(row) AS rows UNWIND rows AS x RETURN x"
    )

    result.columnAs[Long]("x").toList should equal(List(1, 2))
  }

  // TCK'd
  test("unwind a collected expression") {
    val a = createLabeledNode(Map("id" -> 1))
    val b = createLabeledNode(Map("id" -> 2))

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node"
    )

    result.columnAs[Node]("node").toList should equal(List(a, b))
  }

  // TCK'd
  test("create nodes from a list parameter") {
    createLabeledNode(Map("year" -> 2014), "Year")

    val result = updateWithBothPlannersAndCompatibilityMode(
      "UNWIND {events} AS event MATCH (y:Year {year: event.year}) MERGE (y)<-[:IN]-(e:Event {id: event.id}) RETURN e.id AS x ORDER BY x",
      "events" -> List(Map("year" -> 2014, "id" -> 1), Map("year" -> 2014, "id" -> 2))
    )

    assertStats(result, nodesCreated = 2, relationshipsCreated = 2, labelsAdded = 2, propertiesWritten = 2)
    result.columnAs[Long]("x").toList should equal(List(1, 2))
  }

  // TCK'd
  test("double unwinding a list of lists returns one row per item") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH [[1,2,3], [4,5,6]] AS lol UNWIND lol AS x UNWIND x AS y RETURN y"
    )

    result.columnAs[Long]("y").toList should equal(List(1, 2, 3, 4, 5, 6))
  }

  // TCK'd
  test("no rows for unwinding an empty list") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND [] AS empty RETURN empty"
    )

    result.columnAs[Long]("empty").toList should equal(List())
  }

  // TCK'd
  test("no rows for unwinding null") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND null AS empty RETURN empty"
    )

    result.columnAs[Long]("empty").toList should equal(List())
  }

  // TCK'd
  test("one row per item of a list even with duplicates") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS duplicate RETURN duplicate"
    )

    result.columnAs[Long]("duplicate").toList should equal(List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
  }

  // TCK'd
  test("unwind does not remove anything from the context") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH [1, 2, 3] AS list UNWIND list AS x RETURN *"
    )

    result.toList should equal(List(
      Map("list" -> List(1, 2, 3), "x" -> 1),
      Map("list" -> List(1, 2, 3), "x" -> 2),
      Map("list" -> List(1, 2, 3), "x" -> 3)
    ))
  }

  // TCK'd
  test("unwind does not remove variables from scope") {
    val s = createLabeledNode("Start")
    val n = createNode()
    relate(s, n, "X")
    relate(s, n, "Y")
    relate(createNode(), n, "Y")

    val result = executeWithAllPlannersAndCompatibilityMode("""MATCH (a:Start)-[:X]->(b1)
                                         |WITH a, collect(b1) AS bees
                                         |UNWIND bees AS b2
                                         |MATCH (a)-[:Y]->(b2)
                                         |RETURN a, b2""".stripMargin)

    result.toList should equal(List(Map("a" -> s, "b2" -> n)))
  }

  // TCK'd
  test("multiple unwinds after each other work like expected") {
    val result =
      executeWithAllPlannersAndCompatibilityMode( """WITH [1, 2] AS xs, [3, 4] AS ys, [5, 6] AS zs
                             |UNWIND xs AS x
                             |UNWIND ys AS y
                             |UNWIND zs AS z
                             |RETURN *""".stripMargin)

    result.toList should equal(
      List(
        Map("x" -> 1, "y" -> 3, "z" -> 5, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 1, "y" -> 3, "z" -> 6, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 1, "y" -> 4, "z" -> 5, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 1, "y" -> 4, "z" -> 6, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 2, "y" -> 3, "z" -> 5, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 2, "y" -> 3, "z" -> 6, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 2, "y" -> 4, "z" -> 5, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)),
        Map("x" -> 2, "y" -> 4, "z" -> 6, "zs" -> List(5, 6), "ys" -> List(3, 4), "xs" -> List(1, 2)))
    )
  }

  // TCK'd
  test("unwind should work with merge in GH#6057") {
    val query = "UNWIND {props} AS prop MERGE (p:Person {login: prop.login}) SET p.name = prop.name RETURN p.name, p.login"
    val params = "props" -> Seq( Map("login" -> "login1", "name" -> "name1"),
                                 Map("login" -> "login2", "name" -> "name2"))

    val result = updateWithBothPlannersAndCompatibilityMode(query, params)

    assertStats(result, nodesCreated = 2, propertiesWritten = 4, labelsAdded = 2)
    result.toList should equal(List(Map("p.name" -> "name1", "p.login" -> "login1"),
                                    Map("p.name" -> "name2", "p.login" -> "login2")))
  }

  // Performance -- out of scope for TCK
  test("should unwind a long range without going OOM") {
    val expectedResult = 20000000

    val result = executeScalarWithAllPlanners[Long](s"UNWIND range(1, $expectedResult) AS i RETURN count(*) AS c")

    result should equal(expectedResult)
  }

}
