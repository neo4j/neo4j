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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node


class UnwindAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("unwind collection returns individual values") {

    val result = executeWithAllPlanners(
      "UNWIND [1,2,3] as x return x"
    )
    result.columnAs[Int]("x").toList should equal(List(1, 2, 3))
  }

  test("unwind a range") {

    val result = executeWithAllPlanners(
      "UNWIND RANGE(1,3) as x return x"
    )
    result.columnAs[Int]("x").toList should equal(List(1, 2, 3))
  }
  test("unwind a concatenation of collections") {

    val result = executeWithAllPlanners(
      "WITH [1,2,3] AS first, [4,5,6] AS second UNWIND (first + second) as x return x"
    )
    result.columnAs[Int]("x").toList should equal(List(1, 2, 3, 4, 5, 6))
  }

  test("unwind a collected unwound expression") {

    val result = executeWithAllPlanners(
      "UNWIND RANGE(1,2) AS row WITH collect(row) as rows UNWIND rows as x return x"
    )
    result.columnAs[Int]("x").toList should equal(List(1, 2))
  }

  test("unwind a collected expression") {
    val a = createLabeledNode(Map("id" -> 1))
    val b = createLabeledNode(Map("id" -> 2))

    val result = executeWithAllPlanners(
      "MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node"
    )
    result.columnAs[Node]("node").toList should equal(List(a, b))
  }

  test("create nodes from a collection parameter") {
    createLabeledNode(Map("year" -> 2014), "Year")

    val result = executeWithRulePlanner(
      "UNWIND {events} as event MATCH (y:Year {year:event.year}) MERGE (y)<-[:IN]-(e:Event {id:event.id}) RETURN e.id as x order by x",
      "events" -> List(Map("year" -> 2014, "id" -> 1), Map("year" -> 2014, "id" -> 2))
    )
    result.columnAs[Int]("x").toList should equal(List(1, 2))
  }

  test("double unwinding a collection of collections returns one row per item") {
    val result = executeWithAllPlanners(
      "WITH [[1,2,3], [4,5,6]] AS coc UNWIND coc AS x UNWIND x AS y RETURN y"
    )
    result.columnAs[Int]("y").toList should equal(List(1, 2, 3, 4, 5, 6))
  }

  test("no rows for unwinding an empty collection") {
    val result = executeWithAllPlanners(
      "UNWIND [] AS empty RETURN empty"
    )
    result.columnAs[Int]("empty").toList should equal(List())
  }

  test("no rows for unwinding null") {
    val result = executeWithAllPlanners(
      "UNWIND null AS empty RETURN empty"
    )
    result.columnAs[Int]("empty").toList should equal(List())
  }

  test("one row per item of a collection even with duplicates") {
    val result = executeWithAllPlanners(
      "UNWIND [1,1,2,2,3,3,4,4,5,5] AS duplicate RETURN duplicate"
    )
    result.columnAs[Int]("duplicate").toList should equal(List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
  }

  test("unwind does not remove anything from the context") {
    val result = executeWithAllPlanners(
      "WITH [1,2,3] as collection UNWIND collection AS x RETURN *"
    )
    result.toList should equal(List(
      Map("collection" -> List(1, 2, 3), "x" -> 1),
      Map("collection" -> List(1, 2, 3), "x" -> 2),
      Map("collection" -> List(1, 2, 3), "x" -> 3)
    ))
  }

  test("unwind does not remove identifiers from scope") {
    val s1 = createLabeledNode("Start")
    val n2 = createNode()
    relate(s1, n2, "X")
    relate(s1, n2, "Y")
    relate(createNode(), n2, "Y")

    val result = executeWithAllPlanners("""MATCH (a:Start)-[:X]->(b1)
                                         |WITH a, COLLECT(b1) AS bees
                                         |UNWIND bees as b2
                                         |MATCH (a)-[:Y]->(b2)
                                         |RETURN a, b2""".stripMargin)
    result.toList should equal(List(Map("a" -> s1, "b2" -> n2)))
  }

  test("multiple unwinds after each other work like expected") {
    val result =
      executeWithAllPlanners( """WITH [1,2] as XS, [3,4] as YS, [5,6] as ZS
                             |UNWIND XS as X
                             |UNWIND YS as Y
                             |UNWIND ZS as Z
                             |RETURN *""".stripMargin)

    result.toList should equal(
      List(
        Map("X" -> 1, "Y" -> 3, "Z" -> 5, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 1, "Y" -> 3, "Z" -> 6, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 1, "Y" -> 4, "Z" -> 5, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 1, "Y" -> 4, "Z" -> 6, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 2, "Y" -> 3, "Z" -> 5, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 2, "Y" -> 3, "Z" -> 6, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 2, "Y" -> 4, "Z" -> 5, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)),
        Map("X" -> 2, "Y" -> 4, "Z" -> 6, "ZS" -> List(5, 6), "YS" -> List(3, 4), "XS" -> List(1, 2)))
    )
  }

  test("should unwind a long range without going OOM") {
    val expectedResult = 20000000
    val result = executeScalarWithAllPlanners[Long](s"unwind range(1,$expectedResult) as i return count(*) as c")
    result should equal(expectedResult)
  }
}
