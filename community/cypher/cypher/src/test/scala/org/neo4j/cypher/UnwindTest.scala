/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.graphdb.Node

@RunWith(classOf[JUnitRunner])
class UnwindTest extends ExecutionEngineFunSuite {

  test("unwind collection returns individual values") {

    val result = execute(
      "UNWIND [1,2,3] as x return x"
    )
    result.columnAs[Int]("x").toList should equal (List(1,2,3))
  }

  test("unwind a range") {

    val result = execute(
      "UNWIND RANGE(1,3) as x return x"
    )
    result.columnAs[Int]("x").toList should equal (List(1,2,3))
  }
  test("unwind a concatenation of collections") {

    val result = execute(
      "WITH [1,2,3] AS first, [4,5,6] AS second UNWIND (first + second) as x return x"
    )
    result.columnAs[Int]("x").toList should equal (List(1,2,3,4,5,6))
  }

  test("unwind a collected unwound expression") {

    val result = execute(
      "UNWIND RANGE(1,2) AS row WITH collect(row) as rows UNWIND rows as x return x"
    )
    result.columnAs[Int]("x").toList should equal (List(1,2))
  }

  test("unwind a collected expression") {
    createLabeledNode(Map("id"->1))
    createLabeledNode(Map("id"->2))

    val result = execute(
      "MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node.id as x order by x"
    )
    result.columnAs[Int]("x").toList should equal (List(1,2))
  }

  test("create nodes from a collection parameter") {
    createLabeledNode(Map("year"->2014),"Year")

    val result = execute(
      "UNWIND {events} as event MATCH (y:Year {year:event.year}) MERGE (y)<-[:IN]-(e:Event {id:event.id}) RETURN e.id as x order by x",
      "events" -> List(Map("year" -> 2014, "id" -> 1), Map("year" -> 2014, "id" -> 2))
    )
    result.columnAs[Int]("x").toList should equal (List(1,2))
  }

  test("double unwinding a collection of collections returns one row per item") {
    val result = execute(
      "WITH [[1,2,3], [4,5,6]] AS coc UNWIND coc AS x UNWIND x AS y RETURN y"
    )
    result.columnAs[Int]("y").toList should equal (List(1,2,3,4,5,6))
  }

  test("no rows for unwinding an empty collection") {
    val result = execute(
      "UNWIND [] AS empty RETURN empty"
    )
    result.columnAs[Int]("empty").toList should equal (List())
  }

  test("no rows for unwinding null") {
    val result = execute(
      "UNWIND null AS empty RETURN empty"
    )
    result.columnAs[Int]("empty").toList should equal (List())
  }

  test("one row per item of a collection even with duplicates") {
    val result = execute(
      "UNWIND [1,1,2,2,3,3,4,4,5,5] AS duplicate RETURN duplicate"
    )
    result.columnAs[Int]("duplicate").toList should equal (List(1,1,2,2,3,3,4,4,5,5))
  }
}
