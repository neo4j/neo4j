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
package org.neo4j.cypher.docgen

import org.junit.Assert._
import org.junit.Test

class UnwindTest extends DocumentingTestBase {

  def section = "Unwind"

  @Test def simple_unwind() {
    testQuery(
      title = "Unwind a list",
      text = "We want to transform the literal list into rows named `x` and return them.",
      queryText = """UNWIND [1,2,3] as x RETURN x""",
      optionalResultExplanation = "Each value of the original list is returned as an individual row.",
      assertions = (p) => assertEquals(List(1,2,3), p.columnAs[Int]("x").toList)
    )
  }
  @Test def distinct_collection() {
    testQuery(
      title = "Create a distinct list",
      text = "We want to transform a list of duplicates into a set using `DISTINCT`.",
      queryText = """WITH [1,1,2,2] AS coll UNWIND coll AS x WITH DISTINCT x RETURN collect(x) AS set""",
      optionalResultExplanation = "Each value of the original list is unwound and passed through `DISTINCT` to create a unique set.",
      assertions = (p) => assertEquals(List(List(1,2)), p.columnAs[Int]("set").toList)
    )
  }

  @Test def create_data_from_collection_parameter() {
    testQuery(
      title = "Create nodes from a list parameter",
      text = "Create a number of nodes and relationships from a parameter-list without using `FOREACH`.",
      parameters = Map("events" -> List(Map("year" -> 2014, "id" -> 1), Map("year" -> 2014, "id" -> 2))),
      queryText =
        """UNWIND {events} AS event
           MERGE (y:Year {year: event.year})
           MERGE (y)<-[:IN]-(e:Event {id: event.id})
           RETURN e.id AS x ORDER BY x""",
      optionalResultExplanation = "Each value of the original list is unwound and passed through `MERGE` to find or create the nodes and relationships.",
      assertions = (p) => assertEquals(List(1,2), p.columnAs[Int]("x").toList)
    )
  }
}
