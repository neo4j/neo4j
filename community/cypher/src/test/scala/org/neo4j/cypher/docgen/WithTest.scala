/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Test
import org.neo4j.graphdb.Node
import org.junit.Assert._

class WithTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E", "B BLOCKS D")

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Bossman"),
    "C" -> Map("name" -> "Cesar"),
    "D" -> Map("name" -> "David"),
    "E" -> Map("name" -> "Emil")
  )

  def section = "With"

  @Test def filter_on_aggregate_functions_results() {
    testQuery(
      title = "Filter on aggregate function results",
      text = "Aggregated results have to pass through a `WITH` clause to be able to filter on.",
      queryText = """start david=node(%D%) match david--otherPerson-->() with otherPerson, count(*) as foaf where foaf > 1 return otherPerson""",
      returns = """The person connected to David with the at least more than one outgoing relationship will be returned by the query.""",
      assertions = (p) => assertEquals(List(node("A")), p.columnAs[Node]("otherPerson").toList))
  }

  @Test def alternative_way_to_write_with() {
    testQuery(
      title = "Alternative syntax of WITH",
      text = "If you prefer a more visual way of writing your query, you can use " +
        "equal-signs as delimiters before and after the column list. Use at least three " +
        "before the column list, and at least three after.",
      queryText = """
start david=node(%D%) match david--otherPerson-->()
========== otherPerson, count(*) as foaf ==========
set otherPerson.connection_count = foaf """,
      returns = """The person connected to David with the at least more than one outgoing relationship.""",
      assertions = (p) => assertEquals(node("A").getProperty("connection_count"), 2L))
  }
}