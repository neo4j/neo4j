/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.graphdb.Node
import org.junit.Test

class OrderByTest extends DocumentingTestBase {
  def indexProps = List()

  def graphDescription = List("A KNOWS B", "B KNOWS C")

  override val properties = Map(
    "A" -> Map("age" -> 34),
    "B" -> Map("age" -> 34),
    "C" -> Map("age" -> 32))

  def section = "Order by"

  @Test def sortByName() {
    testQuery(
      title = "Order nodes by property",
      text = "ORDER BY is used to sort the output",
      queryText = """start n=(%C%,%A%,%B%) return n order by n.name""",
      returns = """The nodes, sorted by their name.""",
      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def sortByNameReverse() {
    testQuery(
      title = "Order nodes in descending order",
      text = "By adding DESC[ENDING] after the identifier to sort on, the sort will be done in reverse order.",
      queryText = """start n=(%C%,%A%,%B%) return n order by n.name DESC""",
      returns = """The nodes, sorted by their name reversely.""",
      (p) => assertEquals(List(node("C"), node("B"), node("A")), p.columnAs[Node]("n").toList))
  }

  @Test def sortByMultipleColumns() {
    testQuery(
      title = "Order nodes by multiple properties",
      text = "You can sort by multiple properties by stating each identifier in the SORT BY" +
        " statement. Cypher will sort the result by the first column, and for equals values, " +
        "go to the next property listen in the sort by.",
      queryText = """start n=(%C%,%A%,%B%) return n order by n.age, n.name""",
      returns = """The nodes, sorted first by their age, and then by their name.""",
      (p) => assertEquals(List(node("C"), node("A"), node("B")), p.columnAs[Node]("n").toList))
  }
}