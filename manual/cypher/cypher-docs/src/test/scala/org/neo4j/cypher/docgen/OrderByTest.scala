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
package org.neo4j.cypher.docgen

import org.junit.Assert._
import org.junit.Test
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class OrderByTest extends DocumentingTestBase with SoftReset {
  override def graphDescription = List("A KNOWS B", "B KNOWS C")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override val properties = Map(
    "A" -> Map("age" -> 34, "length"->170),
    "B" -> Map("age" -> 34),
    "C" -> Map("age" -> 32, "length"->185))

  def section = "Order by"

  @Test def sortByName() {
    testQuery(
      title = "Order nodes by property",
      text = "+ORDER BY+ is used to sort the output.",
      queryText = """match (n) return n order by n.name""",
      optionalResultExplanation = """The nodes are returned, sorted by their name.""",
      assertions = (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def sortByNameReverse() {
    testQuery(
      title = "Order nodes in descending order",
      text = "By adding +DESC[ENDING]+ after the identifier to sort on, the sort will be done in reverse order.",
      queryText = """match (n) return n order by n.name DESC""",
      optionalResultExplanation = """The example returns the nodes, sorted by their name reversely.""",
      assertions = (p) => assertEquals(List(node("C"), node("B"), node("A")), p.columnAs[Node]("n").toList))
  }

  @Test def sortByMultipleColumns() {
    testQuery(
      title = "Order nodes by multiple properties",
      text = "You can order by multiple properties by stating each identifier in the +ORDER BY+" +
        " clause. Cypher will sort the result by the first identifier listed, and for equals values, " +
        "go to the next property in the `ORDER BY` clause, and so on.",
      queryText = """match (n) return n order by n.age, n.name""",
      optionalResultExplanation = """This returns the nodes, sorted first by their age, and then by their name.""",
      assertions = (p) => assertEquals(List(node("C"), node("A"), node("B")), p.columnAs[Node]("n").toList))
  }

  @Test def order_by_nullable_property() {
    testQuery(
      title = "Ordering NULL",
      text = "When sorting the result set, +NULL+ will always come at the end of the result set for" +
        " ascending sorting, and first when doing descending sort.",
      queryText = """match (n) return n.length, n order by n.length""",
      optionalResultExplanation = """The nodes are returned sorted by the length property, with a node without that property last.""",
      assertions = (p) => assertEquals(List(node("A"), node("C"), node("B")), p.columnAs[Node]("n").toList))
  }
}
