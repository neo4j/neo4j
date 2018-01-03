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

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class SkipTest extends DocumentingTestBase {
  override def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D", "A KNOWS E")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section: String = "Skip"

  @Test def returnFromThree() {
    testQuery(
      title = "Skip first three",
      text = "To return a subset of the result, starting from the fourth result, use the following syntax:",
      queryText = "match (n) return n order by n.name skip 3",
      optionalResultExplanation = "The first three nodes are skipped, and only the last two are returned in the result.",
      assertions = (p) => assertEquals(List(node("D"), node("E")), p.columnAs[Node]("n").toList))
  }

  @Test def returnFromOneLimitTwo() {
    testQuery(
      title = "Return middle two",
      text = "To return a subset of the result, starting from somewhere in the middle, use this syntax:",
      queryText = "match (n) return n order by n.name skip 1 limit 2",
      optionalResultExplanation = "Two nodes from the middle are returned.",
      assertions = (p) => assertEquals(List(node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def returnFromExpression() {
    testQuery(
      title = "Skip first from expression",
      text = "Skip accepts any expression that evaluates to a positive integer as long as it is not referring to any external identifiers:",
      queryText = "match (n) return n order by n.name skip toInt(3*rand()) + 1",
      optionalResultExplanation = "The first three nodes are skipped, and only the last two are returned in the result.",
      assertions = (p) => assertTrue(p.columnAs[Node]("n").nonEmpty))
  }
}

