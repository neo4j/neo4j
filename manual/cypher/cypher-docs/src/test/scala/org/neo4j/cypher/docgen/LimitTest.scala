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

class LimitTest extends DocumentingTestBase with SoftReset {
  override def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D", "A KNOWS E")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section: String = "Limit"

  @Test def returnFirstThree() {
      testQuery(
        title = "Return first part",
        text = "To return a subset of the result, starting from the top, use this syntax:",
        queryText = "match (n) return n order by n.name limit 3",
        optionalResultExplanation = "The top three items are returned by the example query.",
        assertions = (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
    }

  @Test def returnFromExpression() {
    testQuery(
      title = "Return first from expression",
      text = "Limit accepts any expression that evaluates to a positive integer as long as it is not referring to any external identifiers:",
      queryText = "match (n) return n order by n.name limit toInt(3 * rand()) + 1",
      parameters = Map("p" -> 12),
      optionalResultExplanation = "Returns one to three top items",
      assertions = (p) => assertTrue(p.columnAs[Node]("n").nonEmpty))
  }
}

