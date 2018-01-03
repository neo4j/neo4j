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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class ForeachTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {
  override def graphDescription = List("A KNOWS B", "B KNOWS C", "C KNOWS D")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Foreach"

  @Test def mark_all_nodes_along_a_path() {
    testQuery(
      title = "Mark all nodes along a path",
      text = "This query will set the property `marked` to true on all nodes along a path.",
      queryText = "match p = (begin)-[*]->(end) where begin.name='A' and end.name='D' foreach(n in nodes(p) | set n.marked = true)",
      optionalResultExplanation = "Nothing is returned from this query, but four properties are set.",
      assertions = (p) => { assertStats(p, propertiesSet = 4); assertEquals(p.toList.length, 0) })
  }
}
