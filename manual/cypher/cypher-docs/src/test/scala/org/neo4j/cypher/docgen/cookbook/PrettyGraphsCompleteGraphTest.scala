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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.neo4j.cypher.docgen.{SoftReset, DocumentingTestBase}
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle
import org.neo4j.cypher.QueryStatisticsTestSupport

class PrettyGraphsCompleteGraphTest extends DocumentingTestBase with SoftReset with QueryStatisticsTestSupport {
  def section = "cookbook"
  generateInitialGraphForConsole = false
  override val graphvizOptions = "graph [layout=circo]"
  override val graphvizExecutedAfter = true

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  @Test def completeGraph() {
    testQuery(
      title = "Complete graph",
      text =
        """To create this graph, we first create 6 nodes and label them with the Leaf label. We then
          |match all the unique pairs of nodes, and create a relationship between them.
        """.stripMargin,
      queryText = """FOREACH (x in range(1,6) | CREATE (leaf:Leaf {count : x}))
WITH *
MATCH (leaf1:Leaf), (leaf2:Leaf)
WHERE id(leaf1) < id(leaf2)
CREATE (leaf1)-[:X]->(leaf2);""",
      optionalResultExplanation =
"""Nothing is returned by this query.""",
      assertions = (p) => assertStats(p, nodesCreated = 6, propertiesSet = 6, relationshipsCreated = 15, labelsAdded = 6)
    )
  } 
}
