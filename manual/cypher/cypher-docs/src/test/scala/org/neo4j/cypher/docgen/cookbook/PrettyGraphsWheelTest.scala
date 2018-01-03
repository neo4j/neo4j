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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.{DocumentingTestBase, HardReset}
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class PrettyGraphsWheelTest extends DocumentingTestBase with HardReset with QueryStatisticsTestSupport {
  def section = "cookbook"
  generateInitialGraphForConsole = false
  override val graphvizOptions = "graph [layout=neato]"
  override val graphvizExecutedAfter = true

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  @Test def completeGraph() {
    testQuery(
      title = "Wheel graph",
      text =
"""This graph is created in a number of steps:
        
- Create a center node.
- Once per element in the range, create a leaf and connect it to the center.
- Connect neighboring leafs.
- Find the minimum and maximum leaf and connect these.
- Return the id of the center node.""",
      queryText = """CREATE (center)
foreach( x in range(1,6) |
   CREATE (leaf {count:x}), (center)-[:X]->(leaf)
)
WITH center
MATCH (large_leaf)<--(center)-->(small_leaf)
WHERE large_leaf.count = small_leaf.count + 1
CREATE (small_leaf)-[:X]->(large_leaf)

WITH center, min(small_leaf.count) as min, max(large_leaf.count) as max
MATCH (first_leaf)<--(center)-->(last_leaf)
WHERE first_leaf.count = min AND last_leaf.count = max
CREATE (last_leaf)-[:X]->(first_leaf)

RETURN id(center) as id""",
      optionalResultExplanation =
"""The query returns the id of the center node.""",
      assertions = { (p) =>
        assertEquals(List(Map("id" -> 0)), p.toList)
        assertStats(p, nodesCreated = 7, relationshipsCreated = 12, propertiesSet = 6)
      }
    )
  }
}
