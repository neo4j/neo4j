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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class BoostingRecommendationResultsTest extends DocumentingTestBase {
  def graphDescription = List()
  def section = "cookbook"
  generateInitialGraphForConsole = false

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }

  @Test def boostingRecommendations() {
    executeQuery("""create 
clark={name: "Clark Kent"},
lois={name:"Lois Lane"},
jimmy={name:"Jimmy Olsen"},
perry={name:"Perry White"},
cooper={name:"Anderson Cooper"},
dailyplanet={name:"Daily Planet"},
cnn={name:"CNN"},
clark-[:KNOWS {weight: 4}]->lois,
clark-[:KNOWS {weight: 4}]->jimmy,
lois-[:KNOWS {weight: 4}]->perry,
jimmy-[:KNOWS {weight: 4}]->perry,
lois-[:KNOWS {weight: 4}]->cooper,
clark-[:WORKSAT {weight: 2, activity: 45}]->dailyplanet,
jimmy-[:WORKSAT {weight: 2, activity: 10}]->dailyplanet,
perry-[:WORKSAT {weight: 2, activity: 6}]->dailyplanet,
lois-[:WORKSAT {weight: 2, activity: 56}]->dailyplanet,
cooper-[:WORKSAT {weight: 2, activity: 2}]->cnn,
perry-[:WORKSAT {weight: 2, activity: 3}]->cnn""")
    testQuery(
      title = "Boosting with properties on relationships",
      text =
"""This query finds the recommended friends for the origin that are working at the same place as the origin, 
or know a person that the origin knows, also, the origin should not already know the target. This recommendation is 
weighted for the weight of the relationship `r2`, and boosted with a factor of 2, if there is an `activity`-property on that relationship""",
      queryText = """START origin=node:node_auto_index(name = "Clark Kent")
        MATCH origin-[r1:KNOWS|WORKSAT]-(c)-[r2:KNOWS|WORKSAT]-candidate
        WHERE type(r1)=type(r2) AND (NOT (origin-[:KNOWS]-candidate)) 
        RETURN origin.name as origin, candidate.name as candidate, SUM(ROUND(r2.weight +
(COALESCE(r2.activity?, 0) * 2))) as boost 
        ORDER BY boost desc limit 10;""",
      returns =
"""This returns the recommended friends for the origin nodes and their recommendation score.""",
      assertions = (p) => assertEquals(List(
          Map("origin" -> "Clark Kent","candidate" -> "Perry White","boost" -> 22),
          Map("origin" -> "Clark Kent","candidate" -> "Anderson Cooper","boost" -> 4)),p.toList))
  } 
}
