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
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class ComplexSimilarityTest extends DocumentingTestBase {
  def section = "cookbook"

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }
  
  override val setupQueries = List(
      "CREATE (me {name:'me'})-[:ATE {times:10}]->(food {name:'meat'})<-[:ATE {times:5}]-(you {name:'you'})")

  @Test def testSimliarity() {
    testQuery(
      title = "Calculate similarities by complex calculations",
      text =
"""Here, a similarity between two players in a game is calculated by the number of times they have eaten the same food.""",
      queryText = """MATCH (me {name: 'me'})-[r1:ATE]->(food)<-[r2:ATE]-(you)
WITH me,count(distinct r1) as H1,count(distinct r2) as H2,you
MATCH (me)-[r1:ATE]->(food)<-[r2:ATE]-(you)
RETURN sum((1-ABS(r1.times/H1-r2.times/H2))*(r1.times+r2.times)/(H1+H2)) as similarity""",
      optionalResultExplanation = "The two players and their similarity measure.",
      assertions = (p) => assertEquals(List(Map("similarity" -> -30.0)),p.toList))
  } 
}
