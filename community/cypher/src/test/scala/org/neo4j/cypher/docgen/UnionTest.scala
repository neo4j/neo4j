/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.StatisticsChecker
import org.junit.Test
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class UnionTest extends DocumentingTestBase with StatisticsChecker {

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  
  def graphDescription = List(
    "Lucy:Actor KNOWS Kevin:Actor",
    "Lucy ACTS_IN Cypher:Movie",
    "Kevin ACTS_IN Cypher"
  )

  override val properties: Map[String, Map[String, Any]] = Map(
    "Lucy" -> Map("name" -> "Lucy Liu"),
    "Kevin" -> Map("name" -> "Kevin Bacon"),
    "Cypher" -> Map("title" -> "Cypher")
  )

  def section = "Union"

  @Test def union_between_two_queries() {
    testQuery(
      title = "Union two queries",
      text = "Combining the results from two queries is done using UNION ALL",
      queryText =
        """match n:Actor return n.name as name
           UNION ALL
           match n:Movie return n.title as name""",
      returns = "The combined result is returned.",
      assertions = (p) => assert(p.toList === List(Map("name" -> "Lucy Liu"), Map("name" -> "Kevin Bacon"), Map("name" -> "Cypher")))
    )
  }

  @Test def union_between_two_queries_distinct() {
    testQuery(
      title = "Combine two queries and removing duplicates",
      text = "By not uncluding +ALL+ in the +UNION+, duplicates are removed from the combined result set",
      queryText =
        """match n:Actor return n.name as name
           UNION
           match n:Movie return n.title as name""",
      returns = "The combined result is returned.",
      assertions = (p) => assert(p.toList === List(Map("name" -> "Lucy Liu"), Map("name" -> "Kevin Bacon"), Map("name" -> "Cypher")))
    )
  }
}