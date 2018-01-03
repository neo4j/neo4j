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

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.junit.Test
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class UnionTest extends DocumentingTestBase with QueryStatisticsTestSupport {

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  
  override val setupQueries = List("""
create (ah:Actor{name: 'Anthony Hopkins'}),
  (hm:Actor {name: 'Helen Mirren'}),
  (hitchcock:Actor {name: 'Hitchcock'}),
  (hitchcockMovie:Movie {title: 'Hitchcock'}),
  (ah)-[:KNOWS]->(hm),
  (ah)-[:ACTS_IN]->(hitchcockMovie),
  (hm)-[:ACTS_IN]->(hitchcockMovie)
""")

  def section = "Union"

  @Test def union_between_two_queries() {
    testQuery(
      title = "Combine two queries",
      text = "Combining the results from two queries is done using +UNION ALL+.",
      queryText =
        """match (n:Actor) return n.name as name
           UNION ALL
           match (n:Movie) return n.title as name""",
      optionalResultExplanation = "The combined result is returned, including duplicates.",
      assertions = (p) => {
        val result = p.toList
        assert(result.size === 4)
        assert(result.toSet === Set(Map("name" -> "Anthony Hopkins"), Map("name" -> "Helen Mirren"), Map("name" -> "Hitchcock")))
      }
    )
  }

  @Test def union_between_two_queries_distinct() {
    testQuery(
      title = "Combine two queries and remove duplicates",
      text = "By not including +ALL+ in the +UNION+, duplicates are removed from the combined result set",
      queryText =
        """match (n:Actor) return n.name as name
UNION
match (n:Movie) return n.title as name""",
      optionalResultExplanation = "The combined result is returned, without duplicates.",
      assertions = (p) => {
        val result = p.toList
        assert(result.size === 3)
        assert(result.toSet === Set(Map("name" -> "Anthony Hopkins"), Map("name" -> "Helen Mirren"), Map("name" -> "Hitchcock")))
      }
    )
  }
}
