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
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class HyperedgeCommonGroupsTest extends DocumentingTestBase {
  override def graphDescription = List(
      "User1 hasRoleInGroup U1G1R12",
      "U1G1R12 hasGroup Group1",
      "U1G1R12 hasRole Role1",
      "U1G1R12 hasRole Role2",
      "User1 hasRoleInGroup U1G2R23",
      "U1G2R23 hasGroup Group2",
      "U1G2R23 hasRole Role2",
      "U1G2R23 hasRole Role3",
      "User1 hasRoleInGroup U1G3R34",
      "U1G3R34 hasGroup Group3",
      "U1G3R34 hasRole Role3",
      "U1G3R34 hasRole Role4",
      "User2 hasRoleInGroup U2G1R25",
      "U2G1R25 hasGroup Group1",
      "U2G1R25 hasRole Role2",
      "U2G1R25 hasRole Role5",
      "User2 hasRoleInGroup U2G2R34",
      "U2G2R34 hasGroup Group2",
      "U2G2R34 hasRole Role3",
      "U2G2R34 hasRole Role4",
      "User2 hasRoleInGroup U2G3R56",
      "U2G3R56 hasGroup Group3",
      "U2G3R56 hasRole Role5",
      "U2G3R56 hasRole Role6"
      )

    def section = "cookbook"

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }
    @Test def findCommonGroups() {
      testQuery(
        title = "Find common groups based on shared roles",
        text = """Assume a more complicated graph:

1. Two user nodes +User1+, +User2+.
2. +User1+ is in +Group1+, +Group2+, +Group3+.
3. +User1+ has +Role1+, +Role2+ in +Group1+; +Role2+, +Role3+ in +Group2+; +Role3+, +Role4+ in +Group3+ (hyper edges).
4. +User2+ is in +Group1+, +Group2+, +Group3+.
5. +User2+ has +Role2+, +Role5+ in +Group1+; +Role3+, +Role4+ in +Group2+; +Role5+, +Role6+ in +Group3+ (hyper edges).

The graph for this looks like the following (nodes like +U1G2R23+ representing the HyperEdges):

.Graph
include::includes/cypher-hyperedgecommongroups-graph.asciidoc[]

To return +Group1+ and +Group2+ as +User1+ and +User2+ share at least one common role in these two groups, the query looks like this:
               """,
        queryText =
          "match " +
          "(u1)-[:hasRoleInGroup]->(hyperEdge1)-[:hasGroup]->(group), " +
          "(hyperEdge1)-[:hasRole]->(role), " +
          "(u2)-[:hasRoleInGroup]->(hyperEdge2)-[:hasGroup]->(group), " +
          "(hyperEdge2)-[:hasRole]->(role) " +
          "where u1.name = 'User1' and u2.name = 'User2' " +
          "return group.name, count(role) " +
          "order by group.name ASC",
        optionalResultExplanation = "The groups where +User1+ and +User2+ share at least one common role:",
        assertions = (p) => assertEquals(List(Map("group.name" -> "Group1", "count(role)" -> 1), Map("group.name" -> "Group2", "count(role)" -> 1)), p.toList))
    }

  
}
