/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.tooling.{DocBuilder, DocumentingTest, ResultAssertions}

class PrettyGraphsTest extends DocumentingTest with QueryStatisticsTestSupport {
  override def outputPath = "target/docs/dev/ql/cookbook/"
  override def doc = new DocBuilder {
    doc("Pretty graphs", "cypher-cookbook-pretty-graphs")
    synopsis("This section is showing how to create some of the http://en.wikipedia.org/wiki/Gallery_of_named_graphs[named pretty graphs on Wikipedia].")
    section("Star Graph") {
      p("The graph is created by first creating a center node, and then once per element in the range, creates a leaf node and connects it to the center.")
      query( """CREATE (center)
               |FOREACH (x IN range(1,6)| CREATE (leaf),(center)-[:X]->(leaf))
               |RETURN id(center) AS id""", assertAStarIsBorn) {
        p("The query returns the id of the center node.")
        resultTable()
        graphViz("graph [layout=neato]")
      }
    }
    section("Wheel graph") {
      p( """This graph is created in a number of steps:
           |
           |- Create a center node.
           |- Once per element in the range, create a leaf and connect it to the center.
           |- Connect neighboring leafs.
           |- Find the minimum and maximum leaf and connect these.
           |- Return the id of the center node.""")
      query( """CREATE (center)
               |foreach( x in range(1,6) |
               |   CREATE (leaf {count:x}), (center)-[:X]->(leaf)
               |)
               |WITH center
               |MATCH (large_leaf)<--(center)-->(small_leaf)
               |WHERE large_leaf.count = small_leaf.count + 1
               |CREATE (small_leaf)-[:X]->(large_leaf)
               |
               |WITH center, min(small_leaf.count) as min, max(large_leaf.count) as max
               |MATCH (first_leaf)<--(center)-->(last_leaf)
               |WHERE first_leaf.count = min AND last_leaf.count = max
               |CREATE (last_leaf)-[:X]->(first_leaf)
               |
               |RETURN id(center) as id""", assertWheelGraph) {
        p("The query returns the id of the center node.")
        resultTable()
        graphViz("graph [layout=neato]")
      }
    }
    section("Complete graph") {
      p(
        """To create this graph, we first create 6 nodes and label them with the Leaf label.
          |We then match all the unique pairs of nodes, and create a relationship between them.""".stripMargin)
      query( """FOREACH (x IN range(1,6)| CREATE (leaf:Leaf { count : x }))
               |WITH *
               |MATCH (leaf1:Leaf),(leaf2:Leaf)
               |WHERE leaf1.count < leaf2.count
               |CREATE (leaf1)-[:X]->(leaf2)""", assertCompleteGraph) {
        p("Nothing is returned by this query")
        resultTable()
        graphViz("graph [layout=circo]")
      }
    }
    section("Friendship graph") {
      p(
        """This query first creates a center node, and then once per element in the range, creates a cycle graph and connects it to the center.""".stripMargin)
      query( """CREATE (center)
               |FOREACH (x IN range(1,3)| CREATE (leaf1),(leaf2),(center)-[:X]->(leaf1),(center)-[:X]->(leaf2),
               |  (leaf1)-[:X]->(leaf2))
               |RETURN ID(center) AS id""", assertFriendshipGraph) {
        resultTable()
        graphViz("graph [layout=neato]")
      }
    }
  }.build()

  private def assertAStarIsBorn = ResultAssertions { p =>
    assertStats(p, nodesCreated = 7, relationshipsCreated = 6)
  }

  private def assertWheelGraph = ResultAssertions { p =>
    assertStats(p, nodesCreated = 7, relationshipsCreated = 12, propertiesWritten = 6)
  }

  private def assertCompleteGraph = ResultAssertions { p =>
    assertStats(p, nodesCreated = 6, relationshipsCreated = 15, propertiesWritten = 6, labelsAdded = 6)
  }

  private def assertFriendshipGraph = ResultAssertions { p =>
    assertStats(p, nodesCreated = 7, relationshipsCreated = 9)
  }
}
