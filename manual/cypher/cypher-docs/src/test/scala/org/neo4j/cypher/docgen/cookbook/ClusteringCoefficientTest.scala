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

class ClusteringCoefficientTest extends DocumentingTestBase {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "cookbook"
  override val noTitle = true;

  override val setupQueries = List("""
create
(_1 {name: "startnode"}),
(_2),
(_3),
(_4),
(_5),
(_6),
(_7),
(_1)-[:KNOWS]->(_2),
(_1)-[:KNOWS]->(_3),
(_1)-[:KNOWS]->(_4),
(_1)-[:KNOWS]->(_5),
(_2)-[:KNOWS]->(_6),
(_2)-[:KNOWS]->(_7),
(_3)-[:KNOWS]->(_4)""")

  @Test def calculatingClusteringCoefficient() {
    testQuery(
      title = "Calculating the Clustering Coefficient of a friend network",
      text = """In this example, adapted from
http://mypetprojects.blogspot.se/2012/06/social-network-analysis-with-neo4j.html[Niko Gamulins blog post on Neo4j for Social Network Analysis],
the graph in question is showing the 2-hop relationships of a sample person as nodes with `KNOWS` relationships.

The http://en.wikipedia.org/wiki/Clustering_coefficient[clustering coefficient] of a selected node is defined as the probability that two randomly selected neighbors are connected to each other.
With the number of neighbors as `n` and the number of mutual connections between the neighbors `r` the calculation is:

The number of possible connections between two neighbors is `n!/(2!(n-2)!) = 4!/(2!(4-2)!) = 24/4 = 6`,
where `n` is the number of neighbors `n = 4` and the actual number `r` of connections is `1`.
Therefore the clustering coefficient of node 1 is `1/6`.

`n` and `r` are quite simple to retrieve via the following query:""",
              queryText =
      		"""
MATCH (a {name: "startnode"})--(b)
WITH a, count(distinct b) as n
MATCH (a)--()-[r]-()--(a)
RETURN n, count(distinct r) as r
""",
      optionalResultExplanation = "This returns `n` and `r` for the above calculations.",
      assertions = (p) => assertEquals(List(
        Map("n" -> 4, "r" -> 1)), p.toList))
  }
}

