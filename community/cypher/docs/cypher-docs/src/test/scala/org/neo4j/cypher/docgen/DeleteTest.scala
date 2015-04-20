/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.DynamicLabel

class DeleteTest extends DocumentingTestBase with QueryStatisticsTestSupport {
  override def graphDescription = List("Andres KNOWS Tobias", "Andres KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("name"->"Andres", "age" -> 36l),
    "Tobias" -> Map("name"->"Tobias", "age" -> 25l),
    "Peter"  -> Map("name"->"Peter",  "age" -> 34l)
  )

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Delete"

  @Test def delete_single_node() {

    db.inTx(db.createNode(DynamicLabel.label("Useless")))

    testQuery(
      title = "Delete single node",
      text = "To delete a node, use the +DELETE+ clause.",
      queryText = "match (n:Useless) delete n",
      optionalResultExplanation = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertStats(p, nodesDeleted = 1))
  }

  @Test def delete_single_node_with_all_relationships() {
    testQuery(
      title = "Delete a node and connected relationships",
      text = "If you are trying to delete a node with relationships on it, you have to delete these as well.",
      queryText = "match (n {name: 'Andres'})-[r]-() delete n, r",
      optionalResultExplanation = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertStats(p, relationshipsDeleted = 2, nodesDeleted = 1))
  }

  @Test def delete_all_nodes_and_all_relationships() {
    testQuery(
      title = "Delete all nodes and relationships",
      text = "This query isn't for deleting large amounts of data, but is nice when playing around with small example data sets.",
      queryText = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r",
      optionalResultExplanation = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertStats(p, relationshipsDeleted = 2, nodesDeleted = 3))
  }
}
