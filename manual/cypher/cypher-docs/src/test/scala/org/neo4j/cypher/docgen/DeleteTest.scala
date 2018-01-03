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

import org.junit.Test
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class DeleteTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {
  override def graphDescription = List("Andres KNOWS Tobias", "Andres KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map[String, Any]("name"->"Andres", "age" -> 36l),
    "Tobias" -> Map[String, Any]("name"->"Tobias", "age" -> 25l),
    "Peter"  -> Map[String, Any]("name"->"Peter",  "age" -> 34l)
  )

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Delete"

  @Test def delete_single_node() {
    prepareAndTestQuery(
      title = "Delete single node",
      text = "To delete a node, use the +DELETE+ clause.",
      queryText = "match (n:Useless) delete n",
      optionalResultExplanation = "",
      prepare = db => db.inTx(db.createNode(DynamicLabel.label("Useless"))),
      assertions = p => assertStats(p, nodesDeleted = 1))
  }

  @Test def delete_all_nodes_and_all_relationships() {
    testQuery(
      title = "Delete all nodes and relationships",
      text = "This query isn't for deleting large amounts of data, but is nice when playing around with small example data sets.",
      queryText = "MATCH (n) DETACH DELETE n",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, relationshipsDeleted = 2, nodesDeleted = 3))
  }

  @Test def force_delete_a_node() {
    testQuery(
      title = "Delete a node with all its relationships",
      text = "When you want to delete a node and any relationship going to or from it, use +DETACH+ +DELETE+.",
      queryText = "MATCH (n {name:'Andres'}) DETACH DELETE n",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, relationshipsDeleted = 2, nodesDeleted = 1))
  }
}
