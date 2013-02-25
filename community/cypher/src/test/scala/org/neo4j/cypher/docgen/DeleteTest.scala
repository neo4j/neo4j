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

import org.junit.Test
import org.neo4j.graphdb.NotFoundException
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.junit.Assert._

class DeleteTest extends DocumentingTestBase {
  def graphDescription = List("Andres KNOWS Tobias", "Andres KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l),
    "Tobias" -> Map("age" -> 25l),
    "Peter" -> Map("age" -> 34l)
  )

  def section = "Delete"

  @Test def delete_single_node() {
    val id = db.inTx(() => {
      val a = db.createNode()
      a.getId
    })

    testQuery(
      title = "Delete single node",
      text = "To remove a node from the graph, you can delete it with the +DELETE+ clause.",
      queryText = "start n = node(" + id + ") delete n",
      returns = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertIsDeleted(db.getNodeById(id)))
  }

  @Test def delete_single_node_with_all_relationships() {
    testQuery(
      title = "Remove a node and connected relationships",
      text = "If you are trying to remove a node with relationships on it, you have to remove these as well.",
      queryText = "start n = node(%Andres%) match n-[r]-() delete n, r",
      returns = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertIsDeleted(node("Andres")))
  }
}