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
package org.neo4j.cypher.docgen.refcard

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.RefcardTest
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

class DeleteTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT LINK A:Person", "A LINK B", "B LINK C", "C LINK ROOT")
  val title = "DELETE"
  val css = "write c2-2 c4-3 c5-4 c6-2"
  override val linkId = "query-delete"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "delete" =>
        assertStats(result, nodesCreated = 1, nodesDeleted = 1, relationshipsDeleted = 1)
        assert(result.toList.size === 0)
      case "detach-delete" =>
        assertStats(result, nodesCreated = 1, nodesDeleted = 1)
        assert(result.toList.size === 0)
      case "delete-all" =>
        assertStats(result, nodesDeleted = 4, relationshipsDeleted = 3)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=set" =>
        Map("value" -> "a value")
      case "parameters=map" =>
        Map("map" -> Map("property" -> "a value"))
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """
###assertion=delete
MATCH ()-[r]->() WHERE id(r) = 1
CREATE (n)

DELETE n, r
###

Delete a node and a relationship.

###assertion=detach-delete
CREATE (n)

DETACH DELETE n
###
Delete a node and all relationships connected to it.

###assertion=delete-all
MATCH (n) DETACH DELETE n
###
Delete all nodes and relationships from the database.
"""
}
