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

class RelationshipFunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "Relationship Functions"
  val css = "general c2-2 c3-2 c4-3 c5-1 c6-5"
  override val linkId = "query-functions-scalar"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=default" =>
        Map("defaultValue" -> "Bob")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> "Andrés"),
    "B" -> Map("property" -> "Tobias"),
    "C" -> Map("property" -> "Chris"))

  def text = """
###assertion=returns-one
MATCH (n)-[a_relationship]->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

type(a_relationship)###

String representation of the relationship type.

###assertion=returns-one
MATCH (n)-[a_relationship]->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

startNode(a_relationship)###

Start node of the relationship.

###assertion=returns-one
MATCH (n)-[a_relationship]->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

endNode(a_relationship)###

End node of the relationship.

###assertion=returns-one
MATCH (n)-[a_relationship]->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

id(a_relationship)###

The internal id of the relationship.
"""
}
